/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	dbv1beta1 "github.com/MrHachi/mongodb-operator/api/v1beta1"
	mdb "github.com/MrHachi/mongodb-operator/internal/mongodb"
)

const (
	provisioningPodAnnotation    = "db.mrhachi.dev/provisioning" // TODO: move to other file as this is not Progressing phase-specific
	drainingPodAnnotation        = "db.mrhachi.dev/draining"
	drainingCheckpointAnnotation = "db.mrhachi.dev/draining-checkpoint"

	pvcStatusLabel            = "db.mrhachi.dev/status"
	pvcMemberLabel            = "db.mrhachi.dev/member"
	pvcStatusInUse            = "in-use" // TODO: move to other file as this is not Progressing phase-specific
	pvcStatusJettisoned       = "jettisoned"
	pvcJettisonedAtAnnotation = "db.mrhachi.dev/jettisoned-at"
	pvcDataTimeAnnotation     = "db.mrhachi.dev/data-time"
)

var errMajorityCommitCheckpointUnavailable = errors.New("primary majority-commit checkpoint is unavailable")

type drainCheckpoint struct {
	TimestampSeconds   uint32    `json:"timestamp_seconds"`
	TimestampIncrement uint32    `json:"timestamp_increment"`
	Term               int64     `json:"term"`
	WallTime           time.Time `json:"wall_time"`
}

func (r *MongoDBReconciler) reconcileProgressing(ctx context.Context, mongodb *dbv1beta1.MongoDB) (ctrl.Result, error) {
	if err := r.transitionProgressing(ctx, mongodb, dbv1beta1.PhaseProgressing, dbv1beta1.ProgressReasonReconcilingMembers); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.ensureInstanceManagerBinding(ctx, mongodb); err != nil {
		return ctrl.Result{}, fmt.Errorf("ensure instance-manager permissions: %w", err)
	}
	if err := r.ensureInstanceManagerCredentialsSecret(ctx, mongodb); err != nil {
		return ctrl.Result{}, fmt.Errorf("ensure instance-manager credentials: %w", err)
	}
	if err := r.ensureHeadlessService(ctx, mongodb); err != nil {
		return ctrl.Result{}, fmt.Errorf("ensure headless service: %w", err)
	}

	pods, err := r.listManagedReplicaPods(ctx, mongodb)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("list managed replica pods: %w", err)
	}

	for i := range pods {
		// TODO: Handle the case where scale-out occurs during scale-in (and vice-versa)
		// If we're in the process of scale-out/in and a scaling event occurs, we'll want to prioritize
		// in-progress Pods as scaling candidates

		// A selected drain always takes precedence over a new count comparison. This
		// lets a later reconcile resume the same target after a controller restart.
		if _, ok := pods[i].Annotations[drainingPodAnnotation]; ok {
			return r.drainReplicaScaleIn(ctx, mongodb, &pods[i])
		}

		// A provisioning Pod marks an interrupted scale-out operation. Resume its
		// readiness and replica-set join before comparing Pod counts, since its
		// presence may already make observed replicas equal the desired count.
		if _, ok := pods[i].Annotations[provisioningPodAnnotation]; ok {
			return r.provisionReplicaScaleOut(ctx, mongodb, &pods[i], pods)
		}
	}

	desired := int(mongodb.Spec.Replicas)
	observed := len(pods)
	switch {
	case desired < observed:
		return r.reconcileScaleIn(ctx, mongodb, pods)
	case desired > observed:
		return r.reconcileScaleOut(ctx, mongodb, pods)
	default:
		if err := r.transitionReady(ctx, mongodb); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}
}

func (r *MongoDBReconciler) listManagedReplicaPods(ctx context.Context, mongodb *dbv1beta1.MongoDB) ([]corev1.Pod, error) {
	list := &corev1.PodList{}
	if err := r.List(ctx, list, client.InNamespace(mongodb.Namespace), client.MatchingLabels(r.labels(mongodb))); err != nil {
		return nil, fmt.Errorf("list managed replica Pods: %w", err)
	}

	pods := make([]corev1.Pod, 0, len(list.Items))
	for i := range list.Items {
		pod := &list.Items[i]
		for _, owner := range pod.OwnerReferences {
			if owner.UID == mongodb.UID && owner.Kind == dbv1beta1.MongoDBKind {
				pods = append(pods, *pod.DeepCopy())
				break
			}
		}
	}
	return pods, nil
}

// reconcileScaleIn selects a member candidate to drain, marks it with the draining annotation,
// then calls drainReplicaScaleIn to perform the rest of the scale-in procedure.
func (r *MongoDBReconciler) reconcileScaleIn(ctx context.Context, mongodb *dbv1beta1.MongoDB, pods []corev1.Pod) (ctrl.Result, error) {
	for i := range pods {
		if pods[i].DeletionTimestamp != nil {
			return r.waitForReconciliation(ctx, mongodb, "ScaleInPending", "Scale-in is waiting for an existing Pod deletion to complete", 15*time.Second)
		}
	}

	// 1. Read replica-set status and election settings from MongoDB via the instance-manager
	topology, config, err := r.discoverClusterTopology(ctx, mongodb, pods)
	if err != nil {
		return r.waitForReconciliation(ctx, mongodb, "TopologyUnavailable", fmt.Sprintf("Scale-in is waiting for a current replica set topology: %v", err), 15*time.Second)
	}

	// 2. Select a SECONDARY to drain
	targetPod, err := selectScaleInCandidate(topology, config, pods, mongodb.Spec.DrainThreshold.Duration)
	if err != nil {
		return r.waitForReconciliation(ctx, mongodb, "ScaleInDeferred", err.Error(), 15*time.Second)
	}

	// 3. Mark the Pod as draining
	if err := r.markPodDraining(ctx, targetPod); err != nil {
		return ctrl.Result{}, fmt.Errorf("mark pod draining: %w", err)
	}

	// Perform the rest of the scale-in Pod draining procedure
	return r.drainReplicaScaleIn(ctx, mongodb, targetPod)
}

// selectScaleInCandidate returns an error when it can't establish that scale-in
// is safe right now; that does not mean the reconciliation operation failed.
func selectScaleInCandidate(topology *mdb.Topology, config *mdb.ReplicaSetConfig, pods []corev1.Pod, threshold time.Duration) (*corev1.Pod, error) {
	configByID, err := memberConfigsByID(topology, config)
	if err != nil {
		return nil, fmt.Errorf("get member configs: %w", err)
	}

	podsByMemberID, err := stableMemberPods(topology, pods)
	if err != nil {
		return nil, fmt.Errorf("get stable member pods: %w", err)
	}

	secondaries := eligibleDrainSecondaries(topology.Members, podsByMemberID, configByID, threshold)
	if len(secondaries) == 0 {
		return nil, fmt.Errorf("no voting %s has known replication lag within the %s drainThreshold", mdb.StateSecondary, threshold)
	}
	slices.SortFunc(secondaries, func(a, b *mdb.Member) int {
		return cmp.Or(
			cmp.Compare(b.ReplicationLag, a.ReplicationLag),
			cmp.Compare(a.ID, b.ID),
		)
	})

	candidate := secondaries[0]
	if err := verifyScaleInSafety(topology, config, candidate.ID, threshold); err != nil {
		return nil, fmt.Errorf("verify scale-in safety: %w", err)
	}
	return podsByMemberID[candidate.ID], nil
}

// verifyScaleInSafety reports whether removing candidateID preserves voting
// quorum, survivor replication health, and a stable topology/configuration view.
func verifyScaleInSafety(topology *mdb.Topology, config *mdb.ReplicaSetConfig, candidateID int, threshold time.Duration) error {
	configByID, err := memberConfigsByID(topology, config)
	if err != nil {
		return fmt.Errorf("replica set configuration is unstable: %w", err)
	}
	votingMembers, healthyVotingMembers := countHealthyVoters(topology.Members, configByID)
	if votingMembers == 0 {
		return errors.New("replica set voting configuration is unavailable")
	}
	if healthyVotingMembers-1 < votingMembers/2+1 {
		return errors.New("removing a secondary would not preserve the current voting quorum")
	}
	if err := validateSurvivingMembers(topology.Members, configByID, candidateID, threshold); err != nil {
		return fmt.Errorf("validate surviving members: %w", err)
	}
	return nil
}

func stableMemberPods(topology *mdb.Topology, pods []corev1.Pod) (map[int]*corev1.Pod, error) {
	if topology == nil || len(topology.Members) == 0 {
		return nil, errors.New("replica set configuration is unavailable or unstable")
	}

	podsByMemberID := make(map[int]*corev1.Pod, len(topology.Members))
	primaryCount := 0
	for i := range topology.Members {
		member := &topology.Members[i]

		if pod := podForMember(pods, member.Host); pod != nil {
			podsByMemberID[member.ID] = pod
		}

		if member.State == mdb.StatePrimary {
			primaryCount++
			if member.ConfigVersion <= 0 {
				return nil, errors.New("PRIMARY replica set configuration version is unavailable")
			}
			if member.Optimes.AppliedAt.IsZero() {
				return nil, fmt.Errorf("%s applied optime is unavailable", mdb.StatePrimary)
			}
		}
	}

	if primaryCount != 1 {
		return nil, fmt.Errorf("replica set must report exactly one %s before scale-in", mdb.StatePrimary)
	}
	return podsByMemberID, nil
}

func countHealthyVoters(members []mdb.Member, configByID map[int]mdb.MemberConfig) (int, int) {
	total, healthy := 0, 0
	for i := range members {
		member := &members[i]
		config := configByID[member.ID]
		if config.Votes == nil || *(config.Votes) == 0 {
			continue
		}
		total++

		if member.State == mdb.StatePrimary || member.State == mdb.StateSecondary {
			healthy++
		}
	}
	return total, healthy
}

func eligibleDrainSecondaries(
	members []mdb.Member,
	podsByMemberID map[int]*corev1.Pod,
	configByMemberID map[int]mdb.MemberConfig,
	threshold time.Duration,
) []*mdb.Member {
	secondaries := make([]*mdb.Member, 0, len(members))
	for i := range members {
		member := &members[i]

		if member.State != mdb.StateSecondary || !configByMemberID[member.ID].VotingEligible {
			continue
		}

		if !member.LagAvailable ||
			member.ReplicationLag > threshold ||
			member.Optimes.AppliedAt.IsZero() {
			continue
		}

		pod := podsByMemberID[member.ID]
		if pod == nil || pod.Status.PodIP == "" || !podReady(pod) {
			continue
		}

		secondaries = append(secondaries, member)
	}
	return secondaries
}

func validateSurvivingMembers(members []mdb.Member, configByID map[int]mdb.MemberConfig, candidateID int, threshold time.Duration) error {
	for i := range members {
		member := &members[i]
		if member.ID == candidateID || configByID[member.ID].ArbiterOnly {
			continue
		}
		if member.State != mdb.StatePrimary && member.State != mdb.StateSecondary {
			return fmt.Errorf("surviving data-bearing member %q is not healthy (state %s)", member.Host, member.State)
		}
		if member.State == mdb.StateSecondary && (!member.LagAvailable || member.ReplicationLag > threshold) {
			return fmt.Errorf("surviving %s %q is outside the %s replication health threshold", mdb.StateSecondary, member.Host, threshold)
		}
	}
	return nil
}

func (r *MongoDBReconciler) markPodDraining(ctx context.Context, pod *corev1.Pod) error {
	if pod.Annotations[drainingPodAnnotation] != "" {
		return nil
	}

	before := pod.DeepCopy()
	if pod.Annotations == nil {
		pod.Annotations = make(map[string]string)
	}
	pod.Annotations[drainingPodAnnotation] = "selected"

	if err := r.Patch(ctx, pod, client.MergeFrom(before)); err != nil {
		return fmt.Errorf("mark Pod %s/%s as draining: %w", pod.Namespace, pod.Name, err)
	}
	return nil
}

// drainReplicaScaleIn performs the bulk of the scale-in procedure, ensuring instance-manager
// leadership is moved away from the selected Pod and removing it once its optime reaches
// a given majority checkpoint.
// If at any point removing the replica would violate voting quorum or replica-set configuration,
// we defer scale-in and wait for stability to be restored.
func (r *MongoDBReconciler) drainReplicaScaleIn(ctx context.Context, mongodb *dbv1beta1.MongoDB, pod *corev1.Pod) (ctrl.Result, error) {
	pods, err := r.listManagedReplicaPods(ctx, mongodb)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("list managed replica Pods while performing scale-in: %w", err)
	}

	targetFound := false
	for i := range pods {
		if pods[i].Name == pod.Name {
			pod = &pods[i]
			targetFound = true
			break
		}
	}
	if !targetFound {
		return r.waitForReconciliation(ctx, mongodb, "ScaleInPending", fmt.Sprintf("Scale-in target Pod %s is no longer present", pod.Name), 15*time.Second)
	}

	topology, config, err := r.discoverClusterTopology(ctx, mongodb, pods)
	if err != nil {
		// TODO: we will likely want to either waitForReconciliation or return a ReconciliationError, depending on the content of the error returned
		return r.waitForReconciliation(ctx, mongodb, "TopologyUnavailable", fmt.Sprintf("Scale-in is waiting for a current replica set topology: %v", err), 15*time.Second)
	}

	member := topologyMemberForPod(topology, pods, pod)
	checkpoint, checkpointExists, err := persistedDrainCheckpoint(pod)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("ensure drain checkpoint for Pod %s: %w", pod.Name, err)
	}

	// 4. Ensure majority-commit checkpoint has been captured and persisted on the Pod annotations
	// -or-
	// Once member has been removed from MongoDB topology (member from topology == nil):
	// 6. Drain Pod and jettison PVC
	if member == nil {
		var checkpointToPersist *drainCheckpoint
		if checkpointExists {
			checkpointToPersist = &checkpoint
		} else {
			// If removal happened outside this workflow, finish the drain without inventing a data-time.
			logf.FromContext(ctx).Info("WARNING: Draining Pod has no persisted checkpoint; continuing without PVC data-time", "pod", pod.Name)
		}
		if err := r.markDrainedPVC(ctx, mongodb, pod, checkpointToPersist); err != nil {
			return ctrl.Result{}, fmt.Errorf("mark PVC for Pod %s as jettisoned: %w", pod.Name, err)
		}
		if err := r.Delete(ctx, pod); err != nil && !apierrors.IsNotFound(err) {
			return ctrl.Result{}, fmt.Errorf("delete drained Pod %s: %w", pod.Name, err)
		}
		return r.waitForReconciliation(ctx, mongodb, "ScaleInPending", fmt.Sprintf("Scale-in is waiting for drained Pod %s deletion to complete", pod.Name), 15*time.Second)
	}

	if !checkpointExists {
		checkpoint, err = r.ensureDrainCheckpoint(ctx, pod, topology.Optimes.LastCommittedOptime, topology.Optimes.LastCommittedWallTime)
		if err != nil {
			if errors.Is(err, errMajorityCommitCheckpointUnavailable) {
				return r.waitForReconciliation(ctx, mongodb, "DrainCheckpointUnavailable", "Scale-in is waiting for the primary to report a majority-commit checkpoint", 15*time.Second)
			}
			return ctrl.Result{}, fmt.Errorf("ensure drain checkpoint for Pod %s: %w", pod.Name, err)
		}
	}

	if member.State != mdb.StateSecondary {
		return r.waitForReconciliation(ctx, mongodb, "DrainTargetNotSecondary", fmt.Sprintf("Scale-in is waiting for Pod %s to return to SECONDARY before removing it", pod.Name), 15*time.Second)
	}

	// Ensure instance-manager leadership not on draining Pod
	ok, err := r.ensureInstanceManagerLeadershipAwayFrom(ctx, pod)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("ensure leadership is away from Pod %s: %w", pod.Name, err)
	}
	if !ok {
		return r.waitForReconciliation(ctx, mongodb, "LeadershipTransferPending", fmt.Sprintf("Scale-in is waiting for leadership to move away from Pod %s", pod.Name), 15*time.Second)
	}

	// Wait for draining replica's optime to pass checkpoint
	if !optimeAtLeast(member.Optimes.DurableOptime, checkpoint.optime()) {
		return r.waitForReconciliation(ctx, mongodb, "DrainCheckpointPending", fmt.Sprintf("Scale-in is waiting for Pod %s durable optime to reach the captured majority-commit checkpoint", pod.Name), 15*time.Second)
	}

	if err := verifyScaleInSafety(topology, config, member.ID, mongodb.Spec.DrainThreshold.Duration); err != nil {
		return r.waitForReconciliation(ctx, mongodb, "ScaleInDeferred", err.Error(), 15*time.Second)
	}

	primary := podForMember(pods, primaryHost(topology))
	if primary == nil || primary.Status.PodIP == "" || !podReady(primary) {
		return r.waitForReconciliation(ctx, mongodb, "PrimaryUnavailable", "Scale-in is waiting for a Ready PRIMARY Pod to remove the replica-set member", 15*time.Second)
	}

	// 5. Remove member from MongoDB replica-set configuration
	if err := r.managerClientForPod(primary).RemoveMember(ctx, pod.Name); err != nil {
		return ctrl.Result{}, fmt.Errorf("remove Pod %s from replica set using PRIMARY Pod %s: %w", pod.Name, primary.Name, err)
	}
	return r.waitForReconciliation(ctx, mongodb, "MemberRemovalPending", fmt.Sprintf("Scale-in is waiting for the replica set to recognize removal of Pod %s", pod.Name), 15*time.Second)
}

func primaryHost(topology *mdb.Topology) string {
	for i := range topology.Members {
		if topology.Members[i].State == mdb.StatePrimary {
			return topology.Members[i].Host
		}
	}
	return ""
}

func persistedDrainCheckpoint(pod *corev1.Pod) (drainCheckpoint, bool, error) {
	value, exists := pod.Annotations[drainingCheckpointAnnotation]
	if !exists {
		return drainCheckpoint{}, false, nil
	}

	var checkpoint drainCheckpoint
	if err := json.Unmarshal([]byte(value), &checkpoint); err != nil {
		return drainCheckpoint{}, true, fmt.Errorf("decode persisted checkpoint annotation: %w", err)
	}

	if !checkpoint.valid() {
		return drainCheckpoint{}, true, errors.New("persisted checkpoint annotation is invalid")
	}
	return checkpoint, true, nil
}

func (r *MongoDBReconciler) markDrainedPVC(ctx context.Context, mongodb *dbv1beta1.MongoDB, pod *corev1.Pod, checkpoint *drainCheckpoint) error {
	pvcName := ""
	for i := range pod.Spec.Volumes {
		volume := &pod.Spec.Volumes[i]
		if volume.Name == "data" && volume.PersistentVolumeClaim != nil {
			pvcName = volume.PersistentVolumeClaim.ClaimName
			break
		}
	}
	if pvcName == "" {
		return fmt.Errorf("Pod %s has no data PVC volume", pod.Name)
	}

	pvc := &corev1.PersistentVolumeClaim{}
	if err := r.Get(ctx, client.ObjectKey{Namespace: pod.Namespace, Name: pvcName}, pvc); err != nil {
		if apierrors.IsNotFound(err) {
			return fmt.Errorf("data PVC %s for Pod %s was not found: %w", pvcName, pod.Name, err)
		}
		return fmt.Errorf("get data PVC %s for Pod %s: %w", pvcName, pod.Name, err)
	}

	owner := metav1.GetControllerOf(pvc)
	if owner == nil || owner.UID != mongodb.UID || owner.Kind != dbv1beta1.MongoDBKind {
		return fmt.Errorf("data PVC %s is not owned by MongoDB %s/%s", pvcName, mongodb.Namespace, mongodb.Name)
	}

	before := pvc.DeepCopy()
	if pvc.Labels == nil {
		pvc.Labels = make(map[string]string)
	}
	pvc.Labels[pvcStatusLabel] = pvcStatusJettisoned
	if pvc.Annotations == nil {
		pvc.Annotations = make(map[string]string)
	}
	if pvc.Annotations[pvcJettisonedAtAnnotation] == "" {
		pvc.Annotations[pvcJettisonedAtAnnotation] = time.Now().UTC().Format(time.RFC3339Nano)
	}

	if checkpoint == nil {
		delete(pvc.Annotations, pvcDataTimeAnnotation)
	} else {
		pvc.Annotations[pvcDataTimeAnnotation] = checkpoint.WallTime.UTC().Format(time.RFC3339Nano)
	}
	if err := r.Patch(ctx, pvc, client.MergeFrom(before)); err != nil {
		return fmt.Errorf("patch data PVC %s as jettisoned: %w", pvcName, err)
	}
	return nil
}

func (r *MongoDBReconciler) ensureDrainCheckpoint(ctx context.Context, pod *corev1.Pod, optime mdb.Optime, wallTime time.Time) (drainCheckpoint, error) {
	if checkpoint, exists, err := persistedDrainCheckpoint(pod); err != nil {
		return drainCheckpoint{}, err
	} else if exists {
		return checkpoint, nil
	}

	checkpoint := drainCheckpoint{
		TimestampSeconds:   optime.Timestamp.T,
		TimestampIncrement: optime.Timestamp.I,
		Term:               optime.Term,
		WallTime:           wallTime,
	}
	if !checkpoint.valid() {
		return drainCheckpoint{}, errMajorityCommitCheckpointUnavailable
	}

	value, err := json.Marshal(checkpoint)
	if err != nil {
		return drainCheckpoint{}, fmt.Errorf("encode checkpoint annotation: %w", err)
	}

	before := pod.DeepCopy()
	if pod.Annotations == nil {
		pod.Annotations = make(map[string]string)
	}
	pod.Annotations[drainingCheckpointAnnotation] = string(value)

	if err := r.Patch(ctx, pod, client.MergeFrom(before)); err != nil {
		return drainCheckpoint{}, fmt.Errorf("persist checkpoint annotation on Pod %s/%s: %w", pod.Namespace, pod.Name, err)
	}
	return checkpoint, nil
}

func (c drainCheckpoint) optime() mdb.Optime {
	return mdb.Optime{
		Timestamp: primitive.Timestamp{T: c.TimestampSeconds, I: c.TimestampIncrement},
		Term:      c.Term,
	}
}

func (c drainCheckpoint) valid() bool {
	return c.TimestampSeconds != 0 && !c.WallTime.IsZero()
}

func optimeAtLeast(current, checkpoint mdb.Optime) bool {
	if current.Term != -1 && checkpoint.Term != -1 && current.Term != checkpoint.Term {
		return current.Term > checkpoint.Term
	}
	if current.Timestamp.T != checkpoint.Timestamp.T {
		return current.Timestamp.T > checkpoint.Timestamp.T
	}
	return current.Timestamp.I >= checkpoint.Timestamp.I
}

func topologyMemberForPod(topology *mdb.Topology, pods []corev1.Pod, pod *corev1.Pod) *mdb.Member {
	for i := range topology.Members {
		memberPod := podForMember(pods, topology.Members[i].Host)
		if memberPod != nil && memberPod.Name == pod.Name {
			return &topology.Members[i]
		}
	}
	return nil
}

// ensureInstanceManagerLeadershipAwayFrom requests a transfer from the draining
// Pod's instance-manager and reports whether the leadership check allows
// scale-in to proceed.
func (r *MongoDBReconciler) ensureInstanceManagerLeadershipAwayFrom(ctx context.Context, pod *corev1.Pod) (bool, error) {
	manager := r.managerClientForPod(pod)
	if err := manager.TransferLeadership(ctx); err != nil {
		return false, fmt.Errorf("request leadership transfer: %w", err)
	}

	leadership, err := manager.GetLeadership(ctx)
	if err != nil {
		return false, fmt.Errorf("get leadership status: %w", err)
	}
	if leadership == nil || leadership.Leader == "" {
		return false, fmt.Errorf("instance-manager returned an empty leader identity")
	}
	return leadership.Leader != pod.Name, nil
}

// reconcileScaleOut assigns a new replica ID, optionally reclaims a jettisoned PVC,
// and creates the new replica's Pod, before calling provisionReplicaScaleOut to
// perform the rest of the scale-out procedure.
func (r *MongoDBReconciler) reconcileScaleOut(ctx context.Context, mongodb *dbv1beta1.MongoDB, pods []corev1.Pod) (ctrl.Result, error) {
	// 1. Assign a new replica ID (or derive it from a jettisoned PVC to be reclaimed)
	candidateID, candidatePVC, err := r.getScaleOutReplicaCandidate(ctx, mongodb, pods)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("select scale-out candidate: %w", err)
	}

	// 2. Reclaim or provision new PVC
	if candidatePVC != nil {
		if err := r.reclaimReplicaPVC(ctx, candidatePVC); err != nil {
			return ctrl.Result{}, fmt.Errorf("reclaim PVC %s for scale-out: %w", candidatePVC.Name, err)
		}
	} else if err := r.ensurePVC(ctx, mongodb, candidateID); err != nil {
		return ctrl.Result{}, fmt.Errorf("ensure scale-out PVC: %w", err)
	}

	// 3. Ensure replica Pod
	pod, err := r.ensureReplicaPod(ctx, mongodb, candidateID)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("ensure replica Pod: %w", err)
	}

	// Perform the rest of scale-out
	return r.provisionReplicaScaleOut(ctx, mongodb, pod, pods)
}

// provisionReplicaScaleOut waits for the given replica Pod to become ready,
// then joins it to the MongoDB cluster and removes the provisioning
// annotation.
func (r *MongoDBReconciler) provisionReplicaScaleOut(ctx context.Context, mongodb *dbv1beta1.MongoDB, pod *corev1.Pod, pods []corev1.Pod) (ctrl.Result, error) {
	if pod.Status.PodIP == "" || !podReady(pod) {
		return r.waitForReconciliation(ctx, mongodb, "ScaleOutPending", fmt.Sprintf("Scale-out is waiting for replica Pod %s to become ready", pod.Name), 15*time.Second)
	}

	topology, _, err := r.discoverClusterTopology(ctx, mongodb, pods)
	if err != nil {
		return r.waitForReconciliation(ctx, mongodb, "TopologyUnavailable", fmt.Sprintf("Scale-out is waiting for a current replica set topology: %v", err), 15*time.Second)
	}

	member := topologyMemberForPod(topology, pods, pod)
	if member == nil {
		primary := podForMember(pods, primaryHost(topology))
		if primary == nil || primary.Status.PodIP == "" || !podReady(primary) {
			return r.waitForReconciliation(ctx, mongodb, "PrimaryUnavailable", "Scale-out is waiting for a Ready PRIMARY Pod to add the replica-set member", 15*time.Second)
		}

		if err := r.managerClientForPod(primary).AddMember(ctx, pod.Name); err != nil {
			return ctrl.Result{}, fmt.Errorf("add replica Pod %s to replica set using PRIMARY Pod %s: %w", pod.Name, primary.Name, err)
		}
		return r.waitForReconciliation(ctx, mongodb, "MemberAdditionPending", fmt.Sprintf("Scale-out is waiting for replica set membership to converge for Pod %s", pod.Name), 15*time.Second)
	}
	if member.State != mdb.StateSecondary && member.State != mdb.StatePrimary {
		return r.waitForReconciliation(ctx, mongodb, "ScaleOutPending", fmt.Sprintf("Scale-out is waiting for replica Pod %s to become a SECONDARY", pod.Name), 15*time.Second)
	}

	before := pod.DeepCopy()
	delete(pod.Annotations, provisioningPodAnnotation)

	if err := r.Patch(ctx, pod, client.MergeFrom(before)); err != nil {
		return ctrl.Result{}, fmt.Errorf("clear provisioning annotation from Pod %s: %w", pod.Name, err)
	}
	return r.waitForReconciliation(ctx, mongodb, "ScaleOutPending", fmt.Sprintf("Scale-out completed replica Pod %s membership; checking cluster convergence", pod.Name), 15*time.Second)
}

func (r *MongoDBReconciler) reclaimReplicaPVC(ctx context.Context, pvc *corev1.PersistentVolumeClaim) error {
	before := pvc.DeepCopy()
	pvc.Labels[pvcStatusLabel] = pvcStatusInUse
	delete(pvc.Annotations, pvcJettisonedAtAnnotation)
	delete(pvc.Annotations, pvcDataTimeAnnotation)

	if err := r.Patch(ctx, pvc, client.MergeFrom(before)); err != nil {
		return fmt.Errorf("patch PVC %s/%s: %w", pvc.Namespace, pvc.Name, err)
	}
	return nil
}

// getScaleOutReplicaCandidate prioritizes an eligible jettisoned PVC and
// returns its replica ID and PVC. If no eligible PVC exists, it issues the
// next available ID and returns a nil PVC; the caller must create the PVC.
func (r *MongoDBReconciler) getScaleOutReplicaCandidate(ctx context.Context, mongodb *dbv1beta1.MongoDB, pods []corev1.Pod) (string, *corev1.PersistentVolumeClaim, error) {
	occupied := make(map[string]struct{}, len(pods))
	for i := range pods {
		if id, ok := replicaIDFromResourceName(pods[i].Name, mongodb.Name+"-r-", ""); ok {
			occupied[id] = struct{}{}
		}
	}

	pvcList := &corev1.PersistentVolumeClaimList{}
	if err := r.List(ctx, pvcList, client.InNamespace(mongodb.Namespace), client.MatchingLabels(r.labels(mongodb))); err != nil {
		return "", nil, fmt.Errorf("list managed replica PVCs: %w", err)
	}
	slices.SortFunc(pvcList.Items, func(a, b corev1.PersistentVolumeClaim) int { return cmp.Compare(a.Name, b.Name) })

	var candidateID string
	var candidatePVC *corev1.PersistentVolumeClaim
	for i := range pvcList.Items {
		replicaPvc := &pvcList.Items[i]

		owner := metav1.GetControllerOf(replicaPvc)
		if owner == nil || owner.UID != mongodb.UID || owner.Kind != dbv1beta1.MongoDBKind {
			continue
		}
		if replicaPvc.Labels[pvcStatusLabel] != pvcStatusJettisoned {
			continue
		}

		memberId := replicaPvc.Labels[pvcMemberLabel]
		replicaId, ok := strings.CutPrefix(memberId, "r-")
		if !ok || replicaId == "" || !isReplicaID(replicaId) {
			continue
		}
		if _, exists := occupied[replicaId]; exists {
			continue
		}
		candidateID, candidatePVC = replicaId, replicaPvc
		break
	}
	if candidatePVC == nil {
		var err error
		candidateID, err = nextReplicaID(mongodb.Name, pods)
		if err != nil {
			return "", nil, fmt.Errorf("allocate replica ID: %w", err)
		}
	}
	return candidateID, candidatePVC, nil
}

func isReplicaID(id string) bool {
	for _, char := range id {
		if char < 'a' || char > 'z' {
			return false
		}
	}
	return id != ""
}
