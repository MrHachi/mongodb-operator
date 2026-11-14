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
	"errors"
	"fmt"
	"slices"
	"time"

	corev1 "k8s.io/api/core/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	dbv1beta1 "github.com/MrHachi/mongodb-operator/api/v1beta1"
	mdb "github.com/MrHachi/mongodb-operator/internal/mongodb"
)

const (
	drainingPodAnnotation = "db.mrhachi.dev/draining"
	defaultDrainThreshold = 30 * time.Second
)

func (r *MongoDBReconciler) reconcileProgressing(ctx context.Context, mongodb *dbv1beta1.MongoDB) (ctrl.Result, error) {
	if err := r.transitionProgressing(ctx, mongodb, dbv1beta1.PhaseProgressing, dbv1beta1.ProgressReasonReconcilingMembers); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.ensureInstanceManagerBinding(ctx, mongodb); err != nil {
		return ctrl.Result{}, fmt.Errorf("ensure instance-manager permissions: %w", err)
	}
	if err := r.ensureHeadlessService(ctx, mongodb); err != nil {
		return ctrl.Result{}, fmt.Errorf("ensure headless service: %w", err)
	}

	pods, err := r.listManagedReplicaPods(ctx, mongodb)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("list managed replica pods: %w", err)
	}

	// A selected drain always takes precedence over a new count comparison. This
	// lets a later reconcile resume the same target after a controller restart.
	for i := range pods {
		if pods[i].Annotations[drainingPodAnnotation] != "" {
			return r.resumeScaleIn(ctx, mongodb, &pods[i])
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
		return r.reconcileSteadyState(ctx, mongodb, pods)
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

func (r *MongoDBReconciler) reconcileScaleIn(ctx context.Context, mongodb *dbv1beta1.MongoDB, pods []corev1.Pod) (ctrl.Result, error) {
	for i := range pods {
		if pods[i].DeletionTimestamp != nil {
			return r.waitForReconciliation(ctx, mongodb, "ScaleInPending", "Scale-in is waiting for an existing Pod deletion to complete", 15*time.Second)
		}
	}

	threshold := defaultDrainThreshold
	if mongodb.Spec.DrainThreshold != nil {
		threshold = mongodb.Spec.DrainThreshold.Duration
	}
	if threshold <= 0 {
		return ctrl.Result{}, &ReconciliationError{
			Reason:  "InvalidDrainThreshold",
			Message: fmt.Sprintf("drainThreshold %s must be positive", threshold),
			Affects: []string{dbv1beta1.ConditionProgressing},
		}
	}

	topology, config, err := r.discoverClusterTopology(ctx, mongodb, pods)
	if err != nil {
		return r.waitForReconciliation(ctx, mongodb, "TopologyUnavailable", fmt.Sprintf("Scale-in is waiting for a current replica set topology: %v", err), 15*time.Second)
	}

	targetPod, err := selectScaleInCandidate(topology, config, pods, threshold)
	if err != nil {
		return r.waitForReconciliation(ctx, mongodb, "ScaleInDeferred", err.Error(), 15*time.Second)
	}
	if err := r.markPodDraining(ctx, targetPod); err != nil {
		return ctrl.Result{}, fmt.Errorf("mark pod draining: %w", err)
	}
	return r.resumeScaleIn(ctx, mongodb, targetPod)
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

	votingMembers, healthyVotingMembers := countHealthyVoters(topology.Members, configByID)
	if votingMembers == 0 {
		return nil, errors.New("replica set voting configuration is unavailable")
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

	if healthyVotingMembers-1 < votingMembers/2+1 {
		return nil, errors.New("removing a secondary would not preserve the current voting quorum")
	}
	if err := validateSurvivingMembers(topology.Members, configByID, candidate.ID, threshold); err != nil {
		return nil, fmt.Errorf("validate surviving members: %w", err)
	}
	return podsByMemberID[candidate.ID], nil
}

func stableMemberPods(topology *mdb.Topology, pods []corev1.Pod) (map[int]*corev1.Pod, error) {
	if topology == nil || topology.ConfigVersion <= 0 || len(topology.Members) == 0 {
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

func (r *MongoDBReconciler) resumeScaleIn(ctx context.Context, mongodb *dbv1beta1.MongoDB, pod *corev1.Pod) (ctrl.Result, error) {
	ok, err := r.ensureInstanceManagerLeadershipAwayFrom(ctx, pod)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("ensure leadership is away from Pod %s: %w", pod.Name, err)
	}
	if !ok {
		return r.waitForReconciliation(ctx, mongodb, "LeadershipTransferPending", fmt.Sprintf("Scale-in is waiting for leadership to move away from Pod %s", pod.Name), 15*time.Second)
	}
	return r.waitForReconciliation(ctx, mongodb, "ScaleInPending", fmt.Sprintf("Scale-in of Pod %s is awaiting the checkpoint and topology drain steps", pod.Name), 15*time.Second)
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

func (r *MongoDBReconciler) reconcileScaleOut(ctx context.Context, mongodb *dbv1beta1.MongoDB, _ []corev1.Pod) (ctrl.Result, error) {
	return r.waitForReconciliation(ctx, mongodb, "ScaleOutPending", "Scale-out reconciliation is not implemented yet", 15*time.Second)
}

func (r *MongoDBReconciler) reconcileSteadyState(ctx context.Context, mongodb *dbv1beta1.MongoDB, _ []corev1.Pod) (ctrl.Result, error) {
	return r.waitForReconciliation(ctx, mongodb, "ReconciliationPending", "MongoDB member and configuration convergence is not implemented yet", 15*time.Second)
}
