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
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	dbv1beta1 "github.com/MrHachi/mongodb-operator/api/v1beta1"
)

const drainingPodAnnotation = "db.mrhachi.dev/draining"

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
		return ctrl.Result{}, err
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
			if owner.UID == mongodb.UID && owner.Kind == "MongoDB" {
				pods = append(pods, *pod.DeepCopy())
				break
			}
		}
	}
	return pods, nil
}

func (r *MongoDBReconciler) reconcileScaleIn(ctx context.Context, mongodb *dbv1beta1.MongoDB, pods []corev1.Pod) (ctrl.Result, error) {
	// Candidate selection is intentionally deferred until voting quorum,
	// replication-health threshold, and topology-stability checks are defined.
	return r.waitForReconciliation(ctx, mongodb, "ScaleInSelectionPending", "Scale-in is waiting for the safe secondary selection policy", 15*time.Second)
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
	if err := r.ensureInstanceManagerLeadershipAwayFrom(ctx, mongodb, pod); err != nil {
		return r.waitForReconciliation(ctx, mongodb, "LeadershipTransferPending", fmt.Sprintf("Scale-in is waiting for leadership to move away from Pod %s: %v", pod.Name, err), 15*time.Second)
	}
	return r.waitForReconciliation(ctx, mongodb, "ScaleInPending", fmt.Sprintf("Scale-in of Pod %s is awaiting the checkpoint and topology drain steps", pod.Name), 15*time.Second)
}

// ensureInstanceManagerLeadershipAwayFrom is the future instance-manager API
// integration point. Leadership movement is not implemented yet.
func (r *MongoDBReconciler) ensureInstanceManagerLeadershipAwayFrom(_ context.Context, mongodb *dbv1beta1.MongoDB, pod *corev1.Pod) error {
	return fmt.Errorf("leadership API is not implemented for MongoDB %s/%s and Pod %s", mongodb.Namespace, mongodb.Name, pod.Name)
}

func (r *MongoDBReconciler) reconcileScaleOut(ctx context.Context, mongodb *dbv1beta1.MongoDB, _ []corev1.Pod) (ctrl.Result, error) {
	return r.waitForReconciliation(ctx, mongodb, "ScaleOutPending", "Scale-out reconciliation is not implemented yet", 15*time.Second)
}

func (r *MongoDBReconciler) reconcileSteadyState(ctx context.Context, mongodb *dbv1beta1.MongoDB, _ []corev1.Pod) (ctrl.Result, error) {
	return r.waitForReconciliation(ctx, mongodb, "ReconciliationPending", "MongoDB member and configuration convergence is not implemented yet", 15*time.Second)
}

// clearPodDrainingAnnotation is used after a drain has completed or been
// safely abandoned. It re-fetches the Pod so retries do not overwrite changes.
func (r *MongoDBReconciler) clearPodDrainingAnnotation(ctx context.Context, pod *corev1.Pod) error {
	latest := &corev1.Pod{}
	key := types.NamespacedName{Namespace: pod.Namespace, Name: pod.Name}
	if err := r.Get(ctx, key, latest); err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("get draining Pod before clearing annotation: %w", err)
	}
	if latest.Annotations[drainingPodAnnotation] == "" {
		return nil
	}
	before := latest.DeepCopy()
	delete(latest.Annotations, drainingPodAnnotation)
	if err := r.Patch(ctx, latest, client.MergeFrom(before)); err != nil {
		return fmt.Errorf("clear draining annotation from Pod %s/%s: %w", pod.Namespace, pod.Name, err)
	}
	return nil
}
