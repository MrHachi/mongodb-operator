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
	"errors"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	dbv1beta1 "github.com/MrHachi/mongodb-operator/api/v1beta1"
	managerclient "github.com/MrHachi/mongodb-operator/internal/manager/pkg/client"
)

// reconcileInitializing handles the initialization phase of the MongoDB cluster.
func (r *MongoDBReconciler) reconcileInitializing(ctx context.Context, mongodb *dbv1beta1.MongoDB) (ctrl.Result, error) {
	log := logf.FromContext(ctx)
	log.Info("Reconciling initialization phase", "name", mongodb.Name)
	if err := r.ensureInstanceManagerBinding(ctx, mongodb); err != nil {
		return ctrl.Result{}, fmt.Errorf("ensure instance-manager permissions: %w", err)
	}
	if err := r.transitionInitializing(ctx, mongodb, dbv1beta1.ProgressReasonDiscoveringCluster); err != nil {
		return ctrl.Result{}, err
	}

	// Discover existing members before creating the default -r-a member. This
	// prevents a reset phase from creating a new member beside existing data.
	discovery, err := r.discoverPrimary(ctx, mongodb)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("discover replica set primary: %w", err)
	}
	var pod *corev1.Pod
	switch discovery.State {
	case discoveryNoPods:
		if err := r.transitionInitializing(ctx, mongodb, dbv1beta1.ProgressReasonCreatingResources); err != nil {
			return ctrl.Result{}, err
		}
		if err := r.ensureKeyfileSecret(ctx, mongodb); err != nil {
			log.Error(err, "Failed to ensure keyfile secret", "name", mongodb.Name)
			return ctrl.Result{}, fmt.Errorf("ensure keyfile secret: %w", err)
		}
		fallthrough
	case discoveryBootstrapPod:
		if err := r.transitionInitializing(ctx, mongodb, dbv1beta1.ProgressReasonCreatingResources); err != nil {
			return ctrl.Result{}, err
		}
		// Proceed with initializing initial primary
		if err := r.ensurePVC(ctx, mongodb, "a"); err != nil {
			return ctrl.Result{}, fmt.Errorf("ensure initial PVC: %w", err)
		}
		if err := r.ensureReplicaPod(ctx, mongodb, "a"); err != nil {
			return ctrl.Result{}, fmt.Errorf("ensure initial replica pod: %w", err)
		}
		pod = &corev1.Pod{}
		podName := fmt.Sprintf("%s-r-a", mongodb.Name)
		if err := r.Get(ctx, types.NamespacedName{Name: podName, Namespace: mongodb.Namespace}, pod); err != nil {
			return ctrl.Result{}, fmt.Errorf("get initial pod: %w", err)
		}
	case discoveryPrimaryFound:
		// Given that the cluster is running in RS mode and we have a primary, we assume this cluster is already Initialized
		// and move on to reconciling the discovered primary node
		pod = discovery.Primary
	case discoveryPodsNotReady:
		return r.waitForReconciliation(ctx, mongodb, string(dbv1beta1.ProgressReasonWaitingForPod), "Waiting for managed Pods to become ready", 15*time.Second)
	case discoveryPodsUnreachable:
		// Ensure discovery networking before waiting for instance-manager endpoints.
		if err := r.ensureHeadlessService(ctx, mongodb); err != nil {
			return ctrl.Result{}, fmt.Errorf("ensure headless service: %w", err)
		}
		return r.waitForReconciliation(ctx, mongodb, dbv1beta1.ReasonInstanceManagerUnavailable, "Managed Pod instance-manager endpoints are unavailable", 15*time.Second)
	case discoveryPodsWithoutPrimary:
		if err := r.ensureHeadlessService(ctx, mongodb); err != nil {
			return ctrl.Result{}, fmt.Errorf("ensure headless service: %w", err)
		}
		return r.waitForReconciliation(ctx, mongodb, dbv1beta1.ReasonPrimaryUnavailable, "Managed Pods exist but no primary is available", 15*time.Second)
	default:
		return ctrl.Result{}, fmt.Errorf("discover replica set primary: unexpected discovery state %d", discovery.State)
	}

	// Proceed with MongoDB cluster reconciliation, creating a new admin secret if we determined that this isn't a pre-existing cluster (no primary discovered)
	return r.reconcileSelectedPrimary(ctx, mongodb, pod, discovery.State != discoveryPrimaryFound)
}

// reconcileSelectedPrimary is MongoDB-facing reconciliation logic.
// Using the passed MongoDB primary node (Pod), it ensures that RS mode is initiated and that the admin user exists.
func (r *MongoDBReconciler) reconcileSelectedPrimary(ctx context.Context, mongodb *dbv1beta1.MongoDB, pod *corev1.Pod, createAdminSecret bool) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	// This is critical to MongoDB SDAM, so we ensure it as a part of cluster reconciliation
	if err := r.ensureHeadlessService(ctx, mongodb); err != nil {
		return ctrl.Result{}, fmt.Errorf("ensure headless service: %w", err)
	}

	var adminSecret *corev1.Secret
	if !createAdminSecret {
		adminSecret = &corev1.Secret{}
		secretName := fmt.Sprintf("%s-admin-credentials", mongodb.Name)
		if err := r.Get(ctx, types.NamespacedName{Name: secretName, Namespace: mongodb.Namespace}, adminSecret); err != nil {
			if apierrors.IsNotFound(err) {
				return ctrl.Result{}, &ReconciliationError{
					Reason:  dbv1beta1.ReasonCredentialsMissing,
					Message: fmt.Sprintf("Admin credentials Secret %s/%s is missing; restore the cluster's credentials to resume initialization", mongodb.Namespace, secretName),
					Affects: []string{dbv1beta1.ConditionReady, dbv1beta1.ConditionProgressing},
				}
			}
			return ctrl.Result{}, fmt.Errorf("get admin credentials secret: %w", err)
		}

		if len(adminSecret.Data[adminUsernameKey]) == 0 || len(adminSecret.Data[adminPasswordKey]) == 0 {
			return ctrl.Result{}, &ReconciliationError{
				Reason:  dbv1beta1.ReasonCredentialsIncomplete,
				Message: fmt.Sprintf("Admin credentials Secret %s/%s must contain non-empty %q and %q entries; restore the cluster's credentials to resume initialization", mongodb.Namespace, secretName, adminUsernameKey, adminPasswordKey),
				Affects: []string{dbv1beta1.ConditionReady, dbv1beta1.ConditionProgressing},
			}
		}
	} else {
		secret, err := r.ensureAdminCredentialsSecret(ctx, mongodb)
		if err != nil {
			return ctrl.Result{}, fmt.Errorf("ensure admin credentials secret: %w", err)
		}
		adminSecret = secret
	}

	podName := pod.Name
	if !podReady(pod) || pod.Status.PodIP == "" {
		return r.waitForReconciliation(ctx, mongodb, string(dbv1beta1.ProgressReasonWaitingForPod), fmt.Sprintf("Waiting for primary Pod %s to become ready", podName), 15*time.Second)
	}

	managerClient := r.managerClientForPod(pod)
	topology, err := managerClient.GetTopology(ctx)
	if errors.Is(err, managerclient.ErrNotInitialized) {
		// TODO: The instance manager currently maps every replSetGetStatus failure
		// to ErrNotInitialized. Distinguish actual uninitialized state from other
		// command failures, and rediscover unexpected uninitialized members before
		// classifying a persistent block. Gate initiation on explicit bootstrap
		// eligibility rather than only the Pod name.
		if podName != fmt.Sprintf("%s-r-a", mongodb.Name) {
			return ctrl.Result{}, &ReconciliationError{
				Reason:  dbv1beta1.ReasonReplicaSetUninitialized,
				Message: fmt.Sprintf("Instance manager for Pod %s/%s reports an uninitialized replica set; refusing automatic initiation of a non-bootstrap member", mongodb.Namespace, podName),
				Affects: []string{dbv1beta1.ConditionReady, dbv1beta1.ConditionProgressing},
			}
		}
		if err := r.transitionInitializing(ctx, mongodb, dbv1beta1.ProgressReasonInitiatingReplicaSet); err != nil {
			return ctrl.Result{}, err
		}
		if err := managerClient.Initiate(ctx); err != nil {
			return ctrl.Result{}, fmt.Errorf("initiate replica set: %w", err)
		}
		log.Info("Initiated MongoDB replica set", "name", mongodb.Name)
		return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
	}
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("get replica set topology: %w", err)
	}
	if !topologyHasPrimary(topology) {
		return r.waitForReconciliation(ctx, mongodb, dbv1beta1.ReasonPrimaryUnavailable, "Replica set reports no PRIMARY member", 15*time.Second)
	}

	if err := r.transitionInitializing(ctx, mongodb, dbv1beta1.ProgressReasonCreatingAdminUser); err != nil {
		return ctrl.Result{}, err
	}
	username := string(adminSecret.Data[adminUsernameKey])
	password := string(adminSecret.Data[adminPasswordKey])
	if err := managerClient.Authenticate(ctx, username, password); errors.Is(err, managerclient.ErrUnauthenticated) {
		if err := managerClient.CreateAdmin(ctx, username, password); err != nil {
			return ctrl.Result{}, fmt.Errorf("create admin user: %w", err)
		}
		if err := managerClient.Authenticate(ctx, username, password); err != nil {
			return ctrl.Result{}, fmt.Errorf("authenticate newly created admin user: %w", err)
		}
	} else if err != nil {
		return ctrl.Result{}, fmt.Errorf("authenticate admin user: %w", err)
	}

	log.Info("MongoDB cluster bootstrap completed", "name", mongodb.Name)
	if err := r.transitionProgressing(ctx, mongodb, dbv1beta1.PhaseProgressing, dbv1beta1.ProgressReasonReconcilingMembers); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, nil
}

func (r *MongoDBReconciler) transitionInitializing(ctx context.Context, mongodb *dbv1beta1.MongoDB, reason dbv1beta1.ProgressReason) error {
	message, ok := dbv1beta1.ProgressReasonMessage(reason)
	if !ok {
		return fmt.Errorf("get message for progress reason %q: unknown reason", reason)
	}
	return r.updateStatus(ctx, mongodb, dbv1beta1.PhaseInitializing, []metav1.Condition{
		{Type: dbv1beta1.ConditionReady, Status: metav1.ConditionFalse, Reason: dbv1beta1.ReasonInitializing, Message: "MongoDB cluster is not ready"},
		{Type: dbv1beta1.ConditionProgressing, Status: metav1.ConditionTrue, Reason: string(reason), Message: message},
	})
}
