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
	"os"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	dbv1alphav2 "github.com/mrhachi/mongodb-operator/api/v1alphav2"
	"github.com/mrhachi/mongodb-operator/internal/controller/resources"
	podutils "github.com/mrhachi/mongodb-operator/internal/controller/utils/pod"
	imgrclient "github.com/mrhachi/mongodb-operator/internal/manager/client"
)

const (
	defaultSaName = "mongodb-controller"
)

func saName() string {
	saName := os.Getenv("SA_NAME")
	if saName == "" {
		return defaultSaName
	}
	return saName
}

// MongoDBReconciler reconciles a MongoDB object
type MongoDBReconciler struct {
	client.Client
	Scheme *runtime.Scheme

	instanceManagerClient *imgrclient.Client
	kr                    *resources.MongoDB
}

// +kubebuilder:rbac:groups=db.mrhachi.dev,resources=mongodbs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=db.mrhachi.dev,resources=mongodbs/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=db.mrhachi.dev,resources=mongodbs/finalizers,verbs=update
//
// +kubebuilder:rbac:groups="",resources=serviceaccounts,verbs=get;create;update;patch;delete
// +kubebuilder:rbac:groups=rbac.authorization.k8s.io,resources=clusterrolebindings,verbs=get;list;watch;create;update;patch;delete

// Core resources
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=serviceaccounts,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=pods/log,verbs=get
// +kubebuilder:rbac:groups="",resources=services,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=configmaps,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=persistentvolumeclaims,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.24.1/pkg/reconcile
func (r *MongoDBReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := logf.FromContext(ctx)

	db := &dbv1alphav2.MongoDB{}
	if err := r.Get(ctx, req.NamespacedName, db); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	var imgrClientCreateErr error
	r.instanceManagerClient, imgrClientCreateErr = imgrclient.NewInClusterClient(db.Namespace, saName())
	if imgrClientCreateErr != nil {
		return ctrl.Result{}, fmt.Errorf("create in-cluster instance-manager client: %w", imgrClientCreateErr)
	}

	r.kr = resources.NewMongoDB(db)

	// TODO
	// if !db.DeletionTimestamp.IsZero() {
	// 	return r.reconcileDelete(ctx, db) // we need to delete any clusterrolebindings here-they can't have their owner set to the CRD
	// }

	switch db.Status.Phase {
	case "":
		db.Status.Phase = dbv1alphav2.PhaseInitializing

		logger.Info(
			"MongoDB phase unset, will set to initializing",
			"name", db.Name,
		)

		if err := r.Status().Update(ctx, db); err != nil {
			return ctrl.Result{}, fmt.Errorf("set initializing status: %w", err)
		}

		return ctrl.Result{}, nil
	case dbv1alphav2.PhaseInitializing:
		return r.reconcileInitializing(ctx, db)

	// TODO
	// 	case dbv1alphav2.PhaseScaling:
	// 		return r.reconcileScaling(ctx, desired, actual)
	//
	// 	case dbv1alphav2.PhaseReady:
	// 		return r.reconcileSteadyState(ctx, desired, actual)
	//
	// 	case dbv1alphav2.PhaseDegraded:
	// 		return r.reconcileDegraded(ctx, desired, actual)

	default:
		return ctrl.Result{}, fmt.Errorf("unknown phase %q", db.Status.Phase)
	}
}

// SetupWithManager sets up the controller with the Manager.
func (r *MongoDBReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&dbv1alphav2.MongoDB{}).
		Named("mongodb").
		Complete(r)
}

// refreshPod gets the latest state of the Pod with the Kubernetes API.
func (r *MongoDBReconciler) refreshPod(ctx context.Context, pod *corev1.Pod) error {
	if err := r.Client.Get(ctx, client.ObjectKeyFromObject(pod), pod); err != nil {
		// Explicitly don't handle NotFound errors here (leave this to the caller)
		return fmt.Errorf("get latest state of pod: %w", err)
	}
	return nil
}

// ensurePodIp refreshes the Pod object and returns an IP that belongs to it (if found)
// and a boolean indicating whether an IP was found.
func (r *MongoDBReconciler) ensurePodIp(ctx context.Context, pod *corev1.Pod) (string, bool, error) {
	if err := r.refreshPod(ctx, pod); err != nil {
		return "", false, fmt.Errorf("refresh pod: %w", err)
	}
	if running := podutils.IsPodRunning(pod); !running {
		return "", false, nil
	}

	podIP, ok := podutils.GetPodIP(pod)
	if !ok {
		return "", false, errors.New("no IP addresses found for primary Pod")
	}
	return podIP, true, nil
}
