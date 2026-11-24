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
	"net"
	"slices"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apiMeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	kubeclient "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	dbv1beta1 "github.com/MrHachi/mongodb-operator/api/v1beta1"
	controllerconfig "github.com/MrHachi/mongodb-operator/internal/controller/config"
	"github.com/MrHachi/mongodb-operator/internal/controller/discovery"
	"github.com/MrHachi/mongodb-operator/internal/controller/reconciliation"
	"github.com/MrHachi/mongodb-operator/internal/controller/resources"
	managerclient "github.com/MrHachi/mongodb-operator/internal/manager/pkg/client"
)

const instanceManagerRBACFinalizer = "db.mrhachi.dev/instance-manager-rbac"

var (
	errObservedGenerationChanged = errors.New("MongoDB generation changed during reconciliation")
)

// MongoDBReconciler reconciles a MongoDB object
type MongoDBReconciler struct {
	client.Client

	Scheme     *runtime.Scheme
	KubeClient kubeclient.Interface
	Config     *controllerconfig.Config

	// Reads directly from the Kubernetes API, bypassing the manager's cache (for use in updateStatus)
	APIReader client.Reader
}

// +kubebuilder:rbac:groups=db.mrhachi.dev,resources=mongodbs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=db.mrhachi.dev,resources=mongodbs/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=db.mrhachi.dev,resources=mongodbs/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=persistentvolumeclaims,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=services,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=serviceaccounts/token,resourceNames=mongodb-operator-2-controller-manager,verbs=create
// +kubebuilder:rbac:groups=rbac.authorization.k8s.io,resources=clusterrolebindings,verbs=get;create;update;delete

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the MongoDB object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.24.1/pkg/reconcile
func (r *MongoDBReconciler) Reconcile(ctx context.Context, req ctrl.Request) (result ctrl.Result, reconcileErr error) {
	mongodb := &dbv1beta1.MongoDB{}
	defer func() {
		if errors.Is(reconcileErr, errObservedGenerationChanged) {
			result = ctrl.Result{Requeue: true}
			reconcileErr = nil
			return
		}

		if blocked, ok := errors.AsType[*reconciliation.BlockingError](reconcileErr); ok {
			if err := r.blockReconciliation(ctx, mongodb, blocked); err != nil {
				reconcileErr = fmt.Errorf("record reconciliation error status: %w", err)
				return
			}
			logf.FromContext(ctx).Info("Reconciliation is blocked", "name", mongodb.Name, "reason", blocked.Reason, "message", blocked.Message)
			result = ctrl.Result{RequeueAfter: 15 * time.Second}
			reconcileErr = nil
		}
	}()

	log := logf.FromContext(ctx)

	if err := r.Get(ctx, req.NamespacedName, mongodb); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if mongodb.DeletionTimestamp != nil {
		if containsFinalizer(mongodb.Finalizers, instanceManagerRBACFinalizer) {
			if err := r.kubernetesResources().DeleteInstanceManagerBinding(ctx, mongodb); err != nil {
				return ctrl.Result{}, err
			}
			before := mongodb.DeepCopy()
			mongodb.Finalizers = removeFinalizer(mongodb.Finalizers, instanceManagerRBACFinalizer)
			if err := r.Patch(ctx, mongodb, client.MergeFrom(before)); err != nil {
				return ctrl.Result{}, fmt.Errorf("remove instance-manager RBAC finalizer: %w", err)
			}
		}
		return ctrl.Result{}, nil
	}
	if !containsFinalizer(mongodb.Finalizers, instanceManagerRBACFinalizer) {
		before := mongodb.DeepCopy()
		mongodb.Finalizers = append(mongodb.Finalizers, instanceManagerRBACFinalizer)
		if err := r.Patch(ctx, mongodb, client.MergeFrom(before)); err != nil {
			return ctrl.Result{}, fmt.Errorf("add instance-manager RBAC finalizer: %w", err)
		}
		return ctrl.Result{Requeue: true}, nil
	}

	recoveryStatus, err := r.reconcileDegradation(ctx, mongodb)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("reconcile degradation: %w", err)
	}
	if !recoveryStatus.Continue {
		return ctrl.Result{RequeueAfter: recoveryStatus.RequeueAfter}, nil
	}

	switch mongodb.Status.Phase {
	case "":
		log.Info("New MongoDB cluster", "name", mongodb.Name)
		if err := r.transitionInitializing(ctx, mongodb, dbv1beta1.ProgressReasonCreatingResources); err != nil {
			return ctrl.Result{}, err
		}

	case dbv1beta1.PhaseInitializing:
		log.Info("Initializing MongoDB cluster", "name", mongodb.Name)
		result, reconcileErr = r.reconcileInitializing(ctx, mongodb)

	case dbv1beta1.PhaseProgressing:
		log.Info("Reconciling MongoDB cluster", "name", mongodb.Name)
		result, reconcileErr = r.reconcileProgressing(ctx, mongodb)

	case dbv1beta1.PhaseReady:
		log.Info("Configuring ready MongoDB cluster", "name", mongodb.Name)
		result, reconcileErr = r.reconcileReady(ctx, mongodb)

	default:
		log.Info("Migrating unknown phase to Progressing", "phase", mongodb.Status.Phase)
		result, reconcileErr = r.reconcileProgressing(ctx, mongodb)
	}
	if reconcileErr != nil {
		// return the result as-is if a reconciliation error occurred: this supercedes the degradation recovery requeue decision
		return result, reconcileErr
	}

	return withEarlierRequeue(result, recoveryStatus.RequeueAfter), nil
}

func containsFinalizer(finalizers []string, finalizer string) bool {
	return slices.Contains(finalizers, finalizer)
}

func removeFinalizer(finalizers []string, finalizer string) []string {
	remaining := finalizers[:0]
	for _, existing := range finalizers {
		if existing != finalizer {
			remaining = append(remaining, existing)
		}
	}
	return remaining
}

func (r *MongoDBReconciler) transitionProgressing(ctx context.Context, mongodb *dbv1beta1.MongoDB, phase dbv1beta1.MongoDBPhase, reason dbv1beta1.ProgressReason) error {
	message, ok := dbv1beta1.ProgressReasonMessage(reason)
	if !ok {
		return fmt.Errorf("get message for progress reason %q: unknown reason", reason)
	}
	return r.updateStatus(ctx, mongodb, phase, []metav1.Condition{
		{Type: dbv1beta1.ConditionReady, Status: metav1.ConditionFalse, Reason: dbv1beta1.ReasonProgressing, Message: "MongoDB cluster is not ready"},
		{Type: dbv1beta1.ConditionProgressing, Status: metav1.ConditionTrue, Reason: string(reason), Message: message},
	})
}

func (r *MongoDBReconciler) transitionReady(ctx context.Context, mongodb *dbv1beta1.MongoDB) error {
	return r.updateStatus(ctx, mongodb, dbv1beta1.PhaseReady, []metav1.Condition{
		{Type: dbv1beta1.ConditionReady, Status: metav1.ConditionTrue, Reason: dbv1beta1.ReasonReady, Message: "MongoDB cluster is ready"},
		{Type: dbv1beta1.ConditionProgressing, Status: metav1.ConditionFalse, Reason: dbv1beta1.ReasonNotProgressing, Message: "No changes are in progress"},
	})
}

// updateStatus retries status writes against the latest MongoDB resource and
// aborts if its generation has changed since this reconciliation observed it.
// On success, it refreshes mongodb in place with the latest resource version
// and status, which is visible to the caller and any later status writes.
func (r *MongoDBReconciler) updateStatus(
	ctx context.Context,
	mongodb *dbv1beta1.MongoDB,
	phase dbv1beta1.MongoDBPhase,
	conditions []metav1.Condition,
) error {
	observedGeneration := mongodb.Generation
	reader := r.APIReader
	if reader == nil {
		reader = r.Client
	}
	err := retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		latest := &dbv1beta1.MongoDB{}
		if err := reader.Get(ctx, client.ObjectKeyFromObject(mongodb), latest); err != nil {
			return err
		}
		if latest.Generation != observedGeneration {
			return errObservedGenerationChanged
		}

		before := latest.DeepCopy()
		latest.Status.Phase = phase
		for _, condition := range conditions {
			condition.ObservedGeneration = latest.Generation
			apiMeta.SetStatusCondition(&latest.Status.Conditions, condition)
		}
		if equality.Semantic.DeepEqual(before.Status, latest.Status) {
			*mongodb = *latest
			return nil
		}

		if err := r.Status().Update(ctx, latest); err != nil {
			return err
		}
		*mongodb = *latest
		return nil
	})
	if err != nil {
		return fmt.Errorf("update MongoDB status: %w", err)
	}
	return nil
}

func (r *MongoDBReconciler) managerClientForPod(pod *corev1.Pod) *managerclient.Client {
	managerURL := "http://" + net.JoinHostPort(pod.Status.PodIP, "8080")
	return managerclient.New(managerURL, r.KubeClient, r.Config.ServiceAccountNamespace, r.Config.ServiceAccountName)
}

func (r *MongoDBReconciler) discoveryManagerForPod(pod *corev1.Pod) discovery.Manager {
	return r.managerClientForPod(pod)
}

func (r *MongoDBReconciler) kubernetesResources() *resources.KubernetesResources {
	return resources.NewKubernetesResources(r.Client, r.Scheme, r.KubeClient, r.Config)
}

// SetupWithManager sets up the controller with the Manager.
func (r *MongoDBReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if r.Config == nil {
		r.Config = controllerconfig.LoadConfig()
	}
	if r.KubeClient == nil {
		kube, err := kubeclient.NewForConfig(mgr.GetConfig())
		if err != nil {
			return fmt.Errorf("create Kubernetes client: %w", err)
		}
		r.KubeClient = kube
	}
	if r.Config.ServiceAccountNamespace == "" || r.Config.ServiceAccountName == "" {
		return fmt.Errorf("POD_NAMESPACE and SERVICE_ACCOUNT_NAME must be configured")
	}
	if r.Config.InstanceManagerRoleName == "" {
		return fmt.Errorf("INSTANCE_MANAGER_AUTH_ROLE_NAME must be configured")
	}
	if r.APIReader == nil {
		r.APIReader = mgr.GetAPIReader()
	}

	return ctrl.NewControllerManagedBy(mgr).
		For(&dbv1beta1.MongoDB{}, builder.WithPredicates(predicate.Funcs{
			CreateFunc:  func(event.CreateEvent) bool { return true },
			DeleteFunc:  func(event.DeleteEvent) bool { return true },
			GenericFunc: func(event.GenericEvent) bool { return true },
			UpdateFunc: func(e event.UpdateEvent) bool {
				oldMongoDB, oldOK := e.ObjectOld.(*dbv1beta1.MongoDB)
				newMongoDB, newOK := e.ObjectNew.(*dbv1beta1.MongoDB)
				if !oldOK || !newOK {
					return false
				}

				return oldMongoDB.Generation != newMongoDB.Generation ||
					oldMongoDB.Status.Phase != newMongoDB.Status.Phase ||
					hasDegradedStatusCondition(oldMongoDB.Status.Conditions, newMongoDB.Status.Conditions)
			},
		})).
		Named("mongodb").
		Complete(r)
}

func (r *MongoDBReconciler) blockReconciliation(ctx context.Context, mongodb *dbv1beta1.MongoDB, reconciliationErr *reconciliation.BlockingError) error {
	conditions, err := reconciliationErr.Conditions()
	if err != nil {
		return fmt.Errorf("build blocking error conditions: %w", err)
	}

	return r.updateStatus(ctx, mongodb, mongodb.Status.Phase, conditions)
}

// waitForReconciliation records a condition for a state that may resolve on a
// later observation, then requeues after the requested delay. It leaves the
// current phase unchanged, sets Ready=False and Progressing=True using the
// supplied reason and message, and is used when reconciliation must wait for
// an external resource or MongoDB state to become available.
func (r *MongoDBReconciler) waitForReconciliation(ctx context.Context, mongodb *dbv1beta1.MongoDB, reason, message string, requeueAfter time.Duration) (ctrl.Result, error) {
	if err := r.updateStatus(ctx, mongodb, mongodb.Status.Phase, []metav1.Condition{
		{Type: dbv1beta1.ConditionReady, Status: metav1.ConditionFalse, Reason: reason, Message: message},
		{Type: dbv1beta1.ConditionProgressing, Status: metav1.ConditionTrue, Reason: reason, Message: message},
	}); err != nil {
		return ctrl.Result{}, fmt.Errorf("record reconciliation wait status: %w", err)
	}
	logf.FromContext(ctx).Info("Waiting to continue reconciliation", "name", mongodb.Name, "reason", reason, "message", message)
	return ctrl.Result{RequeueAfter: requeueAfter}, nil
}
