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
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"net"
	"net/url"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	api "github.com/mrhachi/single-tenant-mongo-db/api/v1alphav1"
	mrhachidevv1alphav1 "github.com/mrhachi/single-tenant-mongo-db/api/v1alphav1"
	"github.com/mrhachi/single-tenant-mongo-db/internal/mongo"
	"github.com/mrhachi/single-tenant-mongo-db/internal/resources"
)

// SingleTenantMongoDBReconciler reconciles a SingleTenantMongoDB object
type SingleTenantMongoDBReconciler struct {
	client.Client
	*rest.Config
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=db.mrhachi.dev,resources=singletenantmongodbs,verbs=get;list;watch;create;update;patch
// +kubebuilder:rbac:groups=db.mrhachi.dev,resources=singletenantmongodbs/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=db.mrhachi.dev,resources=singletenantmongodbs/finalizers,verbs=update

// Core resources
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=pods/log,verbs=get
// +kubebuilder:rbac:groups="",resources=services,verbs=get;list;watch;create;update;patch
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch;create;update;patch
// +kubebuilder:rbac:groups="",resources=configmaps,verbs=get;list;watch;create;update;patch
// +kubebuilder:rbac:groups="",resources=persistentvolumeclaims,verbs=get;list;watch;create;update;patch
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch

// Apps
// +kubebuilder:rbac:groups=apps,resources=statefulsets,verbs=get;list;watch;create;update;patch
// +kubebuilder:rbac:groups=apps,resources=statefulsets/status,verbs=get

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.24.1/pkg/reconcile
func (r *SingleTenantMongoDBReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	recLog := logf.FromContext(ctx)

	stmdb := &api.SingleTenantMongoDB{}
	if err := r.Get(ctx, req.NamespacedName, stmdb); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// Reconciliation loop
	sts, err := r.reconcileStatefulSet(ctx, stmdb)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("reconcile stateful set: %w", err)
	}

	svc, err := r.reconcileService(ctx, stmdb)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("reconcile service: %w", err)
	}

	if _, err := r.reconcileConfigMap(ctx, stmdb); err != nil {
		return ctrl.Result{}, fmt.Errorf("reconcile config map: %w", err)
	}

	if _, err := r.ensureKeyfileSecret(ctx, stmdb); err != nil {
		return ctrl.Result{}, fmt.Errorf("ensure keyfile: %w", err)
	}

	pods, err := r.GetPods(ctx, sts)
	if err != nil {
		recLog.Info(
			"error getting STS pods, retry",
			"requeue-after", "5s",
			"error", err,
		)
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}

	if ready, count := allPodsReady(pods); !ready {
		stmdb.Status.Phase = "Starting"
		if err := r.Status().Update(ctx, stmdb); err != nil {
			return ctrl.Result{}, err
		}

		recLog.Info(
			"all pods not yet ready, retry",
			"requeue-after", "5s",
			"ready", count,
			"total", len(pods),
		)
		return ctrl.Result{
			RequeueAfter: 5 * time.Second,
		}, nil
	}

	if len(pods) < int(*sts.Spec.Replicas) {
		recLog.Info(
			"pod count not yet equal to STS spec, retry",
			"requeue-after", "5s",
			"actual", len(pods),
			"desired", int(*sts.Spec.Replicas),
		)
		return ctrl.Result{
			RequeueAfter: 5 * time.Second,
		}, nil
	}

	if stmdb.Spec.Replicas == 0 {
		return ctrl.Result{}, nil
	}

	members := make([]mongo.RSMember, len(pods))
	for idx, _ := range pods {
		members[idx] = mongo.MakeRSMember(
			idx,
			sts.Name, svc.Name, sts.Namespace,
		)
	}

	if err := r.ReconcileSecrets(ctx, stmdb); err != nil {
		return ctrl.Result{}, fmt.Errorf("reconcile secrets: %w", err)
	}

	adminSecret := &corev1.Secret{}
	if err := r.Client.Get(
		ctx,
		types.NamespacedName{
			Name:      stmdb.Spec.Admin.SecretRef.Name,
			Namespace: stmdb.Namespace,
		},
		adminSecret,
	); err != nil {
		return ctrl.Result{}, fmt.Errorf("get password for admin user %s: %w", stmdb.Spec.Admin.Username, err)
	}
	adminPassword, ok := adminSecret.Data["password"]
	if !ok {
		return ctrl.Result{}, fmt.Errorf("missing admin password key")
	}
	connstr := fmt.Sprintf(
		"mongodb://%s:%s@%s/admin",
		url.QueryEscape(stmdb.Spec.Admin.Username),
		url.QueryEscape(string(adminPassword)),
		members[0].Host,
	)

	mongoManager, err := mongo.NewMongoRSManager(ctx, connstr)
	if err != nil {
		stmdb.Status.Phase = "MongoUnavailable"
		if err := r.Status().Update(ctx, stmdb); err != nil {
			return ctrl.Result{}, err
		}

		recLog.Info(
			"mongo not yet available, retry",
			"requeue-after", "5s",
			"error", err,
		)
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}
	if err := mongoManager.Reconfigure(ctx, members); err != nil {
		if dnsErr, ok := errors.AsType[*net.DNSError](err); ok {
			recLog.Info(
				"could not resolve pod domain, retry",
				"requeue-after", "5s",
				"error", dnsErr,
			)
			return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
		}

		return ctrl.Result{}, fmt.Errorf("reconfigure mongo replica set: %w", err)
	}

	users := make([]mongo.MongoUser, len(stmdb.Spec.Users))
	for idx, user := range stmdb.Spec.Users {
		userSecret := &corev1.Secret{}
		if err := r.Client.Get(
			ctx,
			types.NamespacedName{
				Name:      user.SecretRef.Name,
				Namespace: stmdb.Namespace,
			},
			userSecret,
		); err != nil {
			return ctrl.Result{}, fmt.Errorf("get password for user %s: %w", user.Username, err)
		}
		userRoles := make([]mongo.MongoRole, len(user.Roles))
		for idx, role := range user.Roles {
			userRoles[idx] = mongo.MakeMongoRole(role.Role, role.Database)
		}
		userPassword, ok := userSecret.Data["password"]
		if !ok {
			return ctrl.Result{}, fmt.Errorf("missing user password key for user %s", user.Username)
		}
		users[idx] = mongo.MakeMongoUser(user.Username, string(userPassword), userRoles)
	}
	if err := mongoManager.ReconcileUsers(ctx, stmdb.Spec.DatabaseName, users); err != nil {
		return ctrl.Result{}, fmt.Errorf("reconcile mongodb users: %w", err)
	}

	stmdb.Status.Phase = "Ready"
	stmdb.Status.ReadyReplicas = int32(len(pods))

	if err := r.Status().Update(ctx, stmdb); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

func isPodReady(pod corev1.Pod) bool {
	for _, condition := range pod.Status.Conditions {
		if condition.Type == corev1.PodReady {
			return condition.Status == corev1.ConditionTrue
		}
	}

	return false
}

func allPodsReady(pods []corev1.Pod) (bool, int) {
	total := len(pods)

	if total == 0 {
		return false, 0
	}

	ready := 0
	for _, pod := range pods {
		if isPodReady(pod) {
			ready++
		}
	}

	return ready == total, ready
}

// SetupWithManager sets up the controller with the Manager.
func (r *SingleTenantMongoDBReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&mrhachidevv1alphav1.SingleTenantMongoDB{}).
		Owns(&appsv1.StatefulSet{}).
		Owns(&corev1.Service{}).
		Owns(&corev1.ConfigMap{}).
		Owns(&corev1.Secret{}).
		Named("singletenantmongodb").
		Complete(r)
}

func (r *SingleTenantMongoDBReconciler) GetPods(ctx context.Context, sts *appsv1.StatefulSet) ([]corev1.Pod, error) {
	var podList corev1.PodList
	selector, err := metav1.LabelSelectorAsSelector(sts.Spec.Selector)
	if err != nil {
		return nil, fmt.Errorf("invalid sts selector: %w", err)
	}
	if err := r.Client.List(
		ctx,
		&podList,
		client.InNamespace(sts.Namespace),
		client.MatchingLabelsSelector{
			Selector: selector,
		},
	); err != nil {
		return nil, fmt.Errorf("list pods: %w", err)
	}
	return podList.Items, nil
}

// Reconciles Secrets
func (r *SingleTenantMongoDBReconciler) ReconcileSecrets(
	ctx context.Context,
	stmdb *api.SingleTenantMongoDB,
) error {
	if _, err := r.ensureKeyfileSecret(ctx, stmdb); err != nil {
		return fmt.Errorf("ensure keyfile secret: %w", err)
	}
	return nil
}

func (r *SingleTenantMongoDBReconciler) reconcileStatefulSet(
	ctx context.Context,
	desired *api.SingleTenantMongoDB,
) (*appsv1.StatefulSet, error) {
	dSts := resources.MakeDesiredSts(desired)
	aSts := &appsv1.StatefulSet{}
	if err := r.Client.Get(
		ctx,
		types.NamespacedName{
			Namespace: desired.Namespace,
			Name:      desired.Name,
		},
		aSts,
	); err != nil {
		switch {
		case apierrors.IsNotFound(err):
			if err := controllerutil.SetControllerReference(
				desired,
				dSts,
				r.Scheme,
			); err != nil {
				return nil, fmt.Errorf("set owner reference: %w", err)
			}
			if err := r.Client.Create(
				ctx,
				dSts,
			); err != nil {
				return nil, fmt.Errorf("create stateful set: %w", err)
			}
			return dSts, nil
		default:
			return nil, fmt.Errorf("find actual stateful set: %w", err)
		}
	}

	original := aSts.DeepCopy()

	aSts.Spec.Replicas = dSts.Spec.Replicas
	// We only set Resources through the CRD, but we can say we own the entire Template
	aSts.Spec.Template = dSts.Spec.Template
	aSts.Spec.VolumeClaimTemplates = dSts.Spec.VolumeClaimTemplates

	if equality.Semantic.DeepEqual(original.Spec, aSts.Spec) {
		return aSts, nil
	}

	if err := r.Client.Patch(
		ctx,
		aSts,
		client.MergeFrom(original),
	); err != nil {
		return nil, fmt.Errorf("patch stateful set: %w", err)
	}

	return aSts, nil
}

func (r *SingleTenantMongoDBReconciler) reconcileService(
	ctx context.Context,
	desired *api.SingleTenantMongoDB,
) (*corev1.Service, error) {
	dSvc := resources.MakeDesiredSvc(desired)
	aSvc := &corev1.Service{}
	if err := r.Client.Get(
		ctx,
		types.NamespacedName{
			Namespace: desired.Namespace,
			Name:      desired.Name,
		},
		aSvc,
	); err != nil {
		switch {
		case apierrors.IsNotFound(err):
			if err := controllerutil.SetControllerReference(
				desired,
				dSvc,
				r.Scheme,
			); err != nil {
				return nil, fmt.Errorf("set owner reference: %w", err)
			}
			if err := r.Client.Create(
				ctx,
				dSvc,
			); err != nil {
				return nil, fmt.Errorf("create service: %w", err)
			}
			return dSvc, nil
		default:
			return nil, fmt.Errorf("find actual service: %w", err)
		}
	}

	original := aSvc.DeepCopy()

	// ClusterIP isn't editable
	aSvc.Spec.Selector = dSvc.Spec.Selector
	aSvc.Spec.Ports = dSvc.Spec.Ports

	if equality.Semantic.DeepEqual(original.Spec, aSvc.Spec) {
		return aSvc, nil
	}

	if err := r.Client.Patch(
		ctx,
		aSvc,
		client.MergeFrom(original),
	); err != nil {
		return nil, fmt.Errorf("patch service: %w", err)
	}

	return aSvc, nil
}

func (r *SingleTenantMongoDBReconciler) reconcileConfigMap(
	ctx context.Context,
	desired *api.SingleTenantMongoDB,
) (*corev1.ConfigMap, error) {
	cmName := fmt.Sprintf("%s-connection", desired.Name)

	dCm := resources.MakeDesiredCm(desired)
	aCm := &corev1.ConfigMap{}
	if err := r.Client.Get(
		ctx,
		types.NamespacedName{
			Namespace: desired.Namespace,
			Name:      cmName,
		},
		aCm,
	); err != nil {
		switch {
		case apierrors.IsNotFound(err):
			if err := controllerutil.SetControllerReference(
				desired,
				dCm,
				r.Scheme,
			); err != nil {
				return nil, fmt.Errorf("set owner reference: %w", err)
			}
			if err := r.Client.Create(
				ctx,
				dCm,
			); err != nil {
				return nil, fmt.Errorf("create config map: %w", err)
			}
			return dCm, nil
		default:
			return nil, fmt.Errorf("find actual config map: %w", err)
		}
	}

	original := aCm.DeepCopy()

	// We own the entire config map here
	aCm.Data = dCm.Data

	if equality.Semantic.DeepEqual(original.Data, aCm.Data) {
		return aCm, nil
	}

	if err := r.Client.Patch(
		ctx,
		aCm,
		client.MergeFrom(original),
	); err != nil {
		return nil, fmt.Errorf("patch config map: %w", err)
	}

	return aCm, nil
}

// We control the keyfile data
func (r *SingleTenantMongoDBReconciler) ensureKeyfileSecret(
	ctx context.Context,
	desired *api.SingleTenantMongoDB,
) (*corev1.Secret, error) {
	secretName := fmt.Sprintf("%s-kf", desired.Name)

	aSecret := &corev1.Secret{}
	if err := r.Client.Get(
		ctx,
		types.NamespacedName{
			Namespace: desired.Namespace,
			Name:      secretName,
		},
		aSecret,
	); err != nil {
		switch {
		case apierrors.IsNotFound(err):
			kfData, err := generateKeyfileData()
			if err != nil {
				return nil, fmt.Errorf("generate keyfile data: %w", err)
			}
			kfSecret := resources.MakeDesiredKeyfileSecret(desired,
				map[string]string{
					"keyfile": kfData,
				},
			)
			if err := controllerutil.SetControllerReference(
				desired,
				kfSecret,
				r.Scheme,
			); err != nil {
				return nil, fmt.Errorf("set owner reference: %w", err)
			}
			if err := r.Client.Create(
				ctx,
				kfSecret,
			); err != nil {
				return nil, fmt.Errorf("create secret: %w", err)
			}
			return kfSecret, nil
		default:
			return nil, fmt.Errorf("find actual secret: %w", err)
		}
	}
	return aSecret, nil
}

func generateKeyfileData() (string, error) {
	b := make([]byte, 756)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(b), nil
}
