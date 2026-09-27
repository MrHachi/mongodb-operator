package controller

import (
	"context"
	"fmt"
	"time"

	"github.com/mrhachi/mongodb-operator/api/v1betav1"
	dbv1betav1 "github.com/mrhachi/mongodb-operator/api/v1betav1"
	rutils "github.com/mrhachi/mongodb-operator/internal/controller/utils/resources"
	"github.com/mrhachi/mongodb-operator/internal/controller/utils/secrets"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
)

func (r *MongoDBReconciler) reconcileInitializing(ctx context.Context, db *v1betav1.MongoDB) (reconcile.Result, error) {
	logger := logf.FromContext(ctx)

	// TODO: check that this replicaset is, in fact, awaiting initialization:
	// 1. rs.Status() should not show that we're running in rs mode, OR
	// 2. rs.Status() should show that the topology only has one
	// 	  replica (the primary), and we should NOT have an admin user

	kfSecret, err := r.ensureKeyfileSecret(ctx)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("ensure keyfile secret: %w", err)
	}
	sa, _, err := r.ensureRBAC(ctx)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("ensure RBAC: %w", err)
	}
	primary, _, err := r.ensurePrimaryPod(ctx, "a", kfSecret.Name, sa.Name)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("ensure primary pod: %w", err)
	}
	_, err = r.ensureHeadlessService(ctx)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("ensure headless service: %w", err)
	}

	primaryIP, ok, err := r.ensurePodIp(ctx, primary)
	if err != nil {
		logger.Error(err,
			"ensure pod IP",
			"podName", primary.Name, "podNamespace", primary.Namespace,
			"podStatus", primary.Status.Phase,
		)
	}
	if !ok {
		// no error and no IP found should mean that the Pod isn't yet running
		logger.Info(
			"primary not yet running",
			"podName", primary.Name, "podNamespace", primary.Namespace,
			"podStatus", primary.Status.Phase,
		)
		return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
	}

	adminPassword, err := r.getAdminSecret(ctx, db.Spec.Admin.SecretRef.Name, db.Namespace)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("get admin password: %w", err)
	}

	if err = r.ensureDbInitialization(ctx, primaryIP, db.Spec.Admin.Username, string(adminPassword)); err != nil {
		return ctrl.Result{}, fmt.Errorf("ensure db initialized: %w", err)
	}

	db.Status.Phase = dbv1betav1.PhaseScaling

	logger.Info(
		"initialization complete, will set to scaling",
		"name", db.Name,
	)

	if err := r.Status().Update(ctx, db); err != nil {
		return ctrl.Result{}, fmt.Errorf("set scaling status: %w", err)
	}

	return ctrl.Result{}, nil
}

func (r *MongoDBReconciler) getAdminSecret(ctx context.Context, secretName, secretNamespace string) ([]byte, error) {
	adminSecret := &corev1.Secret{}
	if err := r.Get(ctx, types.NamespacedName{
		Name:      secretName,
		Namespace: secretNamespace,
	}, adminSecret); err != nil {
		return nil, fmt.Errorf("get admin secret: %w", err)
	}

	adminPassword, ok := adminSecret.Data["password"]
	if !ok {
		return nil, fmt.Errorf("admin secret missing 'password' key")
	}
	return adminPassword, nil
}

func (r *MongoDBReconciler) ensureKeyfileSecret(ctx context.Context) (*corev1.Secret, error) {
	desired := r.kr.DesiredKeyfileSecret()

	return rutils.Ensure(
		ctx, r.kr.MongoDB, r.Client, r.Scheme,
		desired,
		&corev1.Secret{},
		secrets.FillKeyfile,
	)
}

func (r *MongoDBReconciler) ensureRBAC(ctx context.Context) (*corev1.ServiceAccount, *rbacv1.ClusterRoleBinding, error) {
	desiredSa, desiredCrb := r.kr.DesiredReplicaRBAC()

	actualSa, err := rutils.Ensure(
		ctx, r.kr.MongoDB, r.Client, r.Scheme,
		desiredSa,
		&corev1.ServiceAccount{},
	)
	if err != nil {
		return nil, nil, fmt.Errorf("ensure sa: %w", err)
	}
	actualCrb, err := rutils.Ensure(
		ctx, nil, r.Client, r.Scheme,
		desiredCrb,
		&rbacv1.ClusterRoleBinding{},
	)
	if err != nil {
		return nil, nil, fmt.Errorf("ensure crb: %w", err)
	}

	return actualSa, actualCrb, nil
}

func (r *MongoDBReconciler) ensurePrimaryPod(ctx context.Context, primarySuffix, kfSecretName, saName string) (*corev1.Pod, *corev1.PersistentVolumeClaim, error) {
	desiredPod, desiredPvc := r.kr.DesiredReplicaPod(primarySuffix, kfSecretName, saName)

	actualPvc, err := rutils.Ensure(
		ctx, r.kr.MongoDB, r.Client, r.Scheme,
		desiredPvc,
		&corev1.PersistentVolumeClaim{},
	)
	if err != nil {
		return nil, nil, fmt.Errorf("ensure pvc: %w", err)
	}

	actualPod, err := rutils.Ensure(
		ctx, r.kr.MongoDB, r.Client, r.Scheme,
		desiredPod,
		&corev1.Pod{},
	)
	if err != nil {
		return nil, nil, fmt.Errorf("ensure primary pod: %w", err)
	}

	return actualPod, actualPvc, nil
}

func (r *MongoDBReconciler) ensureHeadlessService(ctx context.Context) (*corev1.Service, error) {
	desired := r.kr.DesiredSvc()

	return rutils.Ensure(
		ctx, r.kr.MongoDB, r.Client, r.Scheme,
		desired,
		&corev1.Service{},
	)
}

func (r *MongoDBReconciler) ensureDbInitialization(ctx context.Context, podIP, username, password string) error {
	return r.instanceManagerClient.Initialize(ctx, podIP, username, password)
}
