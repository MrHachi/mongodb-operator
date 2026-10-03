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
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	dbv1beta1 "github.com/MrHachi/mongodb-operator/api/v1beta1"
)

// MongoDBReconciler reconciles a MongoDB object
type MongoDBReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=db.mrhachi.dev,resources=mongodbs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=db.mrhachi.dev,resources=mongodbs/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=db.mrhachi.dev,resources=mongodbs/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch;create;update;patch;delete

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the MongoDB object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.24.1/pkg/reconcile
func (r *MongoDBReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	mongodb := &dbv1beta1.MongoDB{}
	if err := r.Get(ctx, req.NamespacedName, mongodb); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	switch mongodb.Status.Phase {
	case "":
		log.Info("New MongoDB cluster", "name", mongodb.Name)
		mongodb.Status.Phase = "Initializing"
		if err := r.Status().Update(ctx, mongodb); err != nil {
			return ctrl.Result{}, fmt.Errorf("set initializing status: %w", err)
		}
		return ctrl.Result{Requeue: true}, nil

	case "Initializing":
		log.Info("Initializing MongoDB cluster", "name", mongodb.Name)
		return r.ReconcileInitializing(ctx, mongodb)

	case "Scaling":
		log.Info("Scaling MongoDB cluster", "name", mongodb.Name)
		// To be implemented
		return ctrl.Result{}, nil

	case "Ready":
		return ctrl.Result{}, nil

	case "Degraded":
		return ctrl.Result{}, nil

	default:
		log.Info("Unknown phase", "phase", mongodb.Status.Phase)
		return ctrl.Result{}, nil
	}
}

// ReconcileInitializing handles the initialization phase of the MongoDB cluster.
func (r *MongoDBReconciler) ReconcileInitializing(ctx context.Context, mongodb *dbv1beta1.MongoDB) (ctrl.Result, error) {
	log := logf.FromContext(ctx)
	log.Info("Reconciling initialization phase", "name", mongodb.Name)

	// 1. Resource Provisioning
	// - Generate and store Keyfile in a Secret
	if err := r.ensureKeyfileSecret(ctx, mongodb); err != nil {
		log.Error(err, "Failed to ensure keyfile secret", "name", mongodb.Name)
		return ctrl.Result{}, fmt.Errorf("ensureKeyfileSecret: %w", err)
	}

	// - Provision PVC for database data
	// - Deploy the initial primary pod (-r-a)
	// - Create a Headless Service for intra-cluster communication

	// 2. Cluster Bootstrapping
	// - Wait for Primary Pod to be Ready (requeue every 15s if not)
	// - RS Initiation:
	//     - Check RS status (GET /v1/topology)
	//     - Execute RS Initiation if not initialized (POST /v1/initiate)
	// - Admin User Creation:
	//     - Check if admin user is authenticated (POST /v1/authenticate)
	//     - Execute Admin User Creation if unauthenticated (POST /v1/admin)

	// 3. Transition
	// - Update status.phase to "Scaling"

	return ctrl.Result{}, nil
}

func (r *MongoDBReconciler) ensureKeyfileSecret(ctx context.Context, mongodb *dbv1beta1.MongoDB) error {
	secretName := fmt.Sprintf("%s-keyfile", mongodb.Name)
	secret := &corev1.Secret{
		Type: corev1.SecretTypeOpaque,
		ObjectMeta: metav1.ObjectMeta{
			Name:      secretName,
			Namespace: mongodb.Namespace,
		},
	}

	err := r.Get(ctx, types.NamespacedName{Name: secretName, Namespace: mongodb.Namespace}, secret)
	if err == nil {
		// Secret already exists, check if it has data
		if len(secret.Data["mongodb-keyfile"]) > 0 {
			return nil
		}

		log := logf.FromContext(ctx)
		log.Info("Keyfile secret exists but is empty, regenerating", "name", secretName)

		key, err := r.generateKeyfile()
		if err != nil {
			return fmt.Errorf("generate keyfile: %w", err)
		}

		if secret.Data == nil {
			secret.Data = make(map[string][]byte)
		}
		secret.Data["mongodb-keyfile"] = key

		// Ensure labels and owner reference are also set
		if secret.Labels == nil {
			secret.Labels = make(map[string]string)
		}
		secret.Labels["app.kubernetes.io/name"] = mongodb.Name
		secret.Labels["db.mrhachi.dev/mongodb"] = mongodb.Name

		if err := ctrl.SetControllerReference(mongodb, secret, r.Scheme); err != nil {
			return fmt.Errorf("set owner reference on keyfile secret: %w", err)
		}

		if err := r.Update(ctx, secret); err != nil {
			return fmt.Errorf("update keyfile secret: %w", err)
		}
		return nil
	}

	if !apierrors.IsNotFound(err) {
		return fmt.Errorf("get keyfile secret: %w", err)
	}

	// Secret doesn't exist, generate and create it
	log := logf.FromContext(ctx)
	log.Info("Generating keyfile secret", "name", secretName)

	key, err := r.generateKeyfile()
	if err != nil {
		return fmt.Errorf("generate keyfile: %w", err)
	}

	secret = &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      secretName,
			Namespace: mongodb.Namespace,
			Labels: map[string]string{
				"app.kubernetes.io/name": mongodb.Name,
				"db.mrhachi.dev/mongodb": mongodb.Name,
			},
		},
		Type: corev1.SecretTypeOpaque,
		Data: map[string][]byte{
			"mongodb-keyfile": key,
		},
	}

	if err := ctrl.SetControllerReference(mongodb, secret, r.Scheme); err != nil {
		return fmt.Errorf("set owner reference on keyfile secret: %w", err)
	}

	if err := r.Create(ctx, secret); err != nil {
		return fmt.Errorf("create keyfile secret: %w", err)
	}

	return nil
}

func (r *MongoDBReconciler) generateKeyfile() ([]byte, error) {
	key := make([]byte, 1024)
	if _, err := rand.Read(key); err != nil {
		return nil, err
	}
	return key, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *MongoDBReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&dbv1beta1.MongoDB{}).
		Named("mongodb").
		Complete(r)
}
