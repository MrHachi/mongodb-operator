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
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	kubeclient "k8s.io/client-go/kubernetes"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	dbv1beta1 "github.com/MrHachi/mongodb-operator/api/v1beta1"
	managerclient "github.com/MrHachi/mongodb-operator/internal/manager/pkg/client"
	managerconfig "github.com/MrHachi/mongodb-operator/internal/manager/pkg/config"
)

const (
	adminUsernameKey  = "username"
	adminPasswordKey  = "password"
	keyfileVolumeName = "keyfile"
)

// MongoDBReconciler reconciles a MongoDB object
type MongoDBReconciler struct {
	client.Client
	Scheme                  *runtime.Scheme
	KubeClient              kubeclient.Interface
	ServiceAccountNamespace string
	ServiceAccountName      string
}

// +kubebuilder:rbac:groups=db.mrhachi.dev,resources=mongodbs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=db.mrhachi.dev,resources=mongodbs/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=db.mrhachi.dev,resources=mongodbs/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=persistentvolumeclaims,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=services,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=serviceaccounts/token,resourceNames=controller-manager,verbs=create

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
	if err := r.ensurePVC(ctx, mongodb, "a"); err != nil {
		log.Error(err, "Failed to ensure PVC", "name", mongodb.Name, "id", "a")
		return ctrl.Result{}, fmt.Errorf("ensurePVC: %w", err)
	}

	// - Deploy the initial primary pod (-r-a)
	if err := r.ensureReplicaPod(ctx, mongodb, "a"); err != nil {
		log.Error(err, "Failed to ensure replica pod", "name", mongodb.Name, "id", "a")
		return ctrl.Result{}, fmt.Errorf("ensureReplicaPod: %w", err)
	}

	// - Create a Headless Service for intra-cluster communication
	if err := r.ensureHeadlessService(ctx, mongodb); err != nil {
		log.Error(err, "Failed to ensure headless service", "name", mongodb.Name)
		return ctrl.Result{}, fmt.Errorf("ensureHeadlessService: %w", err)
	}

	adminSecret, err := r.ensureAdminCredentialsSecret(ctx, mongodb)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("ensure admin credentials secret: %w", err)
	}

	pod := &corev1.Pod{}
	podName := fmt.Sprintf("%s-r-a", mongodb.Name)
	if err := r.Get(ctx, types.NamespacedName{Name: podName, Namespace: mongodb.Namespace}, pod); err != nil {
		return ctrl.Result{}, fmt.Errorf("get primary pod: %w", err)
	}
	if !podReady(pod) || pod.Status.PodIP == "" {
		log.Info("Waiting for primary pod to become ready", "name", podName)
		return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
	}

	managerURL := fmt.Sprintf("http://%s:8080", pod.Status.PodIP)
	managerClient := managerclient.New(managerURL, r.KubeClient, r.ServiceAccountNamespace, r.ServiceAccountName, managerconfig.InstanceManagerAudience)
	err = managerClient.GetTopology(ctx)
	if errors.Is(err, managerclient.ErrNotInitialized) {
		if err := managerClient.Initiate(ctx); err != nil {
			return ctrl.Result{}, fmt.Errorf("initiate replica set: %w", err)
		}
		log.Info("Initiated MongoDB replica set", "name", mongodb.Name)
		return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
	}
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("get replica set topology: %w", err)
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
	mongodb.Status.Phase = "Scaling"
	if err := r.Status().Update(ctx, mongodb); err != nil {
		return ctrl.Result{}, fmt.Errorf("set scaling status: %w", err)
	}
	return ctrl.Result{}, nil
}

func podReady(pod *corev1.Pod) bool {
	for _, condition := range pod.Status.Conditions {
		if condition.Type == corev1.PodReady {
			return condition.Status == corev1.ConditionTrue
		}
	}
	return false
}

func (r *MongoDBReconciler) ensureAdminCredentialsSecret(ctx context.Context, mongodb *dbv1beta1.MongoDB) (*corev1.Secret, error) {
	secretName := fmt.Sprintf("%s-admin-credentials", mongodb.Name)
	secret := &corev1.Secret{}
	err := r.Get(ctx, types.NamespacedName{Name: secretName, Namespace: mongodb.Namespace}, secret)
	if err == nil {
		if len(secret.Data[adminUsernameKey]) == 0 || len(secret.Data[adminPasswordKey]) == 0 {
			return nil, fmt.Errorf("secret %s is missing username or password", secretName)
		}
		return secret, nil
	}
	if !apierrors.IsNotFound(err) {
		return nil, fmt.Errorf("get admin credentials secret: %w", err)
	}

	password := make([]byte, 32)
	if _, err := rand.Read(password); err != nil {
		return nil, fmt.Errorf("generate admin password: %w", err)
	}
	secret = &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: secretName, Namespace: mongodb.Namespace, Labels: r.labels(mongodb)},
		Type:       corev1.SecretTypeOpaque,
		Data:       map[string][]byte{adminUsernameKey: []byte("admin"), adminPasswordKey: []byte(hex.EncodeToString(password))},
	}
	if err := ctrl.SetControllerReference(mongodb, secret, r.Scheme); err != nil {
		return nil, fmt.Errorf("set owner reference on admin credentials secret: %w", err)
	}
	if err := r.Create(ctx, secret); err != nil {
		return nil, fmt.Errorf("create admin credentials secret: %w", err)
	}
	return secret, nil
}

func (r *MongoDBReconciler) labels(mongodb *dbv1beta1.MongoDB, custom ...string) map[string]string {
	if mongodb == nil {
		return map[string]string{}
	}

	labels := make(map[string]string, 2+(len(custom)/2))
	labels["app.kubernetes.io/name"] = mongodb.Name
	labels["db.mrhachi.dev/mongodb"] = mongodb.Name

	for i := 0; i+1 < len(custom); i += 2 {
		labels[custom[i]] = custom[i+1]
	}

	return labels
}

func (r *MongoDBReconciler) ensureKeyfileSecret(ctx context.Context, mongodb *dbv1beta1.MongoDB) error {
	secretName := fmt.Sprintf("%s-keyfile", mongodb.Name)
	secret := &corev1.Secret{
		Type: corev1.SecretTypeOpaque,
		ObjectMeta: metav1.ObjectMeta{
			Name:      secretName,
			Namespace: mongodb.Namespace,
			Labels:    r.labels(mongodb),
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
			Labels:    r.labels(mongodb),
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

func (r *MongoDBReconciler) ensurePVC(ctx context.Context, mongodb *dbv1beta1.MongoDB, id string) error {
	log := logf.FromContext(ctx)
	pvcName := fmt.Sprintf("%s-r-%s-data", mongodb.Name, id)
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pvcName,
			Namespace: mongodb.Namespace,
			Labels: r.labels(mongodb,
				"db.mrhachi.dev/role", "replica",
				"db.mrhachi.dev/member", fmt.Sprintf("r-%s", id),
			),
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse("20Gi"),
				},
			},
		},
	}

	err := r.Get(ctx, types.NamespacedName{Name: pvcName, Namespace: mongodb.Namespace}, pvc)
	if err == nil {
		return nil
	}
	if !apierrors.IsNotFound(err) {
		return fmt.Errorf("get pvc: %w", err)
	}

	log.Info("Creating PVC", "name", pvcName)
	if err := ctrl.SetControllerReference(mongodb, pvc, r.Scheme); err != nil {
		return fmt.Errorf("set owner reference on pvc: %w", err)
	}

	return r.Create(ctx, pvc)
}

func (r *MongoDBReconciler) ensureReplicaPod(ctx context.Context, mongodb *dbv1beta1.MongoDB, id string) error {
	log := logf.FromContext(ctx)
	podName := fmt.Sprintf("%s-r-%s", mongodb.Name, id)
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      podName,
			Namespace: mongodb.Namespace,
			Labels: r.labels(mongodb,
				"db.mrhachi.dev/role", "replica",
				"db.mrhachi.dev/member", fmt.Sprintf("r-%s", id),
			),
		},
		Spec: corev1.PodSpec{
			Hostname:      podName,
			Subdomain:     mongodb.Name,
			RestartPolicy: corev1.RestartPolicyAlways,
			Containers: []corev1.Container{
				{
					Name:    "setup-keyfile",
					Image:   "mongo:latest",
					Command: []string{"sh", "-c", "chown 999:999 /data/configdb/mongodb.key && chmod 0600 /data/configdb/mongodb.key"},
					VolumeMounts: []corev1.VolumeMount{
						{
							Name:      keyfileVolumeName,
							MountPath: "/data/configdb",
						},
					},
				},
				{
					Name:  "instance-manager",
					Image: "ghcr.io/mrhachi/mongodb-instance-manager:v0.1",
					Env: []corev1.EnvVar{
						{Name: "MONGODB_RS_NAME", Value: mongodb.Name},
						{Name: "MONGODB_HOSTNAME", Value: podName},
						{Name: "MONGODB_SERVICE_NAME", Value: mongodb.Name},
						{Name: "MONGODB_NAMESPACE", Value: mongodb.Namespace},
					},
					Ports: []corev1.ContainerPort{
						{
							Name:          "http",
							ContainerPort: 8080,
						},
					},
				},
				{
					Name:    "mongodb",
					Image:   "mongo:latest",
					Command: []string{"mongod", "--keyFile", "/data/configdb/mongodb.key", "--replSet", mongodb.Name},
					VolumeMounts: []corev1.VolumeMount{
						{
							Name:      "data",
							MountPath: "/data/db",
						},
						{
							Name:      keyfileVolumeName,
							MountPath: "/data/configdb",
						},
					},
				},
			},
			Volumes: []corev1.Volume{
				{
					Name: keyfileVolumeName,
					VolumeSource: corev1.VolumeSource{
						Secret: &corev1.SecretVolumeSource{
							SecretName: fmt.Sprintf("%s-keyfile", mongodb.Name),
						},
					},
				},
				{
					Name: "data",
					VolumeSource: corev1.VolumeSource{
						PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
							ClaimName: fmt.Sprintf("%s-r-%s-data", mongodb.Name, id),
						},
					},
				},
			},
		},
	}

	err := r.Get(ctx, types.NamespacedName{Name: podName, Namespace: mongodb.Namespace}, pod)
	if err == nil {
		return nil
	}
	if !apierrors.IsNotFound(err) {
		return fmt.Errorf("get replica pod: %w", err)
	}

	log.Info("Creating replica pod", "name", podName)
	if err := ctrl.SetControllerReference(mongodb, pod, r.Scheme); err != nil {
		return fmt.Errorf("set owner reference on replica pod: %w", err)
	}

	return r.Create(ctx, pod)
}

func (r *MongoDBReconciler) ensureHeadlessService(ctx context.Context, mongodb *dbv1beta1.MongoDB) error {
	log := logf.FromContext(ctx)
	serviceName := mongodb.Name
	service := &corev1.Service{}
	err := r.Get(ctx, types.NamespacedName{Name: serviceName, Namespace: mongodb.Namespace}, service)
	if err == nil {
		return nil
	}
	if !apierrors.IsNotFound(err) {
		return fmt.Errorf("get headless service: %w", err)
	}

	log.Info("Creating headless service", "name", serviceName)
	service = &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      serviceName,
			Namespace: mongodb.Namespace,
			Labels:    r.labels(mongodb),
		},
		Spec: corev1.ServiceSpec{
			ClusterIP:                corev1.ClusterIPNone,
			PublishNotReadyAddresses: true,
			Selector: r.labels(mongodb,
				"db.mrhachi.dev/role", "replica",
			),
			Ports: []corev1.ServicePort{
				{
					Name:       "mongodb",
					Port:       27017,
					TargetPort: intstr.FromInt32(27017),
				},
			},
		},
	}

	if err := ctrl.SetControllerReference(mongodb, service, r.Scheme); err != nil {
		return fmt.Errorf("set owner reference on headless service: %w", err)
	}
	if err := r.Create(ctx, service); err != nil {
		return fmt.Errorf("create headless service: %w", err)
	}
	return nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *MongoDBReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if r.KubeClient == nil {
		kube, err := kubeclient.NewForConfig(mgr.GetConfig())
		if err != nil {
			return fmt.Errorf("create Kubernetes client: %w", err)
		}
		r.KubeClient = kube
	}
	if r.ServiceAccountNamespace == "" {
		r.ServiceAccountNamespace = os.Getenv("POD_NAMESPACE")
	}
	if r.ServiceAccountName == "" {
		r.ServiceAccountName = os.Getenv("SERVICE_ACCOUNT_NAME")
	}
	if r.ServiceAccountNamespace == "" || r.ServiceAccountName == "" {
		return fmt.Errorf("POD_NAMESPACE and SERVICE_ACCOUNT_NAME must be configured")
	}

	return ctrl.NewControllerManagedBy(mgr).
		For(&dbv1beta1.MongoDB{}).
		Named("mongodb").
		Complete(r)
}
