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
	"fmt"
	"maps"
	"net"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apiMeta "k8s.io/apimachinery/pkg/api/meta"
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
	controllerconfig "github.com/MrHachi/mongodb-operator/internal/controller/config"
	managerclient "github.com/MrHachi/mongodb-operator/internal/manager/pkg/client"
)

const (
	adminUsernameKey  = "username"
	adminPasswordKey  = "password"
	keyfileVolumeName = "keyfile"
)

// MongoDBReconciler reconciles a MongoDB object
type MongoDBReconciler struct {
	client.Client
	Scheme     *runtime.Scheme
	KubeClient kubeclient.Interface
	Config     *controllerconfig.Config
}

// +kubebuilder:rbac:groups=db.mrhachi.dev,resources=mongodbs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=db.mrhachi.dev,resources=mongodbs/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=db.mrhachi.dev,resources=mongodbs/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch;create;update;patch;delete
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
		if err := r.setInitializingStatus(ctx, mongodb, dbv1beta1.ProgressReasonCreatingResources); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil

	case dbv1beta1.PhaseInitializing:
		log.Info("Initializing MongoDB cluster", "name", mongodb.Name)
		return r.reconcileInitializing(ctx, mongodb)

	case dbv1beta1.PhaseProgressing:
		log.Info("Reconciling MongoDB cluster", "name", mongodb.Name)
		// To be implemented
		if err := r.setProgressingStatus(ctx, mongodb, dbv1beta1.PhaseProgressing, dbv1beta1.ProgressReasonReconcilingMembers); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil

	case dbv1beta1.PhaseReady:
		log.Info("Configuring ready MongoDB cluster", "name", mongodb.Name)
		if err := r.setReadyStatus(ctx, mongodb); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil

	case dbv1beta1.PhaseDegraded:
		log.Info("Recovering degraded MongoDB cluster", "name", mongodb.Name)
		return ctrl.Result{}, nil

	default:
		log.Info("Migrating unknown phase to Progressing", "phase", mongodb.Status.Phase)
		if err := r.setProgressingStatus(ctx, mongodb, dbv1beta1.PhaseProgressing, dbv1beta1.ProgressReasonReconcilingMembers); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}
}

func (r *MongoDBReconciler) setProgressingStatus(ctx context.Context, mongodb *dbv1beta1.MongoDB, phase dbv1beta1.MongoDBPhase, reason dbv1beta1.ProgressReason) error {
	message, ok := dbv1beta1.ProgressReasonMessage(reason)
	if !ok {
		return fmt.Errorf("get message for progress reason %q: unknown reason", reason)
	}
	return r.updateStatus(ctx, mongodb, phase, []metav1.Condition{
		{Type: dbv1beta1.ConditionReady, Status: metav1.ConditionFalse, Reason: dbv1beta1.ReasonProgressing, Message: "MongoDB cluster is not ready"},
		{Type: dbv1beta1.ConditionProgressing, Status: metav1.ConditionTrue, Reason: string(reason), Message: message},
		{Type: dbv1beta1.ConditionDegraded, Status: metav1.ConditionFalse, Reason: dbv1beta1.ReasonNoKnownIssues, Message: "No cluster problems detected"},
	})
}

func (r *MongoDBReconciler) setDegradedStatus(ctx context.Context, mongodb *dbv1beta1.MongoDB, reason, message string) error {
	return r.updateStatus(ctx, mongodb, dbv1beta1.PhaseDegraded, []metav1.Condition{
		{Type: dbv1beta1.ConditionReady, Status: metav1.ConditionFalse, Reason: dbv1beta1.ReasonDegraded, Message: message},
		{Type: dbv1beta1.ConditionProgressing, Status: metav1.ConditionFalse, Reason: dbv1beta1.ReasonNotProgressing, Message: "Reconciliation is blocked by a cluster problem"},
		{Type: dbv1beta1.ConditionDegraded, Status: metav1.ConditionTrue, Reason: reason, Message: message},
	})
}

func (r *MongoDBReconciler) setReadyStatus(ctx context.Context, mongodb *dbv1beta1.MongoDB) error {
	return r.updateStatus(ctx, mongodb, dbv1beta1.PhaseReady, []metav1.Condition{
		{Type: dbv1beta1.ConditionReady, Status: metav1.ConditionTrue, Reason: dbv1beta1.ReasonReady, Message: "MongoDB cluster is ready"},
		{Type: dbv1beta1.ConditionProgressing, Status: metav1.ConditionFalse, Reason: dbv1beta1.ReasonNotProgressing, Message: "No changes are in progress"},
		{Type: dbv1beta1.ConditionDegraded, Status: metav1.ConditionFalse, Reason: dbv1beta1.ReasonNoKnownIssues, Message: "No cluster problems detected"},
	})
}

func (r *MongoDBReconciler) updateStatus(
	ctx context.Context,
	mongodb *dbv1beta1.MongoDB,
	phase dbv1beta1.MongoDBPhase,
	conditions []metav1.Condition,
) error {
	before := mongodb.DeepCopy()
	mongodb.Status.Phase = phase
	for _, condition := range conditions {
		condition.ObservedGeneration = mongodb.Generation
		apiMeta.SetStatusCondition(&mongodb.Status.Conditions, condition)
	}
	if equality.Semantic.DeepEqual(before.Status, mongodb.Status) {
		return nil
	}
	if err := r.Status().Update(ctx, mongodb); err != nil {
		return fmt.Errorf("update MongoDB status: %w", err)
	}
	return nil
}

func (r *MongoDBReconciler) managerClientForPod(pod *corev1.Pod) *managerclient.Client {
	managerURL := "http://" + net.JoinHostPort(pod.Status.PodIP, "8080")
	return managerclient.New(managerURL, r.KubeClient, r.Config.ServiceAccountNamespace, r.Config.ServiceAccountName)
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

// ensureResource creates desired when it is absent, or reconciles the
// operator-owned fields of the existing object and patches only when needed.
// reconcile must preserve fields not owned by this controller.
func ensureResource[T client.Object](ctx context.Context, c client.Client, desired T, reconcile func(existing, desired T) error) error {
	key := client.ObjectKeyFromObject(desired)
	existing := desired.DeepCopyObject().(T)
	if err := c.Get(ctx, key, existing); err != nil {
		if !apierrors.IsNotFound(err) {
			return fmt.Errorf("get %T %s: %w", desired, key, err)
		}
		if err := c.Create(ctx, desired); err != nil {
			return fmt.Errorf("create %T %s: %w", desired, key, err)
		}
		return nil
	}

	before := existing.DeepCopyObject().(T)
	if err := reconcile(existing, desired); err != nil {
		return fmt.Errorf("reconcile %T: %w", existing, err)
	}
	if equality.Semantic.DeepEqual(before, existing) {
		return nil
	}
	if err := c.Patch(ctx, existing, client.MergeFrom(before)); err != nil {
		return fmt.Errorf("patch %T %s: %w", existing, key, err)
	}
	return nil
}

func mergeDesiredLabels(existing, desired map[string]string) map[string]string {
	if existing == nil {
		existing = make(map[string]string)
	}
	maps.Copy(existing, desired)
	return existing
}

func (r *MongoDBReconciler) reconcileOwnedMetadata(existing, desired client.Object, mongodb *dbv1beta1.MongoDB) error {
	// This is currently reflected in memory only; we apply this via the Kubernetes API later when the reconciler calls client.Patch() as a part of ensureResource()
	existing.SetLabels(mergeDesiredLabels(existing.GetLabels(), desired.GetLabels()))

	if err := ctrl.SetControllerReference(mongodb, existing, r.Scheme); err != nil {
		return fmt.Errorf("set controller owner reference on %T: %w", existing, err)
	}
	return nil
}

func (r *MongoDBReconciler) ensureKeyfileSecret(ctx context.Context, mongodb *dbv1beta1.MongoDB) error {
	secretName := fmt.Sprintf("%s-keyfile", mongodb.Name)
	key, err := r.generateKeyfile()
	if err != nil {
		return fmt.Errorf("generate keyfile: %w", err)
	}

	secret := &corev1.Secret{
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
	return ensureResource(ctx, r.Client, secret, func(existing, desired *corev1.Secret) error {
		if err := r.reconcileOwnedMetadata(existing, desired, mongodb); err != nil {
			return err
		}
		if len(existing.Data["mongodb-keyfile"]) == 0 {
			if existing.Data == nil {
				existing.Data = make(map[string][]byte)
			}
			existing.Data["mongodb-keyfile"] = key
		}
		if existing.Type == "" {
			existing.Type = desired.Type
		}
		return nil
	})
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

				// Set to "jettisoned" when instance drained and retained for a period
				"db.mrhachi.dev/status", "in-use",
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

	log.Info("Creating PVC", "name", pvcName)
	if err := ctrl.SetControllerReference(mongodb, pvc, r.Scheme); err != nil {
		return fmt.Errorf("set owner reference on pvc: %w", err)
	}

	return ensureResource(ctx, r.Client, pvc, func(existing, desired *corev1.PersistentVolumeClaim) error {
		return r.reconcileOwnedMetadata(existing, desired, mongodb)
	})
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

	log.Info("Creating replica pod", "name", podName)
	if err := ctrl.SetControllerReference(mongodb, pod, r.Scheme); err != nil {
		return fmt.Errorf("set owner reference on replica pod: %w", err)
	}

	return ensureResource(ctx, r.Client, pod, func(existing, desired *corev1.Pod) error {
		return r.reconcileOwnedMetadata(existing, desired, mongodb)
	})
}

func (r *MongoDBReconciler) ensureHeadlessService(ctx context.Context, mongodb *dbv1beta1.MongoDB) error {
	log := logf.FromContext(ctx)
	serviceName := mongodb.Name
	log.Info("Creating headless service", "name", serviceName)
	service := &corev1.Service{
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
	return ensureResource(ctx, r.Client, service, func(existing, desired *corev1.Service) error {
		if existing.Spec.ClusterIP != corev1.ClusterIPNone {
			return fmt.Errorf("service %s/%s is not headless (clusterIP %q); refusing unsafe in-place conversion", existing.Namespace, existing.Name, existing.Spec.ClusterIP)
		}
		if err := r.reconcileOwnedMetadata(existing, desired, mongodb); err != nil {
			return err
		}
		existing.Spec.Selector = mergeDesiredLabels(existing.Spec.Selector, desired.Spec.Selector)
		existing.Spec.PublishNotReadyAddresses = desired.Spec.PublishNotReadyAddresses
		return nil
	})
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

	return ctrl.NewControllerManagedBy(mgr).
		For(&dbv1beta1.MongoDB{}).
		Named("mongodb").
		Complete(r)
}
