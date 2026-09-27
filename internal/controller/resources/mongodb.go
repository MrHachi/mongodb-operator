package resources

import (
	"fmt"
	"maps"
	"os"
	"strings"

	api "github.com/mrhachi/mongodb-operator/api/v1alphav2"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

const (
	instanceManagerRepository      = "ghcr.io/mrhachi/mongodb-instance-manager"
	instanceManagerVersion         = "v0.1"
	managerAuthClusterRoleNameBase = "instance-manager-auth-role"
)

func managerAuthClusterRoleName() string {
	prefix := os.Getenv("CONTROLLER_NAME")
	if prefix == "" {
		return managerAuthClusterRoleNameBase
	}
	return fmt.Sprintf("%s-%s", prefix, managerAuthClusterRoleNameBase)
}

type MongoDB struct {
	*api.MongoDB
}

func NewMongoDB(desired *api.MongoDB) *MongoDB {
	return &MongoDB{desired}
}

func (r *MongoDB) labels(custom map[string]string) map[string]string {
	labels := map[string]string{
		"app.kubernetes.io/name": r.Name,
		"db.mrhachi.dev/mongodb": r.Name,
	}
	maps.Copy(labels, custom)
	return labels
}

func (r *MongoDB) DesiredReplicaRBAC() (*corev1.ServiceAccount, *rbacv1.ClusterRoleBinding) {
	saName := fmt.Sprintf("%s-sa", r.Name)
	crbName := fmt.Sprintf("%s-%s-crb", saName, r.Namespace)
	saLabels := r.labels(map[string]string{
		"db.mrhachi.dev/role": "replica",
	})

	return &corev1.ServiceAccount{
			ObjectMeta: metav1.ObjectMeta{
				Name:      saName,
				Namespace: r.Namespace,
				Labels:    saLabels,
			},
		}, &rbacv1.ClusterRoleBinding{
			ObjectMeta: metav1.ObjectMeta{
				Name:   crbName,
				Labels: r.labels(nil),
			},
			Subjects: []rbacv1.Subject{
				{
					Kind:      "ServiceAccount",
					Name:      saName,
					Namespace: r.Namespace,
				},
			},
			RoleRef: rbacv1.RoleRef{
				Kind:     "ClusterRole",
				Name:     managerAuthClusterRoleName(),
				APIGroup: "rbac.authorization.k8s.io",
			},
		}
}

func (r *MongoDB) DesiredReplicaPod(suffix, keyfileSecretName, saName string) (*corev1.Pod, *corev1.PersistentVolumeClaim) {
	podName := fmt.Sprintf("%s-r-%s", r.Name, suffix)
	svcName := r.Name
	rsName := r.Name

	podLabels := r.labels(map[string]string{
		"db.mrhachi.dev/role":   "replica",
		"db.mrhachi.dev/member": "r-" + suffix,
	})
	pvcLabels := r.labels(map[string]string{
		"db.mrhachi.dev/member": "r-" + suffix,
		"db.mrhachi.dev/status": "in-use", // "jettisoned" after delete
	})
	// set after delete:
	// pvcAnnotations := map[string]string{
	// 	"db.mrhachi.dev/jettisoned-at": timestamp,
	// }

	return &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      podName,
				Namespace: r.Namespace,
				Labels:    podLabels,
			},
			Spec: corev1.PodSpec{
				// STS adds these guys automatically; they're needed for headless service DNS resolution
				Hostname: podName, Subdomain: svcName,

				ServiceAccountName: saName,
				SecurityContext: &corev1.PodSecurityContext{
					RunAsNonRoot: new(true),
					RunAsUser:    new(int64(999)),
					RunAsGroup:   new(int64(999)),
					FSGroup:      new(int64(999)),
				},
				InitContainers: []corev1.Container{
					{
						Name:  "copy-keyfile",
						Image: r.Spec.Image.Tag,
						Command: []string{
							"sh",
							"-c",
							"cp /etc/kf-secret/keyfile /etc/kf/keyfile && chmod 0400 /etc/kf/keyfile && chown 999:999 /etc/kf/keyfile",
						},
						VolumeMounts: []corev1.VolumeMount{
							{
								Name:      "keyfile-secret",
								ReadOnly:  true,
								MountPath: "/etc/kf-secret",
							},
							{
								Name:      "keyfile",
								MountPath: "/etc/kf",
							},
						},
					},
				},
				Volumes: []corev1.Volume{
					{
						Name: r.Name,
						VolumeSource: corev1.VolumeSource{
							PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
								ClaimName: podName,
								ReadOnly:  false,
							},
						},
					},
					{
						Name: "keyfile-secret",
						VolumeSource: corev1.VolumeSource{
							Secret: &corev1.SecretVolumeSource{
								SecretName:  keyfileSecretName,
								DefaultMode: new(int32(0400)),
							},
						},
					},
					{
						Name: "keyfile",
						VolumeSource: corev1.VolumeSource{
							EmptyDir: &corev1.EmptyDirVolumeSource{},
						},
					},
				},
				Containers: []corev1.Container{
					{
						Name:  "instance-manager",
						Image: fmt.Sprintf("%s:%s", instanceManagerRepository, instanceManagerVersion),
						LivenessProbe: &corev1.Probe{
							ProbeHandler: corev1.ProbeHandler{
								HTTPGet: &corev1.HTTPGetAction{
									Path: "/livez",
									Port: intstr.FromInt(8080),
								},
							},
							InitialDelaySeconds: int32(5),
							TimeoutSeconds:      int32(5),
							FailureThreshold:    int32(5),
							PeriodSeconds:       int32(15),
						},
						ReadinessProbe: &corev1.Probe{
							ProbeHandler: corev1.ProbeHandler{
								HTTPGet: &corev1.HTTPGetAction{
									Path: "/readyz",
									Port: intstr.FromInt(8080),
								},
							},
							InitialDelaySeconds: int32(5),
							TimeoutSeconds:      int32(5),
							FailureThreshold:    int32(5),
							PeriodSeconds:       int32(15),
						},
						Env: []corev1.EnvVar{
							{
								Name: "NAMESPACE",
								ValueFrom: &corev1.EnvVarSource{
									FieldRef: &corev1.ObjectFieldSelector{
										FieldPath: "metadata.namespace",
									},
								},
							},
							{
								Name:  "SERVICE_NAME",
								Value: svcName,
							},
							{
								Name:  "RS_NAME",
								Value: rsName,
							},
						},
					},
					{
						Name:            "mongo",
						Image:           r.Spec.Image.Tag,
						ImagePullPolicy: r.Spec.Image.PullPolicy,
						LivenessProbe: &corev1.Probe{
							ProbeHandler: corev1.ProbeHandler{
								TCPSocket: &corev1.TCPSocketAction{
									Port: intstr.FromInt(27017),
								},
							},
							InitialDelaySeconds: int32(15),
						},
						// Delegate startup status check to instance-manager
						StartupProbe: &corev1.Probe{
							ProbeHandler: corev1.ProbeHandler{
								HTTPGet: &corev1.HTTPGetAction{
									Path: "/readyz",
									Port: intstr.FromInt(8080),
								},
							},
							TimeoutSeconds:   int32(5),
							FailureThreshold: int32(30),
							PeriodSeconds:    int32(10),
						},
						// ReadinessProbe replaced by instance-manager /readyz
						Args: []string{
							"--replSet", rsName,
							"--clusterAuthMode", "keyFile",
							"--keyFile", "/etc/kf/keyfile",
							"--bind_ip_all",
						},
						VolumeMounts: []corev1.VolumeMount{
							{
								Name:      r.Name,
								MountPath: "/data/db",
							},
							{
								Name:      "keyfile",
								MountPath: "/etc/kf",
							},
						},
					},
				},
				Resources: &corev1.ResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceCPU:    resource.MustParse(r.Spec.Resources.Requests.Cpu),
						corev1.ResourceMemory: resource.MustParse(r.Spec.Resources.Requests.Memory),
					},
					Limits: corev1.ResourceList{
						corev1.ResourceCPU:    resource.MustParse(r.Spec.Resources.Limits.Cpu),
						corev1.ResourceMemory: resource.MustParse(r.Spec.Resources.Limits.Memory),
					},
				},
			},
		},
		&corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      podName,
				Namespace: r.Namespace,
				Labels:    pvcLabels,
			},
			Spec: corev1.PersistentVolumeClaimSpec{
				AccessModes: []corev1.PersistentVolumeAccessMode{
					corev1.ReadWriteOnce,
				},
				Resources: corev1.VolumeResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceStorage: resource.MustParse(r.Spec.Storage.Size),
					},
				},
			},
		}
}

func (r *MongoDB) DesiredCm() *corev1.ConfigMap {
	cmName := fmt.Sprintf("%s-connection", r.Name)

	var hoststr strings.Builder
	for ord := range r.Spec.Replicas {
		fmt.Fprintf(
			&hoststr,
			"%s-%d.%s.%s.svc.cluster.local:27017",
			r.Name, ord, r.Name, r.Namespace,
		)
		if ord != r.Spec.Replicas-1 {
			hoststr.WriteString(",")
		}
	}

	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cmName,
			Namespace: r.Namespace,
			Labels:    r.labels(nil),
		},
		Data: map[string]string{
			"host":    hoststr.String(),
			"db_name": r.Spec.DatabaseName,
		},
	}
}

func (r *MongoDB) DesiredSvc() *corev1.Service {
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      r.Name,
			Namespace: r.Namespace,
			Labels:    r.labels(nil),
		},
		Spec: corev1.ServiceSpec{
			ClusterIP:                corev1.ClusterIPNone, // Headless Service
			PublishNotReadyAddresses: true,                 // TODO: set conditionally with init flag (only needed during initialization)
			Selector: r.labels(map[string]string{
				"db.mrhachi.dev/role": "replica", // Target replica pods so we don't route traffic to arbiters
			}),
			Ports: []corev1.ServicePort{
				{
					Port: 27017,
				},
			},
		},
	}
}

func (r *MongoDB) DesiredKeyfileSecret() *corev1.Secret {
	secretName := fmt.Sprintf("%s-kf", r.Name)

	// Fill later to avoid unnecessarily generating secret bytes outside of initialization/rotation.
	// byteData := map[string][]byte{
	// 	"keyfile": []byte(kf),
	// }
	return &corev1.Secret{
		Type: corev1.SecretTypeOpaque,
		ObjectMeta: metav1.ObjectMeta{
			Name:      secretName,
			Namespace: r.Namespace,
			Labels:    r.labels(nil),
		},
		// Data: byteData,
	}
}
