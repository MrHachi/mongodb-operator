package resources

import (
	"fmt"
	"strings"

	api "github.com/mrhachi/mongodb-operator/api/v1alphav1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Factory that creates templates to be used by the legacy v1alphav1 singletenantmongodb API
type Legacy struct {
	*api.SingleTenantMongoDB
}

func NewLegacy(desired *api.SingleTenantMongoDB) *Legacy {
	return &Legacy{desired}
}

func (r *Legacy) MakeDesiredSts() *appsv1.StatefulSet {
	secvolName := fmt.Sprintf("%s-kf", r.Name)
	secvolMountPath := "/etc/kf"
	var secvolDefMode int32 = 0400

	labels := map[string]string{
		"app.kubernetes.io/name":      r.Name,
		"app.kubernetes.io/component": "database",
	}

	return &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      r.Name,
			Namespace: r.Namespace,
		},
		Spec: appsv1.StatefulSetSpec{
			Replicas:    &r.Spec.Replicas,
			ServiceName: r.Name,
			Selector: &metav1.LabelSelector{
				MatchLabels: labels,
			},
			VolumeClaimTemplates: []corev1.PersistentVolumeClaim{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: r.Name,
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
				},
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: labels,
				},
				Spec: corev1.PodSpec{
					Volumes: []corev1.Volume{
						{
							Name: "keyfile",
							VolumeSource: corev1.VolumeSource{
								Secret: &corev1.SecretVolumeSource{
									SecretName:  secvolName,
									DefaultMode: &secvolDefMode,
								},
							},
						},
					},
					Containers: []corev1.Container{
						{
							Name:            r.Name,
							Image:           r.Spec.ImageTag,
							ImagePullPolicy: r.Spec.ImagePullPolicy,
							ReadinessProbe: &corev1.Probe{
								ProbeHandler: corev1.ProbeHandler{
									Exec: &corev1.ExecAction{
										Command: []string{
											"mongosh",
											"--quiet",
											"--eval",
											"db.adminCommand('ping').ok",
										},
									},
								},
								InitialDelaySeconds: int32(5),
								FailureThreshold:    int32(5),
								PeriodSeconds:       int32(15),
							},
							VolumeMounts: []corev1.VolumeMount{
								{
									Name:      r.Name,
									MountPath: "/data/db",
								},
								{
									Name:      "keyfile",
									ReadOnly:  true,
									MountPath: secvolMountPath,
								},
							},
							Env: []corev1.EnvVar{
								// Kubernetes already injects this
								// {Name: "HOSTNAME", Value: NAH},
								{Name: "SVC_NAME", Value: r.Name},
								{Name: "NAMESPACE", Value: r.Namespace},
								{Name: "MONGO_ADMIN_USERNAME", Value: r.Spec.Admin.Username},
								{
									Name: "MONGO_ADMIN_PASSWORD",
									ValueFrom: &corev1.EnvVarSource{
										SecretKeyRef: &corev1.SecretKeySelector{
											LocalObjectReference: r.Spec.Admin.SecretRef,
											Key:                  "password",
										},
									},
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
		},
	}
}

func (r *Legacy) MakeDesiredCm() *corev1.ConfigMap {
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
		},
		Data: map[string]string{
			"host":    hoststr.String(),
			"db_name": r.Spec.DatabaseName,
		},
	}
}

func (r *Legacy) MakeDesiredSvc() *corev1.Service {
	labels := map[string]string{
		"app.kubernetes.io/name": r.Name,
	}

	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      r.Name,
			Namespace: r.Namespace,
		},
		Spec: corev1.ServiceSpec{
			ClusterIP: corev1.ClusterIPNone, // Headless Service
			Selector:  labels,
			Ports: []corev1.ServicePort{
				{
					Port: 27017,
				},
			},
		},
	}
}

func (r *Legacy) MakeDesiredKeyfileSecret(data map[string]string) *corev1.Secret {
	secretName := fmt.Sprintf("%s-kf", r.Name)

	byteData := make(map[string][]byte, len(data))
	for k, v := range data {
		byteData[k] = []byte(v)
	}
	return &corev1.Secret{
		Type: corev1.SecretTypeOpaque,
		ObjectMeta: metav1.ObjectMeta{
			Name:      secretName,
			Namespace: r.Namespace,
		},
		Data: byteData,
	}
}
