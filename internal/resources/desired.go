package resources

import (
	"fmt"
	"strings"

	api "github.com/mrhachi/single-tenant-mongo-db/api/v1alphav1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func MakeDesiredSts(desired *api.SingleTenantMongoDB) *appsv1.StatefulSet {
	secvolName := fmt.Sprintf("%s-kf", desired.Name)
	secvolMountPath := "/etc/kf"
	var secvolDefMode int32 = 0400

	// TODO: rename notes-data to something more generic
	imageTag := "notes-data:latest"

	labels := map[string]string{
		"app.kubernetes.io/name":      desired.Name,
		"app.kubernetes.io/component": "database",
	}

	return &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      desired.Name,
			Namespace: desired.Namespace,
		},
		Spec: appsv1.StatefulSetSpec{
			Replicas:    &desired.Spec.Replicas,
			ServiceName: desired.Name,
			Selector: &metav1.LabelSelector{
				MatchLabels: labels,
			},
			VolumeClaimTemplates: []corev1.PersistentVolumeClaim{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: desired.Name,
					},
					Spec: corev1.PersistentVolumeClaimSpec{
						AccessModes: []corev1.PersistentVolumeAccessMode{
							corev1.ReadWriteOnce,
						},
						Resources: corev1.VolumeResourceRequirements{
							Requests: corev1.ResourceList{
								corev1.ResourceStorage: resource.MustParse(desired.Spec.Storage.Size),
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
							Name:            desired.Name,
							Image:           imageTag,
							ImagePullPolicy: corev1.PullNever,
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
									Name:      desired.Name,
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
								{Name: "SVC_NAME", Value: desired.Name},
								{Name: "NAMESPACE", Value: desired.Namespace},
								{Name: "MONGO_ADMIN_USERNAME", Value: desired.Spec.Admin.Username},
								{
									Name: "MONGO_ADMIN_PASSWORD",
									ValueFrom: &corev1.EnvVarSource{
										SecretKeyRef: &corev1.SecretKeySelector{
											LocalObjectReference: desired.Spec.Admin.SecretRef,
											Key:                  "password",
										},
									},
								},
							},
						},
					},
					Resources: &corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse(desired.Spec.Resources.Requests.Cpu),
							corev1.ResourceMemory: resource.MustParse(desired.Spec.Resources.Requests.Memory),
						},
						Limits: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse(desired.Spec.Resources.Limits.Cpu),
							corev1.ResourceMemory: resource.MustParse(desired.Spec.Resources.Limits.Memory),
						},
					},
				},
			},
		},
	}
}

func MakeDesiredSvc(desired *api.SingleTenantMongoDB) *corev1.Service {
	labels := map[string]string{
		"app.kubernetes.io/name": desired.Name,
	}

	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      desired.Name,
			Namespace: desired.Namespace,
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

func MakeDesiredCm(desired *api.SingleTenantMongoDB) *corev1.ConfigMap {
	cmName := fmt.Sprintf("%s-connection", desired.Name)

	var hoststr strings.Builder
	for ord := range desired.Spec.Replicas {
		fmt.Fprintf(
			&hoststr,
			"%s-%d.%s.%s.svc.cluster.local:27017",
			desired.Name, ord, desired.Name, desired.Namespace,
		)
		if ord != desired.Spec.Replicas-1 {
			hoststr.WriteString(",")
		}
	}

	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cmName,
			Namespace: desired.Namespace,
		},
		Data: map[string]string{
			"host":    hoststr.String(),
			"db_name": desired.Spec.DatabaseName,
		},
	}
}

func MakeDesiredKeyfileSecret(desired *api.SingleTenantMongoDB, data map[string]string) *corev1.Secret {
	secretName := fmt.Sprintf("%s-kf", desired.Name)

	byteData := make(map[string][]byte, len(data))
	for k, v := range data {
		byteData[k] = []byte(v)
	}
	return &corev1.Secret{
		Type: corev1.SecretTypeOpaque,
		ObjectMeta: metav1.ObjectMeta{
			Name:      secretName,
			Namespace: desired.Namespace,
		},
		Data: byteData,
	}
}
