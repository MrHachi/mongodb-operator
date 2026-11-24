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

package resources

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	dbv1beta1 "github.com/MrHachi/mongodb-operator/api/v1beta1"
	"github.com/MrHachi/mongodb-operator/internal/controller/reconciliation"
)

func (e *KubernetesResources) EnsureReplicaPod(ctx context.Context, mongodb *dbv1beta1.MongoDB, id string) (*corev1.Pod, error) {
	log := logf.FromContext(ctx)
	podName := fmt.Sprintf("%s-r-%s", mongodb.Name, id)
	instanceManagerSecretMode := int32(0o440)

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      podName,
			Namespace: mongodb.Namespace,
			Annotations: map[string]string{
				ProvisioningPodAnnotation: "true",
			},
			Labels: Labels(mongodb,
				"db.mrhachi.dev/role", "replica",
				"db.mrhachi.dev/member", fmt.Sprintf("r-%s", id),
			),
		},
		Spec: corev1.PodSpec{
			Hostname:      podName,
			Subdomain:     mongodb.Name,
			RestartPolicy: corev1.RestartPolicyAlways,
			SecurityContext: &corev1.PodSecurityContext{
				RunAsNonRoot: new(true),
				RunAsUser:    new(int64(999)),
				RunAsGroup:   new(int64(999)),
				FSGroup:      new(int64(999)),
			},
			InitContainers: []corev1.Container{
				{
					Name:  "setup-keyfile",
					Image: "mongo:latest",
					Command: []string{
						"sh",
						"-c",
						"cp /etc/secret/mongodb.key /etc/keyfile/mongodb.key && chown 999:999 /etc/keyfile/mongodb.key && chmod 0600 /etc/keyfile/mongodb.key",
					},
					VolumeMounts: []corev1.VolumeMount{
						{
							Name:      keyfileVolumeName + "-secret",
							MountPath: "/etc/secret",
						},
						{
							Name:      keyfileVolumeName,
							MountPath: "/etc/keyfile",
						},
					},
				},
			},
			Containers: []corev1.Container{
				{
					Name:  "instance-manager",
					Image: "ghcr.io/mrhachi/mongodb-instance-manager:v0.1",
					Env: []corev1.EnvVar{
						{Name: "MONGODB_RS_NAME", Value: mongodb.Name},
						{Name: "MONGODB_HOSTNAME", Value: podName},
						{Name: "MONGODB_SERVICE_NAME", Value: mongodb.Name},
						{Name: "MONGODB_NAMESPACE", Value: mongodb.Namespace},
						{Name: "OPERATOR_SERVICE_ACCOUNT_NAMESPACE", Value: e.config.ServiceAccountNamespace},
						{Name: "OPERATOR_SERVICE_ACCOUNT_NAME", Value: e.config.ServiceAccountName},
						{Name: "INSTANCE_MANAGER_PASSWORD_FILE", Value: instanceManagerSecretMount + "/" + instanceManagerPasswordKey},
					},
					Ports: []corev1.ContainerPort{
						{
							Name:          "http",
							ContainerPort: 8080,
						},
					},
					LivenessProbe: &corev1.Probe{
						ProbeHandler:  corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{Path: "/livez", Port: intstr.FromString("http"), Scheme: corev1.URISchemeHTTP}},
						PeriodSeconds: 10, TimeoutSeconds: 2,
						FailureThreshold: 3, SuccessThreshold: 1,
					},
					ReadinessProbe: &corev1.Probe{
						ProbeHandler:  corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{Path: "/readyz", Port: intstr.FromString("http"), Scheme: corev1.URISchemeHTTP}},
						PeriodSeconds: 5, TimeoutSeconds: 2,
						FailureThreshold: 3, SuccessThreshold: 1,
					},
					VolumeMounts: []corev1.VolumeMount{
						{
							Name:      instanceManagerSecretVolume + "-secret",
							MountPath: instanceManagerSecretMount,
							ReadOnly:  true,
						},
					},
				},
				{
					Name:  "mongodb",
					Image: "mongo:latest",
					Args: []string{
						"--replSet", mongodb.Name,
						"--clusterAuthMode", "keyFile", "--keyFile", "/etc/keyfile/mongodb.key",
						"--bind_ip_all",
					},
					StartupProbe: &corev1.Probe{
						ProbeHandler:  corev1.ProbeHandler{TCPSocket: &corev1.TCPSocketAction{Port: intstr.FromInt(27017)}},
						PeriodSeconds: 10, TimeoutSeconds: 2,
						FailureThreshold: 30,
					},
					LivenessProbe: &corev1.Probe{
						ProbeHandler:  corev1.ProbeHandler{TCPSocket: &corev1.TCPSocketAction{Port: intstr.FromInt(27017)}},
						PeriodSeconds: 10, TimeoutSeconds: 2,
						FailureThreshold: 3,
					},
					VolumeMounts: []corev1.VolumeMount{
						{
							Name:      "data",
							MountPath: "/data/db",
						},
						{
							Name:      keyfileVolumeName,
							MountPath: "/etc/keyfile",
						},
					},
				},
			},
			Volumes: []corev1.Volume{
				{
					Name: instanceManagerSecretVolume + "-secret",
					VolumeSource: corev1.VolumeSource{
						Secret: &corev1.SecretVolumeSource{
							SecretName:  fmt.Sprintf("%s-instance-manager-credentials", mongodb.Name),
							Items:       []corev1.KeyToPath{{Key: instanceManagerPasswordKey, Path: instanceManagerPasswordKey}},
							DefaultMode: &instanceManagerSecretMode,
						},
					},
				},
				{
					Name: keyfileVolumeName + "-secret",
					VolumeSource: corev1.VolumeSource{
						Secret: &corev1.SecretVolumeSource{
							SecretName: fmt.Sprintf("%s-keyfile", mongodb.Name),
							Items:      []corev1.KeyToPath{{Key: keyfileDataKey, Path: "mongodb.key"}},
						},
					},
				},
				{
					Name: keyfileVolumeName,
					VolumeSource: corev1.VolumeSource{
						EmptyDir: &corev1.EmptyDirVolumeSource{},
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
	if err := ctrl.SetControllerReference(mongodb, pod, e.scheme); err != nil {
		return nil, fmt.Errorf("set owner reference on replica pod: %w", err)
	}

	existing := &corev1.Pod{}
	key := client.ObjectKeyFromObject(pod)
	if err := e.client.Get(ctx, key, existing); err == nil {
		needsReplacement := len(existing.Spec.InitContainers) == 0 ||
			!equality.Semantic.DeepEqual(existing.Spec.InitContainers[0].Command, pod.Spec.InitContainers[0].Command)
		if !needsReplacement {
			needsReplacement = true
			for _, volume := range existing.Spec.Volumes {
				if volume.Name == keyfileVolumeName+"-secret" && volume.Secret != nil && len(volume.Secret.Items) == 1 &&
					volume.Secret.Items[0].Key == keyfileDataKey && volume.Secret.Items[0].Path == "mongodb.key" {
					needsReplacement = false
					break
				}
			}
		}
		if needsReplacement {
			// TODO: Define a safe keyfile rotation strategy before deleting a live member.
			// See docs/adr/003-safe-keyfile-rotation.md for the open questions around
			// primary protection, replication health, mixed-key operation, and replacement retries.
			log.Info("Replica Pod replacement is blocked pending a safe keyfile rotation procedure", "name", podName)
			return nil, &reconciliation.BlockingError{
				Reason:  dbv1beta1.ReasonKeyfileRotationBlocked,
				Affects: []string{dbv1beta1.ConditionProgressing},
				Message: fmt.Sprintf("replica Pod %s needs replacement to update keyfile setup, but no safe replacement procedure is defined; resolve the keyfile setup manually", podName),
			}
		}
	} else if !apierrors.IsNotFound(err) {
		return nil, fmt.Errorf("get replica pod: %w", err)
	}

	if err := ensureResource(ctx, e.client, pod, func(existing, desired *corev1.Pod) error {
		return e.reconcileOwnedMetadata(existing, desired)
	}); err != nil {
		return nil, fmt.Errorf("ensure pod: %w", err)
	}

	pod = &corev1.Pod{}
	if err := e.client.Get(ctx, types.NamespacedName{Name: podName, Namespace: mongodb.Namespace}, pod); err != nil {
		return nil, fmt.Errorf("get replica pod: %w", err)
	}

	return pod, nil
}
