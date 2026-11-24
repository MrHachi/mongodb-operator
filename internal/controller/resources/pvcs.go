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
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	dbv1beta1 "github.com/MrHachi/mongodb-operator/api/v1beta1"
)

func (e *KubernetesResources) EnsurePVC(ctx context.Context, mongodb *dbv1beta1.MongoDB, id string) error {
	log := logf.FromContext(ctx)
	pvcName := fmt.Sprintf("%s-r-%s-data", mongodb.Name, id)
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pvcName,
			Namespace: mongodb.Namespace,
			Labels: Labels(mongodb,
				"db.mrhachi.dev/role", "replica",
				"db.mrhachi.dev/member", fmt.Sprintf("r-%s", id),

				// Set to "jettisoned" when instance drained and retained for a period
				"db.mrhachi.dev/status", PVCStatusInUse,
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
	if err := ctrl.SetControllerReference(mongodb, pvc, e.scheme); err != nil {
		return fmt.Errorf("set owner reference on pvc: %w", err)
	}

	return ensureResource(ctx, e.client, pvc, func(existing, desired *corev1.PersistentVolumeClaim) error {
		return e.reconcileOwnedMetadata(existing, desired)
	})
}
