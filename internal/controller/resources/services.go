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
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	dbv1beta1 "github.com/MrHachi/mongodb-operator/api/v1beta1"
	"github.com/MrHachi/mongodb-operator/internal/controller/reconciliation"
)

func (e *KubernetesResources) EnsureHeadlessService(ctx context.Context, mongodb *dbv1beta1.MongoDB) error {
	log := logf.FromContext(ctx)
	serviceName := mongodb.Name
	log.Info("Creating headless service", "name", serviceName)
	service := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      serviceName,
			Namespace: mongodb.Namespace,
			Labels:    Labels(mongodb),
		},
		Spec: corev1.ServiceSpec{
			ClusterIP:                corev1.ClusterIPNone,
			PublishNotReadyAddresses: true,
			Selector: Labels(mongodb,
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

	if err := ctrl.SetControllerReference(mongodb, service, e.scheme); err != nil {
		return fmt.Errorf("set owner reference on headless service: %w", err)
	}
	existing := &corev1.Service{}
	if err := e.client.Get(ctx, client.ObjectKeyFromObject(service), existing); err == nil {
		if existing.Spec.ClusterIP != corev1.ClusterIPNone {
			return &reconciliation.BlockingError{
				Reason:  dbv1beta1.ReasonServiceNotHeadless,
				Affects: []string{dbv1beta1.ConditionReady, dbv1beta1.ConditionProgressing},
				Message: fmt.Sprintf("Service %s/%s has clusterIP %q; refusing unsafe in-place conversion to headless", existing.Namespace, existing.Name, existing.Spec.ClusterIP),
			}
		}
	} else if !apierrors.IsNotFound(err) {
		return fmt.Errorf("get existing service before headless reconciliation: %w", err)
	}
	return ensureResource(ctx, e.client, service, func(existing, desired *corev1.Service) error {
		if existing.Spec.ClusterIP != corev1.ClusterIPNone {
			return &reconciliation.BlockingError{
				Reason:  dbv1beta1.ReasonServiceNotHeadless,
				Affects: []string{dbv1beta1.ConditionReady, dbv1beta1.ConditionProgressing},
				Message: fmt.Sprintf("Service %s/%s has clusterIP %q; refusing unsafe in-place conversion to headless", existing.Namespace, existing.Name, existing.Spec.ClusterIP),
			}
		}
		if err := e.reconcileOwnedMetadata(existing, desired); err != nil {
			return err
		}
		existing.Spec.Selector = desired.Spec.Selector
		existing.Spec.PublishNotReadyAddresses = desired.Spec.PublishNotReadyAddresses
		return nil
	})
}
