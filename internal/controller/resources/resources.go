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

	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	kubeclient "k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/client"

	dbv1beta1 "github.com/MrHachi/mongodb-operator/api/v1beta1"
	controllerconfig "github.com/MrHachi/mongodb-operator/internal/controller/config"
	"github.com/MrHachi/mongodb-operator/internal/controller/reconciliation"
)

// KubernetesResources creates MongoDB's Kubernetes resources and reconciles their owned fields.
type KubernetesResources struct {
	client     client.Client
	scheme     *runtime.Scheme
	kubeClient kubeclient.Interface
	config     *controllerconfig.Config
}

func NewKubernetesResources(c client.Client, scheme *runtime.Scheme, kubeClient kubeclient.Interface, config *controllerconfig.Config) *KubernetesResources {
	return &KubernetesResources{client: c, scheme: scheme, kubeClient: kubeClient, config: config}
}

func Labels(mongodb *dbv1beta1.MongoDB, custom ...string) map[string]string {
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

	// Check ownership
	expectedOwner, actualOwner := metav1.GetControllerOf(desired), metav1.GetControllerOf(existing)
	if expectedOwner == nil {
		return fmt.Errorf("check ownership of %T %s: desired object has no controller owner reference", existing, key)
	}
	if actualOwner == nil {
		return &reconciliation.BlockingError{
			Reason:  dbv1beta1.ReasonResourceOwnershipConflict,
			Affects: []string{dbv1beta1.ConditionReady, dbv1beta1.ConditionProgressing},
			Message: fmt.Sprintf("existing %T %s has no controller owner reference; refusing adoption", existing, key),
		}
	}
	// Fail if we can't determine that we own the resource
	if actualOwner.APIVersion != expectedOwner.APIVersion ||
		actualOwner.Kind != expectedOwner.Kind ||
		actualOwner.Name != expectedOwner.Name ||
		actualOwner.UID != expectedOwner.UID {
		return &reconciliation.BlockingError{
			Reason:  dbv1beta1.ReasonResourceOwnershipConflict,
			Affects: []string{dbv1beta1.ConditionReady, dbv1beta1.ConditionProgressing},
			Message: fmt.Sprintf(
				"existing %T %s is controlled by %s %q, expected %s %q; refusing to reconcile it",
				existing, key, actualOwner.Kind, actualOwner.Name, expectedOwner.Kind, expectedOwner.Name,
			),
		}
	}

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

func (e *KubernetesResources) reconcileOwnedMetadata(existing, desired client.Object) error {
	// This is currently reflected in memory only; we apply this via the Kubernetes API later when the reconciler calls client.Patch() as a part of ensureResource()
	existing.SetLabels(desired.GetLabels())

	return nil
}
