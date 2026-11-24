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
	"crypto/sha256"
	"encoding/hex"
	"fmt"

	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	dbv1beta1 "github.com/MrHachi/mongodb-operator/api/v1beta1"
)

func instanceManagerBindingName(mongodb *dbv1beta1.MongoDB) string {
	sum := sha256.Sum256([]byte(mongodb.Namespace + "/" + mongodb.Name))
	return "mongodb-instance-manager-" + hex.EncodeToString(sum[:12])
}

func (e *KubernetesResources) EnsureInstanceManagerBinding(ctx context.Context, mongodb *dbv1beta1.MongoDB) error {
	if e.kubeClient == nil {
		return fmt.Errorf("kubernetes client is not configured")
	}
	if e.config == nil || e.config.InstanceManagerRoleName == "" {
		return fmt.Errorf("instance-manager auth ClusterRole name is not configured")
	}

	bindings := e.kubeClient.RbacV1().ClusterRoleBindings()
	name := instanceManagerBindingName(mongodb)
	desired := &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
			Labels: map[string]string{
				"app.kubernetes.io/managed-by": "mongodb-operator",
				"db.mrhachi.dev/namespace":     mongodb.Namespace,
			},
		},
		RoleRef:  rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "ClusterRole", Name: e.config.InstanceManagerRoleName},
		Subjects: []rbacv1.Subject{{Kind: "ServiceAccount", Name: "default", Namespace: mongodb.Namespace}},
	}
	existing, err := bindings.Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		if _, err := bindings.Create(ctx, desired, metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
			return fmt.Errorf("create instance-manager ClusterRoleBinding: %w", err)
		}
		return nil
	}
	if err != nil {
		return fmt.Errorf("get instance-manager ClusterRoleBinding: %w", err)
	}
	if existing.RoleRef != desired.RoleRef {
		return fmt.Errorf("instance-manager ClusterRoleBinding %q references unexpected role %q", name, existing.RoleRef.Name)
	}
	if !equality.Semantic.DeepEqual(existing.Subjects, desired.Subjects) {
		existing.Subjects = desired.Subjects
		if _, err := bindings.Update(ctx, existing, metav1.UpdateOptions{}); err != nil {
			return fmt.Errorf("update instance-manager ClusterRoleBinding subjects: %w", err)
		}
	}
	return nil
}

func (e *KubernetesResources) DeleteInstanceManagerBinding(ctx context.Context, mongodb *dbv1beta1.MongoDB) error {
	if e.kubeClient == nil {
		return fmt.Errorf("kubernetes client is not configured")
	}
	if err := e.kubeClient.RbacV1().ClusterRoleBindings().Delete(ctx, instanceManagerBindingName(mongodb), metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("delete instance-manager ClusterRoleBinding: %w", err)
	}
	return nil
}
