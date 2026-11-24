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
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"

	dbv1beta1 "github.com/MrHachi/mongodb-operator/api/v1beta1"
	"github.com/MrHachi/mongodb-operator/internal/controller/reconciliation"
)

const (
	keyfileDataKey              = "mongodb-keyfile"
	AdminUsernameKey            = "username"
	AdminPasswordKey            = "password"
	keyfileVolumeName           = "keyfile"
	instanceManagerPasswordKey  = "password"
	instanceManagerPasswordSize = 1024
	instanceManagerSecretVolume = "instance-manager-credentials"
	instanceManagerSecretMount  = "/etc/mongodb-operator/credentials"
)

func (e *KubernetesResources) LoadAdminCredentialsSecret(ctx context.Context, mongodb *dbv1beta1.MongoDB) (*corev1.Secret, error) {
	secretName := fmt.Sprintf("%s-admin-credentials", mongodb.Name)
	secret := &corev1.Secret{}
	if err := e.client.Get(ctx, types.NamespacedName{Name: secretName, Namespace: mongodb.Namespace}, secret); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, &reconciliation.BlockingError{
				Reason:  dbv1beta1.ReasonCredentialsMissing,
				Affects: []string{dbv1beta1.ConditionReady, dbv1beta1.ConditionProgressing},
				Message: fmt.Sprintf("Admin credentials Secret %s/%s is missing; restore the cluster's credentials to resume initialization", mongodb.Namespace, secretName),
			}
		}
		return nil, fmt.Errorf("get admin credentials secret: %w", err)
	}

	if len(secret.Data[AdminUsernameKey]) == 0 || len(secret.Data[AdminPasswordKey]) == 0 {
		return nil, &reconciliation.BlockingError{
			Reason:  dbv1beta1.ReasonCredentialsIncomplete,
			Affects: []string{dbv1beta1.ConditionReady, dbv1beta1.ConditionProgressing},
			Message: fmt.Sprintf("Admin credentials Secret %s/%s must contain non-empty %q and %q entries; restore the cluster's credentials to resume initialization", mongodb.Namespace, secretName, AdminUsernameKey, AdminPasswordKey),
		}
	}
	return secret, nil
}

func (e *KubernetesResources) EnsureAdminCredentialsSecret(ctx context.Context, mongodb *dbv1beta1.MongoDB) (*corev1.Secret, error) {
	secretName := fmt.Sprintf("%s-admin-credentials", mongodb.Name)
	secret := &corev1.Secret{}
	err := e.client.Get(ctx, types.NamespacedName{Name: secretName, Namespace: mongodb.Namespace}, secret)
	if err == nil {
		if len(secret.Data[AdminUsernameKey]) == 0 || len(secret.Data[AdminPasswordKey]) == 0 {
			return nil, &reconciliation.BlockingError{
				Reason:  dbv1beta1.ReasonCredentialsIncomplete,
				Affects: []string{dbv1beta1.ConditionReady, dbv1beta1.ConditionProgressing},
				Message: fmt.Sprintf("Admin credentials Secret %s/%s must contain non-empty %q and %q entries; restore the cluster's credentials to resume initialization", mongodb.Namespace, secretName, AdminUsernameKey, AdminPasswordKey),
			}
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
		ObjectMeta: metav1.ObjectMeta{Name: secretName, Namespace: mongodb.Namespace, Labels: Labels(mongodb)},
		Type:       corev1.SecretTypeOpaque,
		Data:       map[string][]byte{AdminUsernameKey: []byte("admin"), AdminPasswordKey: []byte(hex.EncodeToString(password))},
	}
	if err := ctrl.SetControllerReference(mongodb, secret, e.scheme); err != nil {
		return nil, fmt.Errorf("set owner reference on admin credentials secret: %w", err)
	}
	if err := e.client.Create(ctx, secret); err != nil {
		return nil, fmt.Errorf("create admin credentials secret: %w", err)
	}
	return secret, nil
}

func (e *KubernetesResources) EnsureKeyfileSecret(ctx context.Context, mongodb *dbv1beta1.MongoDB) error {
	secretName := fmt.Sprintf("%s-keyfile", mongodb.Name)
	key, err := e.generateKeyfile()
	if err != nil {
		return fmt.Errorf("generate keyfile: %w", err)
	}

	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      secretName,
			Namespace: mongodb.Namespace,
			Labels:    Labels(mongodb),
		},
		Type: corev1.SecretTypeOpaque,
		Data: map[string][]byte{
			keyfileDataKey: key,
		},
	}

	if err := ctrl.SetControllerReference(mongodb, secret, e.scheme); err != nil {
		return fmt.Errorf("set owner reference on keyfile secret: %w", err)
	}
	return ensureResource(ctx, e.client, secret, func(existing, desired *corev1.Secret) error {
		if err := e.reconcileOwnedMetadata(existing, desired); err != nil {
			return err
		}
		if !validKeyfile(existing.Data[keyfileDataKey]) {
			if existing.Data == nil {
				existing.Data = make(map[string][]byte)
			}
			existing.Data[keyfileDataKey] = key
		}
		if existing.Type == "" {
			existing.Type = desired.Type
		}
		return nil
	})
}

func (e *KubernetesResources) EnsureInstanceManagerCredentialsSecret(ctx context.Context, mongodb *dbv1beta1.MongoDB) error {
	secretName := fmt.Sprintf("%s-instance-manager-credentials", mongodb.Name)

	existing := &corev1.Secret{}
	var password []byte
	if err := e.client.Get(ctx, types.NamespacedName{Name: secretName, Namespace: mongodb.Namespace}, existing); err == nil {
		password = existing.Data[instanceManagerPasswordKey]
		if !validInstanceManagerPassword(password) {
			return &reconciliation.BlockingError{
				Reason:  dbv1beta1.ReasonCredentialsIncomplete,
				Affects: []string{dbv1beta1.ConditionReady, dbv1beta1.ConditionProgressing},
				Message: fmt.Sprintf("Instance-manager credentials Secret %s/%s must contain a %d-character URL-safe password; restore the credential to resume reconciliation", mongodb.Namespace, secretName, instanceManagerPasswordSize),
			}
		}
	} else if !apierrors.IsNotFound(err) {
		return fmt.Errorf("get instance-manager credentials Secret: %w", err)
	} else {
		randomBytes := make([]byte, 768)
		if _, err := rand.Read(randomBytes); err != nil {
			return fmt.Errorf("generate instance-manager password: %w", err)
		}
		password = []byte(base64.RawURLEncoding.EncodeToString(randomBytes))
	}

	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      secretName,
			Namespace: mongodb.Namespace,
			Labels:    Labels(mongodb, "app.kubernetes.io/component", "instance-manager"),
		},
		Type: corev1.SecretTypeOpaque,
		Data: map[string][]byte{
			instanceManagerPasswordKey: password,
		},
	}
	if err := ctrl.SetControllerReference(mongodb, secret, e.scheme); err != nil {
		return fmt.Errorf("set owner reference on instance-manager credentials Secret: %w", err)
	}
	return ensureResource(ctx, e.client, secret, func(existing, desired *corev1.Secret) error {
		if err := e.reconcileOwnedMetadata(existing, desired); err != nil {
			return err
		}
		existing.Type = desired.Type

		if !validInstanceManagerPassword(existing.Data[instanceManagerPasswordKey]) {
			return &reconciliation.BlockingError{
				Reason:  dbv1beta1.ReasonCredentialsIncomplete,
				Affects: []string{dbv1beta1.ConditionReady, dbv1beta1.ConditionProgressing},
				Message: fmt.Sprintf("Instance-manager credentials Secret %s/%s must contain a %d-character URL-safe password; restore the credential to resume reconciliation", mongodb.Namespace, secretName, instanceManagerPasswordSize),
			}
		}
		return nil
	})
}

func validInstanceManagerPassword(password []byte) bool {
	if len(password) != instanceManagerPasswordSize {
		return false
	}
	decoded, err := base64.RawURLEncoding.DecodeString(string(password))
	return err == nil && len(decoded) == 768
}

func (e *KubernetesResources) generateKeyfile() ([]byte, error) {
	// MongoDB keyfiles accept base64 characters and must be no longer than
	// 1024 characters. 756 random bytes encode to 1008 base64 characters.
	key := make([]byte, 756)
	if _, err := rand.Read(key); err != nil {
		return nil, err
	}
	encoded := make([]byte, base64.StdEncoding.EncodedLen(len(key)))
	base64.StdEncoding.Encode(encoded, key)
	return encoded, nil
}

func validKeyfile(key []byte) bool {
	if len(key) < 6 || len(key) > 1024 {
		return false
	}
	_, err := base64.StdEncoding.DecodeString(string(key))
	return err == nil
}
