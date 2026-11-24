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
	"errors"
	"fmt"
	"slices"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	dbv1beta1 "github.com/MrHachi/mongodb-operator/api/v1beta1"
	"github.com/MrHachi/mongodb-operator/internal/controller/config"
	"github.com/MrHachi/mongodb-operator/internal/controller/reconciliation"
	"github.com/MrHachi/mongodb-operator/internal/controller/resources"
)

func TestResourceBlockingErrors(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := dbv1beta1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	mongodb := &dbv1beta1.MongoDB{ObjectMeta: metav1.ObjectMeta{Name: "cluster", Namespace: "default"}}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "cluster-r-a", Namespace: "default"}}
	client := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pod).Build()
	kubeResources := resources.NewKubernetesResources(client, scheme, nil, &config.Config{})
	ctx := context.Background()

	t.Run("missing admin credentials", func(t *testing.T) {
		_, err := kubeResources.LoadAdminCredentialsSecret(ctx, mongodb)
		assertBlockingError(t, err, dbv1beta1.ReasonCredentialsMissing, dbv1beta1.ConditionReady, dbv1beta1.ConditionProgressing)
	})
	t.Run("unsafe keyfile rotation", func(t *testing.T) {
		_, err := kubeResources.EnsureReplicaPod(ctx, mongodb, "a")
		assertBlockingError(t, err, dbv1beta1.ReasonKeyfileRotationBlocked, dbv1beta1.ConditionProgressing)
	})
}

func assertBlockingError(t *testing.T, err error, reason string, affects ...string) {
	t.Helper()
	if err == nil {
		t.Fatal("expected a resource error")
	}
	blocked, ok := errors.AsType[*reconciliation.BlockingError](fmt.Errorf("ensure resource: %w", err))
	if !ok || blocked.Reason != reason || blocked.Message == "" || !slices.Equal(blocked.Affects, affects) {
		t.Fatalf("got %v, want blocking reason %q and affected conditions %v", err, reason, affects)
	}
}
