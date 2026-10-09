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
	"fmt"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	dbv1beta1 "github.com/MrHachi/mongodb-operator/api/v1beta1"
)

// waitForReconciliation records a condition that may resolve on a later
// observation, then requeues after the requested delay.
func (r *MongoDBReconciler) waitForReconciliation(ctx context.Context, mongodb *dbv1beta1.MongoDB, reason, message string, requeueAfter time.Duration) (ctrl.Result, error) {
	if err := r.updateStatus(ctx, mongodb, mongodb.Status.Phase, []metav1.Condition{
		{Type: dbv1beta1.ConditionReady, Status: metav1.ConditionFalse, Reason: reason, Message: message},
		{Type: dbv1beta1.ConditionProgressing, Status: metav1.ConditionTrue, Reason: reason, Message: message},
	}); err != nil {
		return ctrl.Result{}, fmt.Errorf("record reconciliation wait status: %w", err)
	}
	logf.FromContext(ctx).Info("Waiting to continue reconciliation", "name", mongodb.Name, "reason", reason, "message", message)
	return ctrl.Result{RequeueAfter: requeueAfter}, nil
}
