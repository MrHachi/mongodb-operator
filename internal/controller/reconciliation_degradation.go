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
	"time"

	apiMeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"

	dbv1beta1 "github.com/MrHachi/mongodb-operator/api/v1beta1"
)

// recoveryResult describes whether phase reconciliation may
// continue and when degradation recovery should be retried.
type recoveryResult struct {
	Continue     bool
	RequeueAfter time.Duration
}

// withEarlierRequeue schedules the earlier positive delay requested by phase
// reconciliation and degradation recovery.
func withEarlierRequeue(phaseResult ctrl.Result, recoveryAfter time.Duration) ctrl.Result {
	if recoveryAfter <= 0 {
		return phaseResult
	}
	if phaseResult.RequeueAfter == 0 || recoveryAfter < phaseResult.RequeueAfter {
		phaseResult.RequeueAfter = recoveryAfter
	}
	return phaseResult
}

// hasDegradedStatusCondition reports whether an update adds a degradation
// condition with status True or transitions one to True. Ready and Progressing
// are excluded because they describe lifecycle progress and would cause status
// update loops.
func hasDegradedStatusCondition(oldConditions, newConditions []metav1.Condition) bool {
	oldByType := make(map[string]metav1.Condition, len(oldConditions))
	for _, condition := range oldConditions {
		oldByType[condition.Type] = condition
	}

	for _, condition := range newConditions {
		if condition.Type == dbv1beta1.ConditionReady || condition.Type == dbv1beta1.ConditionProgressing || condition.Status != metav1.ConditionTrue {
			continue
		}
		if old, found := oldByType[condition.Type]; !found || old.Status != metav1.ConditionTrue {
			return true
		}
	}
	return false
}

// reconcileDegradation checks the aggregate Degraded condition before phase
// reconciliation. When degradation is reported, it provides the entry point
// for condition-specific recovery; the stub currently allows phase processing
// to continue without attempting recovery.
func (r *MongoDBReconciler) reconcileDegradation(_ context.Context, mongodb *dbv1beta1.MongoDB) (recoveryResult, error) {
	degraded := apiMeta.FindStatusCondition(mongodb.Status.Conditions, dbv1beta1.ConditionDegraded)
	if degraded == nil || degraded.Status != metav1.ConditionTrue {
		return recoveryResult{Continue: true}, nil
	}

	// TODO: Inspect each active instance-manager condition and attempt safe
	// recovery for it. The individual degradation condition types are not yet
	// defined. Degraded is the aggregate trigger and should not be recovered as
	// an individual issue.
	// Planned shape:
	// for _, condition := range mongodb.Status.Conditions {
	//     if condition.Type == dbv1beta1.ConditionDegraded || condition.Status != metav1.ConditionTrue {
	//         continue
	//     }
	//     if err := r.recoverDegradation(ctx, mongodb, condition); err != nil {
	//         return recoveryResult{Continue: false}, err
	//     }
	// }
	return recoveryResult{Continue: true}, nil
}
