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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	dbv1beta1 "github.com/MrHachi/mongodb-operator/api/v1beta1"
)

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
