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

package reconciliation

import (
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	dbv1beta1 "github.com/MrHachi/mongodb-operator/api/v1beta1"
)

// BlockingError reports a persistent condition that prevents safe
// reconciliation.
type BlockingError struct {
	Message string
	Reason  string
	// Affects lists operator-owned conditions to set to False.
	Affects []string
}

func (e *BlockingError) Error() string {
	if e == nil {
		return "<nil>"
	}
	return e.Message
}

// Conditions builds the blocking error's status conditions.
func (e *BlockingError) Conditions() ([]metav1.Condition, error) {
	if e.Reason == "" {
		return nil, fmt.Errorf("reconciliation error has empty reason")
	}
	if e.Message == "" {
		return nil, fmt.Errorf("reconciliation error has empty message")
	}
	if len(e.Affects) == 0 {
		return nil, fmt.Errorf("reconciliation error has no affected conditions")
	}

	conditions := make([]metav1.Condition, 0, len(e.Affects))
	for _, conditionType := range e.Affects {
		if conditionType != dbv1beta1.ConditionReady && conditionType != dbv1beta1.ConditionProgressing {
			return nil, fmt.Errorf("reconciliation error cannot affect condition %q", conditionType)
		}
		conditions = append(conditions, metav1.Condition{
			Type:    conditionType,
			Status:  metav1.ConditionFalse,
			Reason:  e.Reason,
			Message: e.Message,
		})
	}
	return conditions, nil
}
