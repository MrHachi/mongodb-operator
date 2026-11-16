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

	ctrl "sigs.k8s.io/controller-runtime"

	dbv1beta1 "github.com/MrHachi/mongodb-operator/api/v1beta1"
)

func (r *MongoDBReconciler) reconcileReady(ctx context.Context, mongodb *dbv1beta1.MongoDB) (ctrl.Result, error) {
	pods, err := r.listManagedReplicaPods(ctx, mongodb)
	if err != nil {
		return ctrl.Result{}, err
	}

	if len(pods) != int(mongodb.Spec.Replicas) {
		return r.reconcileProgressing(ctx, mongodb)
	}

	for i := range pods {
		if _, ok := pods[i].Annotations[drainingPodAnnotation]; ok {
			return r.reconcileProgressing(ctx, mongodb)
		}
		if _, ok := pods[i].Annotations[provisioningPodAnnotation]; ok {
			return r.reconcileProgressing(ctx, mongodb)
		}
	}

	if err := r.transitionReady(ctx, mongodb); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}
