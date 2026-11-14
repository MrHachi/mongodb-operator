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
	"net"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	dbv1beta1 "github.com/MrHachi/mongodb-operator/api/v1beta1"
	managerclient "github.com/MrHachi/mongodb-operator/internal/manager/pkg/client"
	managerdb "github.com/MrHachi/mongodb-operator/internal/manager/pkg/mongodb"
)

type discoveryState uint8

const (
	discoveryNoPods discoveryState = iota
	discoveryBootstrapPod
	discoveryPrimaryFound
	discoveryPodsWithoutPrimary
	discoveryPodsUnreachable
)

type primaryDiscovery struct {
	State   discoveryState
	Primary *corev1.Pod
}

// discoverPrimary finds the managed Pod corresponding to a PRIMARY member.
func (r *MongoDBReconciler) discoverPrimary(ctx context.Context, mongodb *dbv1beta1.MongoDB) (primaryDiscovery, error) {
	podList := &corev1.PodList{}
	if err := r.List(ctx, podList, client.InNamespace(mongodb.Namespace), client.MatchingLabels(r.labels(mongodb))); err != nil {
		return primaryDiscovery{}, fmt.Errorf("list managed Pods: %w", err)
	}

	managedPods := make([]corev1.Pod, 0, len(podList.Items))
	for _, pod := range podList.Items {
		owned := false
		for _, owner := range pod.OwnerReferences {
			if owner.UID == mongodb.UID && owner.Kind == "MongoDB" {
				owned = true
				break
			}
		}
		if owned {
			managedPods = append(managedPods, pod)
		}
	}
	if len(managedPods) == 0 {
		return primaryDiscovery{State: discoveryNoPods}, nil
	}

	var uninitializedPods []corev1.Pod
	endpointResponded := false
	for i := range managedPods {
		pod := &managedPods[i]
		if pod.Status.PodIP == "" || !podReady(pod) {
			continue
		}
		manager := r.managerClientForPod(pod)
		topology, err := manager.GetTopology(ctx)
		if err != nil {
			if errors.Is(err, managerclient.ErrNotInitialized) {
				endpointResponded = true
				uninitializedPods = append(uninitializedPods, *pod)
			}
			continue
		}
		endpointResponded = true
		for _, member := range topology.Members {
			if strings.EqualFold(member.State, "PRIMARY") {
				primaryPod := podForMember(managedPods, member.Host)
				if primaryPod != nil {
					return primaryDiscovery{State: discoveryPrimaryFound, Primary: primaryPod}, nil
				}
			}
		}
	}
	if len(managedPods) == 1 && len(uninitializedPods) == 1 && uninitializedPods[0].Name == fmt.Sprintf("%s-r-a", mongodb.Name) {
		return primaryDiscovery{State: discoveryBootstrapPod, Primary: &uninitializedPods[0]}, nil
	}
	if !endpointResponded {
		return primaryDiscovery{State: discoveryPodsUnreachable}, nil
	}
	return primaryDiscovery{State: discoveryPodsWithoutPrimary}, nil
}

func podForMember(pods []corev1.Pod, host string) *corev1.Pod {
	hostname, _, err := net.SplitHostPort(host)
	if err != nil {
		hostname = host
	}
	name, _, _ := strings.Cut(hostname, ".")
	for i := range pods {
		if pods[i].Name == name {
			return &pods[i]
		}
	}
	return nil
}

func topologyHasPrimary(topology *managerdb.Topology) bool {
	for _, member := range topology.Members {
		if strings.EqualFold(member.State, "PRIMARY") {
			return true
		}
	}
	return false
}
