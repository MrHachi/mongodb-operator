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
	discoveryPodsNotReady
)

type primaryDiscovery struct {
	State   discoveryState
	Primary *corev1.Pod
}

func (r *MongoDBReconciler) discoverPrimaryFromPods(ctx context.Context, mongodb *dbv1beta1.MongoDB, managedPods []corev1.Pod) (primaryDiscovery, error) {
	if len(managedPods) == 0 {
		return primaryDiscovery{State: discoveryNoPods}, nil
	}

	var uninitializedPods []corev1.Pod
	initializationChecked := false
	readyPods := 0
	for i := range managedPods {
		pod := &managedPods[i]
		if pod.Status.PodIP == "" || !podReady(pod) {
			continue
		}
		readyPods++

		manager := r.managerClientForPod(pod)

		topology, err := manager.GetTopology(ctx)
		if err != nil {
			if errors.Is(err, managerclient.ErrNotInitialized) {
				initializationChecked = true
				uninitializedPods = append(uninitializedPods, *pod)
			}
			continue
		}

		initializationChecked = true
		for _, member := range topology.Members {
			if strings.EqualFold(member.State, managerdb.StatePrimary) {
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

	if !initializationChecked {
		if readyPods == 0 {
			return primaryDiscovery{State: discoveryPodsNotReady}, nil
		}
		return primaryDiscovery{State: discoveryPodsUnreachable}, nil
	}
	return primaryDiscovery{State: discoveryPodsWithoutPrimary}, nil
}

// discoverPrimary inspects managed Pods and returns one of these states:
//   - discoveryNoPods: no managed Pods exist.
//   - discoveryBootstrapPod: a single managed Pod exists and its instance-manager
//     reports that replica set is not initiated.
//   - discoveryPrimaryFound: an instance-manager reports a PRIMARY member, and
//     its host matches a managed Pod.
//   - discoveryPodsWithoutPrimary: at least one instance-manager responded, but
//     none reported a PRIMARY member.
//   - discoveryPodsUnreachable: a Ready Pod with an IP exists, but no
//     instance-manager endpoint responded.
//   - discoveryPodsNotReady: managed Pods exist, but none are Ready with an IP.
func (r *MongoDBReconciler) discoverPrimary(ctx context.Context, mongodb *dbv1beta1.MongoDB) (primaryDiscovery, error) {
	podList := &corev1.PodList{}
	if err := r.List(ctx, podList, client.InNamespace(mongodb.Namespace), client.MatchingLabels(r.labels(mongodb))); err != nil {
		return primaryDiscovery{}, fmt.Errorf("list managed Pods: %w", err)
	}

	managedPods := make([]corev1.Pod, 0, len(podList.Items))
	for _, pod := range podList.Items {
		for _, owner := range pod.OwnerReferences {
			if owner.UID == mongodb.UID && owner.Kind == dbv1beta1.MongoDBKind {
				managedPods = append(managedPods, pod)
			}
		}
	}
	return r.discoverPrimaryFromPods(ctx, mongodb, managedPods)
}

// discoverClusterTopology obtains topology and replica-set configuration from
// the discovered PRIMARY's instance-manager endpoint and verifies that both
// responses describe the same configuration and member set.
func (r *MongoDBReconciler) discoverClusterTopology(ctx context.Context, mongodb *dbv1beta1.MongoDB, managedPods []corev1.Pod) (*managerdb.Topology, *managerdb.ReplicaSetConfig, error) {
	discovery, err := r.discoverPrimaryFromPods(ctx, mongodb, managedPods)
	if err != nil {
		return nil, nil, fmt.Errorf("discover replica set primary: %w", err)
	}
	if discovery.State != discoveryPrimaryFound || discovery.Primary == nil {
		return nil, nil, fmt.Errorf("replica set primary is unavailable (discovery state %d)", discovery.State)
	}
	if discovery.Primary.Status.PodIP == "" || !podReady(discovery.Primary) {
		return nil, nil, fmt.Errorf("discovered primary Pod %s is not Ready with an IP", discovery.Primary.Name)
	}

	manager := r.managerClientForPod(discovery.Primary)

	topology, err := manager.GetTopology(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("get topology from primary Pod %s: %w", discovery.Primary.Name, err)
	}

	config, err := manager.GetConfig(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("get configuration from primary Pod %s: %w", discovery.Primary.Name, err)
	}

	if _, err := memberConfigsByID(topology, config); err != nil {
		return nil, nil, fmt.Errorf("primary topology and configuration do not match: %w", err)
	}
	return topology, config, nil
}

func memberConfigsByID(topology *managerdb.Topology, config *managerdb.ReplicaSetConfig) (map[int]managerdb.MemberConfig, error) {
	if topology == nil || config == nil || topology.ConfigVersion <= 0 || config.Version <= 0 {
		return nil, errors.New("replica set topology and/or configuration invalid")
	}
	if config.Version != topology.ConfigVersion {
		return nil, errors.New("replica set topology and configuration versions do not match")
	}
	if len(topology.Members) == 0 || len(topology.Members) != len(config.Members) {
		return nil, errors.New("replica set topology and configuration member lists do not match")
	}

	configs := make(map[int]managerdb.MemberConfig, len(config.Members))
	for _, member := range config.Members {
		if _, exists := configs[member.ID]; exists {
			return nil, fmt.Errorf("replica set configuration contains duplicate member ID %d", member.ID)
		}
		configs[member.ID] = member.Config
	}

	seenMembers := make(map[int]struct{}, len(topology.Members))
	for _, member := range topology.Members {
		if _, exists := seenMembers[member.ID]; exists {
			return nil, fmt.Errorf("replica set topology contains duplicate member ID %d", member.ID)
		}
		seenMembers[member.ID] = struct{}{}

		if _, ok := configs[member.ID]; !ok {
			return nil, fmt.Errorf("replica set configuration is missing member ID %d", member.ID)
		}
	}
	return configs, nil
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
		if strings.EqualFold(member.State, managerdb.StatePrimary) {
			return true
		}
	}
	return false
}
