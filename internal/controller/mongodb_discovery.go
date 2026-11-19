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
	mdb "github.com/MrHachi/mongodb-operator/internal/mongodb"
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
			if strings.EqualFold(member.State, mdb.StatePrimary) {
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
// the discovered PRIMARY's instance-manager endpoint and verifies that their
// configuration versions agree.
func (r *MongoDBReconciler) discoverClusterTopology(ctx context.Context, mongodb *dbv1beta1.MongoDB, managedPods []corev1.Pod) (*mdb.Topology, *mdb.ReplicaSetConfig, error) {
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

	if err := validatePrimaryConfigVersion(topology, config); err != nil {
		return nil, nil, fmt.Errorf("validate PRIMARY configuration version: %w", err)
	}
	return topology, config, nil
}

func primaryConfigVersion(topology *mdb.Topology) (int32, error) {
	if topology == nil {
		return 0, errors.New("received nil topology")
	}
	if len(topology.Members) == 0 {
		return 0, errors.New("replica set topology member list empty")
	}

	primaryCount := 0
	var version int32
	for _, member := range topology.Members {
		if strings.EqualFold(member.State, mdb.StatePrimary) {
			primaryCount++
			version = member.ConfigVersion
		}
	}
	if primaryCount != 1 {
		return 0, fmt.Errorf("replica set topology must report exactly one PRIMARY; found %d", primaryCount)
	}
	if version <= 0 {
		return 0, errors.New("PRIMARY replica set configuration version is invalid")
	}
	return version, nil
}

func validatePrimaryConfigVersion(topology *mdb.Topology, config *mdb.ReplicaSetConfig) error {
	if config == nil {
		return errors.New("received nil config")
	}
	if config.Version <= 0 {
		return errors.New("replica set configuration version is invalid")
	}

	version, err := primaryConfigVersion(topology)
	if err != nil {
		return fmt.Errorf("get PRIMARY configuration version: %w", err)
	}
	if config.Version != version {
		return fmt.Errorf("replica set PRIMARY configuration version %d does not match configuration version %d", version, config.Version)
	}
	return nil
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

func topologyHasPrimary(topology *mdb.Topology) bool {
	for _, member := range topology.Members {
		if strings.EqualFold(member.State, mdb.StatePrimary) {
			return true
		}
	}
	return false
}
