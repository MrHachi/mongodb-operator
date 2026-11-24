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

package discovery

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"

	corev1 "k8s.io/api/core/v1"

	managerclient "github.com/MrHachi/mongodb-operator/internal/manager/pkg/client"
	mdb "github.com/MrHachi/mongodb-operator/internal/mongodb"
)

// State describes the outcome of primary discovery.
type State uint8

const (
	NoPods State = iota
	BootstrapPod
	PrimaryFound
	PodsWithoutPrimary
	PodsUnreachable
	PodsNotReady
)

// PrimaryResult identifies a reported primary or a candidate bootstrap Pod.
type PrimaryResult struct {
	State   State
	Primary *corev1.Pod
}

// Manager reads replica-set state from a Pod’s instance manager.
type Manager interface {
	GetTopology(context.Context) (*mdb.Topology, error)
	GetConfig(context.Context) (*mdb.ReplicaSetConfig, error)
}

// FindPrimary probes Ready managed Pods with IPs. If the only Pod is the named
// bootstrap Pod and its instance manager reports an uninitialized replica set,
// FindPrimary returns BootstrapPod. An empty bootstrap name disables that case.
func FindPrimary(ctx context.Context, managedPods []corev1.Pod, bootstrapPodName string, managerForPod func(*corev1.Pod) Manager) PrimaryResult {
	if len(managedPods) == 0 {
		return PrimaryResult{State: NoPods}
	}

	initializationChecked := false
	readyPods := 0
	for i := range managedPods {
		pod := &managedPods[i]
		if !PodReadyWithIP(pod) {
			continue
		}
		readyPods++

		manager := managerForPod(pod)

		topology, err := manager.GetTopology(ctx)
		if err != nil {
			if errors.Is(err, managerclient.ErrNotInitialized) {
				initializationChecked = true
				if len(managedPods) == 1 && bootstrapPodName != "" && pod.Name == bootstrapPodName {
					return PrimaryResult{State: BootstrapPod, Primary: pod}
				}
			}
			continue
		}

		initializationChecked = true
		for _, member := range topology.Members {
			if strings.EqualFold(member.State, mdb.StatePrimary) {
				primaryPod := PodForMember(managedPods, member.Host)
				if primaryPod != nil {
					return PrimaryResult{State: PrimaryFound, Primary: primaryPod}
				}
			}
		}
	}

	if !initializationChecked {
		if readyPods == 0 {
			return PrimaryResult{State: PodsNotReady}
		}
		return PrimaryResult{State: PodsUnreachable}
	}
	return PrimaryResult{State: PodsWithoutPrimary}
}

// ClusterTopology obtains topology and replica-set configuration from
// the discovered PRIMARY's instance-manager endpoint and verifies that their
// configuration versions agree.
func ClusterTopology(ctx context.Context, managedPods []corev1.Pod, bootstrapPodName string, managerForPod func(*corev1.Pod) Manager) (*mdb.Topology, *mdb.ReplicaSetConfig, error) {
	primary := FindPrimary(ctx, managedPods, bootstrapPodName, managerForPod)
	if primary.State != PrimaryFound || primary.Primary == nil {
		return nil, nil, fmt.Errorf("replica set primary is unavailable (discovery state %d)", primary.State)
	}
	if !PodReadyWithIP(primary.Primary) {
		return nil, nil, fmt.Errorf("discovered primary Pod %s is not Ready with an IP", primary.Primary.Name)
	}

	manager := managerForPod(primary.Primary)

	topology, err := manager.GetTopology(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("get topology from primary Pod %s: %w", primary.Primary.Name, err)
	}

	config, err := manager.GetConfig(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("get configuration from primary Pod %s: %w", primary.Primary.Name, err)
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

// PodForMember matches a replica-set member hostname to a managed Pod.
func PodForMember(pods []corev1.Pod, host string) *corev1.Pod {
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

// TopologyHasPrimary reports whether any member claims to be primary.
func TopologyHasPrimary(topology *mdb.Topology) bool {
	for _, member := range topology.Members {
		if strings.EqualFold(member.State, mdb.StatePrimary) {
			return true
		}
	}
	return false
}

// PodReadyWithIP reports whether a Pod is Ready and has an IP address.
func PodReadyWithIP(pod *corev1.Pod) bool {
	if pod.Status.PodIP == "" {
		return false
	}
	for _, condition := range pod.Status.Conditions {
		if condition.Type == corev1.PodReady {
			return condition.Status == corev1.ConditionTrue
		}
	}
	return false
}
