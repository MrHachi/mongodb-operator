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
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
)

// nextReplicaID returns the first unoccupied alphabetic replica ID. IDs use
// zero-based base 26: a..z, aa, ab, ...
// Occupancy includes managed Pods only. PVCs are handled in scale-out
// reclamation.
func nextReplicaID(clusterName string, pods []corev1.Pod) (string, error) {
	occupied := make(map[string]struct{}, len(pods))

	podPrefix := clusterName + "-r-"
	for i := range pods {
		if id, ok := replicaIDFromResourceName(pods[i].Name, podPrefix, ""); ok {
			occupied[id] = struct{}{}
		}
	}

	for index := uint32(0); ; index++ {
		id := replicaIDForIndex(index)
		if _, exists := occupied[id]; !exists {
			return id, nil
		}
		if index == ^uint32(0) {
			return "", fmt.Errorf("no available replica ID for cluster %q", clusterName)
		}
	}
}

func replicaIDFromResourceName(name, prefix, suffix string) (string, bool) {
	if !strings.HasPrefix(name, prefix) || !strings.HasSuffix(name, suffix) {
		return "", false
	}
	id := strings.TrimSuffix(strings.TrimPrefix(name, prefix), suffix)
	if id == "" {
		return "", false
	}
	for _, char := range id {
		if char < 'a' || char > 'z' {
			return "", false
		}
	}
	return id, true
}

func replicaIDForIndex(index uint32) string {
	var reversed [14]byte // enough for the uint32 range in base 26
	position := len(reversed)
	for {
		reversed[position-1] = byte('a' + index%26)
		position--
		if index < 26 {
			break
		}
		index = index/26 - 1
	}
	return string(reversed[position:])
}
