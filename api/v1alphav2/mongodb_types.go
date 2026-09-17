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

package v1alphav2

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// Generate DeepCopy methods with:
// go install sigs.k8s.io/controller-tools/cmd/controller-gen@latest ; controller-gen object paths=./

// MongoDBSpec defines the desired state of MongoDB
type MongoDBSpec struct {
	DatabaseName string `json:"databaseName"`
	// +kubebuilder:default:=1
	Replicas int32 `json:"replicas,omitempty"`
	// +kubebuilder:default={}
	Scaling ScalingSpec `json:"scaling,omitempty"`
	// +kubebuilder:default:={}
	Image ImageSpec `json:"image,omitempty"`

	// +required
	Admin MongoDBAdminSpec `json:"admin"`

	// +optional
	Users []MongoDBUserSpec `json:"users,omitempty"`

	// +kubebuilder:default:={}
	Storage MongoDBStorageSpec `json:"storage"`

	// +optional
	Resources ResourcesSpec `json:"resources"`
}

// ScalingSpec defines scaling behavior
type ScalingSpec struct {
	// +kubebuilder:default:="5m"
	ReplicationLagThreshold string `json:"replicationLagThreshold,omitempty"`
}

// ImageSpec defines the image to be used for MongoDB
type ImageSpec struct {
	// +kubebuilder:default:="ghcr.io/mrhachi/mongodb:8.3.7-stable"
	Tag string `json:"imageTag,omitempty"`
	// +optional
	PullPolicy corev1.PullPolicy `json:"imagePullPolicy,omitempty"`
}

// MongoDBAdminSpec describes the admin user's username and password secret
type MongoDBAdminSpec struct {
	Username string `json:"username"`

	SecretRef corev1.LocalObjectReference `json:"secretRef"`
}

// MongoDBAdminSpec describes a user's username, password secret, and list of roles
type MongoDBUserSpec struct {
	Username string `json:"username"`

	SecretRef corev1.LocalObjectReference `json:"secretRef"`

	Roles []MongoDBRoleSpec `json:"roles,omitempty"`
}

// MongoDBRoleSpec describes a role and the scope to which it applies
type MongoDBRoleSpec struct {
	Role     string `json:"role"`
	Database string `json:"database"`
}

// MongoDBStorageSpec describes the storage to provision for MongoDB
type MongoDBStorageSpec struct {
	// +kubebuilder:default:="20Gi"
	Size string `json:"size"`
}

type ResourcesSpec struct {
	Requests CapacitySpec `json:"requests"`
	Limits   CapacitySpec `json:"limits"`
}

type CapacitySpec struct {
	Cpu    string `json:"cpu"`
	Memory string `json:"memory"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status

// MongoDB is the Schema for the mongodbs API
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:path=mongodbs,shortName=mdb
type MongoDB struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of MongoDB
	// +required
	Spec MongoDBSpec `json:"spec"`

	// status defines the observed state of MongoDB
	// +optional
	Status MongoDBStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// MongoDBList contains a list of MongoDB
type MongoDBList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []MongoDB `json:"items"`
}

// Reconciliation phase of the MongoDB
type DBPhase string

const (
	PhaseInitializing DBPhase = "Initializing"
	PhaseReady        DBPhase = "Ready"
	PhaseScaling      DBPhase = "Scaling"
	PhaseDegraded     DBPhase = "Degraded"
)

// MongoDBStatus defines the observed state of MongoDB.
type MongoDBStatus struct {
	Phase DBPhase `json:"phase,omitempty"`

	ReadyMembers   int32 `json:"readyMembers,omitempty"`
	DesiredMembers int32 `json:"desiredMembers,omitempty"`

	// The status of each condition is one of True, False, or Unknown.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

func init() {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(SchemeGroupVersion, &MongoDB{}, &MongoDBList{})
		return nil
	})
}
