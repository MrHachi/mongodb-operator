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

package v1beta1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// MongoDBPhase represents a phase in the MongoDB cluster lifecycle.
type MongoDBPhase string

const (
	PhaseInitializing MongoDBPhase = "Initializing"
	PhaseProgressing  MongoDBPhase = "Progressing"
	PhaseReady        MongoDBPhase = "Ready"
	PhaseDegraded     MongoDBPhase = "Degraded"
)

const (
	ConditionReady       = "Ready"
	ConditionProgressing = "Progressing"
	ConditionDegraded    = "Degraded"

	ReasonInitializing   = "Initializing"
	ReasonProgressing    = "Progressing"
	ReasonDegraded       = "Degraded"
	ReasonReady          = "Ready"
	ReasonNotProgressing = "NotProgressing"
	ReasonNoKnownIssues  = "NoKnownIssues"

	ReasonInstanceManagerUnavailable = "InstanceManagerUnavailable"
	ReasonPrimaryUnavailable         = "PrimaryUnavailable"
	ReasonCredentialsMissing         = "CredentialsMissing"
	ReasonCredentialsIncomplete      = "CredentialsIncomplete"
	ReasonReplicaSetUninitialized    = "ReplicaSetUninitialized"
)

// ProgressReason identifies the current operation reported by the Progressing condition.
type ProgressReason string

const (
	ProgressReasonDiscoveringCluster   ProgressReason = "DiscoveringCluster"
	ProgressReasonCreatingResources    ProgressReason = "CreatingResources"
	ProgressReasonWaitingForPod        ProgressReason = "WaitingForPrimaryPod"
	ProgressReasonInitiatingReplicaSet ProgressReason = "InitiatingReplicaSet"
	ProgressReasonCreatingAdminUser    ProgressReason = "CreatingAdminUser"
	ProgressReasonReconcilingMembers   ProgressReason = "ReconcilingMembers"
)

var progressReasonMessages = map[ProgressReason]string{
	ProgressReasonDiscoveringCluster:   "Checking for existing MongoDB members",
	ProgressReasonCreatingResources:    "Creating resources for the initial MongoDB member",
	ProgressReasonWaitingForPod:        "Waiting for the primary Pod to become ready",
	ProgressReasonInitiatingReplicaSet: "Initiating the MongoDB replica set",
	ProgressReasonCreatingAdminUser:    "Ensuring the MongoDB admin user exists",
	ProgressReasonReconcilingMembers:   "Reconciling MongoDB replica set members",
}

// ProgressReasonMessage returns the catalog message for a progress reason.
func ProgressReasonMessage(reason ProgressReason) (string, bool) {
	message, ok := progressReasonMessages[reason]
	return message, ok
}

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

// MongoDBSpec defines the desired state of MongoDB
type MongoDBSpec struct {
	// INSERT ADDITIONAL SPEC FIELDS - desired state of cluster
	// Important: Run "make" to regenerate code after modifying this file
	// The following markers will use OpenAPI v3 schema to validate the value
	// More info: https://book.kubebuilder.io/reference/markers/crd-validation.html

	// foo is an example field of MongoDB. Edit mongodb_types.go to remove/update
	// +optional
	Foo *string `json:"foo,omitempty"`
}

// MongoDBStatus defines the observed state of MongoDB.
type MongoDBStatus struct {
	// Phase represents the current phase of the MongoDB cluster lifecycle
	// Valid values are: Initializing, Progressing, Ready, Degraded
	Phase MongoDBPhase `json:"phase,omitempty"`

	// Conditions represent the observed state of the MongoDB resource. The operator
	// maintains Ready, Progressing, and Degraded conditions. While work is underway,
	// Progressing is True and its Reason identifies the current operation.
	// Degraded is reserved for observed cluster problems that block the desired state.
	// The status of each condition is one of True, False, or Unknown.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status

// MongoDB is the Schema for the mongodbs API
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

func init() {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(SchemeGroupVersion, &MongoDB{}, &MongoDBList{})
		return nil
	})
}
