package v1alphav1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// Generate DeepCopy methods with:
// go install sigs.k8s.io/controller-tools/cmd/controller-gen@latest ; controller-gen object paths=./

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:path=singletenantmongodbs,shortName=stmdb
type SingleTenantMongoDB struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   SingleTenantMongoDBSpec   `json:"spec"`
	Status SingleTenantMongoDBStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type SingleTenantMongoDBList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`

	Items []SingleTenantMongoDB `json:"items"`
}

type SingleTenantMongoDBSpec struct {
	DatabaseName string `json:"databaseName"`
	// +kubebuilder:default:=1
	Replicas int32 `json:"replicas,omitempty"`

	Admin MongoAdminSpec `json:"admin"`

	Users []MongoUserSpec `json:"users,omitempty"`

	Storage   singleTenantMongoDBStorageSpec `json:"storage"`
	Resources resourcesSpec                  `json:"resources"`
}

type MongoAdminSpec struct {
	Username string `json:"username"`

	SecretRef corev1.LocalObjectReference `json:"secretRef"`
}

type MongoUserSpec struct {
	Username string `json:"username"`

	SecretRef corev1.LocalObjectReference `json:"secretRef"`

	Roles []MongoRoleSpec `json:"roles,omitempty"`
}

type MongoRoleSpec struct {
	Role     string `json:"role"`
	Database string `json:"database"`
}

type singleTenantMongoDBStorageSpec struct {
	Size string `json:"size"`
}

type resourcesSpec struct {
	Requests capacitySpec `json:"requests"`
	Limits   capacitySpec `json:"limits"`
}

type capacitySpec struct {
	Cpu    string `json:"cpu"`
	Memory string `json:"memory"`
}

type SingleTenantMongoDBStatus struct {
	Phase string `json:"phase,omitempty"`

	ReadyReplicas int32 `json:"readyReplicas,omitempty"`

	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

func init() {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(SchemeGroupVersion, &SingleTenantMongoDB{}, &SingleTenantMongoDBList{})
		return nil
	})
}
