package resources_test

import (
	"testing"

	api "github.com/mrhachi/mongodb-operator/api/v1betav1"
	"github.com/mrhachi/mongodb-operator/internal/controller/resources"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestDesiredReplicaPod_KeyfileCopy(t *testing.T) {
	desired := &api.MongoDB{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-db",
			Namespace: "default",
		},
		Spec: api.MongoDBSpec{
			Image: api.ImageSpec{
				Tag: "mongo:6.0",
			},
			Resources: api.ResourcesSpec{
				Requests: api.CapacitySpec{Cpu: "100m", Memory: "128Mi"},
				Limits:   api.CapacitySpec{Cpu: "200m", Memory: "256Mi"},
			},
			Storage: api.MongoDBStorageSpec{
				Size: "1Gi",
			},
		},
	}

	mongoRes := resources.NewMongoDB(desired)
	pod, _ := mongoRes.DesiredReplicaPod("a", "test-db-kf", "test-db-sa")

	if len(pod.Spec.InitContainers) != 1 {
		t.Fatalf("expected 1 init container, got %d", len(pod.Spec.InitContainers))
	}

	initC := pod.Spec.InitContainers[0]
	if initC.Name != "copy-keyfile" {
		t.Errorf("expected init container name 'copy-keyfile', got %q", initC.Name)
	}

	expectedCmd := "cp /etc/kf-secret/keyfile /etc/kf/keyfile && chmod 0400 /etc/kf/keyfile && chown 999:999 /etc/kf/keyfile"
	if len(initC.Command) != 3 || initC.Command[2] != expectedCmd {
		t.Errorf("expected command containing %q, got %v", expectedCmd, initC.Command)
	}

	// Verify volumes
	hasSecretVol := false
	hasEmptyDirVol := false
	for _, vol := range pod.Spec.Volumes {
		if vol.Name == "keyfile-secret" && vol.Secret != nil && vol.Secret.SecretName == "test-db-kf" {
			hasSecretVol = true
		}
		if vol.Name == "keyfile" && vol.EmptyDir != nil {
			hasEmptyDirVol = true
		}
	}

	if !hasSecretVol {
		t.Errorf("expected keyfile-secret volume pointing to test-db-kf")
	}
	if !hasEmptyDirVol {
		t.Errorf("expected keyfile emptyDir volume")
	}

	// Verify mongo container volume mount
	var mongoContainerMountPath string
	for _, c := range pod.Spec.Containers {
		if c.Name == "mongo" {
			for _, vm := range c.VolumeMounts {
				if vm.Name == "keyfile" {
					mongoContainerMountPath = vm.MountPath
				}
			}
		}
	}

	if mongoContainerMountPath != "/etc/kf/keyfile" {
		t.Errorf("expected mongo container mountPath '/etc/kf/keyfile' got mountPath %q", mongoContainerMountPath)
	}
}
