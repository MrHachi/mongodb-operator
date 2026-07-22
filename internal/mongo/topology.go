package mongo

import (
	"cmp"
	"fmt"
	"slices"
	"strconv"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
)

type RSMember struct {
	Host     string `bson:"host"`
	ID       int    `bson:"_id"`
	Priority int    `bson:"priority,omitempty"`
}

func MakeRSMember(ord int, stsName, svcName, namespace string) RSMember {
	return RSMember{
		Host: fmt.Sprintf(
			"%s-%d.%s.%s.svc.cluster.local:27017",
			stsName,
			ord,
			svcName,
			namespace,
		),
		ID:       ord,
		Priority: ord,
	}
}

func compareRSMembers(A, B RSMember) int {
	return cmp.Compare(A.ID, B.ID)
}

type MongoTopology struct {
	StsName string
	Ns      string
	SvcName string
	Members []RSMember
}

func (mt MongoTopology) PodZero() (RSMember, bool) {
	for _, member := range mt.Members {
		if member.ID == 0 {
			return member, true
		}
	}
	return RSMember{}, false
}

func MongoTopologyFromStsAndPods(sts *appsv1.StatefulSet, pods []corev1.Pod) (MongoTopology, error) {
	members := make([]RSMember, 0, len(pods))

	for _, pod := range pods {
		if pod.DeletionTimestamp != nil {
			continue
		}

		idxStr, ok := pod.Labels["apps.kubernetes.io/pod-index"]
		if !ok {
			return MongoTopology{}, fmt.Errorf(
				"pod %q missing apps.kubernetes.io/pod-index label",
				pod.Name,
			)
		}

		ordinal, err := strconv.Atoi(idxStr)
		if err != nil {
			return MongoTopology{}, fmt.Errorf(
				"parse pod-index for pod %q: %w",
				pod.Name,
				err,
			)
		}

		members = append(members, RSMember{
			ID: ordinal,
			Host: fmt.Sprintf(
				"%s.%s.%s.svc.cluster.local:27017",
				pod.Name,
				sts.Spec.ServiceName,
				sts.Namespace,
			),
		})
	}

	slices.SortFunc(
		members,
		func(a, b RSMember) int {
			return cmp.Compare(a.ID, b.ID)
		},
	)

	return MongoTopology{
		StsName: sts.Name,
		Ns:      sts.Namespace,
		SvcName: sts.Spec.ServiceName,
		Members: members,
	}, nil
}
