//go:build e2e

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

package e2e

import (
	"encoding/base64"
	"fmt"
	"os/exec"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/MrHachi/mongodb-operator/test/utils"
)

const availabilityClientName = "mongodb-availability-client"

var _ = Describe("MongoDB Progressing phase", func() {
	It("scales members in and out one at a time while serving queries", func() {
		const testNamespace = "mongodb-progressing-e2e"
		const clusterName = "progressing-e2e"

		createMongoDBTestNamespace(testNamespace)
		createMongoDBResourceWithReplicas(testNamespace, clusterName, 3)

		By("waiting for initialization to finish and reconcile a Ready cluster")
		waitForMongoDBStatus(testNamespace, clusterName, "Ready", "False", "NotProgressing", 10*time.Minute)

		By("starting a client that continuously writes and reads through the cluster Service")
		startMongoDBAvailabilityClient(testNamespace, clusterName)

		By("checking that scale-out adds the two desired members one at a time")
		assertReplicaCountChangesByAtMostOne(testNamespace, clusterName, 3, 10*time.Minute)
		waitForMongoDBStatus(testNamespace, clusterName, "Ready", "False", "NotProgressing", 10*time.Minute)

		By("scaling in to one member")
		patchReplicaCount(testNamespace, clusterName, 1)
		assertReplicaCountChangesByAtMostOne(testNamespace, clusterName, 1, 10*time.Minute)
		waitForMongoDBStatus(testNamespace, clusterName, "Ready", "False", "NotProgressing", 10*time.Minute)

		By("confirming two member PVCs were jettisoned")
		jettisonedPVCs := listPVCsWithStatus(testNamespace, clusterName, "jettisoned")
		Expect(jettisonedPVCs).To(HaveLen(2))

		By("scaling back out to two members")
		patchReplicaCount(testNamespace, clusterName, 2)
		assertReplicaCountChangesByAtMostOne(testNamespace, clusterName, 2, 10*time.Minute)
		waitForMongoDBStatus(testNamespace, clusterName, "Ready", "False", "NotProgressing", 10*time.Minute)

		By("confirming one of the jettisoned PVCs was reclaimed by the added member")
		Eventually(func(g Gomega) {
			reclaimed := ""
			for _, pvcName := range jettisonedPVCs {
				if pvcStatus(testNamespace, pvcName) == "in-use" {
					reclaimed = pvcName
					break
				}
			}
			g.Expect(reclaimed).NotTo(BeEmpty())
			podNames := listReplicaPods(testNamespace, clusterName)
			g.Expect(podNames).To(HaveLen(2))
			mounted := false
			for _, podName := range podNames {
				if podUsesPVC(testNamespace, podName, reclaimed) {
					mounted = true
				}
			}
			g.Expect(mounted).To(BeTrue())
		}, 2*time.Minute).Should(Succeed())

		By("confirming the continuous database client stayed healthy throughout scaling")
		Expect(podPhase(testNamespace, availabilityClientName)).To(Equal("Running"))
		logs := podLogs(testNamespace, availabilityClientName)
		Expect(logs).To(ContainSubstring("availability-ok"))
	})
})

func assertReplicaCountChangesByAtMostOne(testNamespace, clusterName string, desired int, timeout time.Duration) {
	previousCount := -1
	var countViolation string
	Eventually(func(g Gomega) {
		pods, terminating := replicaPodSnapshot(testNamespace, clusterName)
		count := len(pods)
		if previousCount >= 0 && abs(count-previousCount) > 1 && countViolation == "" {
			countViolation = fmt.Sprintf("replica Pod count changed from %d to %d", previousCount, count)
		}
		if terminating > 1 && countViolation == "" {
			countViolation = fmt.Sprintf("%d replica Pods were terminating at the same time", terminating)
		}
		previousCount = count
		g.Expect(count).To(Equal(desired))
	}, timeout, time.Second).Should(Succeed())
	Expect(countViolation).To(BeEmpty())
}

func replicaPodSnapshot(testNamespace, clusterName string) ([]string, int) {
	cmd := exec.Command("kubectl", "get", "pods", "-n", testNamespace,
		"-l", "db.mrhachi.dev/mongodb="+clusterName,
		"-o", `jsonpath={range .items[*]}{.metadata.name}{"|"}{.metadata.deletionTimestamp}{"\n"}{end}`)
	output, err := utils.Run(cmd)
	Expect(err).NotTo(HaveOccurred())
	rows := strings.Split(strings.TrimSpace(output), "\n")
	if len(rows) == 1 && rows[0] == "" {
		return nil, 0
	}
	pods := make([]string, 0, len(rows))
	terminating := 0
	for _, row := range rows {
		parts := strings.SplitN(row, "|", 2)
		pods = append(pods, parts[0])
		if len(parts) == 2 && parts[1] != "" {
			terminating++
		}
	}
	return pods, terminating
}

func patchReplicaCount(testNamespace, clusterName string, replicas int) {
	cmd := exec.Command("kubectl", "patch", "mongodb", clusterName, "-n", testNamespace,
		"--type=merge", "-p", fmt.Sprintf(`{"spec":{"replicas":%d}}`, replicas))
	_, err := utils.Run(cmd)
	Expect(err).NotTo(HaveOccurred())
}

func startMongoDBAvailabilityClient(testNamespace, clusterName string) {
	username := secretValue(testNamespace, clusterName+"-admin-credentials", "username")
	password := secretValue(testNamespace, clusterName+"-admin-credentials", "password")
	js := `const collection = db.getSiblingDB("e2e").availability;
while (true) {
	const id = "continuous";
	collection.updateOne(
		{_id:id},
		{$inc:{writes:1}},
		{upsert:true, writeConcern:{w:1}}
	);
	const doc = collection.findOne({_id:id});
	if (!doc || doc.writes < 1) {
		throw new Error("write was not readable");
	}
	print("availability-ok:" + doc.writes);
	sleep(1000);
}`
	overrides := `{
	"spec": {
		"securityContext": {
			"runAsNonRoot":true,
			"runAsUser":999,
			"runAsGroup":999,
			"seccompProfile": {
				"type":"RuntimeDefault"
			}
		},
		"containers": [
			{
				"name":"mongodb-availability-client",
				"image":"mongo:latest",
				"command":[
					"mongosh","--quiet",
					"mongodb://` + clusterName + `.` + testNamespace + `.svc.cluster.local:27017/?replicaSet=` + clusterName + `&authSource=admin",
					"--username","` + username + `",
					"--password","` + password + `",
					"--eval",` + fmt.Sprintf("%q", js) + `
				],
				"securityContext":{
					"allowPrivilegeEscalation":false,
					"capabilities": {
						"drop":["ALL"]
					}
				}
			}
		]
	}
}`

	cmd := exec.Command("kubectl", "run", availabilityClientName, "-n", testNamespace, "--restart=Never", "--image=mongo:latest", "--overrides", overrides)
	_, err := utils.Run(cmd)
	Expect(err).NotTo(HaveOccurred())
	Eventually(func(g Gomega) {
		g.Expect(podPhase(testNamespace, availabilityClientName)).To(Equal("Running"))
		g.Expect(podLogs(testNamespace, availabilityClientName)).To(ContainSubstring("availability-ok"))
	}, 3*time.Minute).Should(Succeed())
}

func secretValue(testNamespace, secretName, key string) string {
	cmd := exec.Command("kubectl", "get", "secret", secretName, "-n", testNamespace,
		"-o", fmt.Sprintf("jsonpath={.data.%s}", key))
	encoded, err := utils.Run(cmd)
	Expect(err).NotTo(HaveOccurred())
	decoded, err := base64.StdEncoding.DecodeString(strings.TrimSpace(encoded))
	Expect(err).NotTo(HaveOccurred())
	return string(decoded)
}

func listReplicaPods(testNamespace, clusterName string) []string {
	cmd := exec.Command("kubectl", "get", "pods", "-n", testNamespace,
		"-l", "db.mrhachi.dev/mongodb="+clusterName,
		"-o", `jsonpath={range .items[*]}{.metadata.name}{"\n"}{end}`)
	output, err := utils.Run(cmd)
	Expect(err).NotTo(HaveOccurred())
	lines := strings.Split(strings.TrimSpace(output), "\n")
	if len(lines) == 1 && lines[0] == "" {
		return nil
	}
	return lines
}

func listPVCsWithStatus(testNamespace, clusterName, status string) []string {
	cmd := exec.Command("kubectl", "get", "pvc", "-n", testNamespace,
		"-l", "db.mrhachi.dev/mongodb="+clusterName+",db.mrhachi.dev/status="+status,
		"-o", `jsonpath={range .items[*]}{.metadata.name}{"\n"}{end}`)
	output, err := utils.Run(cmd)
	Expect(err).NotTo(HaveOccurred())
	lines := strings.Split(strings.TrimSpace(output), "\n")
	if len(lines) == 1 && lines[0] == "" {
		return nil
	}
	return lines
}

func pvcStatus(testNamespace, pvcName string) string {
	cmd := exec.Command("kubectl", "get", "pvc", pvcName, "-n", testNamespace,
		"-o", "jsonpath={.metadata.labels.db\\.mrhachi\\.dev/status}")
	status, err := utils.Run(cmd)
	Expect(err).NotTo(HaveOccurred())
	return strings.TrimSpace(status)
}

func podUsesPVC(testNamespace, podName, pvcName string) bool {
	cmd := exec.Command("kubectl", "get", "pod", podName, "-n", testNamespace,
		"-o", `jsonpath={range .spec.volumes[*]}{.persistentVolumeClaim.claimName}{"\n"}{end}`)
	output, err := utils.Run(cmd)
	Expect(err).NotTo(HaveOccurred())
	return strings.Contains("\n"+output+"\n", "\n"+pvcName+"\n")
}

func podPhase(testNamespace, podName string) string {
	cmd := exec.Command("kubectl", "get", "pod", podName, "-n", testNamespace, "-o", "jsonpath={.status.phase}")
	phase, err := utils.Run(cmd)
	Expect(err).NotTo(HaveOccurred())
	return strings.TrimSpace(phase)
}

func podLogs(testNamespace, podName string) string {
	cmd := exec.Command("kubectl", "logs", podName, "-n", testNamespace)
	output, err := utils.Run(cmd)
	Expect(err).NotTo(HaveOccurred())
	return output
}

func abs(value int) int {
	if value < 0 {
		return -value
	}
	return value
}
