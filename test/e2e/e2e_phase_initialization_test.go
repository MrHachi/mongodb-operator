//go:build e2e
// +build e2e

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
	"os/exec"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/MrHachi/mongodb-operator/test/utils"
)

var _ = Describe("MongoDB initialization", func() {
	Context("Initializing phase", func() {
		It("bootstraps a MongoDB cluster through Initializing", func() {
			const testNamespace = "mongodb-e2e"
			const clusterName = "initializing-e2e"

			createMongoDBTestResource(testNamespace, clusterName)

			By("observing the Initializing phase while the first Pod starts")
			waitForStatus := func(phase, reason string) func(Gomega) {
				return func(g Gomega) {
					cmd := exec.Command("kubectl", "get", "mongodb", clusterName, "-n", testNamespace,
						"-o", "jsonpath={.status.phase}{\"/\"}{.status.conditions[?(@.type=='Progressing')].reason}")
					output, err := utils.Run(cmd)
					g.Expect(err).NotTo(HaveOccurred())
					g.Expect(output).To(Equal(phase + "/" + reason))
				}
			}
			Eventually(waitForStatus("Initializing", "WaitingForPrimaryPod"), 3*time.Minute).Should(Succeed())

			By("checking the initial member resources and generated keyfile")
			for _, resource := range []struct{ kind, name string }{
				{"secret", clusterName + "-keyfile"},
				{"persistentvolumeclaim", clusterName + "-r-a-data"},
				{"pod", clusterName + "-r-a"},
				{"service", clusterName},
			} {
				cmd := exec.Command("kubectl", "get", resource.kind, resource.name, "-n", testNamespace)
				_, err := utils.Run(cmd)
				Expect(err).NotTo(HaveOccurred(), "%s %s should be created", resource.kind, resource.name)
			}
			cmd := exec.Command("kubectl", "get", "secret", clusterName+"-keyfile", "-n", testNamespace,
				"-o", "jsonpath={.data.mongodb-keyfile}")
			keyfile, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())
			Expect(keyfile).NotTo(BeEmpty())

			By("waiting for replica set initialization and admin credentials")
			Eventually(waitForStatus("Progressing", "ReconcilingMembers"), 10*time.Minute).Should(Succeed())
			cmd = exec.Command("kubectl", "get", "secret", clusterName+"-admin-credentials", "-n", testNamespace,
				"-o", "jsonpath={.data.username}")
			username, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())
			Expect(username).NotTo(BeEmpty())

			cmd = exec.Command("kubectl", "get", "secret", clusterName+"-admin-credentials", "-n", testNamespace,
				"-o", "jsonpath={.data.password}")
			password, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())
			Expect(password).NotTo(BeEmpty())
		})

		It("resumes initialization after the operator restarts and recreates deleted resources", func() {
			const testNamespace = "mongodb-recovery-e2e"
			const clusterName = "recovery-e2e"
			createMongoDBTestResource(testNamespace, clusterName)

			By("waiting until the initial Pod exists before restarting the operator")
			Eventually(func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "pod", clusterName+"-r-a", "-n", testNamespace)
				_, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
			}, 3*time.Minute).Should(Succeed())

			By("deleting the controller manager Pod during initialization")
			cmd := exec.Command("kubectl", "delete", "pod", "-l", "control-plane=controller-manager", "-n", namespace)
			_, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())
			Eventually(func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "pods", "-l", "control-plane=controller-manager", "-n", namespace,
					"-o", "jsonpath={.items[0].status.phase}")
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(output).To(Equal("Running"))
			}, 3*time.Minute).Should(Succeed())

			By("deleting managed resources while the cluster is still initializing")
			for _, resource := range []struct{ kind, name string }{{"service", clusterName}, {"pod", clusterName + "-r-a"}} {
				cmd = exec.Command("kubectl", "delete", resource.kind, resource.name, "-n", testNamespace)
				_, err = utils.Run(cmd)
				Expect(err).NotTo(HaveOccurred())
				Eventually(func(g Gomega) {
					cmd := exec.Command("kubectl", "get", resource.kind, resource.name, "-n", testNamespace)
					_, err := utils.Run(cmd)
					g.Expect(err).NotTo(HaveOccurred())
				}, 3*time.Minute).Should(Succeed())
			}

			By("waiting for initialization to resume")
			waitForMongoDBStatus(testNamespace, clusterName, "Progressing", "ReconcilingMembers", 10*time.Minute)
		})

		It("reports incomplete admin credentials as degraded", func() {
			const testNamespace = "mongodb-credentials-e2e"
			const clusterName = "credentials-e2e"
			createMongoDBTestResource(testNamespace, clusterName)
			waitForMongoDBStatus(testNamespace, clusterName, "Progressing", "ReconcilingMembers", 10*time.Minute)

			By("removing the username from the managed credentials Secret")
			cmd := exec.Command("kubectl", "patch", "secret", clusterName+"-admin-credentials", "-n", testNamespace,
				"--type=merge", "-p", `{"data":{"username":""}}`)
			_, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())
			By("resetting the MongoDB phase so existing-cluster credentials are checked")
			cmd = exec.Command("kubectl", "patch", "mongodb", clusterName, "-n", testNamespace, "--subresource=status",
				"--type=merge", "-p", `{"status":{"phase":"Initializing"}}`)
			_, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())
			waitForMongoDBStatus(testNamespace, clusterName, "Degraded", "CredentialsIncomplete", 3*time.Minute)
		})

		It("reports a ClusterIP replacement service as an unsafe mismatch", func() {
			const testNamespace = "mongodb-service-drift-e2e"
			const clusterName = "service-drift-e2e"
			createMongoDBTestNamespace(testNamespace)

			By("creating a standard ClusterIP Service where the operator expects its headless Service")
			cmd := exec.Command("kubectl", "create", "service", "clusterip", clusterName, "--tcp=27017:27017", "-n", testNamespace)
			_, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())
			createMongoDBResource(testNamespace, clusterName)

			By("confirming reconciliation remains blocked and the Service stays ClusterIP")
			Eventually(func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "mongodb", clusterName, "-n", testNamespace, "-o", "jsonpath={.status.phase}")
				phase, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(phase).To(Equal("Initializing"))
				cmd = exec.Command("kubectl", "get", "service", clusterName, "-n", testNamespace, "-o", "jsonpath={.spec.clusterIP}")
				clusterIP, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(clusterIP).NotTo(Equal("None"))
			}, 3*time.Minute).Should(Succeed())
		})
	})
})
