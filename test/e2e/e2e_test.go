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
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/mrhachi/mongodb-operator/test/utils"
)

var controllerPodName string

var _ = Describe("Operator", Ordered, func() {
	// Before running the tests, set up the environment by creating the namespace and
	// enforce the restricted security policy to the namespace.
	BeforeAll(func() {
		By("creating manager namespace")
		cmd := exec.Command("kubectl", "create", "ns", controllerNamespace)
		_, err := utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred(), "Failed to create namespace")

		By("labeling the namespace to enforce the restricted security policy")
		cmd = exec.Command("kubectl", "label", "--overwrite", "ns", controllerNamespace,
			"pod-security.kubernetes.io/enforce=restricted")
		_, err = utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred(), "Failed to label namespace with restricted policy")

		By("creating custom resource namespace")
		cmd = exec.Command("kubectl", "create", "ns", sampleCustomResourceNamespace)
		_, err = utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred(), "Failed to create custom resource namespace")
	})

	// After all tests have been executed, clean up by deleting the namespace and any cluster-scoped resources.
	AfterAll(func() {
		By("removing metrics clusterrolebinding")
		cmd := exec.Command("kubectl", "delete", "clusterrolebinding", controllerMetricsRoleBindingName)
		_, _ = utils.Run(cmd)

		By("removing instance manager clusterrolebindings")
		cmd = exec.Command("kubectl", "delete", "clusterrolebinding", "-l", fmt.Sprintf("db.mrhachi.dev/mongodb=%s", sampleCustomResourceName))
		_, _ = utils.Run(cmd)

		By("removing custom resource namespace")
		cmd = exec.Command("kubectl", "delete", "ns", sampleCustomResourceNamespace)
		_, _ = utils.Run(cmd)

		By("removing manager namespace")
		cmd = exec.Command("kubectl", "delete", "ns", controllerNamespace)
		_, _ = utils.Run(cmd)
	})

	// After each test, check for failures and collect logs, events,
	// and pod descriptions for debugging.
	AfterEach(func() {
		specReport := CurrentSpecReport()
		if specReport.Failed() {
			By("Fetching controller manager pod logs")
			cmd := exec.Command("kubectl", "logs", controllerPodName, "-n", controllerNamespace)
			controllerLogs, err := utils.Run(cmd)
			if err == nil {
				_, _ = fmt.Fprintf(GinkgoWriter, "Controller logs:\n %s", controllerLogs)
			} else {
				_, _ = fmt.Fprintf(GinkgoWriter, "Failed to get Controller logs: %s", err)
			}

			By("Fetching Kubernetes events")
			cmd = exec.Command("kubectl", "get", "events", "-n", controllerNamespace, "--sort-by=.lastTimestamp")
			eventsOutput, err := utils.Run(cmd)
			if err == nil {
				_, _ = fmt.Fprintf(GinkgoWriter, "Kubernetes events:\n%s", eventsOutput)
			} else {
				_, _ = fmt.Fprintf(GinkgoWriter, "Failed to get Kubernetes events: %s", err)
			}

			By("Fetching curl-metrics logs")
			cmd = exec.Command("kubectl", "logs", "curl-metrics", "-n", controllerNamespace)
			metricsOutput, err := utils.Run(cmd)
			if err == nil {
				_, _ = fmt.Fprintf(GinkgoWriter, "Metrics logs:\n %s", metricsOutput)
			} else {
				_, _ = fmt.Fprintf(GinkgoWriter, "Failed to get curl-metrics logs: %s", err)
			}

			By("Fetching controller manager pod description")
			cmd = exec.Command("kubectl", "describe", "pod", controllerPodName, "-n", controllerNamespace)
			podDescription, err := utils.Run(cmd)
			if err == nil {
				fmt.Println("Pod description:\n", podDescription)
			} else {
				fmt.Println("Failed to describe controller pod")
			}
		}
	})

	SetDefaultEventuallyTimeout(2 * time.Minute)
	SetDefaultEventuallyPollingInterval(time.Second)

	Context("Manager", func() {
		BeforeAll(func() {
			By("installing CRDs")
			cmd := exec.Command("make", "install")
			_, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred(), "Failed to install CRDs")

			By("deploying the controller")
			cmd = exec.Command("make", "deploy", fmt.Sprintf("IMG=%s", controllerImage))
			_, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred(), "Failed to deploy the controller")
		})

		AfterAll(func() {
			By("cleaning up the curl pod for metrics")
			cmd := exec.Command("kubectl", "delete", "pod", "curl-metrics", "-n", controllerNamespace)
			_, _ = utils.Run(cmd)

			By("deleting any custom resources")
			cmd = exec.Command("kubectl", "delete", "-f",
				sampleTemplatePath+sampleCustomResourceTemplateName,
				"-n", sampleCustomResourceNamespace)
			_, _ = utils.Run(cmd)

			for _, user := range sampleCustomResourceUsers {
				cmd := exec.Command("kubectl", "delete", "secret",
					user.PasswordSecretName,
					"-n", sampleCustomResourceNamespace)
				_, _ = utils.Run(cmd)
			}

			By("undeploying the controller")
			cmd = exec.Command("make", "undeploy")
			_, _ = utils.Run(cmd)

			By("uninstalling CRDs")
			cmd = exec.Command("make", "uninstall")
			_, _ = utils.Run(cmd)
		})

		It("should run successfully", verifyControllerManager)

		It("should ensure the metrics endpoint is serving metrics", verifyMetricsEndpoint)

		// +kubebuilder:scaffold:e2e-webhooks-checks

		// Apply sample CR and check status.
		It("should successfully install a CR deployment", installCustomResource)

		// Verify CR reconciliation and usability
		It("should reconcile a usable CR", verifyCustomResource)
	})

	Context("Chart", func() {
		BeforeAll(func() {
			By("installing the Helm chart")
			cmd := exec.Command("make", "chart-install", fmt.Sprintf("HELM=helm -n %s", controllerNamespace))
			_, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred(), "Failed to install Helm chart")
		})

		AfterAll(func() {
			By("deleting any custom resources")
			cmd := exec.Command("kubectl", "delete", "-f",
				sampleTemplatePath+sampleCustomResourceTemplateName,
				"-n", sampleCustomResourceNamespace)
			_, _ = utils.Run(cmd)

			for _, user := range sampleCustomResourceUsers {
				cmd := exec.Command("kubectl", "delete", "secret",
					user.PasswordSecretName,
					"-n", sampleCustomResourceNamespace)
				_, _ = utils.Run(cmd)
			}

			By("uninstalling the Helm chart")
			cmd = exec.Command("make", "chart-uninstall", fmt.Sprintf(`HELM=helm -n %s`, controllerNamespace))
			_, _ = utils.Run(cmd)
		})

		It("should run successfully", verifyControllerManager)

		// +kubebuilder:scaffold:e2e-webhooks-checks

		// Apply sample CR and check status.
		It("should successfully install a CR deployment", installCustomResource)

		// Verify CR reconciliation and usability
		It("should reconcile a usable CR", verifyCustomResource)
	})
})

// serviceAccountToken returns a token for the specified service account in the given namespace.
// It uses the Kubernetes TokenRequest API to generate a token by directly sending a request
// and parsing the resulting token from the API response.
func serviceAccountToken() (string, error) {
	const tokenRequestRawString = `{
		"apiVersion": "authentication.k8s.io/v1",
		"kind": "TokenRequest"
	}`

	By("creating temporary file to store the token request")
	secretName := fmt.Sprintf("%s-token-request", controllerName)
	tokenRequestFile := filepath.Join("/tmp", secretName)
	err := os.WriteFile(tokenRequestFile, []byte(tokenRequestRawString), os.FileMode(0o644))
	if err != nil {
		return "", err
	}

	var out string
	verifyTokenCreation := func(g Gomega) {
		By("executing kubectl command to create the token")
		cmd := exec.Command("kubectl", "create", "--raw", fmt.Sprintf(
			"/api/v1/namespaces/%s/serviceaccounts/%s/token",
			controllerNamespace,
			controllerName,
		), "-f", tokenRequestFile)

		output, err := cmd.CombinedOutput()
		g.Expect(err).NotTo(HaveOccurred())

		By("parsing the JSON output to extract the token")
		var token tokenRequest
		err = json.Unmarshal(output, &token)
		g.Expect(err).NotTo(HaveOccurred())

		out = token.Status.Token
	}
	Eventually(verifyTokenCreation).Should(Succeed())

	return out, err
}

// getMetricsOutput retrieves and returns the logs from the curl pod used to access the metrics endpoint.
func getMetricsOutput() (string, error) {
	By("getting the curl-metrics logs")
	cmd := exec.Command("kubectl", "logs", "curl-metrics", "-n", controllerNamespace)
	return utils.Run(cmd)
}

func verifyControllerManager() {
	By("validating that the controller pod is running as expected")
	verifyControllerUp := func(g Gomega) {
		By("getting the name of the controller pod")
		cmd := exec.Command("kubectl", "get",
			"pods", "-l", fmt.Sprintf("control-plane=%s", "mongodb-controller"),
			"-o", "go-template={{ range .items }}"+
				"{{ if not .metadata.deletionTimestamp }}"+
				"{{ .metadata.name }}"+
				"{{ \"\\n\" }}{{ end }}{{ end }}",
			"-n", controllerNamespace,
		)

		podOutput, err := utils.Run(cmd)
		g.Expect(err).NotTo(HaveOccurred(), "Failed to retrieve controller pod information")
		podNames := utils.GetNonEmptyLines(podOutput)
		g.Expect(podNames).To(HaveLen(1), "expected 1 controller pod running")
		controllerPodName = podNames[0]

		By("validating the pod's status")
		cmd = exec.Command("kubectl", "get",
			"pods", controllerPodName, "-o", "jsonpath={.status.phase}",
			"-n", controllerNamespace,
		)
		output, err := utils.Run(cmd)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(output).To(Equal("Running"), "Incorrect controller pod status")
	}
	Eventually(verifyControllerUp).Should(Succeed())
}

func verifyMetricsEndpoint() {
	By("creating a ClusterRoleBinding for the service account to allow access to metrics")
	cmd := exec.Command("kubectl", "create", "clusterrolebinding", controllerMetricsRoleBindingName,
		"--clusterrole=mongodb-controller-metrics-reader",
		fmt.Sprintf("--serviceaccount=%s:%s", controllerNamespace, controllerName),
	)
	_, err := utils.Run(cmd)
	Expect(err).NotTo(HaveOccurred(), "Failed to create ClusterRoleBinding")

	By("validating that the metrics service is available")
	cmd = exec.Command("kubectl", "get", "service", controllerMetricsServiceName, "-n", controllerNamespace)
	_, err = utils.Run(cmd)
	Expect(err).NotTo(HaveOccurred(), "Metrics service should exist")

	By("getting the service account token")
	token, err := serviceAccountToken()
	Expect(err).NotTo(HaveOccurred())
	Expect(token).NotTo(BeEmpty())

	By("ensuring the controller pod is ready")
	verifyControllerPodReady := func(g Gomega) {
		cmd := exec.Command("kubectl", "get", "pod", controllerPodName, "-n", controllerNamespace,
			"-o", "jsonpath={.status.conditions[?(@.type=='Ready')].status}")
		output, err := utils.Run(cmd)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(output).To(Equal("True"), "Controller pod not ready")
	}
	Eventually(verifyControllerPodReady, 3*time.Minute, time.Second).Should(Succeed())

	By("verifying that the controller manager is serving the metrics server")
	verifyMetricsServerStarted := func(g Gomega) {
		cmd := exec.Command("kubectl", "logs", controllerPodName, "-n", controllerNamespace)
		output, err := utils.Run(cmd)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(output).To(ContainSubstring("Serving metrics server"),
			"Metrics server not yet started")
	}
	Eventually(verifyMetricsServerStarted, 3*time.Minute, time.Second).Should(Succeed())

	// +kubebuilder:scaffold:e2e-metrics-webhooks-readiness

	By("creating the curl-metrics pod to access the metrics endpoint")
	cmd = exec.Command("kubectl", "run", "curl-metrics", "--restart=Never",
		"--namespace", controllerNamespace,
		"--image=curlimages/curl:latest",
		"--overrides",
		fmt.Sprintf(`{
					"spec": {
						"containers": [{
							"name": "curl",
							"image": "curlimages/curl:latest",
							"command": ["/bin/sh", "-c"],
							"args": [
								"for i in $(seq 1 30); do curl -v -k -H 'Authorization: Bearer %s' https://%s.%s.svc.cluster.local:8443/metrics && exit 0 || sleep 2; done; exit 1"
							],
							"securityContext": {
								"readOnlyRootFilesystem": true,
								"allowPrivilegeEscalation": false,
								"capabilities": {
									"drop": ["ALL"]
								},
								"runAsNonRoot": true,
								"runAsUser": 1000,
								"seccompProfile": {
									"type": "RuntimeDefault"
								}
							}
						}],
						"serviceAccountName": "%s"
					}
				}`, token, controllerMetricsServiceName, controllerNamespace, controllerName))
	_, err = utils.Run(cmd)
	Expect(err).NotTo(HaveOccurred(), "Failed to create curl-metrics pod")

	By("waiting for the curl-metrics pod to complete.")
	verifyCurlUp := func(g Gomega) {
		cmd := exec.Command("kubectl", "get", "pods", "curl-metrics",
			"-o", "jsonpath={.status.phase}",
			"-n", controllerNamespace)
		output, err := utils.Run(cmd)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(output).To(Equal("Succeeded"), "curl pod in wrong status")
	}
	Eventually(verifyCurlUp, 5*time.Minute).Should(Succeed())

	By("getting the metrics by checking curl-metrics logs")
	verifyMetricsAvailable := func(g Gomega) {
		metricsOutput, err := getMetricsOutput()
		g.Expect(err).NotTo(HaveOccurred(), "Failed to retrieve logs from curl pod")
		g.Expect(metricsOutput).NotTo(BeEmpty())
		g.Expect(metricsOutput).To(ContainSubstring("< HTTP/1.1 200 OK"))
	}
	Eventually(verifyMetricsAvailable, 2*time.Minute).Should(Succeed())
}

func installCustomResource() {
	By("deploying CR prerequisite secrets")
	for _, user := range sampleCustomResourceUsers {
		cmd := exec.Command("kubectl", "create", "secret", "generic",
			user.PasswordSecretName,
			"--from-literal", "password=T3stP@55",
			"-n", sampleCustomResourceNamespace)
		_, err := utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred())
	}

	By("deploying a CR instance.")
	cmd := exec.Command("kubectl", "apply", "-f",
		sampleTemplatePath+sampleCustomResourceTemplateName,
		"-n", sampleCustomResourceNamespace)
	_, err := utils.Run(cmd)
	Expect(err).NotTo(HaveOccurred())

	By("waiting for the CR to become ready.")
	verifyCustomResourceReady := func(g Gomega) {
		cmd := exec.Command("kubectl", "get", sampleCustomResourceTypeName, sampleCustomResourceName,
			"-o", "jsonpath={.status.phase}",
			"-n", sampleCustomResourceNamespace)
		output, err := utils.Run(cmd)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(output).To(Equal("Ready"), "custom resource in wrong status")
	}
	// It takes a while for the STS to create each pod, so give it some time
	Eventually(verifyCustomResourceReady, 5*time.Minute).Should(Succeed())
}

func verifyCustomResource() {
	var desiredCount int

	By("checking the STS is ready.")
	verifyStatefulSetReady := func(g Gomega) {
		// Get desired pod count
		desiredCmd := exec.Command("kubectl", "get", "sts", sampleCustomResourceName,
			"-o", "jsonpath={.spec.replicas}",
			"-n", sampleCustomResourceNamespace)
		desiredCountOutput, err := utils.Run(desiredCmd)
		g.Expect(err).NotTo(HaveOccurred())

		// Get ready pod count
		readyCmd := exec.Command("kubectl", "get", "sts", sampleCustomResourceName,
			"-o", "jsonpath={.status.readyReplicas}",
			"-n", sampleCustomResourceNamespace)
		readyCountOutput, err := utils.Run(readyCmd)
		g.Expect(err).NotTo(HaveOccurred())

		// Compare the twos
		g.Expect(readyCountOutput).To(Equal(desiredCountOutput), "stateful set ready pod count not equal to desired pod count")

		desiredCount, err = strconv.Atoi(desiredCountOutput)
		g.Expect(err).NotTo(HaveOccurred())
	}
	Eventually(verifyStatefulSetReady, 5*time.Minute).Should(Succeed())

	By("checking the service is correctly configured.")
	cmd := exec.Command("kubectl", "get", "svc", sampleCustomResourceName,
		"-o", "jsonpath={.spec.clusterIP}",
		"-n", sampleCustomResourceNamespace)
	output, err := utils.Run(cmd)
	Expect(err).NotTo(HaveOccurred())
	Expect(output).To(Equal("None"), "service clusterIP is wrong")

	By("checking the config map contains the expected host.")

	// Build the expected hostname
	var hostb strings.Builder
	for ord := range desiredCount {
		hostb.WriteString(fmt.Sprintf("%s-%d.%s.%s.svc.cluster.local:%d,",
			sampleCustomResourceName, ord, sampleCustomResourceName, sampleCustomResourceNamespace, sampleCustomResourcePort))
	}
	hostname := hostb.String()
	Expect(hostname).NotTo(Equal(""), "calculated empty hostname (is desired count equal to zero?)")
	hostname = hostname[:len(hostname)-1]

	// Check the actual hostname
	cmd = exec.Command("kubectl", "get", "cm", sampleCustomResourceName+"-connection",
		"-o", "jsonpath={.data.host}",
		"-n", sampleCustomResourceNamespace)
	output, err = utils.Run(cmd)
	Expect(err).NotTo(HaveOccurred())
	Expect(output).To(Equal(hostname), "config map hostname is wrong")

	By("checking the config map contains the expected db_name.")

	// Get the expected database name
	cmd = exec.Command("kubectl", "get", sampleCustomResourceTypeName, sampleCustomResourceName,
		"-o", "jsonpath={.spec.databaseName}",
		"-n", sampleCustomResourceNamespace)
	databaseName, err := utils.Run(cmd)
	Expect(err).NotTo(HaveOccurred())

	// Check the actual database name
	cmd = exec.Command("kubectl", "get", "cm", sampleCustomResourceName+"-connection",
		"-o", "jsonpath={.data.db_name}",
		"-n", sampleCustomResourceNamespace)
	output, err = utils.Run(cmd)
	Expect(err).NotTo(HaveOccurred())
	Expect(output).To(Equal(databaseName), "config map db_name is wrong")

	By("connecting to the reconciled custom resource")
	for idx, user := range sampleCustomResourceUsers {
		pingDatabase := func(g Gomega) {
			// Get the user's password from the secret
			passwordCmd := exec.Command(
				"kubectl", "get", "secret", user.PasswordSecretName,
				"-o", "jsonpath={.data.password}",
				"-n", sampleCustomResourceNamespace,
			)

			passwordB64, err := utils.Run(passwordCmd)
			Expect(err).NotTo(HaveOccurred())

			passwordBytes, err := base64.StdEncoding.DecodeString(passwordB64)
			Expect(err).NotTo(HaveOccurred())

			password := string(passwordBytes)

			// Build the connection string from the asserted config map values
			u := &url.URL{
				Scheme: "mongodb",
				Host:   hostname,
				Path:   "/" + databaseName,
			}
			if user.AuthSource != "" {
				u.RawQuery = url.Values{
					"authSource": []string{user.AuthSource},
				}.Encode()
			}
			u.User = url.UserPassword(user.Username, password)
			connstr := u.String()

			cmd := exec.Command("kubectl", "run", "mongo-client-"+strconv.Itoa(idx),
				"--rm", "-i", "--restart=Never", "--image=mongo:8",
				"-n", sampleCustomResourceNamespace,
				"--command", "--", "mongosh", connstr,
				"--eval", "quit(db.adminCommand({ ping: 1 }).ok == 1 ? 0 : 1)", // exit status 0 if ok, 1 if not
			)

			_, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())
		}
		Eventually(pingDatabase, 5*time.Minute).Should(Succeed())
	}
}

// tokenRequest is a simplified representation of the Kubernetes TokenRequest API response,
// containing only the token field that we need to extract.
type tokenRequest struct {
	Status struct {
		Token string `json:"token"`
	} `json:"status"`
}
