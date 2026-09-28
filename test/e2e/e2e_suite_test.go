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
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/mrhachi/mongodb-operator/test/utils"
)

var (
	// controllerImage is the controller image to be built and loaded for testing.
	controllerImage = "ghcr.io/mrhachi/mongodb-controller:stable"
	// instanceManagerImage is the instance manager image to be built and loaded for testing.
	// Version needs to be set to the version specified in the controller-managed Pod template.
	instanceManagerImage = "ghcr.io/mrhachi/mongodb-instance-manager:v0.1"
	// databaseImage is the database image to be built and loaded for testing.
	databaseImage = "ghcr.io/mrhachi/mongodb:8.3.7-stable"
	// shouldCleanupCertManager tracks whether CertManager was installed by this suite.
	shouldCleanupCertManager = false
)

const (
	controllerNamePrefix = "mongodb-controller"
	// controllerNamespace                    = controllerNamePrefix + "-system"
	controllerName                   = controllerNamePrefix + "-mongodb-controller"
	controllerMetricsServiceName     = controllerNamePrefix + "-mongodb-controller-metrics-service"
	controllerMetricsRoleBindingName = controllerNamePrefix + "-metrics-binding"
	// sampleCustomResourceTypeName     = "mongodb"
	// sampleCustomResourceName         = sampleCustomResourceTypeName + "-sample"
	// sampleCustomResourceNamespace          = "cr-test"
	sampleCustomResourcePort               = 27017
	sampleTemplatePath                     = "config/samples/"
	sampleLegacyCustomResourceTemplateName = "db_v1alphav1_singletenantmongodb.yaml"
	sampleCustomResourceTemplateName       = "db_v1alphav2_mongodb.yaml"
)

// TestE2E runs the e2e test suite to validate the solution in an isolated environment.
// The default setup requires Kind and CertManager.
//
// To enable kubectl kuberc (use custom kubectl configurations), set: KUBECTL_KUBERC=true
// By default, kuberc is disabled to ensure consistent test behavior across different environments.
// To skip CertManager installation, set: CERT_MANAGER_INSTALL_SKIP=true
func TestE2E(t *testing.T) {
	RegisterFailHandler(Fail)
	_, _ = fmt.Fprintf(GinkgoWriter, "Starting controller e2e test suite\n")
	RunSpecs(t, "e2e suite")
}

var _ = BeforeSuite(func() {
	By("building the controller image")
	cmd := exec.Command("make", "docker-build", fmt.Sprintf("IMG=%s", controllerImage))
	_, err := utils.Run(cmd)
	ExpectWithOffset(1, err).NotTo(HaveOccurred(), "Failed to build the controller image")

	By("loading the controller image on Kind")
	err = utils.LoadImageToKindClusterWithName(controllerImage)
	ExpectWithOffset(1, err).NotTo(HaveOccurred(), "Failed to load the controller image into Kind")

	By("building the instance manager image")
	cmd = exec.Command("make", "instance-manager-docker-build", fmt.Sprintf("IMGR_IMG=%s", instanceManagerImage))
	_, err = utils.Run(cmd)
	ExpectWithOffset(1, err).NotTo(HaveOccurred(), "Failed to build the instance manager image")

	By("loading the instance manage image on Kind")
	err = utils.LoadImageToKindClusterWithName(instanceManagerImage)
	ExpectWithOffset(1, err).NotTo(HaveOccurred(), "Failed to load the instance manage image into Kind")

	// Legacy
	By("building the database image")
	cmd = exec.Command("make", "database-docker-build", fmt.Sprintf("DB_IMG=%s", databaseImage))
	_, err = utils.Run(cmd)
	ExpectWithOffset(1, err).NotTo(HaveOccurred(), "Failed to build the database image")

	By("loading the database image on Kind")
	err = utils.LoadImageToKindClusterWithName(databaseImage)
	ExpectWithOffset(1, err).NotTo(HaveOccurred(), "Failed to load the database image into Kind")

	configureKubectlKubeRC()
	setupCertManager()
})

var _ = AfterSuite(func() {
	teardownCertManager()
})

// Disable kubectl kuberc by default for test isolation.
// This prevents local kubectl configurations from affecting test behavior.
// To enable kuberc, set: KUBECTL_KUBERC=true
func configureKubectlKubeRC() {
	if os.Getenv("KUBECTL_KUBERC") != "true" {
		By("disabling kubectl kuberc for test isolation")
		err := os.Setenv("KUBECTL_KUBERC", "false")
		ExpectWithOffset(1, err).NotTo(HaveOccurred(), "Failed to disable kubectl kuberc")
		_, _ = fmt.Fprintf(GinkgoWriter,
			"kubectl kuberc disabled for consistent test behavior (override with KUBECTL_KUBERC=true)\n")
	} else {
		_, _ = fmt.Fprintf(GinkgoWriter, "kubectl kuberc enabled (KUBECTL_KUBERC=true)\n")
	}
}

// setupCertManager installs CertManager if needed for webhook tests.
// Skips installation if CERT_MANAGER_INSTALL_SKIP=true or if already present.
func setupCertManager() {
	if os.Getenv("CERT_MANAGER_INSTALL_SKIP") == "true" {
		_, _ = fmt.Fprintf(GinkgoWriter, "Skipping CertManager installation (CERT_MANAGER_INSTALL_SKIP=true)\n")
		return
	}

	By("checking if CertManager is already installed")
	if utils.IsCertManagerCRDsInstalled() {
		_, _ = fmt.Fprintf(GinkgoWriter, "CertManager is already installed. Skipping installation.\n")
		return
	}

	// Mark for cleanup before installation to handle interruptions and partial installs.
	shouldCleanupCertManager = true

	By("installing CertManager")
	Expect(utils.InstallCertManager()).To(Succeed(), "Failed to install CertManager")
}

// teardownCertManager uninstalls CertManager if it was installed by setupCertManager.
// This ensures we only remove what we installed.
func teardownCertManager() {
	if !shouldCleanupCertManager {
		_, _ = fmt.Fprintf(GinkgoWriter, "Skipping CertManager cleanup (not installed by this suite)\n")
		return
	}

	By("uninstalling CertManager")
	utils.UninstallCertManager()
}

type SampleUser struct {
	Username           string
	PasswordSecretName string
	AuthSource         string
}

// serviceAccountToken returns a token for the specified service account in the given namespace.
// It uses the Kubernetes TokenRequest API to generate a token by directly sending a request
// and parsing the resulting token from the API response.
func serviceAccountToken(saName, namespace string) (string, error) {
	const tokenRequestRawString = `{
		"apiVersion": "authentication.k8s.io/v1",
		"kind": "TokenRequest"
	}`

	By("creating temporary file to store the token request")
	secretName := fmt.Sprintf("%s-token-request", saName)
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
			namespace,
			saName,
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

// tokenRequest is a simplified representation of the Kubernetes TokenRequest API response,
// containing only the token field that we need to extract.
type tokenRequest struct {
	Status struct {
		Token string `json:"token"`
	} `json:"status"`
}
