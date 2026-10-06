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
	"bytes"
	"os/exec"
	"path/filepath"
	"runtime"
	"text/template"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/MrHachi/mongodb-operator/test/utils"
)

func createMongoDBTestNamespace(testNamespace string) {
	By("creating namespace " + testNamespace)
	cmd := exec.Command("kubectl", "create", "namespace", testNamespace)
	_, err := utils.Run(cmd)
	Expect(err).NotTo(HaveOccurred())
	DeferCleanup(func() {
		cmd := exec.Command("kubectl", "delete", "namespace", testNamespace, "--wait=false")
		_, _ = utils.Run(cmd)
	})
}

func createMongoDBResource(testNamespace, clusterName string) {
	By("applying MongoDB resource " + clusterName)

	_, sourceFile, _, ok := runtime.Caller(0)
	Expect(ok).To(BeTrue(), "Failed to locate e2e test source file")
	manifestPath := filepath.Join(filepath.Dir(sourceFile), "..", "data", "mongodb-initializing.yaml")
	manifestTemplate, err := template.ParseFiles(manifestPath)
	Expect(err).NotTo(HaveOccurred())

	var manifest bytes.Buffer
	Expect(manifestTemplate.Execute(&manifest, struct{ Name, Namespace string }{clusterName, testNamespace})).To(Succeed())

	cmd := exec.Command("kubectl", "apply", "-f", "-")
	cmd.Stdin = &manifest
	_, err = utils.Run(cmd)
	Expect(err).NotTo(HaveOccurred())
}

func createMongoDBTestResource(testNamespace, clusterName string) {
	createMongoDBTestNamespace(testNamespace)
	createMongoDBResource(testNamespace, clusterName)
}

func waitForMongoDBStatus(testNamespace, clusterName, phase, reason string, timeout time.Duration) {
	Eventually(func(g Gomega) {
		cmd := exec.Command("kubectl", "get", "mongodb", clusterName, "-n", testNamespace,
			"-o", "jsonpath={.status.phase}{\"/\"}{.status.conditions[?(@.type=='Progressing')].reason}{\"/\"}{.status.conditions[?(@.type=='Degraded')].reason}")
		output, err := utils.Run(cmd)
		g.Expect(err).NotTo(HaveOccurred())
		if phase == "Degraded" {
			g.Expect(output).To(Equal(phase + "/NotProgressing/" + reason))
		} else {
			g.Expect(output).To(Equal(phase + "/" + reason + "/NoKnownIssues"))
		}
	}, timeout).Should(Succeed())
}
