// Copyright 2026 The Agent Substrate Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"go.ate.dev/substrate/internal/podidentity"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
)

func main() {
	kubeconfig := flag.String("kubeconfig", "", "Kubeconfig file")
	kubeContext := flag.String("context", "kind-agentcloud", "Kubeconfig context")
	out := flag.String("out", "", "Certificate output directory")
	flag.Parse()
	if *out == "" {
		fmt.Fprintln(os.Stderr, "--out is required")
		os.Exit(1)
	}
	if err := run(*kubeconfig, *kubeContext, *out); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func run(kubeconfig, kubeContext, out string) error {
	rules := clientcmd.NewDefaultClientConfigLoadingRules()
	rules.ExplicitPath = kubeconfig
	config, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(rules, &clientcmd.ConfigOverrides{CurrentContext: kubeContext}).ClientConfig()
	if err != nil {
		return err
	}
	client, err := kubernetes.NewForConfig(config)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	cert, err := podidentity.RequestCertificate(ctx, client, "agentcloud", "ate-system", "ate-client", 2*time.Minute)
	if err != nil {
		return err
	}
	if err := writeCertificate(cert, out); err != nil {
		return err
	}
	fmt.Printf("Router client certificate expires at %s\n", cert.Leaf.NotAfter.Format(time.RFC3339))
	return nil
}

func writeCertificate(cert tls.Certificate, out string) error {
	if err := os.MkdirAll(out, 0700); err != nil {
		return err
	}
	var chain []byte
	for _, der := range cert.Certificate {
		chain = append(chain, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})...)
	}
	key, err := x509.MarshalPKCS8PrivateKey(cert.PrivateKey)
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(out, "router-key.pem"), pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: key}), 0600); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(out, "router-cert.pem"), chain, 0600); err != nil {
		return err
	}
	return nil
}
