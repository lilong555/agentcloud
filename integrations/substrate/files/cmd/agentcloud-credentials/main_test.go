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
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"os"
	"path/filepath"
	"testing"
)

func TestWriteCertificate(t *testing.T) {
	_, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	certificate := tls.Certificate{Certificate: [][]byte{[]byte("leaf-der"), []byte("ca-der")}, PrivateKey: private}
	directory := filepath.Join(t.TempDir(), "credentials")
	if err := writeCertificate(certificate, directory); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"router-key.pem", "router-cert.pem"} {
		info, err := os.Stat(filepath.Join(directory, name))
		if err != nil {
			t.Fatal(err)
		}
		if info.Mode().Perm() != 0600 {
			t.Fatalf("%s permissions=%o", name, info.Mode().Perm())
		}
	}
	encoded, _ := os.ReadFile(filepath.Join(directory, "router-cert.pem"))
	for _, want := range certificate.Certificate {
		block, rest := pem.Decode(encoded)
		if block == nil || block.Type != "CERTIFICATE" || string(block.Bytes) != string(want) {
			t.Fatalf("certificate chain=%q", encoded)
		}
		encoded = rest
	}
	if len(encoded) != 0 {
		t.Fatalf("unexpected chain suffix=%q", encoded)
	}
	encoded, _ = os.ReadFile(filepath.Join(directory, "router-key.pem"))
	block, _ := pem.Decode(encoded)
	if block == nil || block.Type != "PRIVATE KEY" {
		t.Fatalf("private key PEM=%q", encoded)
	}
	decoded, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil || !private.Equal(decoded) {
		t.Fatalf("private key mismatch: %v", err)
	}
}

func TestWriteCertificateErrors(t *testing.T) {
	directory := t.TempDir()
	obstruction := filepath.Join(directory, "file")
	if err := os.WriteFile(obstruction, []byte("occupied"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := writeCertificate(tls.Certificate{}, filepath.Join(obstruction, "child")); err == nil {
		t.Fatal("expected directory error")
	}
	if err := writeCertificate(tls.Certificate{PrivateKey: struct{}{}}, directory); err == nil {
		t.Fatal("expected unsupported key error")
	}
}

func TestRunRejectsMissingKubeconfig(t *testing.T) {
	if err := run(filepath.Join(t.TempDir(), "missing"), "kind-agentcloud", t.TempDir()); err == nil {
		t.Fatal("expected kubeconfig error")
	}
}
