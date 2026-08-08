package main

import (
	"strings"
	"testing"
)

func TestTargetForRequest(t *testing.T) {
	cfg := Config{
		BaseDomain:      "apps.shiply.test",
		NamespacePrefix: "shiply-prj",
	}

	target, err := targetForRequest(cfg, DeployRequest{
		ProjectSlug:   "Shiply Demo",
		ServiceSlug:   "Web API",
		ContainerPort: 8080,
	})
	if err != nil {
		t.Fatalf("targetForRequest returned error: %v", err)
	}

	if target.Namespace != "shiply-prj-shiply-demo" {
		t.Fatalf("unexpected namespace %q", target.Namespace)
	}
	if target.DeploymentName != "app-web-api" {
		t.Fatalf("unexpected deployment name %q", target.DeploymentName)
	}
	if target.IngressHost != "web-api-shiply-demo.apps.shiply.test" {
		t.Fatalf("unexpected ingress host %q", target.IngressHost)
	}
}

func TestSanitizeDNSLabelTruncates(t *testing.T) {
	value := sanitizeDNSLabel(strings.Repeat("a", 80), "fallback")
	if len(value) != 63 {
		t.Fatalf("expected truncated label length 63, got %d", len(value))
	}
}
