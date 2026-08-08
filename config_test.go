package main

import (
	"testing"
	"time"
)

func TestLoadConfigDefaults(t *testing.T) {
	t.Setenv("PORT", "")
	t.Setenv("RABBITMQ_URL", "")
	t.Setenv("RABBITMQ_EXCHANGE", "")
	t.Setenv("PLATFORM_BASE_DOMAIN", "")
	t.Setenv("K8S_DEFAULT_REPLICAS", "")
	t.Setenv("K8S_ROLLOUT_TIMEOUT", "")

	cfg := loadConfig()

	if cfg.HTTPPort != "8082" {
		t.Fatalf("expected default port 8082, got %q", cfg.HTTPPort)
	}
	if cfg.AppDeployQueue != "shiply.app.deploy" {
		t.Fatalf("unexpected default queue %q", cfg.AppDeployQueue)
	}
	if cfg.DefaultReplicas != 1 {
		t.Fatalf("expected default replicas 1, got %d", cfg.DefaultReplicas)
	}
	if cfg.RolloutTimeout != 5*time.Minute {
		t.Fatalf("expected default rollout timeout 5m, got %s", cfg.RolloutTimeout)
	}
}

func TestLoadConfigEnvOverrides(t *testing.T) {
	t.Setenv("PORT", "9090")
	t.Setenv("RABBITMQ_APP_DEPLOY_QUEUE", "custom.deploy.queue")
	t.Setenv("PLATFORM_BASE_DOMAIN", "apps.shiply.test")
	t.Setenv("K8S_DEFAULT_REPLICAS", "3")
	t.Setenv("K8S_CONTAINER_PORT_DEFAULT", "9000")
	t.Setenv("K8S_ROLLOUT_TIMEOUT", "90s")

	cfg := loadConfig()

	if cfg.HTTPPort != "9090" {
		t.Fatalf("expected overridden port, got %q", cfg.HTTPPort)
	}
	if cfg.AppDeployQueue != "custom.deploy.queue" {
		t.Fatalf("expected overridden queue, got %q", cfg.AppDeployQueue)
	}
	if cfg.BaseDomain != "apps.shiply.test" {
		t.Fatalf("expected base domain override, got %q", cfg.BaseDomain)
	}
	if cfg.DefaultReplicas != 3 {
		t.Fatalf("expected replicas 3, got %d", cfg.DefaultReplicas)
	}
	if cfg.DefaultContainerPort != 9000 {
		t.Fatalf("expected container port 9000, got %d", cfg.DefaultContainerPort)
	}
	if cfg.RolloutTimeout != 90*time.Second {
		t.Fatalf("expected rollout timeout 90s, got %s", cfg.RolloutTimeout)
	}
}
