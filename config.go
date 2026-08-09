package main

import (
	"os"
	"strconv"
	"strings"
	"time"
)

type RegistryConfig struct {
	Host     string
	Username string
	Password string
}

type ResourceDefaults struct {
	CPURequest    string
	CPULimit      string
	MemoryRequest string
	MemoryLimit   string
}

type Config struct {
	HTTPPort                        string
	RabbitMQURL                     string
	RabbitMQExchange                string
	DeploymentExchange              string
	AppDeployQueue                  string
	BuildSucceededRoutingKey        string
	DeploySucceededRoutingKey       string
	DeployFailedRoutingKey          string
	OrchestrationStartedRoutingKey  string
	OrchestrationDeployedRoutingKey string
	OrchestrationRunningRoutingKey  string
	OrchestrationFailedRoutingKey   string
	KubeconfigPath                  string
	BaseDomain                      string
	IngressClassName                string
	NamespacePrefix                 string
	DefaultReplicas                 int32
	DefaultContainerPort            int32
	RolloutTimeout                  time.Duration
	RolloutPollInterval             time.Duration
	ImagePullSecretName             string
	TLSSecretName                   string
	DeploymentProgressDeadline      int32
	ResourceDefaults                ResourceDefaults
	Registry                        RegistryConfig
}

func loadConfig() Config {
	rolloutTimeout := envDurationOrDefault("K8S_ROLLOUT_TIMEOUT", 5*time.Minute)

	return Config{
		HTTPPort:                        envOrDefault("PORT", "8082"),
		RabbitMQURL:                     envOrDefault("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/"),
		RabbitMQExchange:                envOrDefault("RABBITMQ_EXCHANGE", "shiply.services"),
		DeploymentExchange:              envOrDefault("RABBITMQ_DEPLOYMENT_EXCHANGE", "deployment.events"),
		AppDeployQueue:                  envOrDefault("RABBITMQ_APP_DEPLOY_QUEUE", "shiply.app.deploy"),
		BuildSucceededRoutingKey:        envOrDefault("RABBITMQ_BUILD_SUCCEEDED_ROUTING_KEY", "build.succeeded"),
		DeploySucceededRoutingKey:       envOrDefault("RABBITMQ_DEPLOY_SUCCEEDED_ROUTING_KEY", "deploy.succeeded"),
		DeployFailedRoutingKey:          envOrDefault("RABBITMQ_DEPLOY_FAILED_ROUTING_KEY", "deploy.failed"),
		OrchestrationStartedRoutingKey:  envOrDefault("RABBITMQ_ORCHESTRATION_STARTED_ROUTING_KEY", "orchestration.started"),
		OrchestrationDeployedRoutingKey: envOrDefault("RABBITMQ_ORCHESTRATION_DEPLOYED_ROUTING_KEY", "orchestration.deployed"),
		OrchestrationRunningRoutingKey:  envOrDefault("RABBITMQ_ORCHESTRATION_RUNNING_ROUTING_KEY", "orchestration.running"),
		OrchestrationFailedRoutingKey:   envOrDefault("RABBITMQ_ORCHESTRATION_FAILED_ROUTING_KEY", "orchestration.failed"),
		KubeconfigPath:                  os.Getenv("KUBECONFIG"),
		BaseDomain:                      strings.TrimSpace(os.Getenv("PLATFORM_BASE_DOMAIN")),
		IngressClassName:                envOrDefault("K8S_INGRESS_CLASS", "traefik"),
		NamespacePrefix:                 envOrDefault("K8S_NAMESPACE_PREFIX", "shiply-prj"),
		DefaultReplicas:                 envInt32OrDefault("K8S_DEFAULT_REPLICAS", 1),
		DefaultContainerPort:            envInt32OrDefault("K8S_CONTAINER_PORT_DEFAULT", 8080),
		RolloutTimeout:                  rolloutTimeout,
		RolloutPollInterval:             envDurationOrDefault("K8S_ROLLOUT_POLL_INTERVAL", 5*time.Second),
		ImagePullSecretName:             envOrDefault("K8S_IMAGE_PULL_SECRET_NAME", "shiply-registry"),
		TLSSecretName:                   strings.TrimSpace(os.Getenv("K8S_TLS_SECRET_NAME")),
		DeploymentProgressDeadline:      int32(maxInt64(int64(rolloutTimeout/time.Second), 60)),
		ResourceDefaults: ResourceDefaults{
			CPURequest:    envOrDefault("K8S_CPU_REQUEST", "100m"),
			CPULimit:      envOrDefault("K8S_CPU_LIMIT", "500m"),
			MemoryRequest: envOrDefault("K8S_MEMORY_REQUEST", "128Mi"),
			MemoryLimit:   envOrDefault("K8S_MEMORY_LIMIT", "512Mi"),
		},
		Registry: RegistryConfig{
			Host:     strings.TrimSpace(os.Getenv("CONTAINER_REGISTRY_HOST")),
			Username: strings.TrimSpace(os.Getenv("CONTAINER_REGISTRY_USERNAME")),
			Password: os.Getenv("CONTAINER_REGISTRY_PASSWORD"),
		},
	}
}

func envOrDefault(key, fallback string) string {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	return value
}

func envInt32OrDefault(key string, fallback int32) int32 {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}

	parsed, err := strconv.Atoi(value)
	if err != nil || parsed < 1 {
		return fallback
	}
	return int32(parsed)
}

func envDurationOrDefault(key string, fallback time.Duration) time.Duration {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}

	parsed, err := time.ParseDuration(value)
	if err != nil || parsed <= 0 {
		return fallback
	}
	return parsed
}

func maxInt64(first, second int64) int64 {
	if first > second {
		return first
	}
	return second
}
