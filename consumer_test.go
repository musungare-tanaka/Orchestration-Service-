package main

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/rabbitmq/amqp091-go"
)

type stubDeploymentManager struct {
	target          DeploymentTarget
	deployErr       error
	rolloutErr      error
	lastDeployInput DeployRequest
}

func (s *stubDeploymentManager) Deploy(_ context.Context, request DeployRequest) (DeploymentTarget, error) {
	s.lastDeployInput = request
	return s.target, s.deployErr
}

func (s *stubDeploymentManager) WaitForRollout(_ context.Context, _ DeploymentTarget) error {
	return s.rolloutErr
}

type ackRecorder struct {
	ackCount    int
	nackCount   int
	lastRequeue bool
}

func (a *ackRecorder) Ack(_ uint64, _ bool) error {
	a.ackCount++
	return nil
}

func (a *ackRecorder) Nack(_ uint64, _ bool, requeue bool) error {
	a.nackCount++
	a.lastRequeue = requeue
	return nil
}

func (a *ackRecorder) Reject(_ uint64, _ bool) error {
	return nil
}

func TestProcessDeliveryAcksAfterPublishingDeploySucceeded(t *testing.T) {
	manager := &stubDeploymentManager{
		target: DeploymentTarget{
			Namespace:      "shiply-prj-demo",
			DeploymentName: "app-api",
			ServiceName:    "app-api",
			IngressHost:    "api-demo.apps.shiply.test",
			ContainerPort:  8080,
		},
	}

	var published any
	consumer := &DeployConsumer{
		cfg: Config{
			DeploymentExchange:              "deployment.events",
			RabbitMQExchange:                "shiply.services",
			BaseDomain:                      "apps.shiply.test",
			NamespacePrefix:                 "shiply-prj",
			DefaultContainerPort:            8080,
			RolloutTimeout:                  time.Minute,
			DeploySucceededRoutingKey:       "deploy.succeeded",
			OrchestrationStartedRoutingKey:  "orchestration.started",
			OrchestrationDeployedRoutingKey: "orchestration.deployed",
			OrchestrationRunningRoutingKey:  "orchestration.running",
		},
		deployer: manager,
		publish: func(_ context.Context, exchange, routingKey string, event any) error {
			if exchange == "deployment.events" {
				return nil
			}
			if exchange != "shiply.services" || routingKey != "deploy.succeeded" {
				t.Fatalf("unexpected publish target %q %q", exchange, routingKey)
			}
			published = event
			return nil
		},
	}

	recorder := &ackRecorder{}
	consumer.processDelivery(amqp091.Delivery{
		Body:         mustMarshalDeployRequest(t),
		Acknowledger: recorder,
		DeliveryTag:  1,
	})

	if recorder.ackCount != 1 {
		t.Fatalf("expected ack once, got %d", recorder.ackCount)
	}
	if recorder.nackCount != 0 {
		t.Fatalf("expected no nack, got %d", recorder.nackCount)
	}

	event, ok := published.(ServiceEvent[DeploySucceededPayload])
	if !ok {
		t.Fatalf("expected deploy success event, got %T", published)
	}
	if event.Payload.IngressHost != "api-demo.apps.shiply.test" {
		t.Fatalf("unexpected ingress host %q", event.Payload.IngressHost)
	}
}

func TestProcessDeliveryPublishesDeployFailureAndAcks(t *testing.T) {
	manager := &stubDeploymentManager{
		target: DeploymentTarget{
			Namespace:      "shiply-prj-demo",
			DeploymentName: "app-api",
			IngressHost:    "api-demo.apps.shiply.test",
			ContainerPort:  8080,
		},
		rolloutErr: errors.New("rollout failed"),
	}

	var published any
	consumer := &DeployConsumer{
		cfg: Config{
			DeploymentExchange:             "deployment.events",
			RabbitMQExchange:               "shiply.services",
			BaseDomain:                     "apps.shiply.test",
			NamespacePrefix:                "shiply-prj",
			DefaultContainerPort:           8080,
			RolloutTimeout:                 time.Minute,
			DeployFailedRoutingKey:         "deploy.failed",
			DeploySucceededRoutingKey:      "deploy.succeeded",
			OrchestrationStartedRoutingKey: "orchestration.started",
			OrchestrationFailedRoutingKey:  "orchestration.failed",
		},
		deployer: manager,
		publish: func(_ context.Context, exchange, routingKey string, event any) error {
			if exchange == "deployment.events" {
				return nil
			}
			if exchange != "shiply.services" || routingKey != "deploy.failed" {
				t.Fatalf("unexpected publish target %q %q", exchange, routingKey)
			}
			published = event
			return nil
		},
	}

	recorder := &ackRecorder{}
	consumer.processDelivery(amqp091.Delivery{
		Body:         mustMarshalDeployRequest(t),
		Acknowledger: recorder,
		DeliveryTag:  2,
	})

	if recorder.ackCount != 1 {
		t.Fatalf("expected ack once, got %d", recorder.ackCount)
	}
	if recorder.nackCount != 0 {
		t.Fatalf("expected no nack, got %d", recorder.nackCount)
	}

	event, ok := published.(ServiceEvent[DeployFailedPayload])
	if !ok {
		t.Fatalf("expected deploy failure event, got %T", published)
	}
	if event.Payload.ErrorMessage != "rollout failed" {
		t.Fatalf("unexpected error message %q", event.Payload.ErrorMessage)
	}
}

func TestProcessDeliveryRequeuesWhenPublishingFails(t *testing.T) {
	manager := &stubDeploymentManager{
		target: DeploymentTarget{
			Namespace:      "shiply-prj-demo",
			DeploymentName: "app-api",
			ServiceName:    "app-api",
			IngressHost:    "api-demo.apps.shiply.test",
			ContainerPort:  8080,
		},
	}

	consumer := &DeployConsumer{
		cfg: Config{
			DeploymentExchange:              "deployment.events",
			RabbitMQExchange:                "shiply.services",
			BaseDomain:                      "apps.shiply.test",
			NamespacePrefix:                 "shiply-prj",
			DefaultContainerPort:            8080,
			RolloutTimeout:                  time.Minute,
			DeploySucceededRoutingKey:       "deploy.succeeded",
			OrchestrationStartedRoutingKey:  "orchestration.started",
			OrchestrationDeployedRoutingKey: "orchestration.deployed",
			OrchestrationRunningRoutingKey:  "orchestration.running",
		},
		deployer: manager,
		publish: func(_ context.Context, _ string, _ string, _ any) error {
			return errors.New("rabbitmq unavailable")
		},
	}

	recorder := &ackRecorder{}
	consumer.processDelivery(amqp091.Delivery{
		Body:         mustMarshalDeployRequest(t),
		Acknowledger: recorder,
		DeliveryTag:  3,
	})

	if recorder.ackCount != 0 {
		t.Fatalf("expected no ack, got %d", recorder.ackCount)
	}
	if recorder.nackCount != 1 || !recorder.lastRequeue {
		t.Fatalf("expected nack with requeue, got nack=%d requeue=%v", recorder.nackCount, recorder.lastRequeue)
	}
}

func TestProcessDeliveryDropsMalformedMessage(t *testing.T) {
	consumer := &DeployConsumer{}
	recorder := &ackRecorder{}

	consumer.processDelivery(amqp091.Delivery{
		Body:         []byte("{not-json"),
		Acknowledger: recorder,
		DeliveryTag:  4,
	})

	if recorder.ackCount != 0 {
		t.Fatalf("expected no ack, got %d", recorder.ackCount)
	}
	if recorder.nackCount != 1 || recorder.lastRequeue {
		t.Fatalf("expected nack without requeue, got nack=%d requeue=%v", recorder.nackCount, recorder.lastRequeue)
	}
}

func mustMarshalDeployRequest(t *testing.T) []byte {
	t.Helper()

	event := ServiceEvent[BuildSucceededPayload]{
		EventID:      "event-1",
		EventType:    "build.succeeded",
		Timestamp:    json.RawMessage(`"2026-08-08T10:00:00Z"`),
		DeploymentID: "deployment-1",
		ProjectID:    "project-1",
		ServiceID:    "service-1",
		ServiceName:  "Shiply API",
		UserID:       "user-1",
		Payload: BuildSucceededPayload{
			ImageTag:      "ghcr.io/shiply/project/service:abc123",
			CommitSHA:     "abc123",
			ProjectSlug:   "demo",
			ServiceSlug:   "api",
			ContainerPort: 8080,
		},
	}

	body, err := json.Marshal(event)
	if err != nil {
		t.Fatalf("marshal deploy request: %v", err)
	}
	return body
}
