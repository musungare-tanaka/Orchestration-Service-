package main

import (
	"strings"
	"testing"
	"unicode/utf8"
)

func TestNewDeploySucceededEvent(t *testing.T) {
	event := newDeploySucceededEvent(
		ServiceEvent[BuildSucceededPayload]{
			ProjectID: "project-1",
			ServiceID: "service-1",
			UserID:    "user-1",
			Payload: BuildSucceededPayload{
				ImageTag:  "ghcr.io/shiply/app:abc123",
				CommitSHA: "abc123",
			},
		},
		DeploymentTarget{
			Namespace:      "shiply-prj-demo",
			DeploymentName: "app-api",
			ServiceName:    "app-api",
			IngressHost:    "api-demo.apps.shiply.test",
			ContainerPort:  8080,
		},
	)

	if event.EventType != deploySucceededEventType {
		t.Fatalf("expected %q, got %q", deploySucceededEventType, event.EventType)
	}
	if event.Payload.Namespace != "shiply-prj-demo" {
		t.Fatalf("unexpected namespace %q", event.Payload.Namespace)
	}
	if event.Payload.ContainerPort != 8080 {
		t.Fatalf("unexpected container port %d", event.Payload.ContainerPort)
	}
}

func TestNewDeployFailedEvent(t *testing.T) {
	event := newDeployFailedEvent(
		ServiceEvent[BuildSucceededPayload]{
			ProjectID: "project-1",
			ServiceID: "service-1",
			UserID:    "user-1",
			Payload: BuildSucceededPayload{
				ImageTag:  "ghcr.io/shiply/app:abc123",
				CommitSHA: "abc123",
			},
		},
		DeploymentTarget{
			Namespace:      "shiply-prj-demo",
			DeploymentName: "app-api",
			IngressHost:    "api-demo.apps.shiply.test",
			ContainerPort:  8080,
		},
		errTest("rollout failed"),
	)

	if event.EventType != deployFailedEventType {
		t.Fatalf("expected %q, got %q", deployFailedEventType, event.EventType)
	}
	if event.Payload.ErrorMessage != "rollout failed" {
		t.Fatalf("unexpected error message %q", event.Payload.ErrorMessage)
	}
}

func TestDeployFailedMessageSanitizesAndTruncates(t *testing.T) {
	message := string([]byte{'b', 'a', 'd', 0xff}) + strings.Repeat("x", 2100)
	event := newDeployFailedEvent(ServiceEvent[BuildSucceededPayload]{}, DeploymentTarget{}, errTest(message))
	if !utf8.ValidString(event.Payload.ErrorMessage) {
		t.Fatal("failure message is not valid UTF-8")
	}
	if got := len([]rune(event.Payload.ErrorMessage)); got != 2000 {
		t.Fatalf("failure message length = %d runes, want 2000", got)
	}
}

type errTest string

func (e errTest) Error() string { return string(e) }
