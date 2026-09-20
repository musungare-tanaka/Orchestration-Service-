package main

import (
	"encoding/json"
	"errors"
	"testing"
)

func TestDeploymentProgressContract(t *testing.T) {
	request := ServiceEvent[BuildSucceededPayload]{DeploymentID: "deployment-1", ProjectID: "project-1", ServiceID: "service-1", ServiceName: "api"}
	target := DeploymentTarget{IngressHost: "api.example.test", TLSEnabled: true}
	cases := []struct {
		key   string
		event DeploymentEvent
	}{
		{deploymentOrchestrationStartedType, newOrchestrationStartedEvent(request)},
		{deploymentOrchestrationDeployedType, newOrchestrationDeployedEvent(request, target)},
		{deploymentOrchestrationRunningType, newOrchestrationRunningEvent(request, target)},
		{deploymentOrchestrationFailedType, newOrchestrationFailedEvent(request, target, errors.New("rollout failed"))},
	}
	for _, tc := range cases {
		t.Run(tc.key, func(t *testing.T) {
			if tc.event.EventType != tc.key {
				t.Fatalf("routing key %q != eventType %q", tc.key, tc.event.EventType)
			}
			if tc.event.EventID != deploymentEventID(tc.event.DeploymentID, "orchestration", tc.event.Status) {
				t.Fatal("event ID is not deterministic")
			}
			body, err := json.Marshal(tc.event)
			if err != nil || len(body) == 0 || tc.event.DeploymentID == "" || tc.event.Timestamp.IsZero() {
				t.Fatalf("invalid payload: %s (%v)", body, err)
			}
			if tc.event.EventType != deploymentOrchestrationStartedType && tc.event.Metadata["tlsEnabled"] != true {
				t.Fatal("TLS metadata missing")
			}
		})
	}
}
