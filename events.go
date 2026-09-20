package main

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"time"
)

const (
	deploySucceededEventType            = "deploy.succeeded"
	deployFailedEventType               = "deploy.failed"
	deploymentOrchestrationStartedType  = "deployment.orchestration.started"
	deploymentOrchestrationDeployedType = "deployment.orchestration.deployed"
	deploymentOrchestrationRunningType  = "deployment.orchestration.running"
	deploymentOrchestrationFailedType   = "deployment.orchestration.failed"
	orchestrationStatus                 = "ORCHESTRATING"
	deployedStatus                      = "DEPLOYED"
	runningStatus                       = "RUNNING"
	deployFailedStatus                  = "DEPLOY_FAILED"
)

type ServiceEvent[T any] struct {
	EventID      string          `json:"eventId"`
	EventType    string          `json:"eventType"`
	Timestamp    json.RawMessage `json:"timestamp"`
	DeploymentID string          `json:"deploymentId,omitempty"`
	ProjectID    string          `json:"projectId"`
	ServiceID    string          `json:"serviceId"`
	ServiceName  string          `json:"serviceName,omitempty"`
	UserID       string          `json:"userId"`
	Payload      T               `json:"payload"`
}

type BuildSucceededPayload struct {
	ImageTag      string `json:"imageTag"`
	CommitSHA     string `json:"commitSha,omitempty"`
	Builder       string `json:"builder,omitempty"`
	ProjectSlug   string `json:"projectSlug,omitempty"`
	ServiceSlug   string `json:"serviceSlug,omitempty"`
	ContainerPort int32  `json:"containerPort,omitempty"`
}

type DeploymentEvent struct {
	EventID      string         `json:"eventId"`
	DeploymentID string         `json:"deploymentId"`
	ProjectID    string         `json:"projectId"`
	ServiceName  string         `json:"serviceName"`
	EventType    string         `json:"eventType"`
	Status       string         `json:"status"`
	Timestamp    time.Time      `json:"timestamp"`
	Metadata     map[string]any `json:"metadata,omitempty"`
}

type DeploySucceededPayload struct {
	ImageTag       string `json:"imageTag"`
	CommitSHA      string `json:"commitSha,omitempty"`
	Namespace      string `json:"namespace"`
	DeploymentName string `json:"deploymentName"`
	ServiceName    string `json:"serviceName"`
	IngressHost    string `json:"ingressHost"`
	ContainerPort  int32  `json:"containerPort"`
}

type DeployFailedPayload struct {
	ImageTag       string `json:"imageTag,omitempty"`
	CommitSHA      string `json:"commitSha,omitempty"`
	Namespace      string `json:"namespace"`
	DeploymentName string `json:"deploymentName"`
	IngressHost    string `json:"ingressHost"`
	ContainerPort  int32  `json:"containerPort"`
	ErrorMessage   string `json:"errorMessage"`
}

func newDeploySucceededEvent(
	request ServiceEvent[BuildSucceededPayload],
	target DeploymentTarget,
) ServiceEvent[DeploySucceededPayload] {
	return ServiceEvent[DeploySucceededPayload]{
		EventID:      newEventID(),
		EventType:    deploySucceededEventType,
		Timestamp:    marshalTimestamp(time.Now().UTC()),
		DeploymentID: request.DeploymentID,
		ProjectID:    request.ProjectID,
		ServiceID:    request.ServiceID,
		ServiceName:  request.ServiceName,
		UserID:       request.UserID,
		Payload: DeploySucceededPayload{
			ImageTag:       request.Payload.ImageTag,
			CommitSHA:      request.Payload.CommitSHA,
			Namespace:      target.Namespace,
			DeploymentName: target.DeploymentName,
			ServiceName:    target.ServiceName,
			IngressHost:    target.IngressHost,
			ContainerPort:  target.ContainerPort,
		},
	}
}

func newDeployFailedEvent(
	request ServiceEvent[BuildSucceededPayload],
	target DeploymentTarget,
	err error,
) ServiceEvent[DeployFailedPayload] {
	return ServiceEvent[DeployFailedPayload]{
		EventID:      newEventID(),
		EventType:    deployFailedEventType,
		Timestamp:    marshalTimestamp(time.Now().UTC()),
		DeploymentID: request.DeploymentID,
		ProjectID:    request.ProjectID,
		ServiceID:    request.ServiceID,
		ServiceName:  request.ServiceName,
		UserID:       request.UserID,
		Payload: DeployFailedPayload{
			ImageTag:       request.Payload.ImageTag,
			CommitSHA:      request.Payload.CommitSHA,
			Namespace:      target.Namespace,
			DeploymentName: target.DeploymentName,
			IngressHost:    target.IngressHost,
			ContainerPort:  target.ContainerPort,
			ErrorMessage:   err.Error(),
		},
	}
}

func newOrchestrationStartedEvent(
	request ServiceEvent[BuildSucceededPayload],
) DeploymentEvent {
	return DeploymentEvent{
		EventID:      deploymentEventID(request.DeploymentID, "orchestration", orchestrationStatus),
		DeploymentID: request.DeploymentID,
		ProjectID:    request.ProjectID,
		ServiceName:  request.ServiceName,
		EventType:    deploymentOrchestrationStartedType,
		Status:       orchestrationStatus,
		Timestamp:    time.Now().UTC(),
		Metadata: map[string]any{
			"serviceId":     request.ServiceID,
			"imageTag":      request.Payload.ImageTag,
			"commitSha":     request.Payload.CommitSHA,
			"projectSlug":   request.Payload.ProjectSlug,
			"serviceSlug":   request.Payload.ServiceSlug,
			"containerPort": request.Payload.ContainerPort,
		},
	}
}

func newOrchestrationDeployedEvent(
	request ServiceEvent[BuildSucceededPayload],
	target DeploymentTarget,
) DeploymentEvent {
	return DeploymentEvent{
		EventID:      deploymentEventID(request.DeploymentID, "orchestration", deployedStatus),
		DeploymentID: request.DeploymentID,
		ProjectID:    request.ProjectID,
		ServiceName:  request.ServiceName,
		EventType:    deploymentOrchestrationDeployedType,
		Status:       deployedStatus,
		Timestamp:    time.Now().UTC(),
		Metadata: map[string]any{
			"serviceId":             request.ServiceID,
			"imageTag":              request.Payload.ImageTag,
			"commitSha":             request.Payload.CommitSHA,
			"namespace":             target.Namespace,
			"deploymentName":        target.DeploymentName,
			"kubernetesServiceName": target.ServiceName,
			"ingressHost":           target.IngressHost,
			"containerPort":         target.ContainerPort,
			"tlsEnabled":            target.TLSEnabled,
		},
	}
}

func newOrchestrationRunningEvent(
	request ServiceEvent[BuildSucceededPayload],
	target DeploymentTarget,
) DeploymentEvent {
	return DeploymentEvent{
		EventID:      deploymentEventID(request.DeploymentID, "orchestration", runningStatus),
		DeploymentID: request.DeploymentID,
		ProjectID:    request.ProjectID,
		ServiceName:  request.ServiceName,
		EventType:    deploymentOrchestrationRunningType,
		Status:       runningStatus,
		Timestamp:    time.Now().UTC(),
		Metadata: map[string]any{
			"serviceId":             request.ServiceID,
			"imageTag":              request.Payload.ImageTag,
			"commitSha":             request.Payload.CommitSHA,
			"namespace":             target.Namespace,
			"deploymentName":        target.DeploymentName,
			"kubernetesServiceName": target.ServiceName,
			"ingressHost":           target.IngressHost,
			"containerPort":         target.ContainerPort,
			"tlsEnabled":            target.TLSEnabled,
		},
	}
}

func newOrchestrationFailedEvent(
	request ServiceEvent[BuildSucceededPayload],
	target DeploymentTarget,
	err error,
) DeploymentEvent {
	return DeploymentEvent{
		EventID:      deploymentEventID(request.DeploymentID, "orchestration", deployFailedStatus),
		DeploymentID: request.DeploymentID,
		ProjectID:    request.ProjectID,
		ServiceName:  request.ServiceName,
		EventType:    deploymentOrchestrationFailedType,
		Status:       deployFailedStatus,
		Timestamp:    time.Now().UTC(),
		Metadata: map[string]any{
			"serviceId":             request.ServiceID,
			"imageTag":              request.Payload.ImageTag,
			"commitSha":             request.Payload.CommitSHA,
			"namespace":             target.Namespace,
			"deploymentName":        target.DeploymentName,
			"kubernetesServiceName": target.ServiceName,
			"ingressHost":           target.IngressHost,
			"containerPort":         target.ContainerPort,
			"tlsEnabled":            target.TLSEnabled,
			"errorMessage":          err.Error(),
		},
	}
}

func deploymentEventID(deploymentID, stage, status string) string {
	sum := sha256.Sum256([]byte(deploymentID + ":" + stage + ":" + status))
	return hex.EncodeToString(sum[:])
}

func marshalTimestamp(timestamp time.Time) json.RawMessage {
	payload, err := json.Marshal(timestamp)
	if err != nil {
		return json.RawMessage(`null`)
	}
	return json.RawMessage(payload)
}

func newEventID() string {
	var randomBytes [16]byte
	if _, err := rand.Read(randomBytes[:]); err != nil {
		return time.Now().UTC().Format("20060102150405.000000000")
	}

	randomBytes[6] = (randomBytes[6] & 0x0f) | 0x40
	randomBytes[8] = (randomBytes[8] & 0x3f) | 0x80

	encoded := hex.EncodeToString(randomBytes[:])
	return encoded[0:8] + "-" + encoded[8:12] + "-" + encoded[12:16] + "-" + encoded[16:20] + "-" + encoded[20:32]
}
