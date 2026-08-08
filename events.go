package main

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"time"
)

const (
	deploySucceededEventType = "deploy.succeeded"
	deployFailedEventType    = "deploy.failed"
)

type ServiceEvent[T any] struct {
	EventID   string          `json:"eventId"`
	EventType string          `json:"eventType"`
	Timestamp json.RawMessage `json:"timestamp"`
	ProjectID string          `json:"projectId"`
	ServiceID string          `json:"serviceId"`
	UserID    string          `json:"userId"`
	Payload   T               `json:"payload"`
}

type BuildSucceededPayload struct {
	ImageTag      string `json:"imageTag"`
	CommitSHA     string `json:"commitSha,omitempty"`
	Builder       string `json:"builder,omitempty"`
	ProjectSlug   string `json:"projectSlug,omitempty"`
	ServiceSlug   string `json:"serviceSlug,omitempty"`
	ContainerPort int32  `json:"containerPort,omitempty"`
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
		EventID:   newEventID(),
		EventType: deploySucceededEventType,
		Timestamp: marshalTimestamp(time.Now().UTC()),
		ProjectID: request.ProjectID,
		ServiceID: request.ServiceID,
		UserID:    request.UserID,
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
		EventID:   newEventID(),
		EventType: deployFailedEventType,
		Timestamp: marshalTimestamp(time.Now().UTC()),
		ProjectID: request.ProjectID,
		ServiceID: request.ServiceID,
		UserID:    request.UserID,
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
