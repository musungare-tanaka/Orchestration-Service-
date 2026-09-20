package main

import (
	"fmt"
	"strings"
)

type DeploymentTarget struct {
	Namespace      string
	DeploymentName string
	ServiceName    string
	IngressName    string
	IngressHost    string
	ContainerPort  int32
	TLSEnabled     bool
}

type DeployRequest struct {
	ProjectID     string
	ServiceID     string
	ServiceName   string
	UserID        string
	DeploymentID  string
	ImageTag      string
	CommitSHA     string
	ProjectSlug   string
	ServiceSlug   string
	ContainerPort int32
}

func deployRequestFromEvent(cfg Config, event ServiceEvent[BuildSucceededPayload]) DeployRequest {
	port := event.Payload.ContainerPort
	if port <= 0 {
		port = cfg.DefaultContainerPort
	}

	return DeployRequest{
		ProjectID:     event.ProjectID,
		ServiceID:     event.ServiceID,
		ServiceName:   firstNonEmpty(event.ServiceName, event.ServiceID),
		UserID:        event.UserID,
		DeploymentID:  event.DeploymentID,
		ImageTag:      strings.TrimSpace(event.Payload.ImageTag),
		CommitSHA:     strings.TrimSpace(event.Payload.CommitSHA),
		ProjectSlug:   firstNonEmpty(event.Payload.ProjectSlug, event.ProjectID),
		ServiceSlug:   firstNonEmpty(event.Payload.ServiceSlug, event.ServiceID),
		ContainerPort: port,
	}
}

func targetForRequest(cfg Config, request DeployRequest) (DeploymentTarget, error) {
	projectSlug := sanitizeDNSLabel(request.ProjectSlug, "project")
	serviceSlug := sanitizeDNSLabel(request.ServiceSlug, "service")
	host, err := buildIngressHost(serviceSlug, projectSlug, cfg.BaseDomain)
	if err != nil {
		return DeploymentTarget{}, err
	}

	return DeploymentTarget{
		Namespace:      buildNamespaceName(cfg.NamespacePrefix, projectSlug),
		DeploymentName: buildResourceName("app", serviceSlug),
		ServiceName:    buildResourceName("app", serviceSlug),
		IngressName:    buildResourceName("app", serviceSlug),
		IngressHost:    host,
		ContainerPort:  request.ContainerPort,
		TLSEnabled:     cfg.TLSSecretName != "",
	}, nil
}

func buildNamespaceName(prefix, projectSlug string) string {
	return truncateDNSLabel(fmt.Sprintf("%s-%s", sanitizeDNSLabel(prefix, "shiply-prj"), sanitizeDNSLabel(projectSlug, "project")))
}

func buildResourceName(prefix, serviceSlug string) string {
	return truncateDNSLabel(fmt.Sprintf("%s-%s", sanitizeDNSLabel(prefix, "app"), sanitizeDNSLabel(serviceSlug, "service")))
}

func buildIngressHost(serviceSlug, projectSlug, baseDomain string) (string, error) {
	baseDomain = strings.Trim(strings.ToLower(strings.TrimSpace(baseDomain)), ".")
	if baseDomain == "" {
		return "", fmt.Errorf("missing PLATFORM_BASE_DOMAIN")
	}

	leftLabel := truncateDNSLabel(fmt.Sprintf("%s-%s", sanitizeDNSLabel(serviceSlug, "service"), sanitizeDNSLabel(projectSlug, "project")))
	return leftLabel + "." + baseDomain, nil
}

func labelsForRequest(request DeployRequest) map[string]string {
	return map[string]string{
		"shiply.io/managed-by":    "orchestration-service",
		"shiply.io/deployment-id": request.DeploymentID,
		"shiply.io/project-id":    request.ProjectID,
		"shiply.io/project-slug":  sanitizeDNSLabel(request.ProjectSlug, "project"),
		"shiply.io/service-id":    request.ServiceID,
		"shiply.io/service-slug":  sanitizeDNSLabel(request.ServiceSlug, "service"),
	}
}

func sanitizeDNSLabel(value, fallback string) string {
	text := strings.ToLower(strings.TrimSpace(value))
	if text == "" {
		text = fallback
	}

	var builder strings.Builder
	lastDash := false
	for _, r := range text {
		isAlphaNum := (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9')
		if isAlphaNum {
			builder.WriteRune(r)
			lastDash = false
			continue
		}

		if builder.Len() == 0 || lastDash {
			continue
		}
		builder.WriteByte('-')
		lastDash = true
	}

	result := strings.Trim(builder.String(), "-")
	if result == "" {
		result = fallback
	}
	return truncateDNSLabel(result)
}

func truncateDNSLabel(value string) string {
	value = strings.Trim(value, "-")
	if len(value) <= 63 {
		return value
	}
	return strings.Trim(value[:63], "-")
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}
