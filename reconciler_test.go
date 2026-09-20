package main

import (
	"context"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
)

func TestDeployCreatesNamespaceWorkloadServiceIngressAndSecret(t *testing.T) {
	cfg := Config{
		BaseDomain:           "apps.shiply.test",
		NamespacePrefix:      "shiply-prj",
		DefaultReplicas:      1,
		DefaultContainerPort: 8080,
		IngressClassName:     "traefik",
		ImagePullSecretName:  "shiply-registry",
		Registry: RegistryConfig{
			Host:     "ghcr.io",
			Username: "shiply",
			Password: "secret",
		},
		ResourceDefaults: ResourceDefaults{
			CPURequest:    "100m",
			CPULimit:      "500m",
			MemoryRequest: "128Mi",
			MemoryLimit:   "512Mi",
		},
		DeploymentProgressDeadline: 300,
	}

	client := NewKubernetesClientWithClientset(cfg, fake.NewSimpleClientset())
	target, err := client.Deploy(context.Background(), DeployRequest{
		ProjectID:     "project-1",
		ServiceID:     "service-1",
		ProjectSlug:   "demo",
		ServiceSlug:   "api",
		ImageTag:      "ghcr.io/shiply/demo/api:abc123",
		ContainerPort: 8080,
	})
	if err != nil {
		t.Fatalf("Deploy returned error: %v", err)
	}

	if target.Namespace != "shiply-prj-demo" {
		t.Fatalf("unexpected namespace %q", target.Namespace)
	}

	if _, err := client.clientset.CoreV1().Namespaces().Get(context.Background(), target.Namespace, metav1.GetOptions{}); err != nil {
		t.Fatalf("expected namespace, got error: %v", err)
	}
	secret, err := client.clientset.CoreV1().Secrets(target.Namespace).Get(context.Background(), "shiply-registry", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("expected image pull secret, got error: %v", err)
	}
	if secret.Type != corev1.SecretTypeDockerConfigJson {
		t.Fatalf("unexpected secret type %q", secret.Type)
	}

	deployment, err := client.clientset.AppsV1().Deployments(target.Namespace).Get(context.Background(), target.DeploymentName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("expected deployment, got error: %v", err)
	}
	if deployment.Spec.Template.Spec.Containers[0].Image != "ghcr.io/shiply/demo/api:abc123" {
		t.Fatalf("unexpected image %q", deployment.Spec.Template.Spec.Containers[0].Image)
	}
	if deployment.Spec.Template.Spec.Containers[0].Ports[0].ContainerPort != 8080 {
		t.Fatalf("unexpected container port %d", deployment.Spec.Template.Spec.Containers[0].Ports[0].ContainerPort)
	}
	if len(deployment.Spec.Template.Spec.ImagePullSecrets) != 1 || deployment.Spec.Template.Spec.ImagePullSecrets[0].Name != "shiply-registry" {
		t.Fatalf("expected image pull secret attachment, got %#v", deployment.Spec.Template.Spec.ImagePullSecrets)
	}

	service, err := client.clientset.CoreV1().Services(target.Namespace).Get(context.Background(), target.ServiceName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("expected service, got error: %v", err)
	}
	if service.Spec.Ports[0].TargetPort.IntVal != 8080 {
		t.Fatalf("unexpected target port %d", service.Spec.Ports[0].TargetPort.IntVal)
	}

	ingress, err := client.clientset.NetworkingV1().Ingresses(target.Namespace).Get(context.Background(), target.IngressName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("expected ingress, got error: %v", err)
	}
	if ingress.Spec.Rules[0].Host != "api-demo.apps.shiply.test" {
		t.Fatalf("unexpected ingress host %q", ingress.Spec.Rules[0].Host)
	}
}

func TestDeployUpdatesExistingResourcesIdempotently(t *testing.T) {
	cfg := Config{
		BaseDomain:           "apps.shiply.test",
		NamespacePrefix:      "shiply-prj",
		DefaultReplicas:      1,
		DefaultContainerPort: 8080,
		IngressClassName:     "traefik",
		ResourceDefaults: ResourceDefaults{
			CPURequest:    "100m",
			CPULimit:      "500m",
			MemoryRequest: "128Mi",
			MemoryLimit:   "512Mi",
		},
		DeploymentProgressDeadline: 300,
	}

	existing := []runtime.Object{
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "shiply-prj-demo", Labels: map[string]string{"shiply.io/managed-by": "orchestration-service", "shiply.io/project-id": "project-1"}}},
		&appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: "app-api", Namespace: "shiply-prj-demo"}},
		&corev1.Service{
			ObjectMeta: metav1.ObjectMeta{Name: "app-api", Namespace: "shiply-prj-demo"},
			Spec:       corev1.ServiceSpec{ClusterIP: "10.0.0.10"},
		},
		&networkingv1.Ingress{ObjectMeta: metav1.ObjectMeta{Name: "app-api", Namespace: "shiply-prj-demo"}},
	}

	client := NewKubernetesClientWithClientset(cfg, fake.NewSimpleClientset(existing...))
	_, err := client.Deploy(context.Background(), DeployRequest{
		ProjectID:     "project-1",
		ServiceID:     "service-1",
		ProjectSlug:   "demo",
		ServiceSlug:   "api",
		ImageTag:      "ghcr.io/shiply/demo/api:def456",
		ContainerPort: 9090,
	})
	if err != nil {
		t.Fatalf("Deploy returned error: %v", err)
	}

	deployment, err := client.clientset.AppsV1().Deployments("shiply-prj-demo").Get(context.Background(), "app-api", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get deployment: %v", err)
	}
	if deployment.Spec.Template.Spec.Containers[0].Image != "ghcr.io/shiply/demo/api:def456" {
		t.Fatalf("expected updated image, got %q", deployment.Spec.Template.Spec.Containers[0].Image)
	}
	if deployment.Labels["shiply.io/project-id"] != "project-1" {
		t.Fatalf("expected labels to be updated, got %#v", deployment.Labels)
	}

	service, err := client.clientset.CoreV1().Services("shiply-prj-demo").Get(context.Background(), "app-api", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get service: %v", err)
	}
	if service.Spec.ClusterIP != "10.0.0.10" {
		t.Fatalf("expected cluster ip to be preserved, got %q", service.Spec.ClusterIP)
	}
	if service.Spec.Ports[0].TargetPort.IntVal != 9090 {
		t.Fatalf("expected service target port 9090, got %d", service.Spec.Ports[0].TargetPort.IntVal)
	}
}
