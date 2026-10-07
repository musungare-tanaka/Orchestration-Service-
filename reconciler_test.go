package main

import (
	"context"
	"errors"
	"maps"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
)

func TestEnsureDeploymentSelectorUnchangedUpdates(t *testing.T) {
	clientset := fake.NewSimpleClientset(testDeployment("service-1"))
	client := NewKubernetesClientWithClientset(testDeploymentConfig(), clientset)
	if err := client.ensureDeployment(context.Background(), testDeployRequest("service-1"), testDeploymentTarget(), labelsForRequest(testDeployRequest("service-1")), ""); err != nil {
		t.Fatalf("ensureDeployment returned error: %v", err)
	}
	assertDeploymentActions(t, clientset, []string{"get", "get", "update"})
}

func TestEnsureDeploymentSelectorChangedDeletesThenCreates(t *testing.T) {
	clientset := fake.NewSimpleClientset(testDeployment("old-service"))
	client := NewKubernetesClientWithClientset(testDeploymentConfig(), clientset)
	request := testDeployRequest("new-service")
	if err := client.ensureDeployment(context.Background(), request, testDeploymentTarget(), labelsForRequest(request), ""); err != nil {
		t.Fatalf("ensureDeployment returned error: %v", err)
	}
	assertDeploymentActions(t, clientset, []string{"get", "delete", "get", "create"})
	got, err := clientset.AppsV1().Deployments("shiply-prj-demo").Get(context.Background(), "app-api", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get recreated deployment: %v", err)
	}
	if got.Spec.Selector.MatchLabels["shiply.io/service-id"] != "new-service" {
		t.Fatalf("recreated selector = %#v", got.Spec.Selector.MatchLabels)
	}
}

func TestEnsureDeploymentNotFoundCreates(t *testing.T) {
	clientset := fake.NewSimpleClientset()
	client := NewKubernetesClientWithClientset(testDeploymentConfig(), clientset)
	request := testDeployRequest("service-1")
	if err := client.ensureDeployment(context.Background(), request, testDeploymentTarget(), labelsForRequest(request), ""); err != nil {
		t.Fatalf("ensureDeployment returned error: %v", err)
	}
	assertDeploymentActions(t, clientset, []string{"get", "create"})
}

func TestEnsureDeploymentDeleteFailureIsReturned(t *testing.T) {
	clientset := fake.NewSimpleClientset(testDeployment("old-service"))
	wantErr := errors.New("delete rejected")
	clientset.PrependReactor("delete", "deployments", func(ktesting.Action) (bool, runtime.Object, error) { return true, nil, wantErr })
	client := NewKubernetesClientWithClientset(testDeploymentConfig(), clientset)
	request := testDeployRequest("new-service")
	if err := client.ensureDeployment(context.Background(), request, testDeploymentTarget(), labelsForRequest(request), ""); !errors.Is(err, wantErr) {
		t.Fatalf("ensureDeployment error = %v, want %v", err, wantErr)
	}
	assertDeploymentActions(t, clientset, []string{"get", "delete"})
}

func TestEnsureDeploymentInvalidSelectorUpdateFallsBackOnce(t *testing.T) {
	clientset := fake.NewSimpleClientset(testDeployment("service-1"))
	clientset.PrependReactor("update", "deployments", func(ktesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewInvalid(appsv1.SchemeGroupVersion.WithKind("Deployment").GroupKind(), "app-api", field.ErrorList{field.Invalid(field.NewPath("spec", "selector"), nil, "field is immutable")})
	})
	client := NewKubernetesClientWithClientset(testDeploymentConfig(), clientset)
	request := testDeployRequest("service-1")
	if err := client.ensureDeployment(context.Background(), request, testDeploymentTarget(), labelsForRequest(request), ""); err != nil {
		t.Fatalf("ensureDeployment returned error: %v", err)
	}
	assertDeploymentActions(t, clientset, []string{"get", "get", "update", "delete", "get", "create"})
}

func testDeployment(serviceID string) *appsv1.Deployment {
	return &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: "app-api", Namespace: "shiply-prj-demo"}, Spec: appsv1.DeploymentSpec{
		Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"shiply.io/service-id": serviceID}},
	}}
}

func testDeploymentConfig() Config {
	return Config{DefaultReplicas: 1, ResourceDefaults: ResourceDefaults{CPURequest: "100m", CPULimit: "500m", MemoryRequest: "128Mi", MemoryLimit: "512Mi"}}
}

func testDeployRequest(serviceID string) DeployRequest {
	return DeployRequest{ProjectID: "project-1", ServiceID: serviceID, ProjectSlug: "demo", ServiceSlug: "api", ImageTag: "example/image:latest", ContainerPort: 8080}
}

func testDeploymentTarget() DeploymentTarget {
	return DeploymentTarget{Namespace: "shiply-prj-demo", DeploymentName: "app-api", ContainerPort: 8080}
}

func assertDeploymentActions(t *testing.T, clientset *fake.Clientset, want []string) {
	t.Helper()
	var got []string
	for _, action := range clientset.Actions() {
		if action.GetResource().Resource == "deployments" {
			got = append(got, action.GetVerb())
		}
	}
	if len(got) != len(want) {
		t.Fatalf("deployment actions = %v, want %v", got, want)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("deployment actions = %v, want %v", got, want)
		}
	}
}

func namespaceTestLabels(projectID, deploymentID string) map[string]string {
	return map[string]string{
		"shiply.io/managed-by":    "orchestration-service",
		"shiply.io/project-id":    projectID,
		"shiply.io/deployment-id": deploymentID,
		"shiply.io/service-id":    "service-1",
		"shiply.io/service-slug":  "api",
	}
}

func assertNoNamespaceUpdateOrPatch(t *testing.T, clientset *fake.Clientset) {
	t.Helper()
	for _, action := range clientset.Actions() {
		if action.GetResource().Resource == "namespaces" && (action.GetVerb() == "update" || action.GetVerb() == "patch") {
			t.Errorf("unexpected namespace update action: %s", action.GetVerb())
		}
	}
}

func TestEnsureNamespaceCreatesOnlyOwnershipLabels(t *testing.T) {
	clientset := fake.NewSimpleClientset()
	client := NewKubernetesClientWithClientset(Config{}, clientset)
	labels := namespaceTestLabels("project-1", "deploy-1")
	if err := client.ensureNamespace(context.Background(), "shiply-prj-demo", labels); err != nil {
		t.Fatalf("ensureNamespace returned error: %v", err)
	}
	created, err := clientset.CoreV1().Namespaces().Get(context.Background(), "shiply-prj-demo", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get created namespace: %v", err)
	}
	want := map[string]string{"shiply.io/managed-by": "orchestration-service", "shiply.io/project-id": "project-1"}
	if !maps.Equal(created.Labels, want) {
		t.Fatalf("namespace labels = %#v, want %#v", created.Labels, want)
	}
	assertNamespaceActions(t, clientset, []string{"get", "create", "get"})
}

func TestEnsureNamespaceMatchingOwnershipDoesNotWrite(t *testing.T) {
	labels := namespaceTestLabels("project-1", "new-deploy")
	clientset := fake.NewSimpleClientset(&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "shiply-prj-demo", Labels: map[string]string{
		"shiply.io/managed-by": "orchestration-service", "shiply.io/project-id": "project-1", "shiply.io/deployment-id": "old-deploy",
	}}})
	client := NewKubernetesClientWithClientset(Config{}, clientset)
	if err := client.ensureNamespace(context.Background(), "shiply-prj-demo", labels); err != nil {
		t.Fatalf("ensureNamespace returned error: %v", err)
	}
	assertNamespaceActions(t, clientset, []string{"get"})
}

func TestEnsureNamespaceMismatchedOwnershipDoesNotWrite(t *testing.T) {
	clientset := fake.NewSimpleClientset(&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "shiply-prj-demo", Labels: map[string]string{
		"shiply.io/managed-by": "orchestration-service", "shiply.io/project-id": "other-project",
	}}})
	client := NewKubernetesClientWithClientset(Config{}, clientset)
	err := client.ensureNamespace(context.Background(), "shiply-prj-demo", namespaceTestLabels("project-1", "deploy-1"))
	var ownerErr ownershipError
	if !errors.As(err, &ownerErr) {
		t.Fatalf("expected ownershipError, got %v", err)
	}
	assertNamespaceActions(t, clientset, []string{"get"})
}

func TestEnsureNamespaceAlreadyExistsRaceChecksOwnership(t *testing.T) {
	for _, tc := range []struct {
		name      string
		projectID string
		wantErr   bool
	}{
		{name: "matching", projectID: "project-1"},
		{name: "mismatched", projectID: "other-project", wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clientset := fake.NewSimpleClientset()
			var getCalls int
			clientset.PrependReactor("*", "*", func(action ktesting.Action) (bool, runtime.Object, error) {
				if action.GetResource().Resource != "namespaces" {
					return false, nil, nil
				}
				switch action.GetVerb() {
				case "get":
					getCalls++
					if getCalls == 1 {
						return true, nil, apierrors.NewNotFound(corev1.Resource("namespaces"), "shiply-prj-demo")
					}
					return true, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "shiply-prj-demo", Labels: map[string]string{
						"shiply.io/managed-by": "orchestration-service", "shiply.io/project-id": tc.projectID,
					}}}, nil
				case "create":
					return true, nil, apierrors.NewAlreadyExists(corev1.Resource("namespaces"), "shiply-prj-demo")
				default:
					return false, nil, nil
				}
			})
			client := NewKubernetesClientWithClientset(Config{}, clientset)
			err := client.ensureNamespace(context.Background(), "shiply-prj-demo", namespaceTestLabels("project-1", "deploy-1"))
			if tc.wantErr {
				var ownerErr ownershipError
				if !errors.As(err, &ownerErr) {
					t.Fatalf("expected ownershipError, got %v", err)
				}
			} else if err != nil {
				t.Fatalf("ensureNamespace returned error: %v", err)
			}
			assertNamespaceActions(t, clientset, []string{"get", "create", "get"})
		})
	}
}

func assertNamespaceActions(t *testing.T, clientset *fake.Clientset, wantVerbs []string) {
	t.Helper()
	var got []string
	for _, action := range clientset.Actions() {
		if action.GetResource().Resource != "namespaces" {
			continue
		}
		got = append(got, action.GetVerb())
	}
	if len(got) != len(wantVerbs) {
		t.Fatalf("namespace actions = %v, want %v", got, wantVerbs)
	}
	for i := range got {
		if got[i] != wantVerbs[i] {
			t.Fatalf("namespace actions = %v, want %v", got, wantVerbs)
		}
	}
	assertNoNamespaceUpdateOrPatch(t, clientset)
}

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
	container := deployment.Spec.Template.Spec.Containers[0]
	if got := envValue(container.Env, "PORT"); got != "8080" {
		t.Fatalf("PORT = %q, want 8080", got)
	}
	if got := envValue(container.Env, "SERVER_PORT"); got != "8080" {
		t.Fatalf("SERVER_PORT = %q, want 8080", got)
	}
	if container.ReadinessProbe == nil || container.ReadinessProbe.TCPSocket == nil || container.ReadinessProbe.TCPSocket.Port.IntVal != 8080 || container.ReadinessProbe.PeriodSeconds != 5 || container.ReadinessProbe.FailureThreshold != 3 {
		t.Fatalf("unexpected readiness probe: %#v", container.ReadinessProbe)
	}
	if container.StartupProbe == nil || container.StartupProbe.TCPSocket == nil || container.StartupProbe.TCPSocket.Port.IntVal != 8080 || container.StartupProbe.PeriodSeconds != 5 || container.StartupProbe.FailureThreshold != 60 {
		t.Fatalf("unexpected startup probe: %#v", container.StartupProbe)
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
	if ingress.Spec.Rules[0].HTTP.Paths[0].Backend.Service.Port.Number != 80 {
		t.Fatalf("unexpected ingress backend service port %d", ingress.Spec.Rules[0].HTTP.Paths[0].Backend.Service.Port.Number)
	}
}

func envValue(env []corev1.EnvVar, name string) string {
	for _, value := range env {
		if value.Name == name {
			return value.Value
		}
	}
	return ""
}

func TestPlatformPortOverridesUserEnvAndFlowsThroughResources(t *testing.T) {
	cfg := testDeploymentConfig()
	cfg.DefaultContainerPort = 4567
	cfg.BaseDomain = "apps.shiply.test"
	cfg.NamespacePrefix = "shiply-prj"
	cfg.IngressClassName = "traefik"
	request := testDeployRequest("service-1")
	request.Environment = []corev1.EnvVar{{Name: "PORT", Value: "5000"}, {Name: "SERVER_PORT", Value: "8080"}, {Name: "CUSTOM", Value: "kept"}}
	request.ContainerPort = cfg.DefaultContainerPort
	client := NewKubernetesClientWithClientset(cfg, fake.NewSimpleClientset())
	target, err := client.Deploy(context.Background(), request)
	if err != nil {
		t.Fatalf("Deploy returned error: %v", err)
	}
	container, err := getDeployedContainer(client, target)
	if err != nil {
		t.Fatal(err)
	}
	if container.Ports[0].ContainerPort != 4567 || envValue(container.Env, "PORT") != "4567" || envValue(container.Env, "SERVER_PORT") != "4567" || envValue(container.Env, "CUSTOM") != "kept" {
		t.Fatalf("platform port/env mismatch: %#v", container)
	}
	service, _ := client.clientset.CoreV1().Services(target.Namespace).Get(context.Background(), target.ServiceName, metav1.GetOptions{})
	if service.Spec.Ports[0].TargetPort.IntVal != 4567 {
		t.Fatalf("service targetPort = %d, want 4567", service.Spec.Ports[0].TargetPort.IntVal)
	}
	if target.ContainerPort != 4567 {
		t.Fatalf("target port = %d, want 4567", target.ContainerPort)
	}
}

func getDeployedContainer(client *KubernetesClient, target DeploymentTarget) (corev1.Container, error) {
	deployment, err := client.clientset.AppsV1().Deployments(target.Namespace).Get(context.Background(), target.DeploymentName, metav1.GetOptions{})
	if err != nil {
		return corev1.Container{}, err
	}
	return deployment.Spec.Template.Spec.Containers[0], nil
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
