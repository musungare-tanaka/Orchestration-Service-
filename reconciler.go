package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/util/retry"
)

type ownershipError struct{ message string }

func (e ownershipError) Error() string { return e.message }

func (k *KubernetesClient) Deploy(ctx context.Context, request DeployRequest) (DeploymentTarget, error) {
	target, err := targetForRequest(k.cfg, request)
	if err != nil {
		return DeploymentTarget{}, err
	}

	labels := labelsForRequest(request)

	if err := k.ensureNamespace(ctx, target.Namespace, labels); err != nil {
		return target, fmt.Errorf("ensure namespace: %w", err)
	}

	pullSecretName, err := k.ensureRegistryPullSecret(ctx, target.Namespace)
	if err != nil {
		return target, fmt.Errorf("ensure registry pull secret: %w", err)
	}

	if err := k.ensureDeployment(ctx, request, target, labels, pullSecretName); err != nil {
		return target, fmt.Errorf("ensure deployment: %w", err)
	}

	if err := k.ensureService(ctx, target, labels); err != nil {
		return target, fmt.Errorf("ensure service: %w", err)
	}

	if err := k.ensureIngress(ctx, target, labels); err != nil {
		return target, fmt.Errorf("ensure ingress: %w", err)
	}

	return target, nil
}

func (k *KubernetesClient) ensureNamespace(ctx context.Context, namespace string, labels map[string]string) error {
	namespaces := k.clientset.CoreV1().Namespaces()

	current, err := namespaces.Get(ctx, namespace, metav1.GetOptions{})
	if err == nil {
		if current.Labels["shiply.io/managed-by"] != labels["shiply.io/managed-by"] || current.Labels["shiply.io/project-id"] != labels["shiply.io/project-id"] {
			return ownershipError{message: fmt.Sprintf("refusing to update namespace %s: ownership labels do not match request", namespace)}
		}
		if current.Labels == nil {
			current.Labels = map[string]string{}
		}
		for key, value := range labels {
			current.Labels[key] = value
		}
		return retry.RetryOnConflict(retry.DefaultRetry, func() error {
			latest, err := namespaces.Get(ctx, namespace, metav1.GetOptions{})
			if err != nil {
				return err
			}
			if latest.Labels["shiply.io/managed-by"] != labels["shiply.io/managed-by"] || latest.Labels["shiply.io/project-id"] != labels["shiply.io/project-id"] {
				return ownershipError{message: fmt.Sprintf("refusing to update namespace %s: ownership labels do not match request", namespace)}
			}
			if latest.Labels == nil {
				latest.Labels = map[string]string{}
			}
			for key, value := range labels {
				latest.Labels[key] = value
			}
			_, err = namespaces.Update(ctx, latest, metav1.UpdateOptions{})
			return err
		})
	}
	if !apierrors.IsNotFound(err) {
		return err
	}

	_, err = namespaces.Create(ctx, &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name:   namespace,
			Labels: cloneLabels(labels),
		},
	}, metav1.CreateOptions{})
	return err
}

func (k *KubernetesClient) ensureRegistryPullSecret(ctx context.Context, namespace string) (string, error) {
	if k.cfg.ImagePullSecretName == "" {
		return "", nil
	}

	credsConfigured, err := registryCredentialsConfigured(k.cfg.Registry)
	if err != nil {
		return "", err
	}
	if !credsConfigured {
		return "", nil
	}

	payload, err := dockerConfigJSON(k.cfg.Registry)
	if err != nil {
		return "", err
	}

	secrets := k.clientset.CoreV1().Secrets(namespace)
	_, err = secrets.Get(ctx, k.cfg.ImagePullSecretName, metav1.GetOptions{})
	if err == nil {
		err = retry.RetryOnConflict(retry.DefaultRetry, func() error {
			latest, err := secrets.Get(ctx, k.cfg.ImagePullSecretName, metav1.GetOptions{})
			if err != nil {
				return err
			}
			latest.Type = corev1.SecretTypeDockerConfigJson
			latest.Data = map[string][]byte{corev1.DockerConfigJsonKey: payload}
			_, err = secrets.Update(ctx, latest, metav1.UpdateOptions{})
			return err
		})
		return k.cfg.ImagePullSecretName, err
	}
	if !apierrors.IsNotFound(err) {
		return "", err
	}

	_, err = secrets.Create(ctx, &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      k.cfg.ImagePullSecretName,
			Namespace: namespace,
		},
		Type: corev1.SecretTypeDockerConfigJson,
		Data: map[string][]byte{
			corev1.DockerConfigJsonKey: payload,
		},
	}, metav1.CreateOptions{})
	return k.cfg.ImagePullSecretName, err
}

func (k *KubernetesClient) ensureDeployment(
	ctx context.Context,
	request DeployRequest,
	target DeploymentTarget,
	labels map[string]string,
	pullSecretName string,
) error {
	resources, err := buildResourceRequirements(k.cfg.ResourceDefaults)
	if err != nil {
		return err
	}

	replicas := k.cfg.DefaultReplicas
	deployments := k.clientset.AppsV1().Deployments(target.Namespace)
	progressDeadline := k.cfg.DeploymentProgressDeadline

	desired := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      target.DeploymentName,
			Namespace: target.Namespace,
			Labels:    cloneLabels(labels),
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"shiply.io/service-id": request.ServiceID,
				},
			},
			Strategy: appsv1.DeploymentStrategy{
				Type: appsv1.RollingUpdateDeploymentStrategyType,
				RollingUpdate: &appsv1.RollingUpdateDeployment{
					MaxUnavailable: intOrStringPtr(intstr.FromInt(0)),
					MaxSurge:       intOrStringPtr(intstr.FromInt(1)),
				},
			},
			ProgressDeadlineSeconds: &progressDeadline,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: cloneLabels(labels),
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:            target.DeploymentName,
							Image:           request.ImageTag,
							ImagePullPolicy: corev1.PullIfNotPresent,
							Ports: []corev1.ContainerPort{
								{
									Name:          "http",
									ContainerPort: target.ContainerPort,
								},
							},
							Env: []corev1.EnvVar{
								{
									Name:  "PORT",
									Value: fmt.Sprintf("%d", target.ContainerPort),
								},
							},
							Resources: resources,
						},
					},
				},
			},
		},
	}

	if pullSecretName != "" {
		desired.Spec.Template.Spec.ImagePullSecrets = []corev1.LocalObjectReference{
			{Name: pullSecretName},
		}
	}

	_, err = deployments.Get(ctx, target.DeploymentName, metav1.GetOptions{})
	if err == nil {
		return retry.RetryOnConflict(retry.DefaultRetry, func() error {
			latest, err := deployments.Get(ctx, target.DeploymentName, metav1.GetOptions{})
			if err != nil {
				return err
			}
			desired.ResourceVersion = latest.ResourceVersion
			_, err = deployments.Update(ctx, desired, metav1.UpdateOptions{})
			return err
		})
	}
	if !apierrors.IsNotFound(err) {
		return err
	}

	_, err = deployments.Create(ctx, desired, metav1.CreateOptions{})
	return err
}

func (k *KubernetesClient) ensureService(ctx context.Context, target DeploymentTarget, labels map[string]string) error {
	services := k.clientset.CoreV1().Services(target.Namespace)

	desired := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      target.ServiceName,
			Namespace: target.Namespace,
			Labels:    cloneLabels(labels),
		},
		Spec: corev1.ServiceSpec{
			Type: corev1.ServiceTypeClusterIP,
			Selector: map[string]string{
				"shiply.io/service-id": labels["shiply.io/service-id"],
			},
			Ports: []corev1.ServicePort{
				{
					Name:       "http",
					Port:       80,
					Protocol:   corev1.ProtocolTCP,
					TargetPort: intstr.FromInt32(target.ContainerPort),
				},
			},
		},
	}

	_, err := services.Get(ctx, target.ServiceName, metav1.GetOptions{})
	if err == nil {
		return retry.RetryOnConflict(retry.DefaultRetry, func() error {
			latest, err := services.Get(ctx, target.ServiceName, metav1.GetOptions{})
			if err != nil {
				return err
			}
			desired.ResourceVersion = latest.ResourceVersion
			desired.Spec.ClusterIP = latest.Spec.ClusterIP
			desired.Spec.ClusterIPs = latest.Spec.ClusterIPs
			desired.Spec.IPFamilies = latest.Spec.IPFamilies
			desired.Spec.IPFamilyPolicy = latest.Spec.IPFamilyPolicy
			desired.Spec.InternalTrafficPolicy = latest.Spec.InternalTrafficPolicy
			_, err = services.Update(ctx, desired, metav1.UpdateOptions{})
			return err
		})
	}
	if !apierrors.IsNotFound(err) {
		return err
	}

	_, err = services.Create(ctx, desired, metav1.CreateOptions{})
	return err
}

func (k *KubernetesClient) ensureIngress(ctx context.Context, target DeploymentTarget, labels map[string]string) error {
	ingresses := k.clientset.NetworkingV1().Ingresses(target.Namespace)

	pathType := networkingv1.PathTypePrefix
	desired := &networkingv1.Ingress{
		ObjectMeta: metav1.ObjectMeta{
			Name:      target.IngressName,
			Namespace: target.Namespace,
			Labels:    cloneLabels(labels),
		},
		Spec: networkingv1.IngressSpec{
			IngressClassName: stringPtr(k.cfg.IngressClassName),
			Rules: []networkingv1.IngressRule{
				{
					Host: target.IngressHost,
					IngressRuleValue: networkingv1.IngressRuleValue{
						HTTP: &networkingv1.HTTPIngressRuleValue{
							Paths: []networkingv1.HTTPIngressPath{
								{
									Path:     "/",
									PathType: &pathType,
									Backend: networkingv1.IngressBackend{
										Service: &networkingv1.IngressServiceBackend{
											Name: target.ServiceName,
											Port: networkingv1.ServiceBackendPort{Number: 80},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	if k.cfg.TLSSecretName != "" {
		desired.Spec.TLS = []networkingv1.IngressTLS{
			{
				Hosts:      []string{target.IngressHost},
				SecretName: k.cfg.TLSSecretName,
			},
		}
	}

	_, err := ingresses.Get(ctx, target.IngressName, metav1.GetOptions{})
	if err == nil {
		return retry.RetryOnConflict(retry.DefaultRetry, func() error {
			latest, err := ingresses.Get(ctx, target.IngressName, metav1.GetOptions{})
			if err != nil {
				return err
			}
			desired.ResourceVersion = latest.ResourceVersion
			_, err = ingresses.Update(ctx, desired, metav1.UpdateOptions{})
			return err
		})
	}
	if !apierrors.IsNotFound(err) {
		return err
	}

	_, err = ingresses.Create(ctx, desired, metav1.CreateOptions{})
	return err
}

func buildResourceRequirements(defaults ResourceDefaults) (corev1.ResourceRequirements, error) {
	cpuRequest, err := resource.ParseQuantity(defaults.CPURequest)
	if err != nil {
		return corev1.ResourceRequirements{}, fmt.Errorf("parse K8S_CPU_REQUEST: %w", err)
	}
	cpuLimit, err := resource.ParseQuantity(defaults.CPULimit)
	if err != nil {
		return corev1.ResourceRequirements{}, fmt.Errorf("parse K8S_CPU_LIMIT: %w", err)
	}
	memoryRequest, err := resource.ParseQuantity(defaults.MemoryRequest)
	if err != nil {
		return corev1.ResourceRequirements{}, fmt.Errorf("parse K8S_MEMORY_REQUEST: %w", err)
	}
	memoryLimit, err := resource.ParseQuantity(defaults.MemoryLimit)
	if err != nil {
		return corev1.ResourceRequirements{}, fmt.Errorf("parse K8S_MEMORY_LIMIT: %w", err)
	}

	return corev1.ResourceRequirements{
		Requests: corev1.ResourceList{
			corev1.ResourceCPU:    cpuRequest,
			corev1.ResourceMemory: memoryRequest,
		},
		Limits: corev1.ResourceList{
			corev1.ResourceCPU:    cpuLimit,
			corev1.ResourceMemory: memoryLimit,
		},
	}, nil
}

func registryCredentialsConfigured(cfg RegistryConfig) (bool, error) {
	allBlank := strings.TrimSpace(cfg.Host) == "" && strings.TrimSpace(cfg.Username) == "" && cfg.Password == ""
	if allBlank {
		return false, nil
	}

	if strings.TrimSpace(cfg.Host) == "" {
		return false, fmt.Errorf("missing CONTAINER_REGISTRY_HOST")
	}
	if strings.TrimSpace(cfg.Username) == "" {
		return false, fmt.Errorf("missing CONTAINER_REGISTRY_USERNAME")
	}
	if cfg.Password == "" {
		return false, fmt.Errorf("missing CONTAINER_REGISTRY_PASSWORD")
	}
	return true, nil
}

func dockerConfigJSON(cfg RegistryConfig) ([]byte, error) {
	auth := base64.StdEncoding.EncodeToString([]byte(cfg.Username + ":" + cfg.Password))
	payload := map[string]any{
		"auths": map[string]map[string]string{
			cfg.Host: {
				"username": cfg.Username,
				"password": cfg.Password,
				"auth":     auth,
			},
		},
	}
	return json.Marshal(payload)
}

func cloneLabels(labels map[string]string) map[string]string {
	cloned := make(map[string]string, len(labels))
	for key, value := range labels {
		cloned[key] = value
	}
	return cloned
}

func intOrStringPtr(value intstr.IntOrString) *intstr.IntOrString {
	return &value
}

func stringPtr(value string) *string {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	return &value
}
