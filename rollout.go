package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type rolloutTimeoutError struct {
	namespace      string
	deploymentName string
}

func (e rolloutTimeoutError) Error() string {
	return fmt.Sprintf("deployment rollout timed out for %s/%s", e.namespace, e.deploymentName)
}

type rolloutFailedError struct {
	namespace      string
	deploymentName string
	reason         string
	message        string
}

func (e rolloutFailedError) Error() string {
	if e.message != "" {
		return fmt.Sprintf("deployment rollout failed for %s/%s: %s", e.namespace, e.deploymentName, e.message)
	}
	return fmt.Sprintf("deployment rollout failed for %s/%s: %s", e.namespace, e.deploymentName, e.reason)
}

func (k *KubernetesClient) WaitForRollout(ctx context.Context, target DeploymentTarget) error {
	ticker := time.NewTicker(k.cfg.RolloutPollInterval)
	defer ticker.Stop()

	for {
		deployment, err := k.clientset.AppsV1().Deployments(target.Namespace).Get(ctx, target.DeploymentName, metav1.GetOptions{})
		if err != nil {
			return fmt.Errorf("get deployment status: %w", err)
		}

		if err := rolloutConditionError(deployment, target); err != nil {
			return err
		}
		if deploymentRolloutComplete(deployment) {
			return nil
		}

		select {
		case <-ctx.Done():
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				return rolloutTimeoutError{
					namespace:      target.Namespace,
					deploymentName: target.DeploymentName,
				}
			}
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func rolloutConditionError(deployment *appsv1.Deployment, target DeploymentTarget) error {
	for _, condition := range deployment.Status.Conditions {
		if condition.Type == appsv1.DeploymentProgressing && condition.Status == "False" && condition.Reason == "ProgressDeadlineExceeded" {
			return rolloutFailedError{
				namespace:      target.Namespace,
				deploymentName: target.DeploymentName,
				reason:         condition.Reason,
				message:        condition.Message,
			}
		}
		if condition.Type == appsv1.DeploymentReplicaFailure && condition.Status == "True" {
			return rolloutFailedError{
				namespace:      target.Namespace,
				deploymentName: target.DeploymentName,
				reason:         condition.Reason,
				message:        condition.Message,
			}
		}
	}
	return nil
}

func deploymentRolloutComplete(deployment *appsv1.Deployment) bool {
	replicas := int32(1)
	if deployment.Spec.Replicas != nil {
		replicas = *deployment.Spec.Replicas
	}

	return deployment.Generation <= deployment.Status.ObservedGeneration &&
		deployment.Status.UpdatedReplicas == replicas &&
		deployment.Status.Replicas == replicas &&
		deployment.Status.AvailableReplicas == replicas
}

func isRetriableDeployError(err error) bool {
	var timeoutErr rolloutTimeoutError
	if errors.As(err, &timeoutErr) {
		return false
	}

	var failedErr rolloutFailedError
	if errors.As(err, &failedErr) {
		return false
	}

	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	if apierrors.IsTimeout(err) || apierrors.IsServerTimeout(err) || apierrors.IsServiceUnavailable(err) || apierrors.IsTooManyRequests(err) || apierrors.IsInternalError(err) {
		return true
	}

	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}

	var urlErr *url.Error
	return errors.As(err, &urlErr)
}
