package main

import (
	"context"
	"errors"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

func TestWaitForRolloutSucceeds(t *testing.T) {
	clientset := fake.NewSimpleClientset()
	callCount := 0
	clientset.PrependReactor("get", "deployments", func(_ k8stesting.Action) (bool, runtime.Object, error) {
		callCount++
		if callCount == 1 {
			return true, &appsv1.Deployment{
				ObjectMeta: metav1.ObjectMeta{Name: "app-api", Namespace: "shiply-prj-demo", Generation: 2},
				Spec:       appsv1.DeploymentSpec{Replicas: int32Ptr(1)},
				Status: appsv1.DeploymentStatus{
					ObservedGeneration: 1,
					UpdatedReplicas:    0,
					Replicas:           1,
					AvailableReplicas:  0,
				},
			}, nil
		}
		return true, &appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Name: "app-api", Namespace: "shiply-prj-demo", Generation: 2},
			Spec:       appsv1.DeploymentSpec{Replicas: int32Ptr(1)},
			Status: appsv1.DeploymentStatus{
				ObservedGeneration: 2,
				UpdatedReplicas:    1,
				Replicas:           1,
				AvailableReplicas:  1,
			},
		}, nil
	})

	client := NewKubernetesClientWithClientset(Config{RolloutPollInterval: 10 * time.Millisecond}, clientset)
	err := client.WaitForRollout(context.Background(), DeploymentTarget{
		Namespace:      "shiply-prj-demo",
		DeploymentName: "app-api",
	})
	if err != nil {
		t.Fatalf("expected rollout success, got %v", err)
	}
}

func TestWaitForRolloutTimeout(t *testing.T) {
	clientset := fake.NewSimpleClientset(&appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: "app-api", Namespace: "shiply-prj-demo", Generation: 1},
		Spec:       appsv1.DeploymentSpec{Replicas: int32Ptr(1)},
		Status: appsv1.DeploymentStatus{
			ObservedGeneration: 1,
			UpdatedReplicas:    0,
			Replicas:           1,
			AvailableReplicas:  0,
		},
	})

	client := NewKubernetesClientWithClientset(Config{RolloutPollInterval: 10 * time.Millisecond}, clientset)
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()

	err := client.WaitForRollout(ctx, DeploymentTarget{
		Namespace:      "shiply-prj-demo",
		DeploymentName: "app-api",
	})
	var timeoutErr rolloutTimeoutError
	if !errors.As(err, &timeoutErr) {
		t.Fatalf("expected rolloutTimeoutError, got %v", err)
	}
}

func TestWaitForRolloutFailsOnProgressDeadlineExceeded(t *testing.T) {
	clientset := fake.NewSimpleClientset(&appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: "app-api", Namespace: "shiply-prj-demo", Generation: 1},
		Spec:       appsv1.DeploymentSpec{Replicas: int32Ptr(1)},
		Status: appsv1.DeploymentStatus{
			ObservedGeneration: 1,
			Conditions: []appsv1.DeploymentCondition{
				{
					Type:    appsv1.DeploymentProgressing,
					Status:  corev1.ConditionFalse,
					Reason:  "ProgressDeadlineExceeded",
					Message: "ReplicaSet never became ready",
				},
			},
		},
	})

	client := NewKubernetesClientWithClientset(Config{RolloutPollInterval: 10 * time.Millisecond}, clientset)
	err := client.WaitForRollout(context.Background(), DeploymentTarget{
		Namespace:      "shiply-prj-demo",
		DeploymentName: "app-api",
	})

	var failedErr rolloutFailedError
	if !errors.As(err, &failedErr) {
		t.Fatalf("expected rolloutFailedError, got %v", err)
	}
}

func int32Ptr(value int32) *int32 {
	return &value
}
