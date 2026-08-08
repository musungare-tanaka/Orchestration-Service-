package main

import (
	"context"
	"fmt"
	"path/filepath"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/homedir"
)

type DeploymentManager interface {
	Deploy(ctx context.Context, request DeployRequest) (DeploymentTarget, error)
	WaitForRollout(ctx context.Context, target DeploymentTarget) error
}

type KubernetesClient struct {
	cfg       Config
	clientset kubernetes.Interface
}

func NewKubernetesClient(cfg Config) (*KubernetesClient, error) {
	restConfig, err := kubeRESTConfig(cfg)
	if err != nil {
		return nil, err
	}

	clientset, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		return nil, fmt.Errorf("create kubernetes clientset: %w", err)
	}

	return &KubernetesClient{
		cfg:       cfg,
		clientset: clientset,
	}, nil
}

func NewKubernetesClientWithClientset(cfg Config, clientset kubernetes.Interface) *KubernetesClient {
	return &KubernetesClient{
		cfg:       cfg,
		clientset: clientset,
	}
}

func kubeRESTConfig(cfg Config) (*rest.Config, error) {
	inClusterConfig, err := rest.InClusterConfig()
	if err == nil {
		return inClusterConfig, nil
	}

	kubeconfig := cfg.KubeconfigPath
	if kubeconfig == "" && homedir.HomeDir() != "" {
		kubeconfig = filepath.Join(homedir.HomeDir(), ".kube", "config")
	}

	restConfig, kubeconfigErr := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if kubeconfigErr != nil {
		return nil, fmt.Errorf("load kubernetes config: in-cluster=%v, kubeconfig=%w", err, kubeconfigErr)
	}

	return restConfig, nil
}
