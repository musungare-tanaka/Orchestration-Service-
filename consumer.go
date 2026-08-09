package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/rabbitmq/amqp091-go"
)

type publishFunc func(context.Context, string, string, any) error

type deliveryAction struct {
	ack     bool
	requeue bool
}

var (
	actionAck         = deliveryAction{ack: true}
	actionNackDrop    = deliveryAction{}
	actionNackRequeue = deliveryAction{requeue: true}
)

type DeployConsumer struct {
	cfg      Config
	conn     *amqp091.Connection
	ch       *amqp091.Channel
	pub      *RabbitPublisher
	deployer DeploymentManager
	publish  publishFunc
}

func NewDeployConsumer(cfg Config, deployer DeploymentManager) (*DeployConsumer, error) {
	conn, err := amqp091.Dial(cfg.RabbitMQURL)
	if err != nil {
		return nil, fmt.Errorf("connect rabbitmq: %w", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("open rabbitmq channel: %w", err)
	}

	if err := ch.ExchangeDeclare(cfg.RabbitMQExchange, "direct", true, false, false, false, nil); err != nil {
		ch.Close()
		conn.Close()
		return nil, fmt.Errorf("declare exchange: %w", err)
	}

	queue, err := ch.QueueDeclare(cfg.AppDeployQueue, true, false, false, false, nil)
	if err != nil {
		ch.Close()
		conn.Close()
		return nil, fmt.Errorf("declare queue: %w", err)
	}

	if err := ch.QueueBind(queue.Name, cfg.BuildSucceededRoutingKey, cfg.RabbitMQExchange, false, nil); err != nil {
		ch.Close()
		conn.Close()
		return nil, fmt.Errorf("bind queue: %w", err)
	}

	if err := ch.Qos(1, 0, false); err != nil {
		ch.Close()
		conn.Close()
		return nil, fmt.Errorf("set qos: %w", err)
	}

	publisher, err := NewRabbitPublisher(conn, []ExchangeSpec{
		{Name: cfg.RabbitMQExchange, Kind: "direct"},
		{Name: cfg.DeploymentExchange, Kind: "topic"},
	})
	if err != nil {
		ch.Close()
		conn.Close()
		return nil, fmt.Errorf("initialize publisher: %w", err)
	}

	return &DeployConsumer{
		cfg:      cfg,
		conn:     conn,
		ch:       ch,
		pub:      publisher,
		deployer: deployer,
		publish:  nil,
	}, nil
}

func (c *DeployConsumer) Close() {
	if c.pub != nil {
		_ = c.pub.Close()
	}
	if c.ch != nil {
		_ = c.ch.Close()
	}
	if c.conn != nil {
		_ = c.conn.Close()
	}
}

func (c *DeployConsumer) Start() error {
	msgs, err := c.ch.Consume(c.cfg.AppDeployQueue, "", false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("consume queue: %w", err)
	}

	for msg := range msgs {
		c.processDelivery(msg)
	}

	return errors.New("rabbitmq consumer channel closed")
}

func (c *DeployConsumer) processDelivery(msg amqp091.Delivery) {
	action, err := c.handleMessage(msg.Body)
	if err != nil {
		log.Printf("deploy event failed: %v", err)
	}

	if action.ack {
		if ackErr := msg.Ack(false); ackErr != nil {
			log.Printf("ack failed: %v", ackErr)
		}
		return
	}

	if nackErr := msg.Nack(false, action.requeue); nackErr != nil {
		log.Printf("nack failed: %v", nackErr)
	}
}

func (c *DeployConsumer) handleMessage(body []byte) (deliveryAction, error) {
	var event ServiceEvent[BuildSucceededPayload]
	if err := json.Unmarshal(body, &event); err != nil {
		return actionNackDrop, fmt.Errorf("decode deploy event: %w", err)
	}

	request := deployRequestFromEvent(c.cfg, event)
	target, err := targetForRequest(c.cfg, request)
	if err != nil {
		return actionNackDrop, err
	}

	if strings.TrimSpace(request.ImageTag) == "" {
		return c.completeFailure(event, target, errors.New("missing imageTag in build.succeeded event"))
	}

	startedCtx, cancelStarted := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancelStarted()
	if err := c.publishEvent(startedCtx, c.cfg.DeploymentExchange, c.cfg.OrchestrationStartedRoutingKey, newOrchestrationStartedEvent(event)); err != nil {
		return actionNackRequeue, fmt.Errorf("publish orchestration started event: %w", err)
	}

	applyCtx, cancelApply := context.WithTimeout(context.Background(), time.Minute)
	defer cancelApply()

	deployedTarget, err := c.deployer.Deploy(applyCtx, request)
	if err != nil {
		if isRetriableDeployError(err) {
			return actionNackRequeue, fmt.Errorf("deploy resources: %w", err)
		}
		return c.completeFailure(event, target, err)
	}

	if err := c.publishEvent(applyCtx, c.cfg.DeploymentExchange, c.cfg.OrchestrationDeployedRoutingKey, newOrchestrationDeployedEvent(event, deployedTarget)); err != nil {
		return actionNackRequeue, fmt.Errorf("publish orchestration deployed event: %w", err)
	}

	rolloutCtx, cancelRollout := context.WithTimeout(context.Background(), c.cfg.RolloutTimeout)
	defer cancelRollout()

	if err := c.deployer.WaitForRollout(rolloutCtx, deployedTarget); err != nil {
		if isRetriableDeployError(err) {
			return actionNackRequeue, fmt.Errorf("wait for rollout: %w", err)
		}
		return c.completeFailure(event, deployedTarget, err)
	}

	successEvent := newDeploySucceededEvent(event, deployedTarget)
	if err := c.publishEvent(rolloutCtx, c.cfg.RabbitMQExchange, c.cfg.DeploySucceededRoutingKey, successEvent); err != nil {
		return actionNackRequeue, fmt.Errorf("publish deploy succeeded event: %w", err)
	}
	if err := c.publishEvent(rolloutCtx, c.cfg.DeploymentExchange, c.cfg.OrchestrationRunningRoutingKey, newOrchestrationRunningEvent(event, deployedTarget)); err != nil {
		return actionNackRequeue, fmt.Errorf("publish orchestration running event: %w", err)
	}

	log.Printf("deployment succeeded for project=%s service=%s host=%s", event.ProjectID, event.ServiceID, deployedTarget.IngressHost)
	return actionAck, nil
}

func (c *DeployConsumer) completeFailure(
	event ServiceEvent[BuildSucceededPayload],
	target DeploymentTarget,
	deployErr error,
) (deliveryAction, error) {
	failureEvent := newDeployFailedEvent(event, target, deployErr)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	if err := c.publishEvent(ctx, c.cfg.RabbitMQExchange, c.cfg.DeployFailedRoutingKey, failureEvent); err != nil {
		return actionNackRequeue, fmt.Errorf("%v; publish deploy failed event: %w", deployErr, err)
	}
	if err := c.publishEvent(ctx, c.cfg.DeploymentExchange, c.cfg.OrchestrationFailedRoutingKey, newOrchestrationFailedEvent(event, target, deployErr)); err != nil {
		return actionNackRequeue, fmt.Errorf("%v; publish orchestration failed event: %w", deployErr, err)
	}

	return actionAck, deployErr
}

func (c *DeployConsumer) publishEvent(ctx context.Context, exchange, routingKey string, event any) error {
	publish := c.publish
	if publish == nil {
		publish = c.publishJSONEvent
	}
	return publish(ctx, exchange, routingKey, event)
}

func (c *DeployConsumer) publishJSONEvent(ctx context.Context, exchange, routingKey string, event any) error {
	if c.pub == nil {
		return errors.New("rabbitmq publisher is not initialized")
	}
	return c.pub.PublishJSON(ctx, exchange, routingKey, event)
}
