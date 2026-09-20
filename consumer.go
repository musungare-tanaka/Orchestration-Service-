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
	ledger   StageLedger
	deployer DeploymentManager
	publish  publishFunc
}
type deployResultEvents struct {
	ServiceRoutingKey  string          `json:"serviceRoutingKey"`
	ServiceEvent       json.RawMessage `json:"serviceEvent"`
	ProgressRoutingKey string          `json:"progressRoutingKey"`
	ProgressEvent      json.RawMessage `json:"progressEvent"`
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
	if err := declareDeployRetryTopology(ch, cfg); err != nil {
		ch.Close()
		conn.Close()
		return nil, err
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

	consumer := &DeployConsumer{
		cfg:      cfg,
		conn:     conn,
		ch:       ch,
		pub:      publisher,
		deployer: deployer,
		publish:  nil,
	}
	ledger, err := NewPostgresStageLedger(context.Background(), cfg.DatabaseURL, cfg.LedgerSchema)
	if err != nil {
		consumer.Close()
		return nil, fmt.Errorf("initialize stage ledger: %w", err)
	}
	consumer.ledger = ledger
	return consumer, nil
}

func (c *DeployConsumer) Close() {
	if c.ledger != nil {
		_ = c.ledger.Close()
	}
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

	if action.requeue && c.ch != nil {
		if retryErr := c.scheduleRetry(msg, err); retryErr == nil {
			_ = msg.Ack(false)
			return
		}
	}
	if !action.requeue && c.ch != nil {
		if dlqErr := c.copyToDLQ(msg, err); dlqErr == nil {
			_ = msg.Ack(false)
			return
		}
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
	if strings.TrimSpace(event.DeploymentID) == "" {
		return actionNackDrop, errors.New("missing deploymentId")
	}

	request := deployRequestFromEvent(c.cfg, event)
	target, err := targetForRequest(c.cfg, request)
	if err != nil {
		return actionNackDrop, err
	}

	if strings.TrimSpace(request.ImageTag) == "" {
		return actionNackDrop, errors.New("missing imageTag in build.succeeded event")
	}
	if c.ledger != nil {
		disposition, record, err := c.ledger.Claim(context.Background(), event.DeploymentID, "orchestrate", c.cfg.LeaseDuration)
		if err != nil {
			return actionNackRequeue, fmt.Errorf("claim orchestration stage: %w", err)
		}
		switch disposition {
		case ClaimCompleted, ClaimFailed:
			return c.publishStored(record.ResultJSON)
		case ClaimBusy:
			return actionNackRequeue, errors.New("orchestration stage lease is owned by another worker")
		}
	}

	startedCtx, cancelStarted := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancelStarted()
	if err := c.publishProgressEvent(startedCtx, c.cfg.DeploymentExchange, c.cfg.OrchestrationStartedRoutingKey, newOrchestrationStartedEvent(event)); err != nil {
		return actionNackRequeue, fmt.Errorf("publish orchestration started event: %w", err)
	}

	applyCtx, cancelApply := context.WithTimeout(context.Background(), time.Minute)
	defer cancelApply()
	stopRenew := c.startLeaseRenewal(event.DeploymentID)
	defer stopRenew()

	deployedTarget, err := c.deployer.Deploy(applyCtx, request)
	if err != nil {
		if isRetriableDeployError(err) {
			return actionNackRequeue, fmt.Errorf("deploy resources: %w", err)
		}
		return c.completeFailure(event, target, err)
	}

	if err := c.publishProgressEvent(applyCtx, c.cfg.DeploymentExchange, c.cfg.OrchestrationDeployedRoutingKey, newOrchestrationDeployedEvent(event, deployedTarget)); err != nil {
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
	progressEvent := newOrchestrationRunningEvent(event, deployedTarget)
	bundle, err := makeDeployResultEvents(c.cfg.DeploySucceededRoutingKey, successEvent, c.cfg.OrchestrationRunningRoutingKey, progressEvent)
	if err != nil {
		return actionNackRequeue, err
	}
	if c.ledger != nil {
		if err := c.ledger.Complete(rolloutCtx, event.DeploymentID, "orchestrate", bundle); err != nil {
			return actionNackRequeue, fmt.Errorf("persist orchestration result: %w", err)
		}
	}
	if err := c.publishEvent(rolloutCtx, c.cfg.RabbitMQExchange, c.cfg.DeploySucceededRoutingKey, successEvent); err != nil {
		return actionNackRequeue, fmt.Errorf("publish deploy succeeded event: %w", err)
	}
	if err := c.publishProgressEvent(rolloutCtx, c.cfg.DeploymentExchange, c.cfg.OrchestrationRunningRoutingKey, progressEvent); err != nil {
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
	progressEvent := newOrchestrationFailedEvent(event, target, deployErr)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if c.ledger != nil {
		bundle, err := makeDeployResultEvents(c.cfg.DeployFailedRoutingKey, failureEvent, c.cfg.OrchestrationFailedRoutingKey, progressEvent)
		if err != nil {
			return actionNackRequeue, err
		}
		if err := c.ledger.Fail(ctx, event.DeploymentID, "orchestrate", bundle); err != nil {
			return actionNackRequeue, fmt.Errorf("persist deploy failure: %w", err)
		}
	}

	if err := c.publishEvent(ctx, c.cfg.RabbitMQExchange, c.cfg.DeployFailedRoutingKey, failureEvent); err != nil {
		return actionNackRequeue, fmt.Errorf("%v; publish deploy failed event: %w", deployErr, err)
	}
	if err := c.publishProgressEvent(ctx, c.cfg.DeploymentExchange, c.cfg.OrchestrationFailedRoutingKey, progressEvent); err != nil {
		return actionNackRequeue, fmt.Errorf("%v; publish orchestration failed event: %w", deployErr, err)
	}

	return actionAck, deployErr
}

func makeDeployResultEvents(sk string, se any, pk string, pe any) (deployResultEvents, error) {
	s, e := json.Marshal(se)
	if e != nil {
		return deployResultEvents{}, e
	}
	p, e := json.Marshal(pe)
	return deployResultEvents{sk, s, pk, p}, e
}
func (c *DeployConsumer) publishStored(raw json.RawMessage) (deliveryAction, error) {
	var b deployResultEvents
	if e := json.Unmarshal(raw, &b); e != nil {
		return actionNackDrop, e
	}
	var s, p any
	if e := json.Unmarshal(b.ServiceEvent, &s); e != nil {
		return actionNackDrop, e
	}
	if e := json.Unmarshal(b.ProgressEvent, &p); e != nil {
		return actionNackDrop, e
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if e := c.publishEvent(ctx, c.cfg.RabbitMQExchange, b.ServiceRoutingKey, s); e != nil {
		return actionNackRequeue, e
	}
	if e := c.publishProgressEvent(ctx, c.cfg.DeploymentExchange, b.ProgressRoutingKey, p); e != nil {
		return actionNackRequeue, e
	}
	return actionAck, nil
}
func (c *DeployConsumer) startLeaseRenewal(id string) func() {
	if c.ledger == nil {
		return func() {}
	}
	ctx, cancel := context.WithCancel(context.Background())
	d := c.cfg.LeaseDuration / 3
	if d <= 0 {
		d = time.Minute
	}
	go func() {
		t := time.NewTicker(d)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				_ = c.ledger.Renew(ctx, id, "orchestrate", c.cfg.LeaseDuration)
			}
		}
	}()
	return cancel
}
func declareDeployRetryTopology(ch *amqp091.Channel, cfg Config) error {
	if _, e := ch.QueueDeclare(cfg.DLQ, true, false, false, false, nil); e != nil {
		return fmt.Errorf("declare deploy dlq: %w", e)
	}
	for i, d := range cfg.RetryBackoffs {
		name := fmt.Sprintf("%s.%d", cfg.RetryQueuePrefix, i+1)
		args := amqp091.Table{"x-message-ttl": int32(d / time.Millisecond), "x-dead-letter-exchange": cfg.RabbitMQExchange, "x-dead-letter-routing-key": cfg.BuildSucceededRoutingKey}
		if _, e := ch.QueueDeclare(name, true, false, false, false, args); e != nil {
			return e
		}
	}
	return nil
}
func (c *DeployConsumer) scheduleRetry(msg amqp091.Delivery, cause error) error {
	attempt := deployHeaderAttempt(msg.Headers) + 1
	if attempt >= c.cfg.MaxAttempts {
		_ = c.emitExhausted(msg.Body, cause)
		return c.copyToDLQAttempt(msg, cause, attempt)
	}
	idx := attempt - 1
	if idx >= len(c.cfg.RetryBackoffs) {
		idx = len(c.cfg.RetryBackoffs) - 1
	}
	_ = c.emitRetrying(msg.Body, attempt, cause)
	h := deployCloneHeaders(msg.Headers)
	h["x-shiply-attempt"] = int32(attempt)
	h["x-shiply-retry-reason"] = deployReason(cause)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	return c.ch.PublishWithContext(ctx, "", fmt.Sprintf("%s.%d", c.cfg.RetryQueuePrefix, idx+1), false, false, amqp091.Publishing{ContentType: msg.ContentType, DeliveryMode: amqp091.Persistent, Headers: h, Body: msg.Body})
}
func (c *DeployConsumer) copyToDLQ(msg amqp091.Delivery, e error) error {
	return c.copyToDLQAttempt(msg, e, deployHeaderAttempt(msg.Headers))
}
func (c *DeployConsumer) copyToDLQAttempt(msg amqp091.Delivery, e error, a int) error {
	h := deployCloneHeaders(msg.Headers)
	h["x-shiply-dead-letter-reason"] = deployReason(e)
	h["x-shiply-attempt"] = int32(a)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	return c.ch.PublishWithContext(ctx, "", c.cfg.DLQ, false, false, amqp091.Publishing{ContentType: msg.ContentType, DeliveryMode: amqp091.Persistent, Headers: h, Body: msg.Body})
}
func deployHeaderAttempt(h amqp091.Table) int {
	switch v := h["x-shiply-attempt"].(type) {
	case int32:
		return int(v)
	case int64:
		return int(v)
	case int:
		return v
	}
	return 0
}
func deployCloneHeaders(h amqp091.Table) amqp091.Table {
	r := amqp091.Table{}
	for k, v := range h {
		r[k] = v
	}
	return r
}
func deployReason(e error) string {
	if e == nil {
		return "unspecified"
	}
	s := e.Error()
	if len(s) > 500 {
		return s[:500]
	}
	return s
}
func (c *DeployConsumer) emitRetrying(body []byte, attempt int, cause error) error {
	var e ServiceEvent[BuildSucceededPayload]
	if json.Unmarshal(body, &e) != nil {
		return nil
	}
	event := newOrchestrationRetryingEvent(e, attempt, cause)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	return c.publishProgressEvent(ctx, c.cfg.DeploymentExchange, c.cfg.OrchestrationRetryingRoutingKey, event)
}
func (c *DeployConsumer) emitExhausted(body []byte, cause error) error {
	var e ServiceEvent[BuildSucceededPayload]
	if json.Unmarshal(body, &e) != nil {
		return nil
	}
	target, _ := targetForRequest(c.cfg, deployRequestFromEvent(c.cfg, e))
	_, err := c.completeFailure(e, target, fmt.Errorf("retry attempts exhausted: %w", cause))
	return err
}

func (c *DeployConsumer) publishProgressEvent(ctx context.Context, exchange, routingKey string, event any) error {
	if c.publish != nil {
		return c.publish(ctx, exchange, routingKey, event)
	}
	if c.pub == nil {
		return errors.New("rabbitmq publisher is not initialized")
	}
	return c.pub.PublishProgressJSON(ctx, exchange, routingKey, event)
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
