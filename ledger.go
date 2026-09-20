package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	_ "github.com/jackc/pgx/v5/stdlib"
	"regexp"
	"time"
)

type StageState string

const (
	StateInProgress StageState = "IN_PROGRESS"
	StateCompleted  StageState = "COMPLETED"
	StateFailed     StageState = "FAILED"
)

type ClaimDisposition int

const (
	ClaimAcquired ClaimDisposition = iota
	ClaimCompleted
	ClaimBusy
	ClaimFailed
)

type StageRecord struct {
	DeploymentID   string
	Stage          string
	State          StageState
	Attempt        int
	LeaseExpiresAt time.Time
	ResultJSON     json.RawMessage
}
type StageLedger interface {
	Claim(context.Context, string, string, time.Duration) (ClaimDisposition, StageRecord, error)
	Renew(context.Context, string, string, time.Duration) error
	Complete(context.Context, string, string, any) error
	Fail(context.Context, string, string, any) error
	Close() error
}
type PostgresStageLedger struct {
	db    *sql.DB
	table string
}

var schemaPattern = regexp.MustCompile(`^[a-z_][a-z0-9_]*$`)

func NewPostgresStageLedger(ctx context.Context, url, schema string) (*PostgresStageLedger, error) {
	if url == "" {
		return nil, errors.New("DATABASE_URL is required")
	}
	if !schemaPattern.MatchString(schema) {
		return nil, fmt.Errorf("invalid ledger schema %q", schema)
	}
	db, e := sql.Open("pgx", url)
	if e != nil {
		return nil, e
	}
	l := &PostgresStageLedger{db: db, table: schema + ".stage_ledger"}
	if e = db.PingContext(ctx); e != nil {
		db.Close()
		return nil, fmt.Errorf("connect stage ledger: %w", e)
	}
	return l, nil
}
func (l *PostgresStageLedger) Close() error { return l.db.Close() }
func (l *PostgresStageLedger) Claim(ctx context.Context, id, stage string, d time.Duration) (ClaimDisposition, StageRecord, error) {
	tx, e := l.db.BeginTx(ctx, &sql.TxOptions{})
	if e != nil {
		return 0, StageRecord{}, e
	}
	defer tx.Rollback()
	var r StageRecord
	e = tx.QueryRowContext(ctx, `SELECT deployment_id,stage,state,attempt,lease_expires_at,result_json FROM `+l.table+` WHERE deployment_id=$1 AND stage=$2 FOR UPDATE`, id, stage).Scan(&r.DeploymentID, &r.Stage, &r.State, &r.Attempt, &r.LeaseExpiresAt, &r.ResultJSON)
	now := time.Now().UTC()
	if errors.Is(e, sql.ErrNoRows) {
		r = StageRecord{DeploymentID: id, Stage: stage, State: StateInProgress, Attempt: 1, LeaseExpiresAt: now.Add(d)}
		_, e = tx.ExecContext(ctx, `INSERT INTO `+l.table+`(deployment_id,stage,state,attempt,lease_expires_at)VALUES($1,$2,$3,1,$4)`, id, stage, StateInProgress, r.LeaseExpiresAt)
		if e != nil {
			return 0, r, e
		}
		return ClaimAcquired, r, tx.Commit()
	}
	if e != nil {
		return 0, r, e
	}
	if r.State == StateCompleted {
		return ClaimCompleted, r, tx.Commit()
	}
	if r.State == StateFailed {
		return ClaimFailed, r, tx.Commit()
	}
	if r.LeaseExpiresAt.After(now) {
		return ClaimBusy, r, tx.Commit()
	}
	r.Attempt++
	r.LeaseExpiresAt = now.Add(d)
	_, e = tx.ExecContext(ctx, `UPDATE `+l.table+` SET state=$3,attempt=$4,lease_expires_at=$5,updated_at=now() WHERE deployment_id=$1 AND stage=$2`, id, stage, StateInProgress, r.Attempt, r.LeaseExpiresAt)
	if e != nil {
		return 0, r, e
	}
	return ClaimAcquired, r, tx.Commit()
}
func (l *PostgresStageLedger) Renew(ctx context.Context, id, stage string, d time.Duration) error {
	_, e := l.db.ExecContext(ctx, `UPDATE `+l.table+` SET lease_expires_at=$3,updated_at=now() WHERE deployment_id=$1 AND stage=$2 AND state=$4`, id, stage, time.Now().UTC().Add(d), StateInProgress)
	return e
}
func (l *PostgresStageLedger) set(ctx context.Context, id, stage string, s StageState, v any) error {
	b, e := json.Marshal(v)
	if e != nil {
		return e
	}
	_, e = l.db.ExecContext(ctx, `UPDATE `+l.table+` SET state=$3,result_json=$4,lease_expires_at=NULL,updated_at=now() WHERE deployment_id=$1 AND stage=$2`, id, stage, s, b)
	return e
}
func (l *PostgresStageLedger) Complete(ctx context.Context, id, stage string, v any) error {
	return l.set(ctx, id, stage, StateCompleted, v)
}
func (l *PostgresStageLedger) Fail(ctx context.Context, id, stage string, v any) error {
	return l.set(ctx, id, stage, StateFailed, v)
}
