package transactors

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/christianalexander/kvdb"
	"github.com/christianalexander/kvdb/stores"
	"github.com/sirupsen/logrus"
)

// A Transactor is able to orchestrate transactions.
type Transactor interface {
	Execute(ctx context.Context, command kvdb.Command) error
	Begin(ctx context.Context) (transactionID int64, err error)
	Commit(ctx context.Context) error
	Rollback(ctx context.Context) error
}

type transactor struct {
	store               stores.Store
	mu                  sync.Mutex
	transactionCommands map[int64][]kvdb.Command
	latestTransactionID int64
}

// New creates a new Transactor.
func New(store stores.Store) Transactor {
	return &transactor{
		store:               store,
		transactionCommands: make(map[int64][]kvdb.Command),
	}
}

func (t *transactor) Execute(ctx context.Context, command kvdb.Command) error {
	txID, ok := ctx.Value(stores.ContextKeyTransactionID).(int64)
	if command.ShouldAutoTransact() && (!ok || txID == 0) {
		logrus.Printf("Assigned txID %d", txID)
		txID = atomic.AddInt64(&t.latestTransactionID, 1)
		ctx = context.WithValue(ctx, stores.ContextKeyTransactionID, txID)
		defer t.Commit(ctx)
	}

	err := command.Execute(ctx)

	t.mu.Lock()
	t.transactionCommands[txID] = append(t.transactionCommands[txID], command)
	t.mu.Unlock()

	return err
}

func (t *transactor) Begin(ctx context.Context) (transactionID int64, err error) {
	existingID, ok := ctx.Value(stores.ContextKeyTransactionID).(int64)
	if ok && existingID != 0 {
		return 0, fmt.Errorf("can not start a transaction within the existing transaction '%d'", existingID)
	}

	return atomic.AddInt64(&t.latestTransactionID, 1), nil
}

func (t *transactor) Commit(ctx context.Context) error {
	txID, ok := ctx.Value(stores.ContextKeyTransactionID).(int64)
	if !ok || txID == 0 {
		return fmt.Errorf("can not commit without a transaction")
	}

	t.store.Release(ctx)

	t.mu.Lock()
	delete(t.transactionCommands, txID)
	t.mu.Unlock()

	return nil
}

func (t *transactor) Rollback(ctx context.Context) error {
	txID, ok := ctx.Value(stores.ContextKeyTransactionID).(int64)
	if !ok || txID == 0 {
		return fmt.Errorf("can not rollback without a transaction")
	}

	commands, ok := t.transactionCommands[txID]
	if !ok {
		return fmt.Errorf("can not roll back transaction without command history")
	}

	for i := len(commands) - 1; i >= 0; i-- {
		commands[i].Undo(ctx)
	}

	t.store.Release(ctx)

	t.mu.Lock()
	delete(t.transactionCommands, txID)
	t.mu.Unlock()

	return nil
}
