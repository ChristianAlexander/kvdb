package serializable

import (
	"context"
	"fmt"

	"github.com/christianalexander/kvdb/stores"
)

// twoPhaseLockStore implements two-phase locking for serializable isolation.
type twoPhaseLockStore struct {
	stores.Store
	lm lockerMap
}

// NewTwoPhaseLockStore returns a store with two-phase locking for serializable isolation.
func NewTwoPhaseLockStore(store stores.Store) stores.Store {
	return &twoPhaseLockStore{
		Store: store,
		lm: lockerMap{
			lockers: make(map[string]*keyLocker),
			keys:    make(map[int64][]string),
		},
	}
}

func (ts *twoPhaseLockStore) Set(ctx context.Context, key, value string) error {
	txID, ok := ctx.Value(stores.ContextKeyTransactionID).(int64)
	if !ok || txID == 0 {
		return fmt.Errorf("two phase lock store could not set without a transaction ID")
	}

	err := ts.lm.Acquire(ctx, txID, key)
	if err != nil {
		return err
	}

	err = ts.Store.Set(ctx, key, value)
	if err != nil {
		return err
	}

	return nil
}

func (ts *twoPhaseLockStore) Get(ctx context.Context, key string) (string, error) {
	txID, ok := ctx.Value(stores.ContextKeyTransactionID).(int64)
	if !ok || txID == 0 {
		return "", fmt.Errorf("two phase lock store could not get without a transaction ID")
	}

	err := ts.lm.RAcquire(ctx, txID, key)
	if err != nil {
		return "", err
	}
	v, err := ts.Store.Get(ctx, key)
	if err != nil {
		return "", err
	}

	return v, nil
}

func (ts *twoPhaseLockStore) Delete(ctx context.Context, key string) error {
	txID, ok := ctx.Value(stores.ContextKeyTransactionID).(int64)
	if !ok || txID == 0 {
		return fmt.Errorf("two phase lock store could not delete without a transaction ID")
	}

	err := ts.lm.Acquire(ctx, txID, key)
	if err != nil {
		return err
	}

	err = ts.Store.Delete(ctx, key)
	if err != nil {
		return err
	}

	return nil
}

func (ts *twoPhaseLockStore) Keys(ctx context.Context) ([]string, error) {
	txID, ok := ctx.Value(stores.ContextKeyTransactionID).(int64)
	if !ok || txID == 0 {
		return nil, fmt.Errorf("two phase lock store could not get keys without a transaction ID")
	}

	keys, err := ts.Keys(ctx)
	if err != nil {
		return nil, err
	}

	for _, k := range keys {
		err := ts.lm.RAcquire(ctx, txID, k)
		if err != nil {
			return nil, err
		}
	}

	return keys, nil
}

func (ts *twoPhaseLockStore) Release(ctx context.Context) {
	txID, ok := ctx.Value(stores.ContextKeyTransactionID).(int64)
	if !ok || txID == 0 {
		return
	}
	ts.lm.Release(txID)
}
