package commands

import (
	"context"
	"io"

	"github.com/christianalexander/kvdb"
	"github.com/christianalexander/kvdb/stores"
)

// delete is a command that Deletes a value from the store.
type delete struct {
	writer        io.Writer
	store         stores.Store
	key           string
	previousValue string
}

// Execute satisfies the command interface.
func (q *delete) Execute(ctx context.Context) error {
	val, err := q.store.Get(ctx, q.key)
	if err != nil {
		return err
	}
	q.previousValue = val

	err = q.store.Delete(ctx, q.key)
	if err == nil {
		q.writer.Write([]byte("OK\r\n"))
	}

	return err
}

func (q *delete) Undo(ctx context.Context) error {
	if q.previousValue != "" {
		return q.store.Set(ctx, q.key, q.previousValue)
	}
	return nil
}

func (q delete) ShouldAutoTransact() bool {
	return true
}

// NewDelete creates a new delete command.
func NewDelete(writer io.Writer, store stores.Store, key string) kvdb.Command {
	return &delete{
		writer: writer,
		store:  store,
		key:    key,
	}
}
