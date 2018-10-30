package commands

import (
	"context"
	"io"

	"github.com/christianalexander/kvdb"
	"github.com/christianalexander/kvdb/stores"
	"github.com/sirupsen/logrus"
)

// set is a command that sets a value in the store.
type set struct {
	writer                    io.Writer
	store                     stores.Store
	key, value, previousValue string
}

// Execute satisfies the command interface.
func (q *set) Execute(ctx context.Context) error {
	val, _ := q.store.Get(ctx, q.key)
	q.previousValue = val

	err := q.store.Set(ctx, q.key, q.value)
	if err == nil {
		q.writer.Write([]byte("OK\r\n"))
	}

	return err
}

func (q *set) Undo(ctx context.Context) error {
	logrus.Print(q.previousValue)
	if q.previousValue == "" {
		return q.store.Delete(ctx, q.key)
	}

	return q.store.Set(ctx, q.key, q.previousValue)
}

func (q set) ShouldAutoTransact() bool {
	return true
}

// NewSet creates a new set command.
func NewSet(writer io.Writer, store stores.Store, key, value string) kvdb.Command {
	return &set{
		writer: writer,
		store:  store,
		key:    key,
		value:  value,
	}
}
