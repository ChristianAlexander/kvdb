package commands

import (
	"context"
	"io"

	"github.com/christianalexander/kvdb/stores"
)

// set is a command that sets a value in the store.
type set struct {
	writer     io.Writer
	store      stores.Store
	key, value string
}

// Execute satisfies the command interface.
func (q set) Execute(ctx context.Context) error {
	err := q.store.Set(ctx, q.key, q.value)
	if err == nil {
		q.writer.Write([]byte("OK\r\n"))
	}

	return err
}

// NewSet creates a new set command.
func NewSet(writer io.Writer, store stores.Store, key, value string) Command {
	return set{writer, store, key, value}
}
