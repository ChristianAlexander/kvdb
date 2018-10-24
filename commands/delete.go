package commands

import (
	"context"
	"io"

	"github.com/christianalexander/kvdb/stores"
)

// delete is a command that Deletes a value from the store.
type delete struct {
	writer io.Writer
	store  stores.Store
	key    string
}

// Execute satisfies the command interface.
func (q delete) Execute(ctx context.Context) error {
	err := q.store.Delete(ctx, q.key)
	if err == nil {
		q.writer.Write([]byte("OK\r\n"))
	}

	return err
}

// NewDelete creates a new delete command.
func NewDelete(writer io.Writer, store stores.Store, key string) Command {
	return delete{writer, store, key}
}
