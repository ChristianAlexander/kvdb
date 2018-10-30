package commands

import (
	"context"
	"fmt"
	"io"

	"github.com/christianalexander/kvdb"
	"github.com/christianalexander/kvdb/stores"
)

// get is a command that gets a value from the store.
type get struct {
	writer io.Writer
	store  stores.Store
	key    string
}

// Execute satisfies the command interface.
func (q get) Execute(ctx context.Context) error {
	val, err := q.store.Get(ctx, q.key)
	if err != nil {
		q.writer.Write([]byte("\r\n"))
		return nil
	}

	fmt.Fprintln(q.writer, val)
	return nil
}

func (q get) Undo(ctx context.Context) error {
	return nil
}

func (q get) ShouldAutoTransact() bool {
	return true
}

// NewGet creates a new get command.
func NewGet(writer io.Writer, store stores.Store, key string) kvdb.Command {
	return get{writer, store, key}
}
