package commands

import (
	"context"

	"github.com/christianalexander/kvdb"
)

// quit is a command that closes the connection.
type quit struct {
	cancelConnection func() error
}

// Execute satisfies the command interface.
func (q quit) Execute(context.Context) error {
	return q.cancelConnection()
}

func (q quit) Undo(ctx context.Context) error {
	return nil
}

func (q quit) ShouldAutoTransact() bool {
	return false
}

// NewQuit creates a new quit command.
func NewQuit(cancelConnection func() error) kvdb.Command {
	return quit{cancelConnection}
}
