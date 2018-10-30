package commands

import (
	"context"

	"github.com/christianalexander/kvdb"
)

// noop is a command that does nothing.
type noop struct{}

// Execute satisfies the command interface.
func (q noop) Execute(ctx context.Context) error {
	return nil
}

func (q noop) Undo(ctx context.Context) error {
	return nil
}

func (q noop) ShouldAutoTransact() bool {
	return false
}

// NewNoop creates a new noop command.
func NewNoop() kvdb.Command {
	return noop{}
}
