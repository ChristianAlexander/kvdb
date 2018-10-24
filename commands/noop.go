package commands

import (
	"context"
)

// noop is a command that does nothing.
type noop struct{}

// Execute satisfies the command interface.
func (q noop) Execute(ctx context.Context) error {
	return nil
}

// NewNoop creates a new noop command.
func NewNoop() Command {
	return noop{}
}
