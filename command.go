package kvdb

import "context"

// Command is a thing to do.
type Command interface {
	Execute(ctx context.Context) error
	Undo(ctx context.Context) error
	ShouldAutoTransact() bool
}
