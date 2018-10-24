package commands

import "context"

// Command is a thing to do.
type Command interface {
	Execute(ctx context.Context) error
}
