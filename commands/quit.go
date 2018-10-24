package commands

import "context"

// quit is a command that closes the connection.
type quit struct {
	cancelConnection func() error
}

// Execute satisfies the command interface.
func (q quit) Execute(context.Context) error {
	return q.cancelConnection()
}

// NewQuit creates a new quit command.
func NewQuit(cancelConnection func() error) Command {
	return quit{cancelConnection}
}
