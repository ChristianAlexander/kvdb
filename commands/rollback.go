package commands

import (
	"context"
	"fmt"
	"io"

	"github.com/christianalexander/kvdb"
	"github.com/christianalexander/kvdb/transactors"
)

// rollback is a command that completes a transaction.
type rollback struct {
	writer     io.Writer
	transactor transactors.Transactor
	setTxID    func(int64)
}

// Execute satisfies the command interface.
func (q rollback) Execute(ctx context.Context) error {
	q.transactor.Rollback(ctx)
	q.writer.Write([]byte("OK\r\n"))
	q.setTxID(0)

	return nil
}

func (q rollback) Undo(ctx context.Context) error {
	return fmt.Errorf("cannot undo a rollback command")
}

func (q rollback) ShouldAutoTransact() bool {
	return false
}

// NewRollback creates a new rollback command.
func NewRollback(writer io.Writer, transactor transactors.Transactor, setTxID func(int64)) kvdb.Command {
	return rollback{writer, transactor, setTxID}
}
