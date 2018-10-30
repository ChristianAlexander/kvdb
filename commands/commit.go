package commands

import (
	"context"
	"fmt"
	"io"

	"github.com/christianalexander/kvdb"
	"github.com/christianalexander/kvdb/transactors"
)

// commit is a command that completes a transaction.
type commit struct {
	writer     io.Writer
	transactor transactors.Transactor
	setTxID    func(int64)
}

// Execute satisfies the command interface.
func (q commit) Execute(ctx context.Context) error {
	err := q.transactor.Commit(ctx)
	if err == nil {
		q.writer.Write([]byte("OK\r\n"))
		q.setTxID(0)
	}

	return err
}

func (q commit) Undo(ctx context.Context) error {
	return fmt.Errorf("cannot undo a commit command")
}

func (q commit) ShouldAutoTransact() bool {
	return false
}

// NewCommit creates a new commit command.
func NewCommit(writer io.Writer, transactor transactors.Transactor, setTxID func(int64)) kvdb.Command {
	return commit{writer, transactor, setTxID}
}
