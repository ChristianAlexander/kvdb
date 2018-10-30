package commands

import (
	"context"
	"io"

	"github.com/christianalexander/kvdb"
	"github.com/christianalexander/kvdb/transactors"
	"github.com/sirupsen/logrus"
)

// begin is a command that begins a transaction.
type begin struct {
	writer     io.Writer
	transactor transactors.Transactor
	setTxID    func(int64)
}

// Execute satisfies the command interface.
func (q begin) Execute(ctx context.Context) error {
	txID, err := q.transactor.Begin(ctx)
	if err == nil {
		q.writer.Write([]byte("OK\r\n"))
		logrus.Printf("Begin setting txid to %d", txID)
		q.setTxID(txID)
	}

	return err
}

func (q begin) Undo(ctx context.Context) error {
	return nil
}

func (q begin) ShouldAutoTransact() bool {
	return false
}

// NewBegin creates a new begin command.
func NewBegin(writer io.Writer, transactor transactors.Transactor, setTxID func(int64)) kvdb.Command {
	return begin{writer, transactor, setTxID}
}
