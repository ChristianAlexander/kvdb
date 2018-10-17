package protobuf

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/christianalexander/kvdb/stores"
	proto "github.com/golang/protobuf/proto"
)

type protoWriter struct {
	writer io.Writer
}

func NewWriter(writer io.Writer) stores.Writer {
	return protoWriter{writer}
}

func (w protoWriter) Write(ctx context.Context, record stores.Record) error {
	protoRecord := RecordToProto(record)

	out, err := proto.Marshal(protoRecord)
	if err != nil {
		return fmt.Errorf("failed to marshal record %s: %v", record, err)
	}

	lBuf := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(lBuf, uint64(len(out)))

	mr := io.MultiReader(bytes.NewBuffer(lBuf[:n]), bytes.NewBuffer(out))

	_, err = io.Copy(w.writer, mr)
	if err != nil {
		return fmt.Errorf("failed to write record to log: %v", err)
	}

	return nil
}
