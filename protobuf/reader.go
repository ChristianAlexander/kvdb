package protobuf

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/christianalexander/kvdb/stores"
	"github.com/gogo/protobuf/proto"
)

type protoReader struct {
	reader io.Reader
}

func NewReader(reader io.Reader) stores.Reader {
	return protoReader{reader}
}

func (r protoReader) Read(ctx context.Context, records chan<- stores.Record) error {
	br := bufio.NewReader(r.reader)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			l, err := binary.ReadUvarint(br)
			if err != nil {
				return fmt.Errorf("failed to read from record file: %v", err)
			}

			buf := make([]byte, l)
			br.Read(buf)

			var record Record
			err = proto.Unmarshal(buf, &record)
			if err != nil {
				return fmt.Errorf("failed to unmarshal record: %v", err)
			}

			records <- *record.ToRecord()
		}
	}
}
