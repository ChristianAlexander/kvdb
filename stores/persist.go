package stores

import (
	"context"
	"fmt"

	"github.com/sirupsen/logrus"
)

type withPersistence struct {
	Store
	writer Writer
}

func WithPersistence(writer Writer, store Store) Store {
	return &withPersistence{
		Store:  store,
		writer: writer,
	}
}

func (s *withPersistence) Set(ctx context.Context, key string, value string) error {
	txID := ctx.Value(ContextKeyTransactionID).(int64)
	err := s.writer.Write(ctx, Record{
		Kind:          RecordKindSet,
		TransactionID: txID,
		Key:           key,
		Value:         value,
	})
	if err != nil {
		return fmt.Errorf("failed to write set operation: %v", err)
	}

	return s.Store.Set(ctx, key, value)
}

func (s *withPersistence) Delete(ctx context.Context, key string) error {
	txID := ctx.Value(ContextKeyTransactionID).(int64)
	err := s.writer.Write(ctx, Record{
		Kind:          RecordKindDelete,
		TransactionID: txID,
		Key:           key,
	})
	if err != nil {
		return fmt.Errorf("failed to write delete operation: %v", err)
	}

	return s.Store.Delete(ctx, key)
}

func applyRecord(ctx context.Context, pendingTransactionRecords map[int64][]Record, store Store, record Record) {
	logrus.Debugln(record.String())
	switch record.Kind {
	case RecordKindSet:
		if record.TransactionID != 0 {
			pendingTransactionRecords[record.TransactionID] = append(pendingTransactionRecords[record.TransactionID], record)
		} else {
			err := store.Set(ctx, record.Key, record.Value)
			if err != nil {
				logrus.Warnf("Failed to replay set record: %v", err)
			}
		}
	case RecordKindDelete:
		if record.TransactionID != 0 {
			pendingTransactionRecords[record.TransactionID] = append(pendingTransactionRecords[record.TransactionID], record)
		} else {
			err := store.Delete(ctx, record.Key)
			if err != nil {
				logrus.Warnf("Failed to replay delete record: %v", err)
			}
		}
	case RecordKindCommit:
		if records, ok := pendingTransactionRecords[record.TransactionID]; ok {
			for _, r := range records {
				r.TransactionID = 0
				applyRecord(ctx, pendingTransactionRecords, store, r)
			}
		}
	default:
		logrus.Warnf("Received record of unknown type '%s'", record.Kind)
	}
}

func FromPersistence(ctx context.Context, reader Reader, store Store) (Store, error) {
	records := make(chan Record)
	pendingTransactionRecords := make(map[int64][]Record)

	go func() {
		reader.Read(ctx, records)
		close(records)
	}()

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case r, ok := <-records:
			if !ok {
				return store, nil
			}

			applyRecord(ctx, pendingTransactionRecords, store, r)
		}
	}
}
