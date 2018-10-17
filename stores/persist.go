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
	err := s.writer.Write(ctx, Record{
		Kind:  RecordKindSet,
		Key:   key,
		Value: value,
	})
	if err != nil {
		return fmt.Errorf("failed to write set operation: %v", err)
	}

	return s.Store.Set(ctx, key, value)
}

func (s *withPersistence) Delete(ctx context.Context, key string) error {
	err := s.writer.Write(ctx, Record{
		Kind: RecordKindDelete,
		Key:  key,
	})
	if err != nil {
		return fmt.Errorf("failed to write delete operation: %v", err)
	}

	return s.Store.Delete(ctx, key)
}

func FromPersistence(ctx context.Context, reader Reader, store Store) (Store, error) {
	records := make(chan Record)

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

			switch r.Kind {
			case RecordKindSet:
				err := store.Set(ctx, r.Key, r.Value)
				if err != nil {
					logrus.Warnf("Failed to replay set record: %v", err)
				}
				continue
			case RecordKindDelete:
				err := store.Delete(ctx, r.Key)
				if err != nil {
					logrus.Warnf("Failed to replay delete record: %v", err)
				}
				continue
			default:
				logrus.Warnf("Received record of unknown type '%s'", r.Kind)
			}
		}
	}
}
