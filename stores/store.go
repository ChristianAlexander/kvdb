package stores

import "context"

type Store interface {
	Set(ctx context.Context, key, value string) error
	Get(ctx context.Context, key string) (string, error)
	Delete(ctx context.Context, key string) error
	Keys(ctx context.Context) ([]string, error)
	Release(ctx context.Context)
}
