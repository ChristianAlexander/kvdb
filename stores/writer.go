package stores

import (
	"context"
)

type Writer interface {
	Write(ctx context.Context, record Record) error
}
