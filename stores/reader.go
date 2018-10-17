package stores

import (
	"context"
)

type Reader interface {
	Read(ctx context.Context, records chan<- Record) error
}
