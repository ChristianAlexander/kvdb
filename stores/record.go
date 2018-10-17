package stores

import "fmt"

type RecordKind string

const (
	RecordKindSet    RecordKind = "SET"
	RecordKindDelete            = "DEL"
)

type Record struct {
	Kind  RecordKind
	Key   string
	Value string
}

func (r Record) String() string {
	return fmt.Sprintf("%s:%s", r.Key, r.Value)
}
