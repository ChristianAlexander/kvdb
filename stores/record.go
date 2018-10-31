package stores

import "fmt"

type RecordKind string

const (
	RecordKindSet    RecordKind = "SET"
	RecordKindDelete            = "DEL"
	RecordKindCommit            = "COMMIT"
)

type Record struct {
	Kind          RecordKind
	TransactionID int64
	Key           string
	Value         string
}

func (r Record) String() string {
	return fmt.Sprintf("%s:%d:%s:%s", r.Kind, r.TransactionID, r.Key, r.Value)
}
