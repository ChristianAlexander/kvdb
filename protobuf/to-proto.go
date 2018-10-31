package protobuf

import "github.com/christianalexander/kvdb/stores"

func RecordToProto(r stores.Record) *Record {
	return &Record{
		Kind:          recordKindToProto(r.Kind),
		TransactionId: r.TransactionID,
		Key:           r.Key,
		Value:         r.Value,
	}
}

func recordKindToProto(k stores.RecordKind) Record_RecordKind {
	switch k {
	case stores.RecordKindSet:
		return Record_SET
	case stores.RecordKindDelete:
		return Record_DEL
	case stores.RecordKindCommit:
		return Record_CMT
	}

	return Record_SET
}

func recordKindFromProto(k Record_RecordKind) stores.RecordKind {
	switch k {
	case Record_SET:
		return stores.RecordKindSet
	case Record_DEL:
		return stores.RecordKindDelete
	case Record_CMT:
		return stores.RecordKindCommit
	}

	return stores.RecordKindSet
}

func (r Record) ToRecord() *stores.Record {
	return &stores.Record{
		Kind:          recordKindFromProto(r.Kind),
		TransactionID: r.TransactionId,
		Key:           r.Key,
		Value:         r.Value,
	}
}
