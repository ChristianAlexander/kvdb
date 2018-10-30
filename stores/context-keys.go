package stores

type contextKey struct {
	name string
}

// ContextKeyTransactionID is a context key for the transaction ID.
var ContextKeyTransactionID = contextKey{"TXID"}
