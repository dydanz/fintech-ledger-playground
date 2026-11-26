package api

// ClientIDKey is the gin context key the access log reads client_id from.
// Other services on NewBaseEngine set it after authenticating a caller.
const ClientIDKey = ctxClientID
