package api

const (
	ErrorCodeMongoUnauthorized   = "MongoUnauthorized"
	ErrorCodeMongoNotInitialized = "MongoNotInitialized"
)

// ErrorResponse reports a stable machine-readable manager API error.
type ErrorResponse struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}
