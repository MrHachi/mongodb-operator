package mongodb

import (
	"errors"
	"fmt"

	"go.mongodb.org/mongo-driver/mongo"
)

var (
	ErrNotInitialized       = errors.New("replica set is not initialized")
	ErrUnauthorized         = errors.New("MongoDB command requires authentication")
	ErrAuthenticationFailed = errors.New("MongoDB authentication failed")
)

const (
	mongoUnauthorizedCode         = 13
	mongoNotInitializedCode       = 94
	mongoAuthenticationFailedCode = 18

	mongoUserAlreadyExistsCode = 51003
)

func wrapMongoError(operation string, err error) error {
	if commandErr, ok := errors.AsType[mongo.CommandError](err); ok {
		switch commandErr.Code {
		case mongoUnauthorizedCode:
			return fmt.Errorf("%s: %w: %w", operation, ErrUnauthorized, err)
		case mongoNotInitializedCode:
			return fmt.Errorf("%s: %w: %w", operation, ErrNotInitialized, err)
		case mongoAuthenticationFailedCode:
			return fmt.Errorf("%s: %w", operation, ErrAuthenticationFailed)
		}
	}
	return fmt.Errorf("%s: %w", operation, err)
}
