package mongodb

import "errors"

var NotFoundError = errors.New("Not found")
var InvalidDestType = errors.New("Invalid dest type")

type mongoCacheStorageError struct {
	err         error
	notFoundIds []string
}

func NewMongoCacheStorageError(err error, notFoundIds []string) *mongoCacheStorageError {
	return &mongoCacheStorageError{err: err, notFoundIds: notFoundIds}
}

func (e mongoCacheStorageError) Error() string {
	return e.err.Error()
}

func (e mongoCacheStorageError) IsNotFound() bool {
	return errors.Is(e.err, NotFoundError)
}

func (e mongoCacheStorageError) IsInvalidDestType() bool {
	return errors.Is(e.err, InvalidDestType)
}

func (e mongoCacheStorageError) NotFoundIds() []string {
	return e.notFoundIds
}
