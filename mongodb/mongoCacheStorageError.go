package mongodb

import "errors"

var NotFoundError = errors.New("Not found")
var InvalidDestType = errors.New("Invalid dest type")

type mongoCacheStorageError struct {
	err error
}

func NewMongoCacheStorageError(err error) *mongoCacheStorageError {
	return &mongoCacheStorageError{err: err}
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
