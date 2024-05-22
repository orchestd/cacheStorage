package mongodb

import "errors"

var NotFoundError = errors.New("Not found")
var InvalidDestType = errors.New("Invalid dest type")
var VersIsMandatory = errors.New("Vers field is mandatory")

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

func (e mongoCacheStorageError) IsVersMandatory() bool {
	return errors.Is(e.err, VersIsMandatory)
}
