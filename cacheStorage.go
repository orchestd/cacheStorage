package cacheStorage

import "context"
import "go.uber.org/fx"

type Credentials struct {
	Username string
	Password string
	Host     string
	Database string
}

type CacheStorageGetter interface {
	GetById(c context.Context, collectionName string, id interface{}, dest interface{}) error
	GetManyByIds(c context.Context, collectionName string, id []interface{}, dest interface{}) error
	GetAll(c context.Context, collectionName string, dest interface{}) error
}

type CacheStorageSetter interface {
	Insert(c context.Context, collectionName string, id interface{}, item interface{}) error
	InsertMany(c context.Context, collectionName string, items map[interface{}]interface{}) error
	InsertOrUpdate(c context.Context, collectionName string, id interface{}, item interface{}) error
	Update(c context.Context, collectionName string, id interface{}, item interface{}) error
	Remove(c context.Context, collectionName string, id interface{}) error
	RemoveAll(c context.Context, collectionName string) error
}

type CacheStorage interface {
	Connect(c context.Context, username string, password string, host string, database string) error
	Close(context.Context) error
	GetCacheStorageClient() (CacheStorageGetter, CacheStorageSetter)
}

func NewCacheStorageClient(lc fx.Lifecycle, credentials Credentials, cacheStorage CacheStorage) (CacheStorageGetter, CacheStorageSetter) {
	lc.Append(fx.Hook{
		OnStart: func(c context.Context) error {
			return cacheStorage.Connect(c, credentials.Username, credentials.Password, credentials.Host, credentials.Database)
		},
		OnStop: func(c context.Context) error {
			return cacheStorage.Close(c)
		},
	})
	return cacheStorage.GetCacheStorageClient()
}
