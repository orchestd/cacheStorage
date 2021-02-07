package cacheStorage

import (
	"context"
)

type CacheStorageError interface {
	IsNotFound() bool
	IsInvalidDestType() bool
	Error() string
}

type CacheVersion struct {
	CollectionName string
	Version        string
}

type CacheStorageGetter interface {
	GetById(c context.Context, collectionName string, id string, ver string, dest interface{}) CacheStorageError
	GetManyByIds(c context.Context, collectionName string, ids []string, ver string, dest interface{}) CacheStorageError
	GetAll(c context.Context, collectionName string, ver string, dest interface{}) CacheStorageError
	GetLatestVersions(c context.Context) ([]CacheVersion, CacheStorageError)
	GetLatestCollectionVersion(c context.Context, collection string) (CacheVersion, CacheStorageError)
}

type CacheStorageSetter interface {
	Insert(c context.Context, collectionName string, id string, ver string, item interface{}) CacheStorageError
	InsertMany(c context.Context, collectionName string, ver string, items map[string]interface{}) CacheStorageError
	InsertOrUpdate(c context.Context, collectionName string, id string, ver string, item interface{}) CacheStorageError
	Update(c context.Context, collectionName string, id string, ver string, item interface{}) CacheStorageError
	Remove(c context.Context, collectionName string, id string, ver string) CacheStorageError
	RemoveAll(c context.Context, collectionName string, ver string) CacheStorageError
}

type CacheStorage interface {
	Connect(c context.Context, connectionString string, database string) error
	Close(context.Context) error
	GetCacheStorageClient() (CacheStorageGetter, CacheStorageSetter)
}

/*func DefaultCacheStorageClient(lc fx.Lifecycle, credentials credentials.CredentialsGetter,cacheStorage cacheStorage.CacheStorage) (cache.CacheStorageGetter, cache.CacheStorageSetter) {
	creds := credentials.GetCredentials()
	lc.Append(fx.Hook{
		OnStart: func(c context.Context) error {
			return cacheStorage.Connect(c, creds.DbUsername, creds.DbPassword, creds.DbHost, creds.DbName)
		},
		OnStop: func(c context.Context) error {
			return cacheStorage.Close(c)
		},
	})
	return cacheStorage.GetCacheStorageClient()
}*/
