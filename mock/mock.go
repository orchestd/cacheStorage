package mock

import (
	"bitbucket.org/HeilaSystems/cacheStorage"
	"context"
)

type mockStorage struct {
	Username string
	Password string
	Host     string
	Database string
}

func (m mockStorage) Connect(ctx context.Context) error {
	return nil
}

func (m mockStorage) Close(ctx context.Context) error {
	return nil
}

func (m mockStorage) GetCacheStorageClient() (cacheStorage.CacheStorageGetter, cacheStorage.CacheStorageSetter) {
	client :=  mockClient{}
	return client,client
}



