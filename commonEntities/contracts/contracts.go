package contracts

import (
	. "bitbucket.org/HeilaSystems/cacheStorage"
	. "bitbucket.org/HeilaSystems/cacheStorage/commonEntities/models"
	"context"
)

type StoreGetter interface {
	GetById(ctx context.Context, ver string, id string) (Store, CacheStorageError)

	GetMany(ctx context.Context, ver string, ids []string) ([]Store, CacheStorageError)

	GetAll(ctx context.Context, ver string) ([]Store, CacheStorageError)
}

type ChainGetter interface {
	GetById(ctx context.Context, ver string, id string) (Chain, CacheStorageError)

	GetMany(ctx context.Context, ver string, ids []string) ([]Chain, CacheStorageError)

	GetAll(ctx context.Context, ver string) ([]Chain, CacheStorageError)
}
