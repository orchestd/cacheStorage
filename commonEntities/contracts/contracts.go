package contracts

import (
	. "bitbucket.org/HeilaSystems/cacheStorage"
	. "bitbucket.org/HeilaSystems/cacheStorage/commonEntities/models"
	"context"
)

type StoreGetter interface {
	GetById(ctx context.Context, id string) (Store, CacheStorageError)

	GetMany(ctx context.Context, ids []string) ([]Store, CacheStorageError)

	GetAll(ctx context.Context) ([]Store, CacheStorageError)
}

type ChainGetter interface {
	GetById(ctx context.Context, id string) (Chain, CacheStorageError)

	GetMany(ctx context.Context, ids []string) ([]Chain, CacheStorageError)

	GetAll(ctx context.Context) ([]Chain, CacheStorageError)
}
