package middlewares

import "github.com/orchestd/cacheStorage"

type cacheGetterWrapper struct {
	cacheStorage.CacheStorageGetter
}

func CreateCacheGetterWrapper(cacheGetter cacheStorage.CacheStorageGetter, middlewares ...cacheStorage.CacheStorageGetterMiddleware) cacheStorage.CacheStorageGetterWrapper {
	for _, middleware := range middlewares {
		cacheGetter = middleware(cacheGetter)
	}
	return cacheGetterWrapper{
		CacheStorageGetter: cacheGetter,
	}
}

type cacheSetterWrapper struct {
	cacheStorage.CacheStorageSetter
}

func CreateCacheSetterWrapper(cacheSetter cacheStorage.CacheStorageSetter, middlewares ...cacheStorage.CacheStorageSetterMiddleware) cacheStorage.CacheStorageSetterWrapper {
	for _, middleware := range middlewares {
		cacheSetter = middleware(cacheSetter)
	}
	return cacheSetterWrapper{
		CacheStorageSetter: cacheSetter,
	}
}
