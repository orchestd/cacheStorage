package trace

import (
	"bitbucket.org/HeilaSystems/cacheStorage"
	. "bitbucket.org/HeilaSystems/cacheStorage"
	"context"
	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
)

const cacheDbType = "mongodb"

type CacheWrapperConfiguration struct {
	ServiceName string
	DbHost      string
	DbUser      string
}

type CacheTags struct {
	collection *string
	ver        *string
	id         *string
	ids        *[]string
	item       interface{}
	items      interface{}
}

type mongoCacheStorageGetterWrapper struct {
	cacheStorageGetter cacheStorage.CacheStorageGetter
	tracer             opentracing.Tracer
	conf               CacheWrapperConfiguration
}

func NewMongoCacheStorageGetterWrapper(tracer opentracing.Tracer, conf CacheWrapperConfiguration) CacheStorageGetterMiddleware {
	return func(cacheStorageGetter cacheStorage.CacheStorageGetter) CacheStorageGetter {
		return &mongoCacheStorageGetterWrapper{cacheStorageGetter: cacheStorageGetter, tracer: tracer, conf: conf}
	}
}

func getSpanFromContext(c context.Context, operationName string, tracer opentracing.Tracer, conf CacheWrapperConfiguration, tags CacheTags, f func(con context.Context) CacheStorageError) {
	sp, con := opentracing.StartSpanFromContextWithTracer(c, tracer, operationName)
	defer sp.Finish()
	ext.DBType.Set(sp, cacheDbType)
	ext.DBUser.Set(sp, conf.DbUser)
	ext.DBInstance.Set(sp, conf.DbHost)
	ext.DBStatement.Set(sp, operationName)
	ext.Component.Set(sp, conf.ServiceName)
	if tags.collection != nil {
		sp.SetTag("cacheStorage/collection", *tags.collection)
	}
	if tags.id != nil {
		sp.SetTag("id", *tags.id)
	}
	if tags.id != nil {
		sp.SetTag("ids", *tags.ids)
	}
	if tags.ver != nil {
		sp.SetTag("cacheStorage/ver", *tags.ver)
	}
	if tags.item != nil {
		sp.SetTag("item", tags.item)
	}
	if tags.items != nil {
		sp.SetTag("items", tags.items)
	}
	err := f(con)
	if err != nil {
		ext.LogError(sp, err)
	}
}

func (m mongoCacheStorageGetterWrapper) GetLatestVersions(c context.Context) ([]CacheVersion, CacheStorageError) {
	var result []CacheVersion
	var err CacheStorageError
	f := func(con context.Context) CacheStorageError {
		result, err = m.cacheStorageGetter.GetLatestVersions(con)
		return err
	}
	getSpanFromContext(c, "mongodb.driver/GetLatestVersions", m.tracer, m.conf, CacheTags{}, f)
	return result, err
}

func (m mongoCacheStorageGetterWrapper) GetById(c context.Context, collectionName string, id string, ver string, dest interface{}) CacheStorageError {
	var err CacheStorageError
	f := func(con context.Context) CacheStorageError {
		err = m.cacheStorageGetter.GetById(con, collectionName, id, ver, dest)
		return err
	}
	getSpanFromContext(c, "mongodb.driver/GetById", m.tracer, m.conf, CacheTags{
		collection: &collectionName,
		ver:        &ver,
		id:         &id,
	}, f)
	return err
}

func (m mongoCacheStorageGetterWrapper) GetManyByIds(c context.Context, collectionName string, ids []string, ver string, dest interface{}) CacheStorageError {
	var err CacheStorageError
	f := func(con context.Context) CacheStorageError {
		err = m.cacheStorageGetter.GetManyByIds(con, collectionName, ids, ver, dest)
		return err
	}
	getSpanFromContext(c, "mongodb.driver/GetManyByIds", m.tracer, m.conf, CacheTags{
		collection: &collectionName,
		ver:        &ver,
		ids:        &ids,
	}, f)
	return err

}

func (m mongoCacheStorageGetterWrapper) GetAll(c context.Context, collectionName string, ver string, dest interface{}) CacheStorageError {
	var err CacheStorageError
	f := func(con context.Context) CacheStorageError {
		err = m.cacheStorageGetter.GetAll(con, collectionName, ver, dest)
		return err
	}
	getSpanFromContext(c, "mongodb.driver/GetById", m.tracer, m.conf, CacheTags{
		collection: &collectionName,
		ver:        &ver,
	}, f)
	return err
}

func (m mongoCacheStorageGetterWrapper) GetLatestCollectionVersion(c context.Context, collection string) (CacheVersion, CacheStorageError) {
	var result CacheVersion
	var err CacheStorageError
	f := func(con context.Context) CacheStorageError {
		result, err = m.cacheStorageGetter.GetLatestCollectionVersion(con, collection)
		return err
	}
	getSpanFromContext(c, "mongodb.driver/GetLatestCollectionVersion", m.tracer, m.conf, CacheTags{
		collection: &collection,
	}, f)
	return result, err

}

type mongoCacheStorageSetterWrapper struct {
	cacheStorageSetter cacheStorage.CacheStorageSetter
	tracer             opentracing.Tracer
	conf               CacheWrapperConfiguration
}

func NewMongoCacheStorageSetterWrapper(tracer opentracing.Tracer, conf CacheWrapperConfiguration) CacheStorageSetterMiddleware {
	return func(cacheStorageSetter cacheStorage.CacheStorageSetter) CacheStorageSetter {
		return &mongoCacheStorageSetterWrapper{cacheStorageSetter: cacheStorageSetter, tracer: tracer, conf: conf}
	}
}

func (m mongoCacheStorageSetterWrapper) Insert(c context.Context, collectionName string, id string, ver string, item interface{}) CacheStorageError {
	var err CacheStorageError
	f := func(con context.Context) CacheStorageError {
		err = m.cacheStorageSetter.Insert(con, collectionName, id, ver, item)
		return err
	}
	getSpanFromContext(c, "mongodb.driver/Insert", m.tracer, m.conf, CacheTags{
		collection: &collectionName,
		ver:        &ver,
		id:         &id,
		item:       item,
	}, f)
	return err
}

func (m mongoCacheStorageSetterWrapper) InsertMany(c context.Context, collectionName string, ver string, items map[string]interface{}) CacheStorageError {
	var err CacheStorageError
	f := func(con context.Context) CacheStorageError {
		err = m.cacheStorageSetter.InsertMany(con, collectionName, ver, items)
		return err
	}
	getSpanFromContext(c, "mongodb.driver/InsertMany", m.tracer, m.conf, CacheTags{
		collection: &collectionName,
		ver:        &ver,
		items:      &items,
	}, f)
	return err
}

func (m mongoCacheStorageSetterWrapper) InsertOrUpdate(c context.Context, collectionName string, id string, ver string, item interface{}) CacheStorageError {
	var err CacheStorageError
	f := func(con context.Context) CacheStorageError {
		err = m.cacheStorageSetter.InsertOrUpdate(con, collectionName, id, ver, item)
		return err
	}
	getSpanFromContext(c, "mongodb.driver/InsertOrUpdate", m.tracer, m.conf, CacheTags{
		collection: &collectionName,
		ver:        &ver,
		id:         &id,
		item:       item,
	}, f)
	return err
}

func (m mongoCacheStorageSetterWrapper) Update(c context.Context, collectionName string, id string, ver string, item interface{}) CacheStorageError {
	var err CacheStorageError
	f := func(con context.Context) CacheStorageError {
		err = m.cacheStorageSetter.Update(con, collectionName, id, ver, item)
		return err
	}
	getSpanFromContext(c, "mongodb.driver/Update", m.tracer, m.conf, CacheTags{
		collection: &collectionName,
		ver:        &ver,
		id:         &id,
		item:       item,
	}, f)
	return err
}

func (m mongoCacheStorageSetterWrapper) Remove(c context.Context, collectionName string, id string, ver string) CacheStorageError {
	var err CacheStorageError
	f := func(con context.Context) CacheStorageError {
		err = m.cacheStorageSetter.Remove(con, collectionName, id, ver)
		return err
	}
	getSpanFromContext(c, "mongodb.driver/Remove", m.tracer, m.conf, CacheTags{
		collection: &collectionName,
		ver:        &ver,
		id:         &id,
	}, f)
	return err
}

func (m mongoCacheStorageSetterWrapper) RemoveAll(c context.Context, collectionName string, ver string) CacheStorageError {
	var err CacheStorageError
	f := func(con context.Context) CacheStorageError {
		err = m.cacheStorageSetter.RemoveAll(con, collectionName, ver)
		return err
	}
	getSpanFromContext(c, "mongodb.driver/RemoveAll", m.tracer, m.conf, CacheTags{
		collection: &collectionName,
		ver:        &ver,
	}, f)
	return err
}
