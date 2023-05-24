package trace

import (
	"context"
	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
	"github.com/orchestd/cacheStorage"
	. "github.com/orchestd/cacheStorage"
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

func runMongoFuncWithTrace(c context.Context, operationName string, tracer opentracing.Tracer, conf CacheWrapperConfiguration, tags CacheTags, funcToRun func(con context.Context) CacheStorageError) CacheStorageError {
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
	if tags.ids != nil {
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
	sp.SetTag("token", c.Value("token"))
	if err := funcToRun(con); err != nil {
		//handling by logic
		if err.IsNotFound() {
			sp.SetTag("found", "false")
		} else {
			ext.LogError(sp, err)
		}
		return err
	}
	sp.SetTag("found", "true")
	return nil
}

func (m mongoCacheStorageGetterWrapper) GetLatestVersions(c context.Context) ([]CacheVersion, CacheStorageError) {
	var result []CacheVersion
	f := func(con context.Context) (err CacheStorageError) {
		result, err = m.cacheStorageGetter.GetLatestVersions(con)
		return err
	}
	err := runMongoFuncWithTrace(c, "mongodb.driver/GetLatestVersions", m.tracer, m.conf, CacheTags{}, f)
	return result, err
}

func (m mongoCacheStorageGetterWrapper) GetById(c context.Context, collectionName string, id string, ver string, dest interface{}) CacheStorageError {
	f := func(con context.Context) (err CacheStorageError) {
		err = m.cacheStorageGetter.GetById(con, collectionName, id, ver, dest)
		return err
	}

	err := runMongoFuncWithTrace(c, "mongodb.driver/GetById", m.tracer, m.conf, CacheTags{
		collection: &collectionName,
		ver:        &ver,
		id:         &id,
	}, f)

	return err
}

func (m mongoCacheStorageGetterWrapper) GetManyByIds(c context.Context, collectionName string, ids []string, ver string, dest interface{}) CacheStorageError {
	f := func(con context.Context) (err CacheStorageError) {
		err = m.cacheStorageGetter.GetManyByIds(con, collectionName, ids, ver, dest)
		return err
	}

	err := runMongoFuncWithTrace(c, "mongodb.driver/GetManyByIds", m.tracer, m.conf, CacheTags{
		collection: &collectionName,
		ver:        &ver,
		ids:        &ids,
	}, f)
	return err

}

func (m mongoCacheStorageGetterWrapper) GetArrayBySingleId(c context.Context, collectionName string, id string, ver string, dest interface{}) CacheStorageError {
	f := func(con context.Context) (err CacheStorageError) {
		err = m.cacheStorageGetter.GetArrayBySingleId(con, collectionName, id, ver, dest)
		return err
	}

	err := runMongoFuncWithTrace(c, "mongodb.driver/GetArrayBySingleId", m.tracer, m.conf, CacheTags{
		collection: &collectionName,
		ver:        &ver,
		id:         &id,
	}, f)
	return err

}

func (m mongoCacheStorageGetterWrapper) GetAll(c context.Context, collectionName string, ver string, dest interface{}) CacheStorageError {
	f := func(con context.Context) (err CacheStorageError) {
		err = m.cacheStorageGetter.GetAll(con, collectionName, ver, dest)
		return err
	}

	err := runMongoFuncWithTrace(c, "mongodb.driver/GetById", m.tracer, m.conf, CacheTags{
		collection: &collectionName,
		ver:        &ver,
	}, f)
	return err
}

func (m mongoCacheStorageGetterWrapper) GetLatestCollectionVersion(c context.Context, collection string) (CacheVersion, CacheStorageError) {
	var result CacheVersion
	f := func(con context.Context) (err CacheStorageError) {
		result, err = m.cacheStorageGetter.GetLatestCollectionVersion(con, collection)
		return err
	}

	err := runMongoFuncWithTrace(c, "mongodb.driver/GetLatestCollectionVersion", m.tracer, m.conf, CacheTags{
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
	f := func(con context.Context) (err CacheStorageError) {
		err = m.cacheStorageSetter.Insert(con, collectionName, id, ver, item)
		return err
	}

	err := runMongoFuncWithTrace(c, "mongodb.driver/Insert", m.tracer, m.conf, CacheTags{
		collection: &collectionName,
		ver:        &ver,
		id:         &id,
		item:       item,
	}, f)

	return err
}

func (m mongoCacheStorageSetterWrapper) InsertMany(c context.Context, collectionName string, ver string, items map[string]interface{}) CacheStorageError {
	f := func(con context.Context) (err CacheStorageError) {
		err = m.cacheStorageSetter.InsertMany(con, collectionName, ver, items)
		return err
	}
	err := runMongoFuncWithTrace(c, "mongodb.driver/InsertMany", m.tracer, m.conf, CacheTags{
		collection: &collectionName,
		ver:        &ver,
		items:      &items,
	}, f)
	return err
}

func (m mongoCacheStorageSetterWrapper) InsertOrUpdate(c context.Context, collectionName string, id string, ver string, item interface{}) CacheStorageError {
	f := func(con context.Context) (err CacheStorageError) {
		err = m.cacheStorageSetter.InsertOrUpdate(con, collectionName, id, ver, item)
		return err
	}
	err := runMongoFuncWithTrace(c, "mongodb.driver/InsertOrUpdate", m.tracer, m.conf, CacheTags{
		collection: &collectionName,
		ver:        &ver,
		id:         &id,
		item:       item,
	}, f)
	return err
}

func (m mongoCacheStorageSetterWrapper) Update(c context.Context, collectionName string, id string, ver string, item interface{}) CacheStorageError {
	f := func(con context.Context) (err CacheStorageError) {
		err = m.cacheStorageSetter.Update(con, collectionName, id, ver, item)
		return err
	}
	err := runMongoFuncWithTrace(c, "mongodb.driver/Update", m.tracer, m.conf, CacheTags{
		collection: &collectionName,
		ver:        &ver,
		id:         &id,
		item:       item,
	}, f)
	return err
}

func (m mongoCacheStorageSetterWrapper) Remove(c context.Context, collectionName string, id string, ver string) CacheStorageError {
	f := func(con context.Context) (err CacheStorageError) {
		err = m.cacheStorageSetter.Remove(con, collectionName, id, ver)
		return err
	}
	err := runMongoFuncWithTrace(c, "mongodb.driver/Remove", m.tracer, m.conf, CacheTags{
		collection: &collectionName,
		ver:        &ver,
		id:         &id,
	}, f)
	return err
}

func (m mongoCacheStorageSetterWrapper) RemoveAll(c context.Context, collectionName string, ver string) CacheStorageError {
	f := func(con context.Context) (err CacheStorageError) {
		err = m.cacheStorageSetter.RemoveAll(con, collectionName, ver)
		return err
	}
	err := runMongoFuncWithTrace(c, "mongodb.driver/RemoveAll", m.tracer, m.conf, CacheTags{
		collection: &collectionName,
		ver:        &ver,
	}, f)
	return err
}

func (m mongoCacheStorageSetterWrapper) GetAndLockById(c context.Context, collectionName string, id string, dest interface{}) CacheStorageError {
	f := func(con context.Context) (err CacheStorageError) {
		err = m.cacheStorageSetter.GetAndLockById(con, collectionName, id, dest)
		return err
	}
	err := runMongoFuncWithTrace(c, "mongodb.driver/getAndLockById", m.tracer, m.conf, CacheTags{
		collection: &collectionName,
	}, f)
	return err
}

func (m mongoCacheStorageSetterWrapper) ReleaseLockedById(c context.Context, collectionName string, id string) CacheStorageError {
	f := func(con context.Context) (err CacheStorageError) {
		err = m.cacheStorageSetter.ReleaseLockedById(con, collectionName, id)
		return err
	}
	err := runMongoFuncWithTrace(c, "mongodb.driver/releaseLockedById", m.tracer, m.conf, CacheTags{
		collection: &collectionName,
	}, f)
	return err
}
