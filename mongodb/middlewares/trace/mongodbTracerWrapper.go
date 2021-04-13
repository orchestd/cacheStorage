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
	DbHost string
	DbUser string
}

type mongoCacheStorageGetterWrapper struct {
	cacheStorageGetter cacheStorage.CacheStorageGetter
	tracer opentracing.Tracer
	conf CacheWrapperConfiguration
}

func NewMongoCacheStorageGetterWrapper(tracer opentracing.Tracer , conf CacheWrapperConfiguration) CacheStorageGetterMiddleware {
	return func(cacheStorageGetter cacheStorage.CacheStorageGetter ) CacheStorageGetter {
		return &mongoCacheStorageGetterWrapper{cacheStorageGetter: cacheStorageGetter,tracer: tracer,conf: conf}
	}
}
func getSpanFromContext(c context.Context, operationName string,tracer opentracing.Tracer , conf CacheWrapperConfiguration) (opentracing.Span,context.Context){
	sp , con := opentracing.StartSpanFromContextWithTracer(c,tracer,operationName)
	ext.DBType.Set(sp, cacheDbType)
	ext.DBUser.Set(sp,conf.DbUser)
	ext.DBInstance.Set(sp,conf.DbHost)
	ext.DBStatement.Set(sp,operationName)
	ext.Component.Set(sp,conf.ServiceName)
	return sp,con
}

func (m mongoCacheStorageGetterWrapper) GetLatestVersions(c context.Context) ([]CacheVersion, CacheStorageError) {
	sp , con := getSpanFromContext(c,"mongodb.driver/GetLatestVersions",m.tracer,m.conf)
	defer sp.Finish()

	result , err := m.cacheStorageGetter.GetLatestVersions(con)
	if err != nil {
		ext.LogError(sp, err)
	}
	return result,err
}

func (m mongoCacheStorageGetterWrapper)  GetById(c context.Context, collectionName string, id string, ver string, dest interface{}) CacheStorageError{
	sp , con := getSpanFromContext(c,"mongodb.driver/GetById",m.tracer,m.conf)
	defer sp.Finish()
	sp.SetTag("cacheStorage/collection" ,collectionName)
	sp.SetTag("cacheStorage/ver",ver)
	sp.SetTag("id",id)
	err := m.cacheStorageGetter.GetById(con,collectionName,id,ver,dest)
	if err != nil {
		ext.LogError(sp, err)
	}
	return err
}

func (m mongoCacheStorageGetterWrapper) GetManyByIds(c context.Context, collectionName string, ids []string, ver string, dest interface{}) CacheStorageError{
	sp , con := getSpanFromContext(c,"mongodb.driver/GetManyByIds",m.tracer,m.conf)
	defer sp.Finish()
	sp.SetTag("cacheStorage/collection" ,collectionName)
	sp.SetTag("cacheStorage/ver",ver)
	sp.SetTag("ids",ids)
	err := m.cacheStorageGetter.GetManyByIds(con,collectionName,ids,ver,dest)
	if err != nil {
		ext.LogError(sp, err)
	}
	return err

}

func (m mongoCacheStorageGetterWrapper) GetAll(c context.Context, collectionName string, ver string, dest interface{}) CacheStorageError {
	sp , con := getSpanFromContext(c,"mongodb.driver/GetById",m.tracer,m.conf)
	sp.SetTag("cacheStorage/collecttion" ,collectionName)
	sp.SetTag("cacheStorage/ver",ver)
	defer sp.Finish()
	err := m.cacheStorageGetter.GetAll(con,collectionName,ver,dest)
	if err != nil {
		ext.LogError(sp, err)
	}
	return err
}

func (m mongoCacheStorageGetterWrapper)	GetLatestCollectionVersion(c context.Context, collection string) (CacheVersion, CacheStorageError) {
	sp , con := getSpanFromContext(c,"mongodb.driver/GetLatestCollectionVersion",m.tracer,m.conf)
	sp.SetTag("cacheStorage/collection" ,collection)
	defer sp.Finish()
	result, err:= m.cacheStorageGetter.GetLatestCollectionVersion(con,collection)
	if err != nil{
		ext.LogError(sp, err)
	}
	return result,err

}


type mongoCacheStorageSetterWrapper struct {
	cacheStorageSetter cacheStorage.CacheStorageSetter
	tracer opentracing.Tracer
	conf CacheWrapperConfiguration

}

func NewMongoCacheStorageSetterWrapper(tracer opentracing.Tracer, conf CacheWrapperConfiguration) CacheStorageSetterMiddleware {
	return func(cacheStorageSetter cacheStorage.CacheStorageSetter) CacheStorageSetter {
		return &mongoCacheStorageSetterWrapper{cacheStorageSetter: cacheStorageSetter,tracer :tracer ,conf: conf}
	}
}

func (m mongoCacheStorageSetterWrapper) Insert(c context.Context, collectionName string, id string, ver string, item interface{}) CacheStorageError{
	sp , con := getSpanFromContext(c,"mongodb.driver/Insert",m.tracer,m.conf)
	defer sp.Finish()
	sp.SetTag("cacheStorage/collection" ,collectionName)
	sp.SetTag("cacheStorage/ver",ver)
	sp.SetTag("id",id)
	sp.SetTag("item",item)
	err :=  m.cacheStorageSetter.Insert(con,collectionName,id,ver,item)
	if err != nil {
		ext.LogError(sp, err)
	}
	return err
}

func (m mongoCacheStorageSetterWrapper) InsertMany(c context.Context, collectionName string, ver string, items map[string]interface{}) CacheStorageError{
	sp , con := getSpanFromContext(c,"mongodb.driver/InsertMany",m.tracer,m.conf)
	defer sp.Finish()
	sp.SetTag("cacheStorage/collection" ,collectionName)
	sp.SetTag("cacheStorage/ver",ver)
	sp.SetTag("items",items)

	err := m.cacheStorageSetter.InsertMany(con,collectionName,ver,items)
	if err != nil {
		ext.LogError(sp, err)
	}
	return err
}

func (m mongoCacheStorageSetterWrapper) InsertOrUpdate(c context.Context, collectionName string, id string, ver string, item interface{}) CacheStorageError{
	sp , con := getSpanFromContext(c,"mongodb.driver/InsertOrUpdate",m.tracer,m.conf)
	defer sp.Finish()
	sp.SetTag("cacheStorage/collection" ,collectionName)
	sp.SetTag("cacheStorage/ver",ver)
	sp.SetTag("id",id)
	sp.SetTag("item",item)
	err := m.cacheStorageSetter.InsertOrUpdate(con,collectionName,id,ver,item)
	if err != nil {
		ext.LogError(sp, err)
	}
	return err
}

func (m mongoCacheStorageSetterWrapper) Update(c context.Context, collectionName string, id string, ver string, item interface{}) CacheStorageError{
	sp , con := getSpanFromContext(c,"mongodb.driver/Update",m.tracer,m.conf)
	defer sp.Finish()
	sp.SetTag("cacheStorage/collection" ,collectionName)
	sp.SetTag("cacheStorage/ver",ver)
	sp.SetTag("id",id)
	sp.SetTag("item",item)
	err := m.cacheStorageSetter.Update(con,collectionName,id,ver,item)
	if err != nil {
		ext.LogError(sp, err)
	}
	return err
}

func (m mongoCacheStorageSetterWrapper) Remove(c context.Context, collectionName string, id string, ver string) CacheStorageError{
	sp , con := getSpanFromContext(c,"mongodb.driver/Remove",m.tracer,m.conf)
	defer sp.Finish()
	sp.SetTag("cacheStorage/collection" ,collectionName)
	sp.SetTag("cacheStorage/ver",ver)
	sp.SetTag("id",id)
	err:= m.cacheStorageSetter.Remove(con,collectionName,id,ver)
	if err != nil {
		ext.LogError(sp, err)
	}
	return err
}

func (m mongoCacheStorageSetterWrapper) RemoveAll(c context.Context, collectionName string, ver string) CacheStorageError{
	sp , con := getSpanFromContext(c,"mongodb.driver/RemoveAll",m.tracer,m.conf)
	defer sp.Finish()
	sp.SetTag("cacheStorage/collection" ,collectionName)
	sp.SetTag("cacheStorage/ver",ver)
	err := m.cacheStorageSetter.RemoveAll(con,collectionName,ver)
	if err != nil {
		ext.LogError(sp, err)
	}
	return err
}

