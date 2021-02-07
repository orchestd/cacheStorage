package mongodb

import (
	. "bitbucket.org/HeilaSystems/cacheStorage"
	"context"
	"encoding/json"
	"fmt"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"log"
	"reflect"
)

const Latest = "latest"

const idField = "id"
const verField = "ver"

const cacheVersionsCollectionName = "cacheVersions"

type CacheWrapper struct {
	Id   string
	Ver  string
	Data string
}

/*
func (w CacheWrapper) AddData(i interface{}) CacheWrapper {
	gob.Register(i)
	var data bytes.Buffer
	err := gob.NewEncoder(&data).Encode(i)
	if err != nil {
		log.Fatal(err)
	}
	w.Data = data.Bytes()
	return w
}

func (w CacheWrapper) ExtractData(i interface{}) error {
	data := bytes.NewBuffer(w.Data)
	err := gob.NewDecoder(data).Decode(i)
	return err
}
*/

func (w CacheWrapper) AddData(i interface{}) CacheWrapper {
	b, err := json.Marshal(i)
	if err != nil {
		log.Fatal(err)
	}
	w.Data = string(b)
	return w
}

func (w CacheWrapper) ExtractData(i interface{}) error {
	err := json.Unmarshal([]byte(w.Data), i)
	return err
}

func checkDestType(i interface{}, pointer, nonNil, isMap bool) error {
	value := reflect.ValueOf(i)
	if pointer && value.Kind() != reflect.Ptr {
		return fmt.Errorf("dest must be a pointer, not a value")
	}
	if nonNil && value.IsNil() {
		return fmt.Errorf("dest must be a non nil pointer")
	}
	direct := reflect.Indirect(value)
	if isMap && direct.Kind() != reflect.Map {
		return fmt.Errorf("dest must be a map[string]yourCacheType")
	}
	return nil
}

func getMapValueType(i interface{}) reflect.Type {
	return reflect.TypeOf(i).Elem()
}

func (m mongodbClient) getVersion(c context.Context, ver string, collection string) (string, CacheStorageError) {
	if ver == Latest {
		ver, err := m.GetLatestCollectionVersion(c, collection)
		return ver.Version, err
	}
	return ver, nil
}

type mongodbClient struct {
	storage *mongodbCacheStorage
}

func (m mongodbClient) GetLatestVersions(c context.Context) ([]CacheVersion, CacheStorageError) {
	cacheVersions := make(map[string]CacheVersion)
	var versions []CacheVersion
	err := m.GetAll(c, cacheVersionsCollectionName, "1", cacheVersions)
	if err != nil {
		return versions, err
	}
	for i := range cacheVersions {
		versions = append(versions, cacheVersions[i])
	}
	return versions, nil
}

func (m mongodbClient) GetLatestCollectionVersion(c context.Context, collection string) (CacheVersion, CacheStorageError) {
	cacheVersion := CacheVersion{}
	err := m.GetById(c, cacheVersionsCollectionName, collection, "1", &cacheVersion)
	if err != nil {
		return cacheVersion, err
	}
	return cacheVersion, nil
}

func (m mongodbClient) GetById(ctx context.Context, collectionName string, id string, ver string, dest interface{}) CacheStorageError {
	if ver == Latest {
		latestVer, cErr := m.getVersion(ctx, ver, collectionName)
		if cErr != nil {
			return cErr
		}
		ver = latestVer
	}
	err := checkDestType(dest, true, true, false)
	if err != nil {
		return NewMongoCacheStorageError(fmt.Errorf("%w: %q", InvalidDestType, err))
	}
	result := m.storage.database.Collection(collectionName).FindOne(ctx, bson.M{idField: id, verField: ver})
	if result.Err() != nil {
		if result.Err() == mongo.ErrNoDocuments {
			return NewMongoCacheStorageError(fmt.Errorf("%w: %q", NotFoundError, result.Err()))
		} else {
			return NewMongoCacheStorageError(result.Err())
		}
	}
	var wrap CacheWrapper
	err = result.Decode(&wrap)
	if err != nil {
		return NewMongoCacheStorageError(err)
	}
	err = wrap.ExtractData(dest)
	if err != nil {
		return NewMongoCacheStorageError(err)
	}
	return nil
}

func (m mongodbClient) GetManyByIds(ctx context.Context, collectionName string, ids []string, ver string, dest interface{}) CacheStorageError {
	if ver == Latest {
		latestVer, cErr := m.getVersion(ctx, ver, collectionName)
		if cErr != nil {
			return cErr
		}
		ver = latestVer
	}
	err := checkDestType(dest, false, true, true)
	if err != nil {
		return NewMongoCacheStorageError(fmt.Errorf("%w: %q", InvalidDestType, err))
	}
	filter := bson.M{verField: ver}
	if len(ids) > 0 {
		filter = bson.M{verField: ver, idField: bson.M{"$in": ids}}
	}
	cur, err := m.storage.database.Collection(collectionName).Find(ctx, filter)
	if err != nil {
		return NewMongoCacheStorageError(err)
	}
	foundElementsCount := 0
	for cur.Next(ctx) {
		var wrap CacheWrapper
		err := cur.Decode(&wrap)
		if err != nil {
			return NewMongoCacheStorageError(err)
		}
		destItemType := getMapValueType(dest)
		destItemP := reflect.New(destItemType)
		destItem := reflect.Indirect(destItemP)
		err = wrap.ExtractData(destItemP.Interface())
		if err != nil {
			return NewMongoCacheStorageError(err)
		}
		reflect.ValueOf(dest).SetMapIndex(reflect.ValueOf(wrap.Id), destItem)
		foundElementsCount++
	}
	if foundElementsCount < len(ids) {
		return NewMongoCacheStorageError(NotFoundError)
	}
	return nil
}

func (m mongodbClient) GetAll(ctx context.Context, collectionName string, ver string, dest interface{}) CacheStorageError {
	return m.GetManyByIds(ctx, collectionName, nil, ver, dest)
}

func (m mongodbClient) Insert(ctx context.Context, collectionName string, id string, ver string, item interface{}) CacheStorageError {
	wrap := CacheWrapper{Id: id, Ver: ver}.AddData(item)
	_, err := m.storage.database.Collection(collectionName).InsertOne(ctx, wrap)
	if err != nil {
		return NewMongoCacheStorageError(err)
	}
	return nil
}

func (m mongodbClient) InsertMany(ctx context.Context, collectionName string, ver string, items map[string]interface{}) CacheStorageError {
	var wraps []interface{}
	for id, v := range items {
		wraps = append(wraps, CacheWrapper{Id: id, Ver: ver}.AddData(v))
	}
	_, err := m.storage.database.Collection(collectionName).InsertMany(ctx, wraps)
	if err != nil {
		return NewMongoCacheStorageError(err)
	}
	return nil
}

func (m mongodbClient) InsertOrUpdate(ctx context.Context, collectionName string, id string, ver string, item interface{}) CacheStorageError {
	if count, err := m.storage.database.Collection(collectionName).CountDocuments(ctx, bson.M{idField: id, verField: ver}); err != nil {
		return NewMongoCacheStorageError(err)
	} else if count > 0 {
		return m.Update(ctx, collectionName, id, ver, item)
	} else {
		return m.Insert(ctx, collectionName, id, ver, item)
	}
}

func (m mongodbClient) Update(ctx context.Context, collectionName string, id string, ver string, item interface{}) CacheStorageError {
	_, err := m.storage.database.Collection(collectionName).ReplaceOne(ctx, bson.M{idField: id, verField: ver}, CacheWrapper{Id: id, Ver: ver}.AddData(item))
	if err != nil {
		return NewMongoCacheStorageError(err)
	}
	return nil
}

func (m mongodbClient) Remove(ctx context.Context, collectionName string, id string, ver string) CacheStorageError {
	_, err := m.storage.database.Collection(collectionName).DeleteOne(ctx, bson.M{idField: id, verField: ver})
	if err != nil {
		return NewMongoCacheStorageError(err)
	}
	return nil
}

func (m mongodbClient) RemoveAll(ctx context.Context, collectionName string, ver string) CacheStorageError {
	_, err := m.storage.database.Collection(collectionName).DeleteMany(ctx, bson.M{})
	if err != nil {
		return NewMongoCacheStorageError(err)
	}
	return nil
}
