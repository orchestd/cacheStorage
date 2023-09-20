package mongodb

import (
	"context"
	"encoding/json"
	"fmt"
	. "github.com/orchestd/cacheStorage"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"log"
	"reflect"
	"time"
)

const Latest = "latest"

const idField = "id"
const verField = "ver"

const cacheVersionsCollectionName = "cacheVersions"

type LockedItem struct {
	LockedAt time.Time `json:"lockedAt"`
	LockedBy string    `json:"lockedBy"`
}

type CacheWrapper struct {
	Id     string      `json:"id"`
	Ver    string      `json:"ver"`
	Data   string      `json:"data"`
	Locked *LockedItem `json:"locked"`
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

func checkDestType(i interface{}, pointer, nonNil, isMap bool, isSlice bool) error {
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
	if isSlice && direct.Kind() != reflect.Slice {
		return fmt.Errorf("dest must be a Slice[]yourCacheType and not %v", direct.Kind())
	}
	return nil
}

func getMapValueType(i interface{}) reflect.Type {
	return reflect.TypeOf(i).Elem()
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
	err := checkDestType(dest, true, true, false, false)
	if err != nil {
		return NewMongoCacheStorageError(fmt.Errorf("%w: %q", InvalidDestType, err), nil)
	}
	result := m.storage.database.Collection(collectionName).FindOne(ctx, bson.M{idField: id, verField: ver})
	if result.Err() != nil {
		if result.Err() == mongo.ErrNoDocuments {
			return NewMongoCacheStorageError(fmt.Errorf("%w: %q", NotFoundError, result.Err()), nil)
		} else {
			return NewMongoCacheStorageError(result.Err(), nil)
		}
	}
	var wrap CacheWrapper
	err = result.Decode(&wrap)
	if err != nil {
		return NewMongoCacheStorageError(err, nil)
	}
	err = wrap.ExtractData(dest)
	if err != nil {
		return NewMongoCacheStorageError(err, nil)
	}
	return nil
}

func (m mongodbClient) getMany(ctx context.Context, collectionName string, filterByIds []string, ver string, dest interface{}) CacheStorageError {
	err := checkDestType(dest, false, true, true, false)
	if err != nil {
		return NewMongoCacheStorageError(fmt.Errorf("%w: %q", InvalidDestType, err), nil)
	}
	filter := bson.M{verField: ver}
	if len(filterByIds) > 0 {
		filter = bson.M{verField: ver, idField: bson.M{"$in": filterByIds}}
	}
	cur, err := m.storage.database.Collection(collectionName).Find(ctx, filter)
	if err != nil {
		return NewMongoCacheStorageError(err, nil)
	}
	foundElementIds := make(map[string]bool)
	for cur.Next(ctx) {
		var wrap CacheWrapper
		err := cur.Decode(&wrap)
		if err != nil {
			return NewMongoCacheStorageError(err, nil)
		}
		destItemType := getMapValueType(dest)
		destItemP := reflect.New(destItemType)
		destItem := reflect.Indirect(destItemP)
		err = wrap.ExtractData(destItemP.Interface())
		if err != nil {
			return NewMongoCacheStorageError(err, nil)
		}
		reflect.ValueOf(dest).SetMapIndex(reflect.ValueOf(wrap.Id), destItem)
		foundElementIds[wrap.Id] = true
	}
	if len(foundElementIds) < len(filterByIds) {
		var notFoundElements []string
		for _, id := range filterByIds {
			if _, ok := foundElementIds[id]; !ok {
				notFoundElements = append(notFoundElements, id)
			}
		}
		// len == 0 meaning the func got ids with duplicates
		if len(notFoundElements) > 0 {
			err := fmt.Errorf("elements with id: %v not found in collection %v by version %v", notFoundElements, collectionName, ver)
			return NewMongoCacheStorageError(fmt.Errorf("%w: %q", NotFoundError, err), notFoundElements)
		}
	}
	return nil
}

func (m mongodbClient) GetManyByIds(ctx context.Context, collectionName string, ids []string, ver string, dest interface{}) CacheStorageError {
	if len(ids) == 0 {
		return nil
	}
	return m.getMany(ctx, collectionName, ids, ver, dest)
}

func (m mongodbClient) GetArrayBySingleId(ctx context.Context, collectionName string, id string, ver string, dest interface{}) CacheStorageError {
	err := checkDestType(dest, false, true, false, true)
	if err != nil {
		return NewMongoCacheStorageError(fmt.Errorf("%w: %q", InvalidDestType, err), nil)
	}
	cur, err := m.storage.database.Collection(collectionName).Find(ctx, bson.M{idField: id, verField: ver})
	if err != nil {
		return NewMongoCacheStorageError(err, nil)
	}

	destVal := reflect.ValueOf(dest).Elem()
	for cur.Next(ctx) {
		var wrap CacheWrapper
		err := cur.Decode(&wrap)
		if err != nil {
			return NewMongoCacheStorageError(err, nil)
		}
		destItemP := reflect.New(destVal.Type().Elem())
		destItem := reflect.Indirect(destItemP)
		err = wrap.ExtractData(destItemP.Interface())
		if err != nil {
			return NewMongoCacheStorageError(err, nil)
		}
		destVal.Set(reflect.Append(destVal, destItem))

	}
	return nil
}

func (m mongodbClient) GetAll(ctx context.Context, collectionName string, ver string, dest interface{}) CacheStorageError {
	return m.getMany(ctx, collectionName, nil, ver, dest)
}

func (m mongodbClient) Insert(ctx context.Context, collectionName string, id string, ver string, item interface{}) CacheStorageError {
	wrap := CacheWrapper{Id: id, Ver: ver}.AddData(item)
	_, err := m.storage.database.Collection(collectionName).InsertOne(ctx, wrap)
	if err != nil {
		return NewMongoCacheStorageError(err, nil)
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
		return NewMongoCacheStorageError(err, nil)
	}
	return nil
}

func (m mongodbClient) InsertOrUpdate(ctx context.Context, collectionName string, id string, ver string, item interface{}) CacheStorageError {
	if count, err := m.storage.database.Collection(collectionName).CountDocuments(ctx, bson.M{idField: id, verField: ver}); err != nil {
		return NewMongoCacheStorageError(err, nil)
	} else if count > 0 {
		return m.Update(ctx, collectionName, id, ver, item)
	} else {
		return m.Insert(ctx, collectionName, id, ver, item)
	}
}

func (m mongodbClient) Update(ctx context.Context, collectionName string, id string, ver string, item interface{}) CacheStorageError {
	_, err := m.storage.database.Collection(collectionName).ReplaceOne(ctx, bson.M{idField: id, verField: ver}, CacheWrapper{Id: id, Ver: ver}.AddData(item))
	if err != nil {
		return NewMongoCacheStorageError(err, nil)
	}
	return nil
}

func (m mongodbClient) Remove(ctx context.Context, collectionName string, id string, ver string) CacheStorageError {
	_, err := m.storage.database.Collection(collectionName).DeleteOne(ctx, bson.M{idField: id, verField: ver})
	if err != nil {
		return NewMongoCacheStorageError(err, nil)
	}
	return nil
}

func (m mongodbClient) RemoveAll(ctx context.Context, collectionName string, ver string) CacheStorageError {
	_, err := m.storage.database.Collection(collectionName).DeleteMany(ctx, bson.M{"ver": ver})
	if err != nil {
		return NewMongoCacheStorageError(err, nil)
	}
	return nil
}

func (m mongodbClient) GetAndLockById(c context.Context, collectionName string, id string, dest interface{}) CacheStorageError {
	traceId := c.Value("Uber-Trace-Id")
	update := []bson.M{
		{
			"$set": bson.M{
				"locked": bson.M{
					"$cond": bson.M{
						"if": bson.M{
							"$or": []bson.M{
								{"$eq": bson.A{"$locked", nil}},
								{"$ne": bson.A{bson.M{"$type": "$locked"}, "object"}},
								{"$gt": bson.A{
									bson.M{"$dateDiff": bson.M{"startDate": "$locked.lockedAt", "endDate": "$$NOW", "unit": "second"}}, 30},
								},
							},
						},
						"then": bson.M{"lockedAt": "$$NOW", "lockedBy": traceId},
						"else": "$locked",
					},
				},
			},
		},
	}
	opts := options.FindOneAndUpdate().SetReturnDocument(options.After)

	for {
		result := m.storage.database.Collection(collectionName).FindOneAndUpdate(c, bson.M{idField: id}, update, opts)
		if result.Err() != nil {
			if result.Err() == mongo.ErrNoDocuments {
				return NewMongoCacheStorageError(fmt.Errorf("%w: %q", NotFoundError, result.Err()), nil)
			} else {
				return NewMongoCacheStorageError(result.Err(), nil)
			}
		}
		var wrap CacheWrapper
		err := result.Decode(&wrap)
		if err != nil {
			return NewMongoCacheStorageError(err, nil)
		}
		if wrap.Locked.LockedBy != traceId {
			time.Sleep(50 * time.Millisecond)
		} else {
			err := wrap.ExtractData(dest)
			if err != nil {
				return NewMongoCacheStorageError(err, nil)
			}
			return nil
		}
	}
}

/*
ReleaseLockedById in most cases will do nothing, cause the "update" function inserts a record without a lock and
therefore "automatically releases" the record a specific session locked
*/
func (m mongodbClient) ReleaseLockedById(c context.Context, collectionName string, id string) CacheStorageError {
	traceId := c.Value("Uber-Trace-Id")

	filter := bson.M{idField: id, "locked.lockedBy": traceId}
	update := []bson.M{{"$set": bson.M{"locked": nil}}}
	_, err := m.storage.database.Collection(collectionName).UpdateOne(c, filter, update)
	if err != nil {
		return NewMongoCacheStorageError(err, nil)
	}
	return nil
}
