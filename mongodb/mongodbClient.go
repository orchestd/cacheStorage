package mongodb

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"log"
	"reflect"
)

const idField = "id"

type CacheWrapper struct {
	Id   interface{}
	Data []byte
}

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

func checkDestType(i interface{}, pointer, nonNil, slice bool) error {
	value := reflect.ValueOf(i)
	if pointer && value.Kind() != reflect.Ptr {
		return fmt.Errorf("dest must be a pointer, not a value")
	}
	if nonNil && value.IsNil() {
		return fmt.Errorf("dest must be a non nil pointer")
	}
	direct := reflect.Indirect(value)
	if slice && direct.Kind() != reflect.Slice {
		return fmt.Errorf("dest must be a slice")
	}
	return nil
}

func getSliceType(i interface{}) reflect.Type {
	return reflect.TypeOf(i).Elem().Elem()
}

type mongodbClient struct {
	database *mongo.Database
}

func (m mongodbClient) GetById(ctx context.Context, collectionName string, id interface{}, dest interface{}) error {
	err := checkDestType(dest, true, true, false)
	if err != nil {
		return err
	}
	result := m.database.Collection(collectionName).FindOne(ctx, bson.M{idField: id})
	if result.Err() != nil {
		return result.Err()
	}
	var wrap CacheWrapper
	err = result.Decode(&wrap)
	if err != nil {
		return err
	}
	err = wrap.ExtractData(dest)
	if err != nil {
		return err
	}
	return nil
}

func (m mongodbClient) GetManyByIds(ctx context.Context, collectionName string, id []interface{}, dest interface{}) error {
	err := checkDestType(dest, true, true, true)
	if err != nil {
		return err
	}
	var filter bson.M
	if len(id) > 0 {
		filter = bson.M{idField: bson.M{"$in": id}}
	}
	cur, err := m.database.Collection(collectionName).Find(ctx, filter)
	if err != nil {
		return err
	}
	destValue := reflect.ValueOf(dest).Elem()
	for cur.Next(ctx) {
		var wrap CacheWrapper
		err := cur.Decode(&wrap)
		if err != nil {
			return err
		}
		destItemType := getSliceType(dest)
		destItemP := reflect.New(destItemType)
		destItem := reflect.Indirect(destItemP)
		err = wrap.ExtractData(destItemP.Interface())
		if err != nil {
			return err
		}
		destValue.Set(reflect.Append(destValue, destItem))
	}
	return nil
}

func (m mongodbClient) GetAll(ctx context.Context, collectionName string, dest interface{}) error {
	return m.GetManyByIds(ctx, collectionName, nil, dest)
}

func (m mongodbClient) Insert(ctx context.Context, collectionName string, id interface{}, item interface{}) error {
	wrap := CacheWrapper{Id: id}.AddData(item)
	_, err := m.database.Collection(collectionName).InsertOne(ctx, wrap)
	return err
}

func (m mongodbClient) InsertMany(ctx context.Context, collectionName string, items map[interface{}]interface{}) error {
	var wraps []interface{}
	for id, v := range items {
		wraps = append(wraps, CacheWrapper{Id: id}.AddData(v))
	}
	_, err := m.database.Collection(collectionName).InsertMany(ctx, wraps)
	return err
}

func (m mongodbClient) InsertOrUpdate(ctx context.Context, collectionName string, id interface{}, item interface{}) error {
	if count, err := m.database.Collection(collectionName).CountDocuments(ctx, bson.M{idField: id}); err != nil {
		return err
	} else if count > 0 {
		return m.Update(ctx, collectionName, id, item)
	} else {
		return m.Insert(ctx, collectionName, id, item)
	}
}

func (m mongodbClient) Update(ctx context.Context, collectionName string, id interface{}, item interface{}) error {
	_, err := m.database.Collection(collectionName).ReplaceOne(ctx, bson.M{idField: id}, CacheWrapper{Id: id}.AddData(item))
	return err
}

func (m mongodbClient) Remove(ctx context.Context, collectionName string, id interface{}) error {
	_, err := m.database.Collection(collectionName).DeleteOne(ctx, bson.M{idField: id})
	return err
}

func (m mongodbClient) RemoveAll(c context.Context, collectionName string) error {
	return nil
}
