package mongodb

import "context"

type mongodbClient struct {
}

func (m mongodbClient) GetById(c context.Context, collectionName string, id interface{}, dest interface{}) error {
	return nil
}

func (m mongodbClient) GetManyByIds(c context.Context, collectionName string, id []interface{}, dest interface{}) error {
	return nil
}

func (m mongodbClient) GetAll(c context.Context, collectionName string, dest interface{}) error {
	return nil
}

func (m mongodbClient) Insert(c context.Context, collectionName string, id interface{}, item interface{}) error {
	return nil
}

func (m mongodbClient) InsertMany(c context.Context, collectionName string, items map[interface{}]interface{}) error {
	return nil
}

func (m mongodbClient) InsertOrUpdate(c context.Context, collectionName string, id interface{}, item interface{}) error {
	return nil
}

func (m mongodbClient) Update(c context.Context, collectionName string, id interface{}, item interface{}) error {
	return nil
}

func (m mongodbClient) Remove(c context.Context, collectionName string, id interface{}) error {
	return nil
}

func (m mongodbClient) RemoveAll(c context.Context, collectionName string) error {
	return nil
}
