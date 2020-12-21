package mock

import "context"

type mockClient struct {
}

func (m mockClient) GetById(c context.Context, collectionName string, id interface{}, dest interface{}) error {
	return nil
}

func (m mockClient) GetManyByIds(c context.Context, collectionName string, id []interface{}, dest interface{}) error {
	return nil
}

func (m mockClient) GetAll(c context.Context, collectionName string, dest interface{}) error {
	return nil
}

func (m mockClient) Insert(c context.Context, collectionName string, id interface{}, item interface{}) error {
	return nil
}

func (m mockClient) InsertMany(c context.Context, collectionName string, items map[interface{}]interface{}) error {
	return nil
}

func (m mockClient) InsertOrUpdate(c context.Context, collectionName string, id interface{}, item interface{}) error {
	return nil
}

func (m mockClient) Update(c context.Context, collectionName string, id interface{}, item interface{}) error {
	return nil
}

func (m mockClient) Remove(c context.Context, collectionName string, id interface{}) error {
	return nil
}

func (m mockClient) RemoveAll(c context.Context, collectionName string) error {
	return nil
}
