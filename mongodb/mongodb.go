package mongodb

import (
	"bitbucket.org/HeilaSystems/cacheStorage"
	"context"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type mongodbCacheStorage struct {
	client   *mongo.Client
	database *mongo.Database
}

func NewMongoDbCacheStorage() *mongodbCacheStorage {
	return &mongodbCacheStorage{}
}

func (s mongodbCacheStorage) Connect(c context.Context, credentials cacheStorage.Credentials) error {
	client, err := mongo.NewClient(options.Client().ApplyURI(credentials.Host))
	if err != nil {
		return err
	}
	s.client = client
	err = client.Connect(c)
	if err != nil {
		return err
	}
	s.database = client.Database(credentials.Database)
	return nil
}

func (s mongodbCacheStorage) Close(c context.Context) error {
	return s.client.Disconnect(c)
}

func (s mongodbCacheStorage) GetCacheStorageClient() (cacheStorage.CacheStorageGetter, cacheStorage.CacheStorageSetter) {
	client := mongodbClient{}
	return client, client
}
