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

func (s *mongodbCacheStorage) Connect(c context.Context, username string, password string, host string, database string) error {
	client, err := mongo.NewClient(options.Client().ApplyURI(host))
	if err != nil {
		return err
	}
	s.client = client
	err = client.Connect(c)
	if err != nil {
		return err
	}
	s.database = client.Database(database)
	return nil
}

func (s mongodbCacheStorage) Close(c context.Context) error {
	return s.client.Disconnect(c)
}

func (s mongodbCacheStorage) GetCacheStorageClient() (cacheStorage.CacheStorageGetter, cacheStorage.CacheStorageSetter) {
	client := mongodbClient{database: s.database}
	return client, client
}
