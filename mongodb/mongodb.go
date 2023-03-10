package mongodb

import (
	"context"
	"github.com/orchestd/cacheStorage"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type mongodbCacheStorage struct {
	client   *mongo.Client
	database *mongo.Database
}

func NewMongoDbCacheStorage() cacheStorage.CacheStorage {
	return &mongodbCacheStorage{}
}

func (s *mongodbCacheStorage) Connect(c context.Context, host, userName, userPw, database string) error {
	credential := options.Credential{
		Username: userName,
		Password: userPw,
	}
	client, err := mongo.NewClient(options.Client().ApplyURI(host).SetAuth(credential))
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

func (s *mongodbCacheStorage) GetCacheStorageClient() (cacheStorage.CacheStorageGetter, cacheStorage.CacheStorageSetter) {
	client := mongodbClient{storage: s}
	return client, client
}
