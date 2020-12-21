package mock

import (
	"bitbucket.org/HeilaSystems/cacheStorage"
	"container/list"
)

type mockConfig struct {
	Username string
	Password string
	Host     string
	Database string
}

type defaultMockBuilder struct {
	ll *list.List
}

func Builder() cacheStorage.CacheStorageBuilder{
	return &defaultMockBuilder{ll: list.New()}
}

func (d *defaultMockBuilder) SetUsername(username string) cacheStorage.CacheStorageBuilder{
	d.ll.PushBack(func(cfg *mockConfig){
		cfg.Username = username
	})
	return d
}

func (d *defaultMockBuilder) SetPassword(password string) cacheStorage.CacheStorageBuilder {
	d.ll.PushBack(func(cfg *mockConfig){
		cfg.Password = password
	})
	return d
}

func (d *defaultMockBuilder) SetHost(host string) cacheStorage.CacheStorageBuilder {
	d.ll.PushBack(func(cfg *mockConfig){
		cfg.Host = host
	})
	return d
}

func (d *defaultMockBuilder) SetDatabaseName(dbName string) cacheStorage.CacheStorageBuilder {
	d.ll.PushBack(func(cfg *mockConfig){
		cfg.Database = dbName
	})
	return d
}

func (d *defaultMockBuilder) Build() cacheStorage.CacheStorage {
	mockCfg := &mockConfig{}
	for e := d.ll.Front(); e != nil; e = e.Next() {
		f := e.Value.(func(cfg *mockConfig))
		f(mockCfg)
	}
	return mockStorage{
		Username: mockCfg.Username,
		Password: mockCfg.Password,
		Host:     mockCfg.Host,
		Database: mockCfg.Database,
	}
}



