package mongodb

import (
	"bitbucket.org/HeilaSystems/cacheStorage"
	"context"
	"fmt"
	"github.com/ory/dockertest"
	. "github.com/smartystreets/goconvey/convey"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"log"
	"os"
	"testing"
	"time"
)

var testCollectionName = "catalog"
var testCacheVersionsCollectionName = "cacheVersions"

var cache cacheStorage.CacheStorage

type TestCatalogItem struct {
	Id    string
	Name  string
	Price float32
}

var testVersion = "1"

var testCatalogItem1 = TestCatalogItem{Id: "1", Name: "Item1", Price: 10.20}
var testCatalogItem2 = TestCatalogItem{Id: "2", Name: "Item2", Price: 20.30}
var testCatalogItem3 = TestCatalogItem{Id: "3", Name: "Item3", Price: 30.40}
var testCatalogItem4 = TestCatalogItem{Id: "4", Name: "Item4", Price: 40.50}

func initTestCollection(host string) error {
	client, err := mongo.NewClient(options.Client().ApplyURI(host))
	if err != nil {
		return err
	}
	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	err = client.Connect(ctx)
	db := client.Database("test")
	err = db.CreateCollection(ctx, testCollectionName)
	if err != nil {
		return err
	}
	err = db.CreateCollection(ctx, testCacheVersionsCollectionName)
	if err != nil {
		return err
	}
	collection := db.Collection(testCollectionName)

	testCatalog := []interface{}{
		CacheWrapper{Id: "1", Ver: testVersion}.AddData(testCatalogItem1),
		CacheWrapper{Id: "2", Ver: testVersion}.AddData(testCatalogItem2),
		CacheWrapper{Id: "3", Ver: testVersion}.AddData(testCatalogItem3),
		CacheWrapper{Id: "4", Ver: testVersion}.AddData(testCatalogItem4),
	}
	_, err = collection.InsertMany(ctx, testCatalog)
	if err != nil {
		return err
	}
	testVersions := []interface{}{
		CacheWrapper{Id: "stores", Ver: "1"}.AddData(cacheStorage.CacheVersion{CollectionName: "stores", Version: "2"}),
		CacheWrapper{Id: "storeOpeningHours", Ver: "1"}.AddData(cacheStorage.CacheVersion{CollectionName: "storeOpeningHours", Version: "4"}),
		CacheWrapper{Id: "occasions", Ver: "1"}.AddData(cacheStorage.CacheVersion{CollectionName: "occasions", Version: "7"}),
	}
	_, err = db.Collection(testCacheVersionsCollectionName).InsertMany(ctx, testVersions)
	if err != nil {
		return err
	}
	err = client.Disconnect(ctx)
	return err
}

func TestMain(m *testing.M) {
	pool, err := dockertest.NewPool("")
	if err != nil {
		log.Fatalf("Could not connect to docker: %s", err)
	}

	resource, err := pool.Run("mongo", "4.4", nil)
	err = resource.Expire(120)
	if err != nil {
		log.Fatalf("Could not start resource: %s", err)
	}

	if err := pool.Retry(func() error {
		host := fmt.Sprintf("mongodb://localhost:%s", resource.GetPort("27017/tcp"))
		if err := initTestCollection(host); err != nil {
			return err
		}
		cache = NewMongoDbCacheStorage()
		ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
		err := cache.Connect(ctx, host, "test")
		return err
	}); err != nil {
		log.Fatalf("Could not connect to docker: %s", err)
	}
	code := m.Run()

	//cache.Close(context.TODO())

	if err := pool.Purge(resource); err != nil {
		log.Fatalf("Could not purge resource: %s", err)
	}

	os.Exit(code)
}

func TestGetById(t *testing.T) {
	cacheGetter, _ := cache.GetCacheStorageClient()
	var testCatalogItem TestCatalogItem
	Convey("Getting test item by ID = 1 and non pointer dest", t, func() {
		err := cacheGetter.GetById(context.TODO(), testCollectionName, "1", testVersion, testCatalogItem)
		So(err, ShouldNotBeNil)
		So(err.IsInvalidDestType(), ShouldBeTrue)
	})
	Convey("Getting existing test item by ID = 1", t, func() {
		err := cacheGetter.GetById(context.TODO(), testCollectionName, "1", testVersion, &testCatalogItem)
		So(err, ShouldBeNil)
		So(testCatalogItem.Id, ShouldEqual, testCatalogItem1.Id)
		So(testCatalogItem.Name, ShouldEqual, testCatalogItem1.Name)
		So(testCatalogItem.Price, ShouldEqual, testCatalogItem1.Price)
	})
	Convey("Getting non existent test item by ID = 9", t, func() {
		err := cacheGetter.GetById(context.TODO(), testCollectionName, "9", testVersion, &testCatalogItem)
		So(err, ShouldNotBeNil)
		So(err.IsNotFound(), ShouldBeTrue)
	})
}

func TestGetLatestVersion(t *testing.T) {
	cacheGetter, _ := cache.GetCacheStorageClient()
	Convey("Getting cache versions", t, func() {
		versions, err := cacheGetter.GetLatestVersions(context.TODO())
		So(err, ShouldBeNil)
		So(len(versions), ShouldEqual, 3)
		So(versions[0].CollectionName, ShouldEqual, "stores")
		So(versions[0].Version, ShouldEqual, "2")
	})
}

func TestGetManyByIds(t *testing.T) {
	cacheGetter, _ := cache.GetCacheStorageClient()
	var testCatalogItem TestCatalogItem
	Convey("Getting test item by ID = 1, 2, 3 and non map dest", t, func() {
		err := cacheGetter.GetManyByIds(context.TODO(), testCollectionName, []string{"1"}, testVersion, &testCatalogItem)
		So(err, ShouldNotBeNil)
		So(err.IsInvalidDestType(), ShouldBeTrue)
	})

	Convey("Getting 3 existing test items by ID = 1, 2, 3", t, func() {
		testCatalogItems := make(map[string]TestCatalogItem)
		err := cacheGetter.GetManyByIds(context.TODO(), testCollectionName, []string{"1", "2", "3"}, testVersion, testCatalogItems)
		So(err, ShouldBeNil)
		So(len(testCatalogItems), ShouldEqual, 3)
		So(testCatalogItems["1"].Id, ShouldEqual, testCatalogItem1.Id)
		So(testCatalogItems["1"].Name, ShouldEqual, testCatalogItem1.Name)
		So(testCatalogItems["1"].Price, ShouldEqual, testCatalogItem1.Price)
	})

	Convey("Getting 2 existing test items by ID = 1, 2 and one non existent item by ID = 9", t, func() {
		testCatalogItems := make(map[string]TestCatalogItem)
		err := cacheGetter.GetManyByIds(context.TODO(), testCollectionName, []string{"1", "2", "9"}, testVersion, testCatalogItems)
		So(err, ShouldNotBeNil)
		So(err.IsNotFound(), ShouldBeTrue)
		So(len(testCatalogItems), ShouldEqual, 2)
	})
}

func TestGetAll(t *testing.T) {
	testCatalogItems := make(map[string]TestCatalogItem)

	cacheGetter, _ := cache.GetCacheStorageClient()
	Convey("Getting all items from the test collection", t, func() {
		err := cacheGetter.GetAll(context.TODO(), testCollectionName, testVersion, testCatalogItems)
		So(err, ShouldBeNil)
		So(len(testCatalogItems), ShouldEqual, 4)
		So(testCatalogItems["1"].Id, ShouldEqual, testCatalogItem1.Id)
		So(testCatalogItems["1"].Name, ShouldEqual, testCatalogItem1.Name)
		So(testCatalogItems["1"].Price, ShouldEqual, testCatalogItem1.Price)
	})
}

func TestInsert(t *testing.T) {
	cacheGetter, cacheSetter := cache.GetCacheStorageClient()
	testCatalogItem := TestCatalogItem{Id: "5", Name: "Item5", Price: 50.60}
	Convey("Inserting test item with ID = 5", t, func() {
		err := cacheSetter.Insert(context.TODO(), testCollectionName, "5", testVersion, testCatalogItem)
		So(err, ShouldBeNil)
	})

	var insertedItem TestCatalogItem
	Convey("Getting inserted test item with ID = 5", t, func() {
		cacheGetter.GetById(context.TODO(), testCollectionName, "5", testVersion, &insertedItem)
		So(insertedItem.Id, ShouldEqual, testCatalogItem.Id)
		So(insertedItem.Name, ShouldEqual, testCatalogItem.Name)
		So(insertedItem.Price, ShouldEqual, testCatalogItem.Price)
	})
}

func TestInsertMany(t *testing.T) {
	cacheGetter, cacheSetter := cache.GetCacheStorageClient()
	testCatalogItems := map[string]interface{}{
		"6": TestCatalogItem{Id: "6", Name: "Item6", Price: 50.60},
		"7": TestCatalogItem{Id: "7", Name: "Item7", Price: 50.60},
		"8": TestCatalogItem{Id: "8", Name: "Item8", Price: 50.60},
	}
	Convey("Inserting test items with ID = 6, 7, 8", t, func() {
		err := cacheSetter.InsertMany(context.TODO(), testCollectionName, testVersion, testCatalogItems)
		So(err, ShouldBeNil)
	})
	insertedItems := make(map[string]TestCatalogItem)
	Convey("Getting inserted test items with ID = 6, 7, 8", t, func() {
		cacheGetter.GetManyByIds(context.TODO(), testCollectionName, []string{"6", "7", "8"}, testVersion, insertedItems)
		So(len(insertedItems), ShouldEqual, 3)
	})
}

func TestUpdate(t *testing.T) {
	cacheGetter, cacheSetter := cache.GetCacheStorageClient()
	testCatalogItem1.Name = testCatalogItem1.Name + "!"
	Convey("Updating existing test item with ID = 1", t, func() {
		err := cacheSetter.Update(context.TODO(), testCollectionName, "1", testVersion, testCatalogItem1)
		So(err, ShouldBeNil)
	})

	Convey("Getting updated test item with ID = 9 and name = Item9!", t, func() {
		testCatalogItem := TestCatalogItem{}
		cacheGetter.GetById(context.TODO(), testCollectionName, "1", testVersion, &testCatalogItem)
		So(testCatalogItem.Id, ShouldEqual, testCatalogItem1.Id)
		So(testCatalogItem.Name, ShouldEqual, testCatalogItem1.Name)
		So(testCatalogItem.Price, ShouldEqual, testCatalogItem1.Price)
	})
}

func TestInsertOrUpdate(t *testing.T) {
	cacheGetter, cacheSetter := cache.GetCacheStorageClient()
	testCatalogItem := TestCatalogItem{Id: "9", Name: "Item9", Price: 99.69}
	Convey("Inserting or updating non exist test item with ID = 9", t, func() {
		err := cacheSetter.InsertOrUpdate(context.TODO(), testCollectionName, "9", testVersion, testCatalogItem)
		So(err, ShouldBeNil)
	})
	var insertedItem TestCatalogItem
	Convey("Getting inserted test item with ID = 9 and name = Item9", t, func() {
		cacheGetter.GetById(context.TODO(), testCollectionName, "9", testVersion, &insertedItem)
		So(insertedItem.Id, ShouldEqual, testCatalogItem.Id)
		So(insertedItem.Name, ShouldEqual, testCatalogItem.Name)
		So(insertedItem.Price, ShouldEqual, testCatalogItem.Price)
	})

	testCatalogItem.Name = testCatalogItem.Name + "!"
	Convey("Inserting or updating exist test item with ID = 9", t, func() {
		err := cacheSetter.InsertOrUpdate(context.TODO(), testCollectionName, "9", testVersion, testCatalogItem)
		So(err, ShouldBeNil)
	})

	Convey("Getting updated test item with ID = 9 and name = Item9!", t, func() {
		cacheGetter.GetById(context.TODO(), testCollectionName, "9", testVersion, &insertedItem)
		So(insertedItem.Id, ShouldEqual, testCatalogItem.Id)
		So(insertedItem.Name, ShouldEqual, testCatalogItem.Name)
		So(insertedItem.Price, ShouldEqual, testCatalogItem.Price)
	})
}

func TestRemove(t *testing.T) {
	cacheGetter, cacheSetter := cache.GetCacheStorageClient()
	Convey("Removing test item with ID = 1", t, func() {
		err := cacheSetter.Remove(context.TODO(), testCollectionName, "1", testVersion)
		So(err, ShouldBeNil)
	})
	Convey("Getting removed test item with ID = 1", t, func() {
		insertedItems := make(map[string]TestCatalogItem)
		cacheGetter.GetManyByIds(context.TODO(), testCollectionName, []string{"1"}, testVersion, insertedItems)
		So(len(insertedItems), ShouldEqual, 0)
	})
}

func TestRemoveAll(t *testing.T) {
	cacheGetter, cacheSetter := cache.GetCacheStorageClient()
	Convey("Removing all items", t, func() {
		err := cacheSetter.RemoveAll(context.TODO(), testCollectionName, testVersion)
		So(err, ShouldBeNil)
	})
	Convey("Getting all items", t, func() {
		insertedItems := make(map[string]TestCatalogItem)
		cacheGetter.GetAll(context.TODO(), testCollectionName, testVersion, insertedItems)
		So(len(insertedItems), ShouldEqual, 0)
	})
}
