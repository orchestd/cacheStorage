package configuration

type CacheStorageConfiguration struct {
	CacheDBName *string `json:"CACHE_DB_NAME,omitempty"`
	CacheHost   *string `json:"CACHE_HOST,omitempty"`
}
