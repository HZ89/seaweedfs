package redis

import (
	"github.com/go-redis/redis"
	"gitlab.momenta.works/kubetrain/seaweedfs/weed/filer2"
	"gitlab.momenta.works/kubetrain/seaweedfs/weed/util"
)

func init() {
	filer2.Stores = append(filer2.Stores, &RedisClusterStore{})
}

type RedisClusterStore struct {
	UniversalRedisStore
}

func (store *RedisClusterStore) GetName() string {
	return "redis_cluster"
}

func (store *RedisClusterStore) Initialize(configuration util.Configuration) (err error) {
	return store.initialize(
		configuration.GetStringSlice("addresses"),
	)
}

func (store *RedisClusterStore) initialize(addresses []string) (err error) {
	store.Client = redis.NewClusterClient(&redis.ClusterOptions{
		Addrs: addresses,
	})
	return
}
