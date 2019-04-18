package redis

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/go-redis/redis"
	"gitlab.momenta.works/kubetrain/seaweedfs/weed/filer2"
	"gitlab.momenta.works/kubetrain/seaweedfs/weed/glog"
	"gitlab.momenta.works/kubetrain/seaweedfs/weed/util"
)

func init() {
	filer2.Stores = append(filer2.Stores, &RedisStore{})
}

type RedisStore struct {
	client *redis.Ring
}

func (store *RedisStore) GetName() string {
	return "redis"
}

func (store *RedisStore) Initialize(configuration util.Configuration) (err error) {
	return store.initialize(
		configuration.GetString("address"),
		configuration.GetString("password"),
		configuration.GetInt("database"),
	)
}

func (store *RedisStore) initialize(hostPort string, password string, database int) (err error) {
	hosts := strings.Split(hostPort, ",")
	ropt := new(redis.RingOptions)
	ropt.Addrs = make(map[string]string)
	for i, h := range hosts {
		ropt.Addrs[fmt.Sprintf("shard-%d", i)] = h
	}
	ropt.DB = database
	ropt.Password = password
	store.client = redis.NewRing(ropt)
	return
}

func (store *RedisStore) BeginTransaction(ctx context.Context) (context.Context, error) {
	return ctx, nil
}
func (store *RedisStore) CommitTransaction(ctx context.Context) error {
	return nil
}
func (store *RedisStore) RollbackTransaction(ctx context.Context) error {
	return nil
}

func (store *RedisStore) InsertEntry(ctx context.Context, entry *filer2.Entry) (err error) {

	value, err := entry.EncodeAttributesAndChunks()
	if err != nil {
		return fmt.Errorf("encoding %s %+v: %v", entry.FullPath, entry.Attr, err)
	}
	_, err = store.client.Set(string(entry.FullPath), value, time.Duration(entry.TtlSec)*time.Second).Result()

	if err != nil {
		return fmt.Errorf("persisting %s : %v", entry.FullPath, err)
	}

	dir, name := entry.FullPath.DirAndName()
	if name != "" {
		_, err = store.client.SAdd(genDirectoryListKey(dir), name).Result()
		if err != nil {
			return fmt.Errorf("persisting %s in parent dir: %v", entry.FullPath, err)
		}
	}

	return nil
}

func (store *RedisStore) UpdateEntry(ctx context.Context, entry *filer2.Entry) (err error) {

	return store.InsertEntry(ctx, entry)
}

func (store *RedisStore) FindEntry(ctx context.Context, fullpath filer2.FullPath) (entry *filer2.Entry, err error) {

	data, err := store.client.Get(string(fullpath)).Result()
	if err == redis.Nil {
		return nil, filer2.ErrNotFound
	}

	if err != nil {
		return nil, fmt.Errorf("get %s : %v", fullpath, err)
	}

	entry = &filer2.Entry{
		FullPath: fullpath,
	}
	err = entry.DecodeAttributesAndChunks([]byte(data))
	if err != nil {
		return entry, fmt.Errorf("decode %s : %v", entry.FullPath, err)
	}

	return entry, nil
}

func (store *RedisStore) DeleteEntry(ctx context.Context, fullpath filer2.FullPath) (err error) {

	_, err = store.client.Del(string(fullpath)).Result()

	if err != nil {
		return fmt.Errorf("delete %s : %v", fullpath, err)
	}

	dir, name := fullpath.DirAndName()
	if name != "" {
		_, err = store.client.SRem(genDirectoryListKey(dir), name).Result()
		if err != nil {
			return fmt.Errorf("delete %s in parent dir: %v", fullpath, err)
		}
	}

	return nil
}

func (store *RedisStore) ListDirectoryEntries(ctx context.Context, fullpath filer2.FullPath, startFileName string, inclusive bool,
	limit int) (entries []*filer2.Entry, err error) {

	members, err := store.client.SMembers(genDirectoryListKey(string(fullpath))).Result()
	if err != nil {
		return nil, fmt.Errorf("list %s : %v", fullpath, err)
	}

	// skip
	if startFileName != "" {
		var t []string
		for _, m := range members {
			if strings.Compare(m, startFileName) >= 0 {
				if m == startFileName {
					if inclusive {
						t = append(t, m)
					}
				} else {
					t = append(t, m)
				}
			}
		}
		members = t
	}

	// sort
	sort.Slice(members, func(i, j int) bool {
		return strings.Compare(members[i], members[j]) < 0
	})

	// limit
	if limit < len(members) {
		members = members[:limit]
	}

	// fetch entry meta
	for _, fileName := range members {
		path := filer2.NewFullPath(string(fullpath), fileName)
		entry, err := store.FindEntry(ctx, path)
		if err != nil {
			glog.V(0).Infof("list %s : %v", path, err)
		} else {
			entries = append(entries, entry)
		}
	}

	return entries, err
}
