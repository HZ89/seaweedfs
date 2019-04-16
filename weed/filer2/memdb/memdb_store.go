package memdb

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/HZ89/seaweedfs/weed/filer2"
	"github.com/HZ89/seaweedfs/weed/util"
)

func init() {
	filer2.Stores = append(filer2.Stores, &MemDbStore{})
}

type MemDbStore struct {
	smap *sync.Map
}

func (store *MemDbStore) GetName() string {
	return "memory"
}

func (store *MemDbStore) Initialize(configuration util.Configuration) (err error) {
	store.smap = new(sync.Map)
	return nil
}

func (store *MemDbStore) BeginTransaction(ctx context.Context) (context.Context, error) {
	return ctx, nil
}
func (store *MemDbStore) CommitTransaction(ctx context.Context) error {
	return nil
}
func (store *MemDbStore) RollbackTransaction(ctx context.Context) error {
	return nil
}

func (store *MemDbStore) InsertEntry(ctx context.Context, entry *filer2.Entry) (err error) {
	store.smap.Store(entry.FullPath, entry)
	return nil
}

func (store *MemDbStore) UpdateEntry(ctx context.Context, entry *filer2.Entry) (err error) {
	_, ok := store.smap.Load(entry.FullPath)
	if !ok {
		return filer2.ErrNotFound
	}
	store.smap.Store(entry.FullPath, entry)
	return nil
}

func (store *MemDbStore) FindEntry(ctx context.Context, fullpath filer2.FullPath) (entry *filer2.Entry, err error) {
	data, ok := store.smap.Load(fullpath)
	if !ok {
		return nil, filer2.ErrNotFound
	}

	entry, ok = data.(*filer2.Entry)
	if !ok {
		return nil, fmt.Errorf("unexpected data, key: %s", fullpath)
	}
	return entry, nil
}

func (store *MemDbStore) DeleteEntry(ctx context.Context, fullpath filer2.FullPath) (err error) {
	store.smap.Delete(fullpath)
	return nil
}

func (store *MemDbStore) ListDirectoryEntries(ctx context.Context, fullpath filer2.FullPath, startFileName string, inclusive bool, limit int) (entries []*filer2.Entry, err error) {

	startFrom := string(fullpath)
	if startFileName != "" {
		startFrom = startFrom + "/" + startFileName
	}
	store.smap.Range(func(key, value interface{}) bool {
		stringKey, ok := key.(string)
		if !ok {
			return false
		}
		if stringKey == string(fullpath) {
			return true
		}
		if strings.HasPrefix(stringKey, startFrom) {
			entry, ok := value.(*filer2.Entry)
			if !ok {
				return false
			}
			if len(entries) < limit {
				entries = append(entries, entry)
			} else {
				return false
			}
		}
		return true
	})

	return entries, nil
}
