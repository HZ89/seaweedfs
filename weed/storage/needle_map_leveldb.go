package storage

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/syndtr/goleveldb/leveldb/opt"

	"github.com/HZ89/seaweedfs/weed/glog"
	"github.com/HZ89/seaweedfs/weed/storage/needle"
	"github.com/HZ89/seaweedfs/weed/storage/types"
	"github.com/HZ89/seaweedfs/weed/util"
	"github.com/syndtr/goleveldb/leveldb"
)

type LevelDbNeedleMap struct {
	dbFileName string
	db         *leveldb.DB
	baseNeedleMapper
}

func NewLevelDbNeedleMap(dbFileName string, indexFile *os.File, opts *opt.Options) (m *LevelDbNeedleMap, err error) {
	m = &LevelDbNeedleMap{dbFileName: dbFileName}
	m.indexFile = indexFile
	if !isLevelDbFresh(dbFileName, indexFile) {
		glog.V(0).Infof("Start to Generate %s from %s", dbFileName, indexFile.Name())
		generateLevelDbFile(dbFileName, indexFile)
		glog.V(0).Infof("Finished Generating %s from %s", dbFileName, indexFile.Name())
	}
	glog.V(1).Infof("Opening %s...", dbFileName)

	if m.db, err = leveldb.OpenFile(dbFileName, opts); err != nil {
		return
	}
	glog.V(1).Infof("Loading %s...", indexFile.Name())
	mm, indexLoadError := newNeedleMapMetricFromIndexFile(indexFile)
	if indexLoadError != nil {
		return nil, indexLoadError
	}
	m.mapMetric = *mm
	return
}

func isLevelDbFresh(dbFileName string, indexFile *os.File) bool {
	// normally we always write to index file first
	dbLogFile, err := os.Open(filepath.Join(dbFileName, "LOG"))
	if err != nil {
		return false
	}
	defer dbLogFile.Close()
	dbStat, dbStatErr := dbLogFile.Stat()
	indexStat, indexStatErr := indexFile.Stat()
	if dbStatErr != nil || indexStatErr != nil {
		glog.V(0).Infof("Can not stat file: %v and %v", dbStatErr, indexStatErr)
		return false
	}

	return dbStat.ModTime().After(indexStat.ModTime())
}

func generateLevelDbFile(dbFileName string, indexFile *os.File) error {
	db, err := leveldb.OpenFile(dbFileName, nil)
	if err != nil {
		return err
	}
	defer db.Close()
	return WalkIndexFile(indexFile, func(key types.NeedleId, offset types.Offset, size uint32) error {
		if !offset.IsZero() && size != types.TombstoneFileSize {
			levelDbWrite(db, key, offset, size)
		} else {
			levelDbDelete(db, key)
		}
		return nil
	})
}

func (m *LevelDbNeedleMap) Get(key types.NeedleId) (element *needle.NeedleValue, ok bool) {
	bytes := make([]byte, types.NeedleIdSize)
	types.NeedleIdToBytes(bytes[0:types.NeedleIdSize], key)
	data, err := m.db.Get(bytes, nil)
	if err != nil || len(data) != types.OffsetSize+types.SizeSize {
		return nil, false
	}
	offset := types.BytesToOffset(data[0:types.OffsetSize])
	size := util.BytesToUint32(data[types.OffsetSize : types.OffsetSize+types.SizeSize])
	return &needle.NeedleValue{Key: types.NeedleId(key), Offset: offset, Size: size}, true
}

func (m *LevelDbNeedleMap) Put(key types.NeedleId, offset types.Offset, size uint32) error {
	var oldSize uint32
	if oldNeedle, ok := m.Get(key); ok {
		oldSize = oldNeedle.Size
	}
	m.logPut(key, oldSize, size)
	// write to index file first
	if err := m.appendToIndexFile(key, offset, size); err != nil {
		return fmt.Errorf("cannot write to indexfile %s: %v", m.indexFile.Name(), err)
	}
	return levelDbWrite(m.db, key, offset, size)
}

func levelDbWrite(db *leveldb.DB,
	key types.NeedleId, offset types.Offset, size uint32) error {

	bytes := make([]byte, types.NeedleIdSize+types.OffsetSize+types.SizeSize)
	types.NeedleIdToBytes(bytes[0:types.NeedleIdSize], key)
	types.OffsetToBytes(bytes[types.NeedleIdSize:types.NeedleIdSize+types.OffsetSize], offset)
	util.Uint32toBytes(bytes[types.NeedleIdSize+types.OffsetSize:types.NeedleIdSize+types.OffsetSize+types.SizeSize], size)

	if err := db.Put(bytes[0:types.NeedleIdSize], bytes[types.NeedleIdSize:types.NeedleIdSize+types.OffsetSize+types.SizeSize], nil); err != nil {
		return fmt.Errorf("failed to write leveldb: %v", err)
	}
	return nil
}
func levelDbDelete(db *leveldb.DB, key types.NeedleId) error {
	bytes := make([]byte, types.NeedleIdSize)
	types.NeedleIdToBytes(bytes, key)
	return db.Delete(bytes, nil)
}

func (m *LevelDbNeedleMap) Delete(key types.NeedleId, offset types.Offset) error {
	if oldNeedle, ok := m.Get(key); ok {
		m.logDelete(oldNeedle.Size)
	}
	// write to index file first
	if err := m.appendToIndexFile(key, offset, types.TombstoneFileSize); err != nil {
		return err
	}
	return levelDbDelete(m.db, key)
}

func (m *LevelDbNeedleMap) Close() {
	m.indexFile.Close()
	m.db.Close()
}

func (m *LevelDbNeedleMap) Destroy() error {
	m.Close()
	os.Remove(m.indexFile.Name())
	return os.RemoveAll(m.dbFileName)
}
