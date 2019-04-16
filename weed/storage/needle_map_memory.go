package storage

import (
	"io"
	"os"

	"github.com/HZ89/seaweedfs/weed/glog"
	"github.com/HZ89/seaweedfs/weed/storage/needle"
	"github.com/HZ89/seaweedfs/weed/storage/types"
)

type NeedleMap struct {
	m needle.NeedleValueMap

	baseNeedleMapper
}

func NewCompactNeedleMap(file *os.File) *NeedleMap {
	nm := &NeedleMap{
		m: needle.NewCompactMap(),
	}
	nm.indexFile = file
	return nm
}

func NewBtreeNeedleMap(file *os.File) *NeedleMap {
	nm := &NeedleMap{
		m: needle.NewBtreeMap(),
	}
	nm.indexFile = file
	return nm
}

const (
	RowsToRead = 1024
)

func LoadCompactNeedleMap(file *os.File) (*NeedleMap, error) {
	nm := NewCompactNeedleMap(file)
	return doLoading(file, nm)
}

func LoadBtreeNeedleMap(file *os.File) (*NeedleMap, error) {
	nm := NewBtreeNeedleMap(file)
	return doLoading(file, nm)
}

func doLoading(file *os.File, nm *NeedleMap) (*NeedleMap, error) {
	e := WalkIndexFile(file, func(key types.NeedleId, offset types.Offset, size uint32) error {
		nm.MaybeSetMaxFileKey(key)
		if !offset.IsZero() && size != types.TombstoneFileSize {
			nm.FileCounter++
			nm.FileByteCounter = nm.FileByteCounter + uint64(size)
			oldOffset, oldSize := nm.m.Set(types.NeedleId(key), offset, size)
			// glog.V(3).Infoln("reading key", key, "offset", offset*NeedlePaddingSize, "size", size, "oldSize", oldSize)
			if !oldOffset.IsZero() && oldSize != types.TombstoneFileSize {
				nm.DeletionCounter++
				nm.DeletionByteCounter = nm.DeletionByteCounter + uint64(oldSize)
			}
		} else {
			oldSize := nm.m.Delete(types.NeedleId(key))
			// glog.V(3).Infoln("removing key", key, "offset", offset*NeedlePaddingSize, "size", size, "oldSize", oldSize)
			nm.DeletionCounter++
			nm.DeletionByteCounter = nm.DeletionByteCounter + uint64(oldSize)
		}
		return nil
	})
	glog.V(1).Infof("max file key: %d for file: %s", nm.MaxFileKey(), file.Name())
	return nm, e
}

// walks through the index file, calls fn function with each key, offset, size
// stops with the error returned by the fn function
func WalkIndexFile(r *os.File, fn func(key types.NeedleId, offset types.Offset, size uint32) error) error {
	var readerOffset int64
	bytes := make([]byte, types.NeedleEntrySize*RowsToRead)
	count, e := r.ReadAt(bytes, readerOffset)
	glog.V(3).Infoln("file", r.Name(), "readerOffset", readerOffset, "count", count, "e", e)
	readerOffset += int64(count)
	var (
		key    types.NeedleId
		offset types.Offset
		size   uint32
		i      int
	)

	for count > 0 && e == nil || e == io.EOF {
		for i = 0; i+types.NeedleEntrySize <= count; i += types.NeedleEntrySize {
			key, offset, size = IdxFileEntry(bytes[i : i+types.NeedleEntrySize])
			if e = fn(key, offset, size); e != nil {
				return e
			}
		}
		if e == io.EOF {
			return nil
		}
		count, e = r.ReadAt(bytes, readerOffset)
		glog.V(3).Infoln("file", r.Name(), "readerOffset", readerOffset, "count", count, "e", e)
		readerOffset += int64(count)
	}
	return e
}

func (nm *NeedleMap) Put(key types.NeedleId, offset types.Offset, size uint32) error {
	_, oldSize := nm.m.Set(types.NeedleId(key), offset, size)
	nm.logPut(key, oldSize, size)
	return nm.appendToIndexFile(key, offset, size)
}
func (nm *NeedleMap) Get(key types.NeedleId) (element *needle.NeedleValue, ok bool) {
	element, ok = nm.m.Get(types.NeedleId(key))
	return
}
func (nm *NeedleMap) Delete(key types.NeedleId, offset types.Offset) error {
	deletedBytes := nm.m.Delete(types.NeedleId(key))
	nm.logDelete(deletedBytes)
	return nm.appendToIndexFile(key, offset, types.TombstoneFileSize)
}
func (nm *NeedleMap) Close() {
	_ = nm.indexFile.Close()
}
func (nm *NeedleMap) Destroy() error {
	nm.Close()
	return os.Remove(nm.indexFile.Name())
}
