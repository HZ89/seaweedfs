package storage

import (
	"encoding/hex"

	"gitlab.momenta.works/kubetrain/seaweedfs/weed/storage/types"
)

type FileId struct {
	VolumeId VolumeId
	Key      types.NeedleId
	Cookie   types.Cookie
}

func NewFileIdFromNeedle(VolumeId VolumeId, n *Needle) *FileId {
	return &FileId{VolumeId: VolumeId, Key: n.Id, Cookie: n.Cookie}
}

func NewFileId(VolumeId VolumeId, key uint64, cookie uint32) *FileId {
	return &FileId{VolumeId: VolumeId, Key: types.Uint64ToNeedleId(key), Cookie: types.Uint32ToCookie(cookie)}
}

func (n *FileId) String() string {
	return n.VolumeId.String() + "," + formatNeedleIdCookie(n.Key, n.Cookie)
}

func formatNeedleIdCookie(key types.NeedleId, cookie types.Cookie) string {
	bytes := make([]byte, types.NeedleIdSize+types.CookieSize)
	types.NeedleIdToBytes(bytes[0:types.NeedleIdSize], key)
	types.CookieToBytes(bytes[types.NeedleIdSize:types.NeedleIdSize+types.CookieSize], cookie)
	nonzero_index := 0
	for ; bytes[nonzero_index] == 0; nonzero_index++ {
	}
	return hex.EncodeToString(bytes[nonzero_index:])
}
