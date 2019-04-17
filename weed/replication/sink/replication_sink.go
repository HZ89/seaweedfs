package sink

import (
	"context"

	"gitlab.momenta.works/kubetrain/seaweedfs/weed/pb/filer_pb"
	"gitlab.momenta.works/kubetrain/seaweedfs/weed/replication/source"
	"gitlab.momenta.works/kubetrain/seaweedfs/weed/util"
)

type ReplicationSink interface {
	GetName() string
	Initialize(configuration util.Configuration) error
	DeleteEntry(ctx context.Context, key string, isDirectory, deleteIncludeChunks bool) error
	CreateEntry(ctx context.Context, key string, entry *filer_pb.Entry) error
	UpdateEntry(ctx context.Context, key string, oldEntry, newEntry *filer_pb.Entry, deleteIncludeChunks bool) (foundExistingEntry bool, err error)
	GetSinkToDirectory() string
	SetSourceFiler(s *source.FilerSource)
}

var (
	Sinks []ReplicationSink
)
