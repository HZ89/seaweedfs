package S3Sink

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"gitlab.momenta.works/kubetrain/seaweedfs/weed/filer2"
	"gitlab.momenta.works/kubetrain/seaweedfs/weed/glog"
	"gitlab.momenta.works/kubetrain/seaweedfs/weed/pb/filer_pb"
	"gitlab.momenta.works/kubetrain/seaweedfs/weed/replication/sink"
	"gitlab.momenta.works/kubetrain/seaweedfs/weed/replication/source"
	"gitlab.momenta.works/kubetrain/seaweedfs/weed/util"
)

type S3Sink struct {
	conn        s3iface.S3API
	region      string
	bucket      string
	dir         string
	filerSource *source.FilerSource
}

func init() {
	sink.Sinks = append(sink.Sinks, &S3Sink{})
}

func (s3sink *S3Sink) GetName() string {
	return "s3"
}

func (s3sink *S3Sink) GetSinkToDirectory() string {
	return s3sink.dir
}

func (s3sink *S3Sink) Initialize(configuration util.Configuration) error {
	glog.V(0).Infof("sink.s3.region: %v", configuration.GetString("region"))
	glog.V(0).Infof("sink.s3.bucket: %v", configuration.GetString("bucket"))
	glog.V(0).Infof("sink.s3.directory: %v", configuration.GetString("directory"))
	return s3sink.initialize(
		configuration.GetString("aws_access_key_id"),
		configuration.GetString("aws_secret_access_key"),
		configuration.GetString("region"),
		configuration.GetString("bucket"),
		configuration.GetString("directory"),
	)
}

func (s3sink *S3Sink) SetSourceFiler(s *source.FilerSource) {
	s3sink.filerSource = s
}

func (s3sink *S3Sink) initialize(awsAccessKeyId, aswSecretAccessKey, region, bucket, dir string) error {
	s3sink.region = region
	s3sink.bucket = bucket
	s3sink.dir = dir

	config := &aws.Config{
		Region: aws.String(s3sink.region),
	}
	if awsAccessKeyId != "" && aswSecretAccessKey != "" {
		config.Credentials = credentials.NewStaticCredentials(awsAccessKeyId, aswSecretAccessKey, "")
	}

	sess, err := session.NewSession(config)
	if err != nil {
		return fmt.Errorf("create aws session: %v", err)
	}
	s3sink.conn = s3.New(sess)

	return nil
}

func (s3sink *S3Sink) DeleteEntry(ctx context.Context, key string, isDirectory, deleteIncludeChunks bool) error {

	key = cleanKey(key)

	if isDirectory {
		key = key + "/"
	}

	return s3sink.deleteObject(key)

}

func (s3sink *S3Sink) CreateEntry(ctx context.Context, key string, entry *filer_pb.Entry) error {

	key = cleanKey(key)

	if entry.IsDirectory {
		return nil
	}

	uploadId, err := s3sink.createMultipartUpload(key, entry)
	if err != nil {
		return err
	}

	totalSize := filer2.TotalSize(entry.Chunks)
	chunkViews := filer2.ViewFromChunks(entry.Chunks, 0, int(totalSize))

	var parts []*s3.CompletedPart
	var wg sync.WaitGroup
	for chunkIndex, chunk := range chunkViews {
		partId := chunkIndex + 1
		wg.Add(1)
		go func(chunk *filer2.ChunkView) {
			defer wg.Done()
			if part, uploadErr := s3sink.uploadPart(ctx, key, uploadId, partId, chunk); uploadErr != nil {
				err = uploadErr
			} else {
				parts = append(parts, part)
			}
		}(chunk)
	}
	wg.Wait()

	if err != nil {
		s3sink.abortMultipartUpload(key, uploadId)
		return err
	}

	return s3sink.completeMultipartUpload(ctx, key, uploadId, parts)

}

func (s3sink *S3Sink) UpdateEntry(ctx context.Context, key string, oldEntry, newEntry *filer_pb.Entry, deleteIncludeChunks bool) (foundExistingEntry bool, err error) {
	key = cleanKey(key)
	// TODO improve efficiency
	return false, nil
}

func cleanKey(key string) string {
	if strings.HasPrefix(key, "/") {
		key = key[1:]
	}
	return key
}
