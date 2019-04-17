package operation

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"gitlab.momenta.works/kubetrain/seaweedfs/weed/glog"
	"gitlab.momenta.works/kubetrain/seaweedfs/weed/pb/master_pb"
	"gitlab.momenta.works/kubetrain/seaweedfs/weed/pb/volume_server_pb"
	"gitlab.momenta.works/kubetrain/seaweedfs/weed/util"
	"google.golang.org/grpc"
)

func WithVolumeServerClient(volumeServer string, grpcDialOption grpc.DialOption, fn func(volume_server_pb.VolumeServerClient) error) error {

	ctx := context.Background()

	grpcAddress, err := toVolumeServerGrpcAddress(volumeServer)
	if err != nil {
		return err
	}

	return util.WithCachedGrpcClient(ctx, func(grpcConnection *grpc.ClientConn) error {
		client := volume_server_pb.NewVolumeServerClient(grpcConnection)
		return fn(client)
	}, grpcAddress, grpcDialOption)

}

func toVolumeServerGrpcAddress(volumeServer string) (grpcAddress string, err error) {
	sepIndex := strings.LastIndex(volumeServer, ":")
	port, err := strconv.Atoi(volumeServer[sepIndex+1:])
	if err != nil {
		glog.Errorf("failed to parse volume server address: %v", volumeServer)
		return "", err
	}
	return fmt.Sprintf("%s:%d", volumeServer[0:sepIndex], port+10000), nil
}

func withMasterServerClient(masterServer string, grpcDialOption grpc.DialOption, fn func(masterClient master_pb.SeaweedClient) error) error {

	ctx := context.Background()

	masterGrpcAddress, parseErr := util.ParseServerToGrpcAddress(masterServer)
	if parseErr != nil {
		return fmt.Errorf("failed to parse master grpc %v", masterServer)
	}

	return util.WithCachedGrpcClient(ctx, func(grpcConnection *grpc.ClientConn) error {
		client := master_pb.NewSeaweedClient(grpcConnection)
		return fn(client)
	}, masterGrpcAddress, grpcDialOption)

}
