package stats

import "gitlab.momenta.works/kubetrain/seaweedfs/weed/pb/volume_server_pb"

func NewDiskStatus(path string) (disk *volume_server_pb.DiskStatus) {
	disk = &volume_server_pb.DiskStatus{Dir: path}
	fillInDiskStatus(disk)
	return
}
