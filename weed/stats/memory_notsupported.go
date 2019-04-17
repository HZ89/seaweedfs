// +build !linux

package stats

import "gitlab.momenta.works/kubetrain/seaweedfs/weed/pb/volume_server_pb"

func fillInMemStatus(status *volume_server_pb.MemStatus) {
	return
}
