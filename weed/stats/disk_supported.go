// +build !windows,!openbsd,!netbsd,!plan9,!solaris

package stats

import (
	"syscall"

	"gitlab.momenta.works/kubetrain/seaweedfs/weed/pb/volume_server_pb"
)

func fillInDiskStatus(disk *volume_server_pb.DiskStatus) {
	fs := syscall.Statfs_t{}
	err := syscall.Statfs(disk.Dir, &fs)
	if err != nil {
		return
	}
	disk.All = fs.Blocks * uint64(fs.Bsize)
	disk.Free = fs.Bfree * uint64(fs.Bsize)
	disk.Used = disk.All - disk.Free
	return
}
