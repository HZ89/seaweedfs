// +build linux darwin

package command

import (
	"fmt"
	"os"
	"os/user"
	"path"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/viper"
	"gitlab.momenta.works/kubetrain/seaweedfs/weed/security"
	"gitlab.momenta.works/kubetrain/seaweedfs/weed/server"

	"github.com/seaweedfs/fuse"
	"github.com/seaweedfs/fuse/fs"
	"gitlab.momenta.works/kubetrain/seaweedfs/weed/filesys"
	"gitlab.momenta.works/kubetrain/seaweedfs/weed/glog"
	"gitlab.momenta.works/kubetrain/seaweedfs/weed/util"
)

func runMount(cmd *Command, args []string) bool {

	weed_server.LoadConfiguration("security", false)

	fmt.Printf("This is SeaweedFS version %s %s %s\n", util.VERSION, runtime.GOOS, runtime.GOARCH)
	if *mountOptions.dir == "" {
		fmt.Printf("Please specify the mount directory via \"-dir\"")
		return false
	}
	if *mountOptions.chunkSizeLimitMB <= 0 {
		fmt.Printf("Please specify a reasonable buffer size.")
		return false
	}

	fuse.Unmount(*mountOptions.dir)

	// detect mount folder mode
	mountMode := os.ModeDir | 0755
	if fileInfo, err := os.Stat(*mountOptions.dir); err == nil {
		mountMode = os.ModeDir | fileInfo.Mode()
	}

	mountName := path.Base(*mountOptions.dir)

	// detect current user
	uid, gid := uint32(0), uint32(0)
	if u, err := user.Current(); err == nil {
		if parsedId, pe := strconv.ParseUint(u.Uid, 10, 32); pe == nil {
			uid = uint32(parsedId)
		}
		if parsedId, pe := strconv.ParseUint(u.Gid, 10, 32); pe == nil {
			gid = uint32(parsedId)
		}
	}

	util.SetupProfiling(*mountCpuProfile, *mountMemProfile)

	options := []fuse.MountOption{
		fuse.VolumeName(mountName),
		fuse.FSName("SeaweedFS"),
		fuse.Subtype("SeaweedFS"),
		fuse.NoAppleDouble(),
		fuse.NoAppleXattr(),
		fuse.NoBrowse(),
		fuse.AutoXattr(),
		fuse.ExclCreate(),
		fuse.DaemonTimeout("3600"),
		fuse.AllowSUID(),
		fuse.DefaultPermissions(),
		fuse.MaxReadahead(1024 * 128),
		fuse.AsyncRead(),
		fuse.WritebackCache(),
	}
	if *mountOptions.allowOthers {
		options = append(options, fuse.AllowOther())
	}

	c, err := fuse.Mount(*mountOptions.dir, options...)
	if err != nil {
		glog.Fatal(err)
		return false
	}

	util.OnInterrupt(func() {
		fuse.Unmount(*mountOptions.dir)
		c.Close()
	})

	filerGrpcAddress, err := parseFilerGrpcAddress(*mountOptions.filer)
	if err != nil {
		glog.Fatal(err)
		return false
	}

	mountRoot := *mountOptions.filerMountRootPath
	if mountRoot != "/" && strings.HasSuffix(mountRoot, "/") {
		mountRoot = mountRoot[0 : len(mountRoot)-1]
	}

	err = fs.Serve(c, filesys.NewSeaweedFileSystem(&filesys.Option{
		FilerGrpcAddress:   filerGrpcAddress,
		GrpcDialOption:     security.LoadClientTLS(viper.Sub("grpc"), "client"),
		FilerMountRootPath: mountRoot,
		Collection:         *mountOptions.collection,
		Replication:        *mountOptions.replication,
		TtlSec:             int32(*mountOptions.ttlSec),
		ChunkSizeLimit:     int64(*mountOptions.chunkSizeLimitMB) * 1024 * 1024,
		DataCenter:         *mountOptions.dataCenter,
		DirListingLimit:    *mountOptions.dirListingLimit,
		EntryCacheTtl:      3 * time.Second,
		MountUid:           uid,
		MountGid:           gid,
		MountMode:          mountMode,
	}))
	if err != nil {
		fuse.Unmount(*mountOptions.dir)
	}

	// check if the mount process has an error to report
	<-c.Ready
	if err := c.MountError; err != nil {
		glog.Fatal(err)
	}

	return true
}
