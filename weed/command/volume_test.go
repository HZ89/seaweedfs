package command

import (
	"net/http"
	"testing"
	"time"

	"gitlab.momenta.works/kubetrain/seaweedfs/weed/glog"
)

func TestXYZ(t *testing.T) {
	glog.V(0).Infoln("Last-Modified", time.Unix(int64(1373273596), 0).UTC().Format(http.TimeFormat))
}
