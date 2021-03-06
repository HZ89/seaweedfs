package weed_server

import (
	"net/http"

	"github.com/chrislusf/raft"
	ui "gitlab.momenta.works/kubetrain/seaweedfs/weed/server/master_ui"
	"gitlab.momenta.works/kubetrain/seaweedfs/weed/stats"
	"gitlab.momenta.works/kubetrain/seaweedfs/weed/util"
)

func (ms *MasterServer) uiStatusHandler(w http.ResponseWriter, r *http.Request) {
	infos := make(map[string]interface{})
	infos["Version"] = util.VERSION
	args := struct {
		Version    string
		Topology   interface{}
		RaftServer raft.Server
		Stats      map[string]interface{}
		Counters   *stats.ServerStats
	}{
		util.VERSION,
		ms.Topo.ToMap(),
		ms.Topo.RaftServer,
		infos,
		serverStats,
	}
	ui.StatusTpl.Execute(w, args)
}
