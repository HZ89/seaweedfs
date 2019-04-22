package weed_server

import (
	"net/http"

	"gitlab.momenta.works/kubetrain/seaweedfs/weed/glog"
	"gitlab.momenta.works/kubetrain/seaweedfs/weed/security"
	"gitlab.momenta.works/kubetrain/seaweedfs/weed/server/metrics"
	"gitlab.momenta.works/kubetrain/seaweedfs/weed/storage"

	"github.com/spf13/viper"
	"google.golang.org/grpc"
)

type VolumeServer struct {
	MasterNodes    []string
	currentMaster  string
	pulseSeconds   int
	dataCenter     string
	rack           string
	store          *storage.Store
	guard          *security.Guard
	grpcDialOption grpc.DialOption

	needleMapKind     storage.NeedleMapType
	FixJpgOrientation bool
	ReadRedirect      bool
}

func NewVolumeServer(adminMux, publicMux *http.ServeMux, ip string,
	port int, publicUrl string,
	folders []string, maxCounts []int,
	needleMapKind storage.NeedleMapType,
	masterNodes []string, pulseSeconds int,
	dataCenter string, rack string,
	whiteList []string,
	fixJpgOrientation bool,
	readRedirect bool) *VolumeServer {

	v := viper.GetViper()
	signingKey := v.GetString("jwt.signing.key")
	enableUiAccess := v.GetBool("access.ui")
	metrics.VolumeRegisterMetrics()

	vs := &VolumeServer{
		pulseSeconds:      pulseSeconds,
		dataCenter:        dataCenter,
		rack:              rack,
		needleMapKind:     needleMapKind,
		FixJpgOrientation: fixJpgOrientation,
		ReadRedirect:      readRedirect,
		grpcDialOption:    security.LoadClientTLS(viper.Sub("grpc"), "volume"),
	}
	vs.MasterNodes = masterNodes
	vs.store = storage.NewStore(port, ip, publicUrl, folders, maxCounts, vs.needleMapKind)

	vs.guard = security.NewGuard(whiteList, signingKey)

	handleStaticResources(adminMux)
	if signingKey == "" || enableUiAccess {
		// only expose the volume server details for safe environments
		adminMux.HandleFunc("/ui/index.html", vs.uiStatusHandler)
		adminMux.HandleFunc("/status", vs.guard.WhiteList(vs.statusHandler))
		adminMux.HandleFunc("/metrics", vs.guard.WhiteList(vs.metricsHandler))
	}
	adminMux.HandleFunc("/", vs.privateStoreHandler)
	if publicMux != adminMux {
		// separated admin and public port
		handleStaticResources(publicMux)
		publicMux.HandleFunc("/", vs.publicReadOnlyHandler)
	}

	go vs.heartbeat()

	return vs
}

func (vs *VolumeServer) Shutdown() {
	glog.V(0).Infoln("Shutting down volume server...")
	vs.store.Close()
	glog.V(0).Infoln("Shut down successfully!")
}

func (vs *VolumeServer) jwt(fileId string) security.EncodedJwt {
	return security.GenJwt(vs.guard.SigningKey, fileId)
}

func (vs *VolumeServer) metricsHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method == "DELETE" {
		metrics.MasterReset()
		writeJsonQuiet(w, r, http.StatusOK, "metrics reset")
		return
	}
	defaultMetricsHandler(w, r)
}
