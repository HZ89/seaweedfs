package weed_server

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"gitlab.momenta.works/kubetrain/seaweedfs/weed/glog"
	"gitlab.momenta.works/kubetrain/seaweedfs/weed/operation"
	"gitlab.momenta.works/kubetrain/seaweedfs/weed/stats"
	"gitlab.momenta.works/kubetrain/seaweedfs/weed/storage"
	"gitlab.momenta.works/kubetrain/seaweedfs/weed/util"

	"github.com/gorilla/mux"
	"github.com/prometheus/client_golang/prometheus"
	statik "github.com/rakyll/statik/fs"
	_ "gitlab.momenta.works/kubetrain/seaweedfs/weed/statik"
	"google.golang.org/grpc"
)

var serverStats *stats.ServerStats
var startTime = time.Now()
var statikFS http.FileSystem
var defaultMetricsHandler = prometheus.Handler().ServeHTTP

func init() {
	serverStats = stats.NewServerStats()
	go serverStats.Start()
	statikFS, _ = statik.New()
}

func writeJson(w http.ResponseWriter, r *http.Request, httpStatus int, obj interface{}) (err error) {
	var bytes []byte
	if r.FormValue("pretty") != "" {
		bytes, err = json.MarshalIndent(obj, "", "  ")
	} else {
		bytes, err = json.Marshal(obj)
	}
	if err != nil {
		return
	}
	callback := r.FormValue("callback")
	if callback == "" {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(httpStatus)
		_, err = w.Write(bytes)
	} else {
		w.Header().Set("Content-Type", "application/javascript")
		w.WriteHeader(httpStatus)
		if _, err = w.Write([]uint8(callback)); err != nil {
			return
		}
		if _, err = w.Write([]uint8("(")); err != nil {
			return
		}
		fmt.Fprint(w, string(bytes))
		if _, err = w.Write([]uint8(")")); err != nil {
			return
		}
	}

	return
}

// wrapper for writeJson - just logs errors
func writeJsonQuiet(w http.ResponseWriter, r *http.Request, httpStatus int, obj interface{}) {
	if err := writeJson(w, r, httpStatus, obj); err != nil {
		glog.V(0).Infof("error writing JSON %+v status %d: %v", obj, httpStatus, err)
	}
}
func writeJsonError(w http.ResponseWriter, r *http.Request, httpStatus int, err error) {
	m := make(map[string]interface{})
	m["error"] = err.Error()
	writeJsonQuiet(w, r, httpStatus, m)
}

func debug(params ...interface{}) {
	glog.V(4).Infoln(params...)
}

func submitForClientHandler(w http.ResponseWriter, r *http.Request, masterUrl string, grpcDialOption grpc.DialOption) {
	m := make(map[string]interface{})
	if r.Method != "POST" {
		writeJsonError(w, r, http.StatusMethodNotAllowed, errors.New("Only submit via POST!"))
		return
	}

	debug("parsing upload file...")
	fname, data, mimeType, pairMap, isGzipped, originalDataSize, lastModified, _, _, pe := storage.ParseUpload(r)
	if pe != nil {
		writeJsonError(w, r, http.StatusBadRequest, pe)
		return
	}

	debug("assigning file id for", fname)
	r.ParseForm()
	count := uint64(1)
	if r.FormValue("count") != "" {
		count, pe = strconv.ParseUint(r.FormValue("count"), 10, 32)
		if pe != nil {
			writeJsonError(w, r, http.StatusBadRequest, pe)
			return
		}
	}
	ar := &operation.VolumeAssignRequest{
		Count:       count,
		Replication: r.FormValue("replication"),
		Collection:  r.FormValue("collection"),
		Ttl:         r.FormValue("ttl"),
	}
	assignResult, ae := operation.Assign(masterUrl, grpcDialOption, ar)
	if ae != nil {
		writeJsonError(w, r, http.StatusInternalServerError, ae)
		return
	}

	url := "http://" + assignResult.Url + "/" + assignResult.Fid
	if lastModified != 0 {
		url = url + "?ts=" + strconv.FormatUint(lastModified, 10)
	}

	debug("upload file to store", url)
	uploadResult, err := operation.Upload(url, fname, bytes.NewReader(data), isGzipped, mimeType, pairMap, assignResult.Auth)
	if err != nil {
		writeJsonError(w, r, http.StatusInternalServerError, err)
		return
	}

	m["fileName"] = fname
	m["fid"] = assignResult.Fid
	m["fileUrl"] = assignResult.PublicUrl + "/" + assignResult.Fid
	m["size"] = originalDataSize
	m["eTag"] = uploadResult.ETag
	writeJsonQuiet(w, r, http.StatusCreated, m)
	return
}

func parseURLPath(path string) (vid, fid, filename, ext string, isVolumeIdOnly bool) {
	switch strings.Count(path, "/") {
	case 3:
		parts := strings.Split(path, "/")
		vid, fid, filename = parts[1], parts[2], parts[3]
		ext = filepath.Ext(filename)
	case 2:
		parts := strings.Split(path, "/")
		vid, fid = parts[1], parts[2]
		dotIndex := strings.LastIndex(fid, ".")
		if dotIndex > 0 {
			ext = fid[dotIndex:]
			fid = fid[0:dotIndex]
		}
	default:
		sepIndex := strings.LastIndex(path, "/")
		commaIndex := strings.LastIndex(path[sepIndex:], ",")
		if commaIndex <= 0 {
			vid, isVolumeIdOnly = path[sepIndex+1:], true
			return
		}
		dotIndex := strings.LastIndex(path[sepIndex:], ".")
		vid = path[sepIndex+1 : commaIndex]
		fid = path[commaIndex+1:]
		ext = ""
		if dotIndex > 0 {
			fid = path[commaIndex+1 : dotIndex]
			ext = path[dotIndex:]
		}
	}
	return
}

func healthzHandler(w http.ResponseWriter, r *http.Request) {
	m := make(map[string]interface{})
	m["Version"] = util.VERSION
	writeJsonQuiet(w, r, http.StatusOK, m)
}

func handleStaticResources(defaultMux *http.ServeMux) {
	defaultMux.Handle("/favicon.ico", http.FileServer(statikFS))
	defaultMux.Handle("/seaweedfsstatic/", http.StripPrefix("/seaweedfsstatic", http.FileServer(statikFS)))
}

func handleStaticResources2(r *mux.Router) {
	r.Handle("/favicon.ico", http.FileServer(statikFS))
	r.PathPrefix("/seaweedfsstatic/").Handler(http.StripPrefix("/seaweedfsstatic", http.FileServer(statikFS)))
}
