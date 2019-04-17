package main

import (
	"bytes"
	"flag"
	"fmt"
	"log"
	"math/rand"

	"github.com/spf13/viper"
	"gitlab.momenta.works/kubetrain/seaweedfs/weed/security"
	"gitlab.momenta.works/kubetrain/seaweedfs/weed/server"

	"gitlab.momenta.works/kubetrain/seaweedfs/weed/operation"
	"gitlab.momenta.works/kubetrain/seaweedfs/weed/util"
)

var (
	master = flag.String("master", "127.0.0.1:9333", "the master server")
	repeat = flag.Int("n", 5, "repeat how many times")
)

func main() {
	flag.Parse()

	weed_server.LoadConfiguration("security", false)
	grpcDialOption := security.LoadClientTLS(viper.Sub("grpc"), "client")

	for i := 0; i < *repeat; i++ {
		assignResult, err := operation.Assign(*master, grpcDialOption, &operation.VolumeAssignRequest{Count: 1})
		if err != nil {
			log.Fatalf("assign: %v", err)
		}

		data := make([]byte, 1024)
		rand.Read(data)
		reader := bytes.NewReader(data)

		targetUrl := fmt.Sprintf("http://%s/%s", assignResult.Url, assignResult.Fid)

		_, err = operation.Upload(targetUrl, fmt.Sprintf("test%d", i), reader, false, "", nil, assignResult.Auth)
		if err != nil {
			log.Fatalf("upload: %v", err)
		}

		util.Delete(targetUrl, string(assignResult.Auth))

		util.Get(fmt.Sprintf("http://%s/vol/vacuum", *master))

	}

}
