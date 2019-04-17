package weed_server

import (
	"net/http"
	"os"

	"google.golang.org/grpc"

	"github.com/spf13/viper"
	"gitlab.momenta.works/kubetrain/seaweedfs/weed/filer2"
	_ "gitlab.momenta.works/kubetrain/seaweedfs/weed/filer2/cassandra"
	_ "gitlab.momenta.works/kubetrain/seaweedfs/weed/filer2/leveldb"
	_ "gitlab.momenta.works/kubetrain/seaweedfs/weed/filer2/memdb"
	_ "gitlab.momenta.works/kubetrain/seaweedfs/weed/filer2/mysql"
	_ "gitlab.momenta.works/kubetrain/seaweedfs/weed/filer2/postgres"
	_ "gitlab.momenta.works/kubetrain/seaweedfs/weed/filer2/redis"
	"gitlab.momenta.works/kubetrain/seaweedfs/weed/glog"
	"gitlab.momenta.works/kubetrain/seaweedfs/weed/notification"
	_ "gitlab.momenta.works/kubetrain/seaweedfs/weed/notification/aws_sqs"
	_ "gitlab.momenta.works/kubetrain/seaweedfs/weed/notification/gocdk_pub_sub"
	_ "gitlab.momenta.works/kubetrain/seaweedfs/weed/notification/google_pub_sub"
	_ "gitlab.momenta.works/kubetrain/seaweedfs/weed/notification/kafka"
	_ "gitlab.momenta.works/kubetrain/seaweedfs/weed/notification/log"
	"gitlab.momenta.works/kubetrain/seaweedfs/weed/security"
)

type FilerOption struct {
	Masters            []string
	Collection         string
	DefaultReplication string
	RedirectOnRead     bool
	DisableDirListing  bool
	MaxMB              int
	DirListingLimit    int
	DataCenter         string
	DefaultLevelDbDir  string
	DisableHttp        bool
}

type FilerServer struct {
	option         *FilerOption
	secret         security.SigningKey
	filer          *filer2.Filer
	grpcDialOption grpc.DialOption
}

func NewFilerServer(defaultMux, readonlyMux *http.ServeMux, option *FilerOption) (fs *FilerServer, err error) {

	fs = &FilerServer{
		option:         option,
		grpcDialOption: security.LoadClientTLS(viper.Sub("grpc"), "filer"),
	}

	if len(option.Masters) == 0 {
		glog.Fatal("master list is required!")
	}

	fs.filer = filer2.NewFiler(option.Masters, fs.grpcDialOption)

	go fs.filer.KeepConnectedToMaster()

	v := viper.GetViper()
	if !LoadConfiguration("filer", false) {
		v.Set("leveldb.enabled", true)
		v.Set("leveldb.dir", option.DefaultLevelDbDir)
		_, err := os.Stat(option.DefaultLevelDbDir)
		if os.IsNotExist(err) {
			os.MkdirAll(option.DefaultLevelDbDir, 0755)
		}
	}
	LoadConfiguration("notification", false)

	fs.filer.LoadConfiguration(v)

	notification.LoadConfiguration(v.Sub("notification"))

	handleStaticResources(defaultMux)
	if !option.DisableHttp {
		defaultMux.HandleFunc("/", fs.filerHandler)
	}
	if defaultMux != readonlyMux {
		readonlyMux.HandleFunc("/", fs.readonlyFilerHandler)
	}

	return fs, nil
}

func (fs *FilerServer) jwt(fileId string) security.EncodedJwt {
	return security.GenJwt(fs.secret, fileId)
}

func LoadConfiguration(configFileName string, required bool) (loaded bool) {

	// find a filer store
	viper.SetConfigName(configFileName)     // name of config file (without extension)
	viper.AddConfigPath(".")                // optionally look for config in the working directory
	viper.AddConfigPath("$HOME/.seaweedfs") // call multiple times to add many search paths
	viper.AddConfigPath("/etc/seaweedfs/")  // path to look for the config file in

	glog.V(1).Infof("Reading %s.toml from %s", configFileName, viper.ConfigFileUsed())

	if err := viper.MergeInConfig(); err != nil { // Handle errors reading the config file
		glog.V(1).Infof("Reading %s: %v", viper.ConfigFileUsed(), err)
		if required {
			glog.Fatalf("Failed to load %s.toml file from current directory, or $HOME/.seaweedfs/, or /etc/seaweedfs/"+
				"\n\nPlease follow this example and add a filer.toml file to "+
				"current directory, or $HOME/.seaweedfs/, or /etc/seaweedfs/:\n"+
				"    https://github.com//blob/master/weed/%s.toml\n"+
				"\nOr use this command to generate the default toml file\n"+
				"    weed scaffold -config=%s -output=.\n\n\n",
				configFileName, configFileName, configFileName)
		} else {
			return false
		}
	}

	return true

}
