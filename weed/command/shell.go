package command

import (
	"github.com/spf13/viper"
	"gitlab.momenta.works/kubetrain/seaweedfs/weed/security"
	"gitlab.momenta.works/kubetrain/seaweedfs/weed/server"
	"gitlab.momenta.works/kubetrain/seaweedfs/weed/shell"
)

var (
	shellOptions shell.ShellOptions
)

func init() {
	cmdShell.Run = runShell // break init cycle
	shellOptions.Masters = cmdShell.Flag.String("master", "localhost:9333", "comma-separated master servers")
}

var cmdShell = &Command{
	UsageLine: "shell",
	Short:     "run interactive administrative commands",
	Long: `run interactive administrative commands.

  `,
}

func runShell(command *Command, args []string) bool {

	weed_server.LoadConfiguration("security", false)
	shellOptions.GrpcDialOption = security.LoadClientTLS(viper.Sub("grpc"), "client")

	shellOptions.FilerHost = "localhost"
	shellOptions.FilerPort = 8888
	shellOptions.Directory = "/"

	shell.RunShell(shellOptions)

	return true

}
