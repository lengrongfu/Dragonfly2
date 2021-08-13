/*
 *     Copyright 2020 The Dragonfly Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cmd

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"syscall"
	"time"

	"d7y.io/dragonfly/v2/client/config"
	"d7y.io/dragonfly/v2/client/dfget"
	"d7y.io/dragonfly/v2/cmd/dependency"
	"d7y.io/dragonfly/v2/internal/constants"
	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/internal/dflog/logcore"
	"d7y.io/dragonfly/v2/internal/dfpath"
	"d7y.io/dragonfly/v2/pkg/basic"
	"d7y.io/dragonfly/v2/pkg/basic/dfnet"
	"d7y.io/dragonfly/v2/pkg/rpc/dfdaemon/client"
	"d7y.io/dragonfly/v2/pkg/unit"
	"d7y.io/dragonfly/v2/pkg/util/net/iputils"
	"d7y.io/dragonfly/v2/version"
	"github.com/gofrs/flock"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"gopkg.in/yaml.v3"
)

var (
	dfgetConfig *config.DfgetConfig
)

var dfgetDescription = `dfget is the client of dragonfly which takes a role of peer in a P2P network.
When user triggers a file downloading task, dfget will download the pieces of
file from other peers. Meanwhile, it will act as an uploader to support other
peers to download pieces from it if it owns them. In addition, dfget has the
abilities to provide more advanced functionality, such as network bandwidth
limit, transmission encryption and so on.`

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:                "dfget url -O path",
	Short:              "the P2P client of dragonfly",
	Long:               dfgetDescription,
	Args:               cobra.MaximumNArgs(1),
	DisableAutoGenTag:  true,
	FParseErrWhitelist: cobra.FParseErrWhitelist{UnknownFlags: true},
	RunE: func(cmd *cobra.Command, args []string) error {
		start := time.Now()

		if err := logcore.InitDfget(dfgetConfig.Console); err != nil {
			return errors.Wrap(err, "init client dfget logger")
		}

		// Convert config
		if err := dfgetConfig.Convert(args); err != nil {
			return err
		}

		// Validate config
		if err := dfgetConfig.Validate(); err != nil {
			return err
		}

		fmt.Printf("--%s--  %s\n", start.Format("2006-01-02 15:04:05"), dfgetConfig.URL)
		fmt.Printf("current user[%s] output path[%s]\n", basic.Username, dfgetConfig.Output)
		fmt.Printf("dfget version[%s] default peer ip[%s]\n", version.GitVersion, iputils.HostIP)

		//  do get file
		err := runDfget()

		msg := fmt.Sprintf("download success: %t cost: %dms error:[%v]", err == nil, time.Now().Sub(start).Milliseconds(), err)
		logger.With("url", dfgetConfig.URL).Info(msg)
		fmt.Println(msg)

		return errors.Wrapf(err, "download url[%s]", dfgetConfig.URL)
	},
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		logger.Error(err)
		os.Exit(1)
	}
}

func init() {
	// Initialize default dfget config
	dfgetConfig = config.NewDfgetConfig()
	// Initialize cobra
	dependency.InitCobra(rootCmd, false, dfgetConfig)

	// Add flags
	flagSet := rootCmd.Flags()

	flagSet.StringP("url", "u", dfgetConfig.URL,
		"Download one file from the url, equivalent to the command's first position argument")

	flagSet.StringP("output", "O", dfgetConfig.Output,
		"Destination path which is used to store the downloaded file, it must be a full path")

	flagSet.Duration("timeout", dfgetConfig.Timeout, "Timeout for the downloading task, 0 is infinite")

	flagSet.String("limit", unit.Bytes(dfgetConfig.RateLimit).String(),
		"The downloading network bandwidth limit per second in format of G(B)/g/M(B)/m/K(B)/k/B, pure number will be parsed as Byte, 0 is infinite")

	flagSet.String("digest", dfgetConfig.Digest,
		"Check the integrity of the downloaded file with digest, in format of md5:xxx or sha256:yyy")

	flagSet.String("tag", dfgetConfig.Tag,
		"Different tags for the same url will be divided into different P2P overlay, it conflicts with --digest")

	flagSet.String("filter", dfgetConfig.Filter,
		"Filter the query parameters of the url, P2P overlay is the same one if the filtered url is same, "+
			"in format of key&sign, which will filter 'key' and 'sign' query parameters")

	flagSet.StringArrayP("header", "H", dfgetConfig.Header, "url header, eg: --header='Accept: *' --header='Host: abc'")

	flagSet.Bool("disable-back-source", dfgetConfig.DisableBackSource,
		"Disable downloading directly from source when the daemon fails to download file")

	flagSet.StringP("pattern", "p", dfgetConfig.Pattern, "The downloading pattern: p2p/cdn/source")

	flagSet.BoolP("show-progress", "b", dfgetConfig.ShowProgress, "Show progress bar, it conflicts with --console")

	flagSet.String("callsystem", dfgetConfig.CallSystem, "The caller name which is mainly used for statistics and access control")

	// Bind cmd flags
	if err := viper.BindPFlags(flagSet); err != nil {
		panic(errors.Wrap(err, "bind dfget flags to viper"))
	}
}

// runDfget does some init operations and starts to download.
func runDfget() error {
	// Dfget config values
	s, _ := yaml.Marshal(dfgetConfig)
	logger.Infof("client dfget configuration:\n%s", string(s))

	ff := dependency.InitMonitor(dfgetConfig.Verbose, dfgetConfig.PProfPort, dfgetConfig.Telemetry)
	defer ff()

	var (
		daemonClient client.DaemonClient
		err          error
	)

	if dfgetConfig.Pattern != constants.SourcePattern {
		logger.Info("start to check and spawn daemon")
		if daemonClient, err = checkAndSpawnDaemon(); err != nil {
			logger.Errorf("check and spawn daemon error: %v", err)
		} else {
			logger.Info("check and spawn daemon success")
		}
	}

	return dfget.Download(dfgetConfig, daemonClient)
}

// checkAndSpawnDaemon do checking at three checkpoints
func checkAndSpawnDaemon() (client.DaemonClient, error) {
	target := dfnet.NetAddr{Type: dfnet.UNIX, Addr: dfpath.DaemonSockPath}
	daemonClient, err := client.GetClientByAddr([]dfnet.NetAddr{target})
	if err != nil {
		return nil, err
	}

	// 1.Check without lock
	if daemonClient.CheckHealth(context.Background(), target) == nil {
		return daemonClient, nil
	}

	lock := flock.New(dfpath.DfgetLockPath)
	lock.Lock()
	defer lock.Unlock()

	// 2.Check with lock
	if daemonClient.CheckHealth(context.Background(), target) == nil {
		return daemonClient, nil
	}

	cmd := exec.Command(os.Args[0], "daemon", "--launcher", strconv.Itoa(os.Getpid()))
	cmd.Stdin = nil
	cmd.Stdout = nil
	cmd.Stderr = nil
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}

	logger.Info("do start daemon")

	err = cmd.Start()
	if err != nil {
		return nil, err
	}

	// 3. check health with at least 5s timeout
	tick := time.NewTicker(50 * time.Millisecond)
	defer tick.Stop()
	timeout := time.After(5 * time.Second)

	for {
		select {
		case <-timeout:
			return nil, errors.New("the daemon is unhealthy")
		case <-tick.C:
			if err = daemonClient.CheckHealth(context.Background(), target); err != nil {
				continue
			}
			return daemonClient, nil
		}
	}
}
