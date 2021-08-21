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

// cdn server
package server

import (
	"context"
	"d7y.io/dragonfly/v2/pkg/compression"
	"fmt"
	"runtime"
	"time"

	"d7y.io/dragonfly/v2/cdnsystem/config"
	"d7y.io/dragonfly/v2/cdnsystem/daemon/cdn"
	"d7y.io/dragonfly/v2/cdnsystem/daemon/cdn/storage"
	"d7y.io/dragonfly/v2/cdnsystem/daemon/gc"
	"d7y.io/dragonfly/v2/cdnsystem/daemon/progress"
	"d7y.io/dragonfly/v2/cdnsystem/daemon/task"
	"d7y.io/dragonfly/v2/cdnsystem/plugins"
	"d7y.io/dragonfly/v2/cdnsystem/server/service"
	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/pkg/retry"
	"d7y.io/dragonfly/v2/pkg/rpc"
	"d7y.io/dragonfly/v2/pkg/rpc/cdnsystem/server"
	"d7y.io/dragonfly/v2/pkg/rpc/manager"
	"d7y.io/dragonfly/v2/pkg/util/net/iputils"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
)

type Server struct {
	config        *config.Config
	seedServer    server.SeederServer
	managerClient manager.ManagerClient
	managerConn   *grpc.ClientConn
}

// New creates a brand new server instance.
func New(cfg *config.Config) (*Server, error) {
	s := &Server{config: cfg}

	if ok := storage.IsSupport(cfg.StorageMode); !ok {
		return nil, fmt.Errorf("os %s is not support storage mode %s", runtime.GOOS, cfg.StorageMode)
	}
	if err := plugins.Initialize(cfg.Plugins); err != nil {
		return nil, err
	}

	// Progress manager
	progressMgr, err := progress.NewManager()
	if err != nil {
		return nil, errors.Wrapf(err, "create progress manager")
	}

	// Storage manager
	storageMgr, ok := storage.Get(cfg.StorageMode)
	if !ok {
		return nil, fmt.Errorf("can not find storage pattern %s", cfg.StorageMode)
	}
	// CDN manager
	cdnMgr, err := cdn.NewManager(cfg, storageMgr, progressMgr)
	if err != nil {
		return nil, errors.Wrapf(err, "create cdn manager")
	}
	// Task manager
	taskMgr, err := task.NewManager(cfg, cdnMgr, progressMgr)
	if err != nil {
		return nil, errors.Wrapf(err, "create task manager")
	}
	storageMgr.Initialize(taskMgr)
	// GC manager
	if err != nil {
		return nil, errors.Wrapf(err, "create gc manager")
	}
	// dynamic compress
	compressConfig := compression.CompressConfig{
		Ratio:          cfg.Compress.Ratio,
		Algorithm:      cfg.Compress.Algorithm,
		DetectChanSize: cfg.Compress.DetectChanSize,
		ConcurrentSize: cfg.Compress.ConcurrentSize,
	}
	defaultCompress := compression.NewDefaultCompressDetect(compressConfig)
	defaultCompress.Run()

	cdnSeedServer, err := service.NewCdnSeedServer(cfg, taskMgr)
	if err != nil {
		return nil, errors.Wrap(err, "create seedServer")
	}
	s.seedServer = cdnSeedServer

	// Manager client
	if cfg.Manager.Addr != "" {
		managerConn, err := grpc.Dial(
			cfg.Manager.Addr,
			grpc.WithInsecure(),
			grpc.WithBlock(),
		)
		if err != nil {
			logger.Errorf("did not connect: %v", err)
			return nil, err
		}
		s.managerClient = manager.NewManagerClient(managerConn)
		s.managerConn = managerConn

		// Register to manager
		if err := s.register(context.Background()); err != nil {
			return nil, err
		}
	}

	return s, nil
}

func (s *Server) Serve() (err error) {
	defer func() {
		if rec := recover(); rec != nil {
			err = errors.New(fmt.Sprintf("%v", rec))
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// Start gc
	err = gc.StartGC(ctx)
	if err != nil {
		return err
	}

	if s.managerClient != nil {
		go retry.Run(ctx, func() (interface{}, bool, error) {
			if err := s.keepAlive(ctx); err != nil {
				logger.Errorf("keepalive to manager failed %v", err)
				return nil, false, err
			}
			return nil, false, nil
		},
			s.config.Manager.KeepAlive.RetryInitBackOff,
			s.config.Manager.KeepAlive.RetryMaxBackOff,
			s.config.Manager.KeepAlive.RetryMaxAttempts,
			nil,
		)
	}

	err = rpc.StartTCPServer(s.config.ListenPort, s.config.ListenPort, s.seedServer)
	if err != nil {
		return errors.Wrap(err, "start tcp server")
	}
	return nil
}

func (s *Server) Stop() {
	s.managerConn.Close()
}

func (s *Server) register(ctx context.Context) error {
	ip := s.config.AdvertiseIP
	port := int32(s.config.ListenPort)
	idc := s.config.Host.IDC
	location := s.config.Host.Location
	downloadPort := int32(s.config.DownloadPort)
	cdnClusterID := uint64(s.config.Manager.CDNClusterID)

	cdn, err := s.managerClient.UpdateCDN(ctx, &manager.UpdateCDNRequest{
		SourceType:   manager.SourceType_CDN_SOURCE,
		HostName:     iputils.HostName,
		Ip:           ip,
		Port:         port,
		Idc:          idc,
		Location:     location,
		DownloadPort: downloadPort,
		CdnClusterId: cdnClusterID,
	})
	if err != nil {
		logger.Errorf("update cdn %s to manager failed %v", cdn.HostName, err)
		return err
	}
	logger.Infof("update cdn %s to manager successfully", cdn.HostName)
	return nil
}

func (s *Server) keepAlive(ctx context.Context) error {
	cdnClusterID := uint64(s.config.Manager.CDNClusterID)
	stream, err := s.managerClient.KeepAlive(ctx)
	if err != nil {
		logger.Errorf("create keepalive failed: %v\n", err)
		return err
	}

	tick := time.NewTicker(s.config.Manager.KeepAlive.Interval)
	hostName := iputils.HostName
	for {
		select {
		case <-tick.C:
			if err := stream.Send(&manager.KeepAliveRequest{
				HostName:   hostName,
				SourceType: manager.SourceType_CDN_SOURCE,
				ClusterId:  cdnClusterID,
			}); err != nil {
				logger.Errorf("%s send keepalive failed: %v\n", hostName, err)
				return err
			}
		}
	}
}
