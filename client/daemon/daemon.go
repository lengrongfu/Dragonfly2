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

package daemon

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/pkg/errors"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"golang.org/x/sync/errgroup"
	"golang.org/x/time/rate"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"d7y.io/dragonfly/v2/client/clientutil"
	"d7y.io/dragonfly/v2/client/config"
	"d7y.io/dragonfly/v2/client/daemon/gc"
	"d7y.io/dragonfly/v2/client/daemon/peer"
	"d7y.io/dragonfly/v2/client/daemon/proxy"
	"d7y.io/dragonfly/v2/client/daemon/rpcserver"
	"d7y.io/dragonfly/v2/client/daemon/storage"
	"d7y.io/dragonfly/v2/client/daemon/upload"
	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/internal/dfpath"
	"d7y.io/dragonfly/v2/internal/idgen"
	"d7y.io/dragonfly/v2/pkg/basic/dfnet"
	"d7y.io/dragonfly/v2/pkg/rpc"
	"d7y.io/dragonfly/v2/pkg/rpc/scheduler"
	schedulerclient "d7y.io/dragonfly/v2/pkg/rpc/scheduler/client"
	"d7y.io/dragonfly/v2/pkg/util/net/iputils"
)

type Daemon interface {
	Serve() error
	Stop()

	// ExportTaskManager returns the underlay peer.TaskManager for downloading when embed dragonfly in custom binary
	ExportTaskManager() peer.TaskManager
	// ExportPeerHost returns the underlay scheduler.PeerHost for scheduling
	ExportPeerHost() *scheduler.PeerHost
}

type clientDaemon struct {
	once *sync.Once
	done chan bool

	schedPeerHost *scheduler.PeerHost

	Option config.DaemonOption

	RPCManager     rpcserver.Server
	UploadManager  upload.Manager
	ProxyManager   proxy.Manager
	StorageManager storage.Manager
	GCManager      gc.Manager

	PeerTaskManager peer.TaskManager
	PieceManager    peer.PieceManager
}

var _ Daemon = (*clientDaemon)(nil)

func New(opt *config.DaemonOption) (Daemon, error) {
	host := &scheduler.PeerHost{
		Uuid:           idgen.UUIDString(),
		Ip:             opt.Host.AdvertiseIP,
		RpcPort:        int32(opt.Download.PeerGRPC.TCPListen.PortRange.Start),
		DownPort:       0,
		HostName:       iputils.HostName,
		SecurityDomain: opt.Host.SecurityDomain,
		Location:       opt.Host.Location,
		Idc:            opt.Host.IDC,
		NetTopology:    opt.Host.NetTopology,
	}

	var opts []grpc.DialOption
	if opt.Options.Telemetry.Jaeger != "" {
		opts = append(opts, grpc.WithChainUnaryInterceptor(otelgrpc.UnaryClientInterceptor()), grpc.WithChainStreamInterceptor(otelgrpc.StreamClientInterceptor()))
	}
	sched, err := schedulerclient.GetClientByAddr(opt.Scheduler.NetAddrs, opts...)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get schedulers")
	}

	// Storage.Option.DataPath is same with Daemon DataDir
	opt.Storage.DataPath = opt.DataDir
	storageManager, err := storage.NewStorageManager(opt.Storage.StoreStrategy, &opt.Storage,
		/* gc callback */
		func(request storage.CommonTaskRequest) {
			er := sched.LeaveTask(context.Background(), &scheduler.PeerTarget{
				TaskId: request.TaskID,
				PeerId: request.PeerID,
			})
			if er != nil {
				logger.Errorf("step 4:leave task %s/%s, error: %v", request.TaskID, request.PeerID, er)
			} else {
				logger.Infof("step 4:leave task %s/%s state ok", request.TaskID, request.PeerID)
			}
		})
	if err != nil {
		return nil, err
	}

	pieceManager, err := peer.NewPieceManager(storageManager,
		peer.WithLimiter(rate.NewLimiter(opt.Download.TotalRateLimit.Limit, int(opt.Download.TotalRateLimit.Limit))),
		peer.WithCalculateDigest(opt.Download.CalculateDigest))
	if err != nil {
		return nil, err
	}
	peerTaskManager, err := peer.NewPeerTaskManager(host, pieceManager, storageManager, sched, opt.Scheduler,
		opt.Download.PerPeerRateLimit.Limit, opt.Storage.Multiplex)
	if err != nil {
		return nil, err
	}

	// TODO(jim): more server options
	var downloadServerOption []grpc.ServerOption
	if !opt.Download.DownloadGRPC.Security.Insecure {
		tlsCredentials, err := loadGPRCTLSCredentials(opt.Download.DownloadGRPC.Security)
		if err != nil {
			return nil, err
		}
		downloadServerOption = append(downloadServerOption, grpc.Creds(tlsCredentials))
	}
	var peerServerOption []grpc.ServerOption
	if !opt.Download.PeerGRPC.Security.Insecure {
		tlsCredentials, err := loadGPRCTLSCredentials(opt.Download.PeerGRPC.Security)
		if err != nil {
			return nil, err
		}
		peerServerOption = append(peerServerOption, grpc.Creds(tlsCredentials))
	}
	rpcManager, err := rpcserver.NewServer(host, peerTaskManager, storageManager, downloadServerOption, peerServerOption)
	if err != nil {
		return nil, err
	}

	var proxyManager proxy.Manager
	proxyManager, err = proxy.NewProxyManager(host, peerTaskManager, opt.Proxy)
	if err != nil {
		return nil, err
	}

	uploadManager, err := upload.NewUploadManager(storageManager,
		upload.WithLimiter(rate.NewLimiter(opt.Upload.RateLimit.Limit, int(opt.Upload.RateLimit.Limit))))
	if err != nil {
		return nil, err
	}

	return &clientDaemon{
		once:          &sync.Once{},
		done:          make(chan bool),
		schedPeerHost: host,
		Option:        *opt,

		RPCManager:      rpcManager,
		PeerTaskManager: peerTaskManager,
		PieceManager:    pieceManager,
		ProxyManager:    proxyManager,
		UploadManager:   uploadManager,
		StorageManager:  storageManager,
		GCManager:       gc.NewManager(opt.GCInterval.Duration),
	}, nil
}

func loadGPRCTLSCredentials(opt config.SecurityOption) (credentials.TransportCredentials, error) {
	// Load certificate of the CA who signed client's certificate
	pemClientCA, err := ioutil.ReadFile(opt.CACert)
	if err != nil {
		return nil, err
	}

	certPool := x509.NewCertPool()
	if !certPool.AppendCertsFromPEM(pemClientCA) {
		return nil, fmt.Errorf("failed to add client CA's certificate")
	}

	// Load server's certificate and private key
	serverCert, err := tls.LoadX509KeyPair(opt.Cert, opt.Key)
	if err != nil {
		return nil, err
	}

	// Create the credentials and return it
	if opt.TLSConfig == nil {
		opt.TLSConfig = &tls.Config{
			Certificates: []tls.Certificate{serverCert},
			ClientAuth:   tls.RequireAndVerifyClientCert,
			ClientCAs:    certPool,
		}
	} else {
		opt.TLSConfig.Certificates = []tls.Certificate{serverCert}
		opt.TLSConfig.ClientAuth = tls.RequireAndVerifyClientCert
		opt.TLSConfig.ClientCAs = certPool
	}

	return credentials.NewTLS(opt.TLSConfig), nil
}

func (*clientDaemon) prepareTCPListener(opt config.ListenOption, withTLS bool) (net.Listener, int, error) {
	if len(opt.TCPListen.Namespace) > 0 {
		runtime.LockOSThread()
		defer runtime.UnlockOSThread()
		recoverFunc, err := switchNetNamespace(opt.TCPListen.Namespace)
		if err != nil {
			logger.Errorf("failed to change net namespace: %v", err)
			return nil, -1, err
		}
		defer func() {
			err := recoverFunc()
			if err != nil {
				logger.Errorf("failed to recover net namespace: %v", err)
			}
		}()
	}

	var (
		ln   net.Listener
		port int
		err  error
	)
	if opt.TCPListen != nil {
		ln, port, err = rpc.ListenWithPortRange(opt.TCPListen.Listen, opt.TCPListen.PortRange.Start, opt.TCPListen.PortRange.End)
	}
	if err != nil {
		return nil, -1, err
	}
	// when use grpc, tls config is in server option
	if !withTLS || opt.Security.Insecure {
		return ln, port, err
	}

	if opt.Security.Cert == "" || opt.Security.Key == "" {
		return nil, -1, errors.New("empty cert or key for tls")
	}

	// Create the TLS ClientOption with the CA pool and enable Client certificate validation
	if opt.Security.TLSConfig == nil {
		opt.Security.TLSConfig = &tls.Config{}
	}
	tlsConfig := opt.Security.TLSConfig
	if opt.Security.CACert != "" {
		caCert, err := ioutil.ReadFile(opt.Security.CACert)
		if err != nil {
			return nil, -1, err
		}
		caCertPool := x509.NewCertPool()
		caCertPool.AppendCertsFromPEM(caCert)
		tlsConfig.ClientCAs = caCertPool
		tlsConfig.ClientAuth = tls.RequireAndVerifyClientCert
	}
	tlsConfig.Certificates = make([]tls.Certificate, 1)
	tlsConfig.Certificates[0], err = tls.LoadX509KeyPair(opt.Security.Cert, opt.Security.Key)
	if err != nil {
		return nil, -1, err
	}

	return tls.NewListener(ln, tlsConfig), port, nil
}

func (cd *clientDaemon) Serve() error {
	cd.GCManager.Start()
	// TODO remove this field, and use directly dfpath.DaemonSockPath
	cd.Option.Download.DownloadGRPC.UnixListen.Socket = dfpath.DaemonSockPath
	// prepare download service listen
	if cd.Option.Download.DownloadGRPC.UnixListen == nil {
		return errors.New("download grpc unix listen option is empty")
	}
	_ = os.Remove(cd.Option.Download.DownloadGRPC.UnixListen.Socket)
	downloadListener, err := rpc.Listen(dfnet.NetAddr{
		Type: dfnet.UNIX,
		Addr: cd.Option.Download.DownloadGRPC.UnixListen.Socket,
	})
	if err != nil {
		logger.Errorf("failed to listen for download grpc service: %v", err)
		return err
	}

	// prepare peer service listen
	if cd.Option.Download.PeerGRPC.TCPListen == nil {
		return errors.New("peer grpc tcp listen option is empty")
	}
	peerListener, peerPort, err := cd.prepareTCPListener(cd.Option.Download.PeerGRPC, false)
	if err != nil {
		logger.Errorf("failed to listen for peer grpc service: %v", err)
		return err
	}
	cd.schedPeerHost.RpcPort = int32(peerPort)

	// prepare upload service listen
	if cd.Option.Upload.TCPListen == nil {
		return errors.New("upload tcp listen option is empty")
	}
	uploadListener, uploadPort, err := cd.prepareTCPListener(cd.Option.Upload.ListenOption, true)
	if err != nil {
		logger.Errorf("failed to listen for upload service: %v", err)
		return err
	}
	cd.schedPeerHost.DownPort = int32(uploadPort)

	g := errgroup.Group{}
	// serve download grpc service
	g.Go(func() error {
		defer downloadListener.Close()
		logger.Infof("serve download grpc at unix://%s", cd.Option.Download.DownloadGRPC.UnixListen.Socket)
		if err := cd.RPCManager.ServeDownload(downloadListener); err != nil {
			logger.Errorf("failed to serve for download grpc service: %v", err)
			return err
		}
		return nil
	})

	// serve peer grpc service
	g.Go(func() error {
		defer peerListener.Close()
		logger.Infof("serve peer grpc at %s://%s", peerListener.Addr().Network(), peerListener.Addr().String())
		if err := cd.RPCManager.ServePeer(peerListener); err != nil {
			logger.Errorf("failed to serve for peer grpc service: %v", err)
			return err
		}
		return nil
	})

	if cd.ProxyManager.IsEnabled() {
		// prepare proxy service listen
		if cd.Option.Proxy.TCPListen == nil {
			return errors.New("proxy tcp listen option is empty")
		}
		proxyListener, proxyPort, err := cd.prepareTCPListener(cd.Option.Proxy.ListenOption, true)
		if err != nil {
			logger.Errorf("failed to listen for proxy service: %v", err)
			return err
		}
		// serve proxy service
		g.Go(func() error {
			defer proxyListener.Close()
			logger.Infof("serve proxy at tcp://%s:%d", cd.Option.Proxy.TCPListen.Listen, proxyPort)
			if err = cd.ProxyManager.Serve(proxyListener); err != nil && err != http.ErrServerClosed {
				logger.Errorf("failed to serve for proxy service: %v", err)
				return err
			} else if err == http.ErrServerClosed {
				logger.Infof("proxy service closed")
			}
			return nil
		})
	}

	// serve upload service
	g.Go(func() error {
		defer uploadListener.Close()
		logger.Infof("serve upload service at %s://%s", uploadListener.Addr().Network(), uploadListener.Addr().String())
		if err := cd.UploadManager.Serve(uploadListener); err != nil && err != http.ErrServerClosed {
			logger.Errorf("failed to serve for upload service: %v", err)
			return err
		} else if err == http.ErrServerClosed {
			logger.Infof("upload service closed")
		}
		return nil
	})

	if cd.Option.AliveTime.Duration > 0 {
		g.Go(func() error {
			select {
			case <-time.After(cd.Option.AliveTime.Duration):
				var keepalives = []clientutil.KeepAlive{
					cd.StorageManager,
					cd.RPCManager,
				}
				var keep bool
				for _, keepalive := range keepalives {
					if keepalive.Alive(cd.Option.AliveTime.Duration) {
						keep = true
					}
				}
				if !keep {
					cd.Stop()
					logger.Infof("alive time reached, stop daemon")
				}
			case <-cd.done:
				logger.Infof("peer host done, stop watch alive time")
			}
			return nil
		})
	}

	werr := g.Wait()
	cd.Stop()
	return werr
}

func (cd *clientDaemon) Stop() {
	cd.once.Do(func() {
		close(cd.done)
		cd.GCManager.Stop()
		cd.RPCManager.Stop()
		cd.UploadManager.Stop()

		if cd.ProxyManager.IsEnabled() {
			cd.ProxyManager.Stop()
		}

		if !cd.Option.KeepStorage {
			logger.Infof("keep storage disabled")
			cd.StorageManager.CleanUp()
		}
	})
}

func (cd *clientDaemon) ExportTaskManager() peer.TaskManager {
	return cd.PeerTaskManager
}

func (cd *clientDaemon) ExportPeerHost() *scheduler.PeerHost {
	return cd.schedPeerHost
}
