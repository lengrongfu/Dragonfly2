/*
 *     Copyright 2022 The Dragonfly Authors
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

package rpcserver

import (
	"context"
	"fmt"
	"math"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"d7y.io/dragonfly/v2/client/clientutil"
	"d7y.io/dragonfly/v2/client/config"
	"d7y.io/dragonfly/v2/client/daemon/peer"
	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/pkg/idgen"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
	"d7y.io/dragonfly/v2/pkg/rpc/base/common"
	"d7y.io/dragonfly/v2/pkg/rpc/cdnsystem"
	"d7y.io/dragonfly/v2/pkg/rpc/scheduler"
	"d7y.io/dragonfly/v2/pkg/util/rangeutils"
)

type seeder struct {
	server *server
}

func (s *seeder) GetPieceTasks(ctx context.Context, request *base.PieceTaskRequest) (*base.PiecePacket, error) {
	return s.server.GetPieceTasks(ctx, request)
}

func (s *seeder) SyncPieceTasks(tasksServer cdnsystem.Seeder_SyncPieceTasksServer) error {
	return s.server.SyncPieceTasks(tasksServer)
}

func (s *seeder) ObtainSeeds(seedRequest *cdnsystem.SeedRequest, seedsServer cdnsystem.Seeder_ObtainSeedsServer) error {
	s.server.Keep()
	if seedRequest.UrlMeta == nil {
		seedRequest.UrlMeta = &base.UrlMeta{}
	}

	req := peer.SeedTaskRequest{
		PeerTaskRequest: scheduler.PeerTaskRequest{
			Url:         seedRequest.Url,
			UrlMeta:     seedRequest.UrlMeta,
			PeerId:      idgen.SeedPeerID(s.server.peerHost.Ip),
			PeerHost:    s.server.peerHost,
			HostLoad:    nil,
			IsMigrating: false,
		},
		Limit:      0,
		Callsystem: "",
		Range:      nil, // following code will update Range
	}

	log := logger.With("peer", req.PeerId, "task", seedRequest.TaskId, "component", "seedService")

	if len(req.UrlMeta.Range) > 0 {
		r, err := rangeutils.ParseRange(req.UrlMeta.Range, math.MaxInt)
		if err != nil {
			err = fmt.Errorf("parse range %s error: %s", req.UrlMeta.Range, err)
			log.Errorf(err.Error())
			return err
		}
		req.Range = &clientutil.Range{
			Start:  int64(r.StartIndex),
			Length: int64(r.Length()),
		}
	}

	resp, err := s.server.peerTaskManager.StartSeedTask(seedsServer.Context(), &req)
	if err != nil {
		log.Errorf("start seed task error: %s", err.Error())
		return err
	}

	if resp.SubscribeResponse.Storage == nil {
		err = fmt.Errorf("invalid SubscribeResponse.Storage")
		log.Errorf("%s", err.Error())
		return err
	}

	if resp.SubscribeResponse.Success == nil && resp.SubscribeResponse.Fail == nil {
		err = fmt.Errorf("both of SubscribeResponse.Success and SubscribeResponse.Fail is nil")
		log.Errorf("%s", err.Error())
		return err
	}

	log.Infof("start seed task")

	err = seedsServer.Send(
		&cdnsystem.PieceSeed{
			PeerId: req.PeerId,
			HostId: req.PeerHost.Id,
			PieceInfo: &base.PieceInfo{
				PieceNum: common.BeginOfPiece,
			},
			Done: false,
		})
	if err != nil {
		resp.Span.RecordError(err)
		log.Errorf("send piece seed error: %s", err.Error())
		return err
	}

	sync := seedSynchronizer{
		SeedTaskResponse:    resp,
		SugaredLoggerOnWith: log,
		seedsServer:         seedsServer,
		seedTaskRequest:     &req,
		startNanoSecond:     time.Now().UnixNano(),
	}
	defer resp.Span.End()

	return sync.sendPieceSeeds()
}

type seedSynchronizer struct {
	*peer.SeedTaskResponse
	*logger.SugaredLoggerOnWith
	seedsServer     cdnsystem.Seeder_ObtainSeedsServer
	seedTaskRequest *peer.SeedTaskRequest
	startNanoSecond int64
	attributeSent   bool
}

func (s *seedSynchronizer) sendPieceSeeds() (err error) {
	var (
		ctx     = s.Context
		desired int32
	)
	for {
		select {
		case <-ctx.Done():
			err = ctx.Err()
			s.Errorf("context done due to %s", err.Error())
			s.Span.RecordError(err)
			s.Span.SetAttributes(config.AttributeSeedTaskSuccess.Bool(false))
			return err
		case <-s.Success:
			s.Infof("seed task success, send reminding piece seeds")
			err = s.sendRemindingPieceSeeds(desired)
			if err != nil {
				s.Span.RecordError(err)
				s.Span.SetAttributes(config.AttributeSeedTaskSuccess.Bool(false))
			} else {
				s.Span.SetAttributes(config.AttributeSeedTaskSuccess.Bool(true))
			}
			return err
		case <-s.Fail:
			s.Error("seed task failed")
			s.Span.RecordError(err)
			s.Span.SetAttributes(config.AttributeSeedTaskSuccess.Bool(false))
			return status.Errorf(codes.Internal, "seed task failed")
		case p := <-s.PieceInfoChannel:
			s.Infof("receive piece info, num: %d, ordered num: %d, finish: %v", p.Num, p.OrderedNum, p.Finished)
			desired, err = s.sendOrderedPieceSeeds(desired, p.OrderedNum, p.Finished)
			if err != nil {
				s.Span.RecordError(err)
				s.Span.SetAttributes(config.AttributeSeedTaskSuccess.Bool(false))
				return err
			}
			if p.Finished {
				s.Debugf("send piece seeds finished")
				s.Span.SetAttributes(config.AttributeSeedTaskSuccess.Bool(true))
				return nil
			}
		}
	}
}

func (s *seedSynchronizer) sendRemindingPieceSeeds(desired int32) error {
	for {
		pp, err := s.Storage.GetPieces(s.Context,
			&base.PieceTaskRequest{
				TaskId:   s.TaskID,
				StartNum: uint32(desired),
				Limit:    16,
			})
		if err != nil {
			s.Errorf("get pieces error %s, desired: %d", err.Error(), desired)
			return err
		}
		if !s.attributeSent {
			exa, err := s.Storage.GetExtendAttribute(s.Context, nil)
			if err != nil {
				s.Errorf("get extend attribute error: %s", err.Error())
				return err
			}
			pp.ExtendAttribute = exa
			s.attributeSent = true
		}

		// we must send done to scheduler
		if len(pp.PieceInfos) == 0 {
			ps := s.compositePieceSeed(pp, nil)
			ps.Done, ps.EndTime = true, uint64(time.Now().UnixNano())
			s.Infof("seed tasks start time: %d, end time: %d, cost: %dms", ps.BeginTime, ps.EndTime, (ps.EndTime-ps.BeginTime)/1000000)
			err = s.seedsServer.Send(&ps)
			if err != nil {
				s.Errorf("send reminding piece seeds error: %s", err.Error())
				return err
			}
		}

		for _, p := range pp.PieceInfos {
			if p.PieceNum != desired {
				s.Errorf("desired piece %d, not found", desired)
				return status.Errorf(codes.Internal, "seed task piece %d not found", desired)
			}
			ps := s.compositePieceSeed(pp, p)
			if p.PieceNum == pp.TotalPiece-1 {
				ps.Done, ps.EndTime = true, uint64(time.Now().UnixNano())
				s.Infof("seed tasks start time: %d, end time: %d, cost: %dms", ps.BeginTime, ps.EndTime, (ps.EndTime-ps.BeginTime)/1000000)
			}

			err = s.seedsServer.Send(&ps)
			if err != nil {
				s.Errorf("send reminding piece seeds error: %s", err.Error())
				return err
			}

			s.Span.AddEvent(fmt.Sprintf("send piece %d ok", desired))
			desired++
		}
		if desired == pp.TotalPiece {
			s.Debugf("send reminding piece seeds ok")
			return nil
		}
	}
}

func (s *seedSynchronizer) sendOrderedPieceSeeds(desired, orderedNum int32, finished bool) (int32, error) {
	cur := desired
	for ; cur <= orderedNum; cur++ {
		pp, err := s.Storage.GetPieces(s.Context,
			&base.PieceTaskRequest{
				TaskId:   s.TaskID,
				StartNum: uint32(cur),
				Limit:    1,
			})
		if err != nil {
			s.Errorf("get pieces error %s, desired: %d", err.Error(), cur)
			return -1, err
		}
		if len(pp.PieceInfos) < 1 {
			s.Errorf("desired pieces %d not found", cur)
			return -1, fmt.Errorf("get seed piece %d info failed", cur)
		}
		if !s.attributeSent {
			exa, err := s.Storage.GetExtendAttribute(s.Context, nil)
			if err != nil {
				s.Errorf("get extend attribute error: %s", err.Error())
				return -1, err
			}
			pp.ExtendAttribute = exa
			s.attributeSent = true
		}

		ps := s.compositePieceSeed(pp, pp.PieceInfos[0])
		if cur == orderedNum && finished {
			ps.Done, ps.EndTime = true, uint64(time.Now().UnixNano())
			s.Infof("seed tasks start time: %d, end time: %d, cost: %dms", ps.BeginTime, ps.EndTime, (ps.EndTime-ps.BeginTime)/1000000)
		}
		err = s.seedsServer.Send(&ps)
		if err != nil {
			s.Errorf("send ordered piece seeds error: %s", err.Error())
			return -1, err
		}
		s.Debugf("send piece %d seeds ok", cur)
		s.Span.AddEvent(fmt.Sprintf("send piece %d ok", cur))
	}
	return cur, nil
}

func (s *seedSynchronizer) compositePieceSeed(pp *base.PiecePacket, piece *base.PieceInfo) cdnsystem.PieceSeed {
	return cdnsystem.PieceSeed{
		PeerId:          s.seedTaskRequest.PeerId,
		HostId:          s.seedTaskRequest.PeerHost.Id,
		PieceInfo:       piece,
		ContentLength:   pp.ContentLength,
		TotalPieceCount: pp.TotalPiece,
		BeginTime:       uint64(s.startNanoSecond),
	}
}