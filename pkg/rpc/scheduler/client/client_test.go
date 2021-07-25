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

package client

import (
	"context"
	"reflect"
	"testing"
	"time"

	"d7y.io/dragonfly/v2/pkg/basic/dfnet"
	"d7y.io/dragonfly/v2/pkg/rpc"
	"d7y.io/dragonfly/v2/pkg/rpc/scheduler"
	"github.com/golang/mock/gomock"
	"google.golang.org/grpc"
)

func Test_schedulerClient_RegisterPeerTask(t *testing.T) {
	ctrl := gomock.NewController(t)
	scMock := NewMockSchedulerClient(ctrl)
	scMock.EXPECT()
	type fields struct {
		Connection *rpc.Connection
	}
	type args struct {
		ctx  context.Context
		ptr  *scheduler.PeerTaskRequest
		opts []grpc.CallOption
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantRr  *scheduler.RegisterResult
		wantErr bool
	}{
		{
			name: "register success",
			fields: fields{
				Connection: rpc.NewConnection(context.Background(), "scheduler-static", []dfnet.NetAddr{
					{
						Type: dfnet.TCP,
						Addr: "127.0.0.1",
					}, {
						Type: dfnet.TCP,
						Addr: "127.0.0.2",
					},
				}, []rpc.ConnOption{
					rpc.WithConnExpireTime(5 * time.Minute),
				}),
			},
			args: args{
				ctx: context.Background(),
				ptr: &scheduler.PeerTaskRequest{
					Url:         "",
					Filter:      "",
					BizId:       "",
					UrlMeta:     nil,
					PeerId:      "",
					PeerHost:    nil,
					HostLoad:    nil,
					IsMigrating: false,
				},
				opts: nil,
			},
			wantRr:  nil,
			wantErr: false,
		},
		//{
		//	name: "register fail",
		//	fields: fields{
		//		Connection: rpc.NewConnection(context.Background(), "scheduler-static", []dfnet.NetAddr{}, []rpc.ConnOption{
		//			rpc.WithConnExpireTime(5 * time.Minute),
		//		}),
		//	},
		//	args:    args{},
		//	wantRr:  nil,
		//	wantErr: false,
		//}, {
		//	name: "register tryMigrate",
		//	fields: fields{
		//		rpc.NewConnection(context.Background(), "scheduler-static", []dfnet.NetAddr{}, []rpc.ConnOption{
		//			rpc.WithConnExpireTime(5 * time.Minute),
		//		}),
		//	},
		//	args:    args{},
		//	wantRr:  nil,
		//	wantErr: false,
		//},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sc := &schedulerClient{
				Connection: tt.fields.Connection,
			}
			gotRr, err := sc.RegisterPeerTask(tt.args.ctx, tt.args.ptr, tt.args.opts...)
			if (err != nil) != tt.wantErr {
				t.Errorf("RegisterPeerTask() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(gotRr, tt.wantRr) {
				t.Errorf("RegisterPeerTask() gotRr = %v, want %v", gotRr, tt.wantRr)
			}
		})
	}
}

func Test_schedulerClient_ReportPeerResult(t *testing.T) {
	type fields struct {
		Connection *rpc.Connection
	}
	type args struct {
		ctx  context.Context
		pr   *scheduler.PeerResult
		opts []grpc.CallOption
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sc := &schedulerClient{
				Connection: tt.fields.Connection,
			}
			if err := sc.ReportPeerResult(tt.args.ctx, tt.args.pr, tt.args.opts...); (err != nil) != tt.wantErr {
				t.Errorf("ReportPeerResult() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_schedulerClient_ReportPieceResult(t *testing.T) {
	type fields struct {
		Connection *rpc.Connection
	}
	type args struct {
		ctx    context.Context
		taskID string
		ptr    *scheduler.PeerTaskRequest
		opts   []grpc.CallOption
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    PeerPacketStream
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sc := &schedulerClient{
				Connection: tt.fields.Connection,
			}
			got, err := sc.ReportPieceResult(tt.args.ctx, tt.args.taskID, tt.args.ptr, tt.args.opts...)
			if (err != nil) != tt.wantErr {
				t.Errorf("ReportPieceResult() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ReportPieceResult() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_schedulerClient_LeaveTask(t *testing.T) {
	type fields struct {
		Connection *rpc.Connection
	}
	type args struct {
		ctx  context.Context
		pt   *scheduler.PeerTarget
		opts []grpc.CallOption
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sc := &schedulerClient{
				Connection: tt.fields.Connection,
			}
			if err := sc.LeaveTask(tt.args.ctx, tt.args.pt, tt.args.opts...); (err != nil) != tt.wantErr {
				t.Errorf("LeaveTask() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
