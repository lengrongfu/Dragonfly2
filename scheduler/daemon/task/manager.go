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

package task

import (
	"sync"
	"time"

	"d7y.io/dragonfly/v2/scheduler/config"
	"d7y.io/dragonfly/v2/scheduler/daemon"
	"d7y.io/dragonfly/v2/scheduler/types"
)

type manager struct {
	peerManager              daemon.PeerMgr
	cleanupExpiredTaskTicker *time.Ticker
	taskTTL                  time.Duration
	taskTTI                  time.Duration
	taskMap                  sync.Map
}

func NewManager(cfg *config.GCConfig, peerManager daemon.PeerMgr) daemon.TaskMgr {
	m := &manager{
		peerManager:              peerManager,
		cleanupExpiredTaskTicker: time.NewTicker(cfg.TaskGCInterval),
		taskTTL:                  cfg.TaskTTL,
		taskTTI:                  cfg.TaskTTI,
	}
	go m.cleanupTasks()
	return m
}

var _ daemon.TaskMgr = (*manager)(nil)

func (m *manager) Delete(taskID string) {
	m.taskMap.Delete(taskID)
}

func (m *manager) Add(task *types.Task) {
	m.taskMap.Store(task.TaskID, task)
}

func (m *manager) Get(taskID string) (task *types.Task, ok bool) {
	item, ok := m.taskMap.Load(taskID)
	if !ok {
		return nil, false
	}
	return item.(*types.Task), true
}

func (m *manager) GetOrAdd(task *types.Task) (actual *types.Task, loaded bool) {
	item, loaded := m.taskMap.LoadOrStore(task.TaskID, task)
	if loaded {
		return item.(*types.Task), true
	}
	return task, false
}

func (m *manager) cleanupTasks() {
	for range m.cleanupExpiredTaskTicker.C {
		m.taskMap.Range(func(key, value interface{}) bool {
			task := value.(*types.Task)
			elapse := time.Since(task.GetLastAccessTime())
			if elapse > m.taskTTI && task.IsSuccess() {
				task.SetStatus(types.TaskStatusZombie)
			}
			if elapse > m.taskTTL {
				taskID := key.(string)
				// TODO lock
				m.Delete(taskID)
				peers := m.peerManager.ListPeersByTask(taskID)
				for _, peer := range peers {
					m.peerManager.Delete(peer.PeerID)
				}
			}
			return true
		})
	}
}
