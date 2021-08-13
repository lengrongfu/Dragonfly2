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

package types

type SchedulerParams struct {
	ID uint `uri:"id" binding:"required"`
}

type CreateSchedulerRequest struct {
	HostName           string                 `json:"host_name" binding:"required"`
	VIPs               string                 `json:"vips" binding:"omitempty"`
	IDC                string                 `json:"idc" binding:"required"`
	Location           string                 `json:"location" binding:"omitempty"`
	NetConfig          map[string]interface{} `json:"net_config" binding:"omitempty"`
	IP                 string                 `json:"ip" binding:"required"`
	Port               int32                  `json:"port" binding:"required"`
	SchedulerClusterID uint                   `json:"scheduler_cluster_id" binding:"required"`
}

type UpdateSchedulerRequest struct {
	VIPs               string                 `json:"vips" binding:"omitempty"`
	IDC                string                 `json:"idc" binding:"omitempty"`
	Location           string                 `json:"location" binding:"omitempty"`
	NetConfig          map[string]interface{} `json:"net_config" binding:"omitempty"`
	IP                 string                 `json:"ip" binding:"omitempty"`
	Port               int32                  `json:"port" binding:"omitempty"`
	SchedulerID        uint                   `json:"scheduler_id" binding:"omitempty"`
	SchedulerClusterID uint                   `json:"scheduler_cluster_id" binding:"omitempty"`
}

type GetSchedulersQuery struct {
	Page               int    `form:"page" binding:"omitempty,gte=1"`
	PerPage            int    `form:"per_page" binding:"omitempty,gte=1,lte=50"`
	HostName           string `form:"host_name" binding:"omitempty"`
	IDC                string `form:"idc" binding:"omitempty"`
	Location           string `form:"location" binding:"omitempty"`
	IP                 string `form:"ip" binding:"omitempty"`
	Status             string `form:"status" binding:"omitempty,oneof=active inactive"`
	SchedulerClusterID uint   `json:"scheduler_cluster_id" binding:"required"`
}
