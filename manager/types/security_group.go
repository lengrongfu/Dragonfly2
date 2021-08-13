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

type SecurityGroupParams struct {
	ID uint `uri:"id" binding:"required"`
}

type AddSchedulerClusterToSecurityGroupParams struct {
	ID                 uint `uri:"id" binding:"required"`
	SchedulerClusterID uint `uri:"scheduler_cluster_id" binding:"required"`
}

type AddCDNClusterToSecurityGroupParams struct {
	ID           uint `uri:"id" binding:"required"`
	CDNClusterID uint `uri:"cdn_cluster_id" binding:"required"`
}

type CreateSecurityGroupRequest struct {
	Name        string `json:"name" binding:"required"`
	BIO         string `json:"bio" binding:"omitempty"`
	Domain      string `json:"domain" binding:"required"`
	ProxyDomain string `json:"proxy_domain" binding:"omitempty"`
}

type UpdateSecurityGroupRequest struct {
	Name        string `json:"name" binding:"omitempty"`
	BIO         string `json:"bio" binding:"omitempty"`
	Domain      string `json:"domain" binding:"omitempty"`
	ProxyDomain string `json:"proxy_domain" binding:"omitempty"`
}

type GetSecurityGroupsQuery struct {
	Page    int    `form:"page" binding:"omitempty,gte=1"`
	PerPage int    `form:"per_page" binding:"omitempty,gte=1,lte=50"`
	Name    string `form:"name" binding:"omitempty"`
	Domain  string `form:"domain" binding:"omitempty"`
}
