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

package model

type SecurityGroup struct {
	Model
	Name              string             `gorm:"column:name;type:varchar(256);index:uk_security_group_name,unique;not null;comment:name" json:"name"`
	BIO               string             `gorm:"column:bio;type:varchar(1024);comment:biography" json:"bio"`
	Domain            string             `gorm:"column:domain;type:varchar(256);index:uk_security_group_domain,unique;not null;comment:domain" json:"domain"`
	ProxyDomain       string             `gorm:"column:proxy_domain;type:varchar(1024);comment:proxy domain" json:"proxy_domain"`
	SchedulerClusters []SchedulerCluster `json:"-"`
	CDNClusters       []CDNCluster       `json:"-"`
}
