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

package service

import (
	"d7y.io/dragonfly/v2/manager/model"
	"d7y.io/dragonfly/v2/manager/types"
)

func (s *rest) CreateSchedulerCluster(json types.CreateSchedulerClusterRequest) (*model.SchedulerCluster, error) {
	schedulerCluster := model.SchedulerCluster{
		Name:         json.Name,
		BIO:          json.BIO,
		Config:       json.Config,
		ClientConfig: json.ClientConfig,
		Scopes:       json.Scopes,
		IsDefault:    json.IsDefault,
	}

	if err := s.db.Create(&schedulerCluster).Error; err != nil {
		return nil, err
	}

	return &schedulerCluster, nil
}

func (s *rest) CreateSchedulerClusterWithSecurityGroupDomain(json types.CreateSchedulerClusterRequest) (*model.SchedulerCluster, error) {
	securityGroup := model.SecurityGroup{
		Domain: json.SecurityGroupDomain,
	}
	if err := s.db.First(&securityGroup).Error; err != nil {
		return s.CreateSchedulerCluster(json)
	}

	schedulerCluster := model.SchedulerCluster{
		Name:         json.Name,
		BIO:          json.BIO,
		Config:       json.Config,
		ClientConfig: json.ClientConfig,
		Scopes:       json.Scopes,
		IsDefault:    json.IsDefault,
	}

	if err := s.db.Model(&securityGroup).Association("SchedulerClusters").Append(&schedulerCluster); err != nil {
		return nil, err
	}

	return &schedulerCluster, nil
}

func (s *rest) DestroySchedulerCluster(id uint) error {
	if err := s.db.Unscoped().Delete(&model.SchedulerCluster{}, id).Error; err != nil {
		return err
	}

	return nil
}

func (s *rest) UpdateSchedulerCluster(id uint, json types.UpdateSchedulerClusterRequest) (*model.SchedulerCluster, error) {
	schedulerCluster := model.SchedulerCluster{}
	if err := s.db.First(&schedulerCluster, id).Updates(model.SchedulerCluster{
		Name:         json.Name,
		BIO:          json.BIO,
		Config:       json.Config,
		ClientConfig: json.ClientConfig,
		Scopes:       json.Scopes,
		IsDefault:    json.IsDefault,
	}).Error; err != nil {
		return nil, err
	}

	return &schedulerCluster, nil
}

func (s *rest) UpdateSchedulerClusterWithSecurityGroupDomain(id uint, json types.UpdateSchedulerClusterRequest) (*model.SchedulerCluster, error) {
	securityGroup := model.SecurityGroup{
		Domain: json.SecurityGroupDomain,
	}
	if err := s.db.First(&securityGroup).Error; err != nil {
		return s.UpdateSchedulerCluster(id, json)
	}

	schedulerCluster := model.SchedulerCluster{
		Name:         json.Name,
		BIO:          json.BIO,
		Config:       json.Config,
		ClientConfig: json.ClientConfig,
		Scopes:       json.Scopes,
		IsDefault:    json.IsDefault,
	}

	if err := s.db.Model(&securityGroup).Association("SchedulerClusters").Append(&schedulerCluster); err != nil {
		return nil, err
	}

	return &schedulerCluster, nil
}

func (s *rest) GetSchedulerCluster(id uint) (*model.SchedulerCluster, error) {
	schedulerCluster := model.SchedulerCluster{}
	if err := s.db.First(&schedulerCluster, id).Error; err != nil {
		return nil, err
	}

	return &schedulerCluster, nil
}

func (s *rest) GetSchedulerClusters(q types.GetSchedulerClustersQuery) (*[]model.SchedulerCluster, error) {
	schedulerClusters := []model.SchedulerCluster{}
	if err := s.db.Scopes(model.Paginate(q.Page, q.PerPage)).Where(&model.SchedulerCluster{
		Name: q.Name,
	}).Find(&schedulerClusters).Error; err != nil {
		return nil, err
	}

	return &schedulerClusters, nil
}

func (s *rest) SchedulerClusterTotalCount(q types.GetSchedulerClustersQuery) (int64, error) {
	var count int64
	if err := s.db.Model(&model.SchedulerCluster{}).Where(&model.SchedulerCluster{
		Name: q.Name,
	}).Count(&count).Error; err != nil {
		return 0, err
	}

	return count, nil
}

func (s *rest) AddSchedulerToSchedulerCluster(id, schedulerID uint) error {
	schedulerCluster := model.SchedulerCluster{}
	if err := s.db.First(&schedulerCluster, id).Error; err != nil {
		return err
	}

	scheduler := model.Scheduler{}
	if err := s.db.First(&scheduler, schedulerID).Error; err != nil {
		return err
	}

	if err := s.db.Model(&schedulerCluster).Association("Schedulers").Append(&scheduler); err != nil {
		return err
	}

	return nil
}
