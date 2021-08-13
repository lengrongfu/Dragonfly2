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

package handlers

import (
	"net/http"

	"d7y.io/dragonfly/v2/manager/types"
	"github.com/gin-gonic/gin"
)

// @Summary Get PermissionGroups
// @Description Get PermissionGroups
// @Tags permission
// @Produce json
// @Success 200 {object} RoutesInfo
// @Failure 400 {object} HTTPError
// @Failure 500 {object} HTTPError
// @Router /permission/groups [get]

func (h *Handlers) GetPermissionGroups(g *gin.Engine) func(ctx *gin.Context) {
	return func(ctx *gin.Context) {

		permissionGroups := h.Service.GetPermissionGroups(g)

		ctx.JSON(http.StatusOK, permissionGroups)
	}
}

// @Summary Get User Roles
// @Description Get User Roles
// @Tags permission
// @Produce json
// @Success 200 {object} RoutesInfo
// @Failure 400 {object} HTTPError
// @Failure 500 {object} HTTPError
// @Router /permission/roles/{subject} [get]

func (h *Handlers) GetRolesForUser(ctx *gin.Context) {
	var params types.UserRolesParams
	if err := ctx.ShouldBindUri(&params); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}
	roles, err := h.Service.GetRolesForUser(params.Subject)
	if err != nil {
		ctx.Error(err)
		return
	}
	ctx.JSON(http.StatusOK, gin.H{"roles": roles})

}

// @Summary Judge User Role
// @Description Judge User Role
// @Tags permission
// @Produce json
// @Success 200 {object}
// @Failure 400 {object} HTTPError
// @Failure 500 {object} HTTPError
// @Router /permission/{subject}/{object}/{action} [get]

func (h *Handlers) HasRoleForUser(ctx *gin.Context) {
	var params types.UserHasRoleParams
	if err := ctx.ShouldBindUri(&params); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}
	if params.Subject == "admin" {
		ctx.JSON(http.StatusOK, gin.H{"has": true})
		return
	}
	has, err := h.Service.HasRoleForUser(params.Subject, params.Object, params.Action)
	if err != nil {
		ctx.Error(err)
		return
	}
	ctx.JSON(http.StatusOK, gin.H{"has": has})
}

// @Summary Create Permission
// @Description Create Permission by json config
// @Tags permission
// @Accept json
// @Produce json
// @Success 200
// @Failure 400 {object} HTTPError
// @Failure 500 {object} HTTPError
// @Router /permission [post]

func (h *Handlers) CreatePermission(ctx *gin.Context) {
	var json types.PolicyRequest
	if err := ctx.ShouldBindJSON(&json); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}
	err := h.Service.CreatePermission(json)
	if err != nil {
		ctx.Error(err)
		return
	}
	ctx.Status(http.StatusOK)
}

// @Summary Destroy Permission
// @Description Destroy Permission by json config
// @Tags permission
// @Accept json
// @Produce json
// @Success 200
// @Failure 400 {object} HTTPError
// @Failure 500 {object} HTTPError
// @Router /permission [delete]

func (h *Handlers) DestroyPermission(ctx *gin.Context) {
	var json types.PolicyRequest
	if err := ctx.ShouldBindJSON(&json); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}
	err := h.Service.DestroyPermission(json)
	if err != nil {
		ctx.Error(err)
		return
	}
	ctx.Status(http.StatusOK)
}
