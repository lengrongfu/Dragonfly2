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

// @Summary Create Preheat
// @Description create by json config
// @Tags Preheat
// @Accept json
// @Produce json
// @Param CDN body types.CreatePreheatRequest true "Preheat"
// @Success 200 {object} types.CreatePreheatResponse
// @Failure 400 {object} HTTPError
// @Failure 404 {object} HTTPError
// @Failure 500 {object} HTTPError
// @Router /preheats [post]
func (h *Handlers) CreatePreheat(ctx *gin.Context) {
	var json types.CreatePreheatRequest
	if err := ctx.ShouldBindJSON(&json); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	preheat, err := h.Service.CreatePreheat(json)
	if err != nil {
		ctx.Error(err)
		return
	}

	ctx.JSON(http.StatusOK, preheat)
}

// @Summary Get Preheat
// @Description Get Preheat by id
// @Tags Preheat
// @Accept json
// @Produce json
// @Param id path string true "id"
// @Success 200 {object} types.GetPreheatResponse
// @Failure 400 {object} HTTPError
// @Failure 404 {object} HTTPError
// @Failure 500 {object} HTTPError
// @Router /preheats/{id} [get]
func (h *Handlers) GetPreheat(ctx *gin.Context) {
	var params types.PreheatParams
	if err := ctx.ShouldBindUri(&params); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	preheat, err := h.Service.GetPreheat(params.ID)
	if err != nil {
		ctx.Error(err)
		return
	}

	ctx.JSON(http.StatusOK, preheat)
}
