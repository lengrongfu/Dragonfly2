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

// @Summary Create CDN
// @Description create by json config
// @Tags CDN
// @Accept json
// @Produce json
// @Param CDN body types.CreateCDNRequest true "CDN"
// @Success 200 {object} model.CDN
// @Failure 400 {object} HTTPError
// @Failure 404 {object} HTTPError
// @Failure 500 {object} HTTPError
// @Router /cdns [post]
func (h *Handlers) CreateCDN(ctx *gin.Context) {
	var json types.CreateCDNRequest
	if err := ctx.ShouldBindJSON(&json); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	cdn, err := h.Service.CreateCDN(json)
	if err != nil {
		ctx.Error(err)
		return
	}

	ctx.JSON(http.StatusOK, cdn)
}

// @Summary Destroy CDN
// @Description Destroy by id
// @Tags CDN
// @Accept json
// @Produce text
// @Param id path string true "id"
// @Success 200
// @Failure 400 {object} HTTPError
// @Failure 404 {object} HTTPError
// @Failure 500 {object} HTTPError
// @Router /cdns/{id} [delete]
func (h *Handlers) DestroyCDN(ctx *gin.Context) {
	var params types.CDNParams
	if err := ctx.ShouldBindUri(&params); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	err := h.Service.DestroyCDN(params.ID)
	if err != nil {
		ctx.Error(err)
		return
	}

	ctx.Status(http.StatusOK)
}

// @Summary Update CDN
// @Description Update by json config
// @Tags CDN
// @Accept json
// @Produce json
// @Param id path string true "id"
// @Param CDN body types.UpdateCDNRequest true "CDN"
// @Success 200 {object} model.CDN
// @Failure 400 {object} HTTPError
// @Failure 404 {object} HTTPError
// @Failure 500 {object} HTTPError
// @Router /cdns/{id} [patch]
func (h *Handlers) UpdateCDN(ctx *gin.Context) {
	var params types.CDNParams
	if err := ctx.ShouldBindUri(&params); err != nil {
		ctx.Error(err)
		return
	}

	var json types.UpdateCDNRequest
	if err := ctx.ShouldBindJSON(&json); err != nil {
		ctx.Error(err)
		return
	}

	cdn, err := h.Service.UpdateCDN(params.ID, json)
	if err != nil {
		ctx.Error(err)
		return
	}

	ctx.JSON(http.StatusOK, cdn)
}

// @Summary Get CDN
// @Description Get CDN by id
// @Tags CDN
// @Accept json
// @Produce json
// @Param id path string true "id"
// @Success 200 {object} model.CDN
// @Failure 400 {object} HTTPError
// @Failure 404 {object} HTTPError
// @Failure 500 {object} HTTPError
// @Router /cdns/{id} [get]
func (h *Handlers) GetCDN(ctx *gin.Context) {
	var params types.CDNParams
	if err := ctx.ShouldBindUri(&params); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	cdn, err := h.Service.GetCDN(params.ID)
	if err != nil {
		ctx.Error(err)
		return
	}

	ctx.JSON(http.StatusOK, cdn)
}

// @Summary Get CDNs
// @Description Get CDNs
// @Tags CDN
// @Accept json
// @Produce json
// @Param page query int true "current page" default(0)
// @Param per_page query int true "return max item count, default 10, max 50" default(10) minimum(2) maximum(50)
// @Success 200 {object} []model.CDN
// @Failure 400 {object} HTTPError
// @Failure 404 {object} HTTPError
// @Failure 500 {object} HTTPError
// @Router /cdns [get]
func (h *Handlers) GetCDNs(ctx *gin.Context) {
	var query types.GetCDNsQuery
	if err := ctx.ShouldBindQuery(&query); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	h.setPaginationDefault(&query.Page, &query.PerPage)
	cdns, err := h.Service.GetCDNs(query)
	if err != nil {
		ctx.Error(err)
		return
	}

	totalCount, err := h.Service.CDNTotalCount(query)
	if err != nil {
		ctx.Error(err)
		return
	}

	h.setPaginationLinkHeader(ctx, query.Page, query.PerPage, int(totalCount))
	ctx.JSON(http.StatusOK, cdns)
}
