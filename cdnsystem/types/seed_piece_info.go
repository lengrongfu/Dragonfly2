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

import (
	"d7y.io/dragonfly/v2/pkg/util/rangeutils"
)

// SeedPiece
type SeedPiece struct {
	PieceStyle  PieceFormat // 0: PlainUnspecified
	PieceNum    int32
	PieceMd5    string
	PieceRange  *rangeutils.Range
	OriginRange *rangeutils.Range
	PieceLen    int32
}

type PieceFormat int8

const (
	PlainUnspecified PieceFormat = 0

	GzipCompressAlgorithm PieceFormat = 10
)
