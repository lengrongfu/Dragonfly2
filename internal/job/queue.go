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

package job

import (
	"fmt"

	"github.com/pkg/errors"
)

type Queue string

func GetSchedulerQueue(hostname string) (Queue, error) {
	if hostname == "" {
		return Queue(""), errors.New("empty hostname config is not specified")
	}

	return Queue(fmt.Sprintf("scheduler_%s", hostname)), nil
}

func GetCDNQueue(hostname string) (Queue, error) {
	if hostname == "" {
		return Queue(""), errors.New("empty hostname config is not specified")
	}

	return Queue(fmt.Sprintf("cdn_%s", hostname)), nil
}

func (q Queue) String() string {
	return string(q)
}
