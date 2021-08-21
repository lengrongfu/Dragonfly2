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
//go:generate mockgen -destination ./mock/mock_storage_mgr.go -package mock d7y.io/dragonfly/v2/cdnsystem/daemon/cdn/storage Manager

package storage

import (
	"fmt"
	"io"
	"reflect"
	"strconv"
	"strings"
	"time"

	"d7y.io/dragonfly/v2/cdnsystem/daemon"
	"d7y.io/dragonfly/v2/cdnsystem/plugins"
	"d7y.io/dragonfly/v2/cdnsystem/storedriver"
	"d7y.io/dragonfly/v2/cdnsystem/types"
	"d7y.io/dragonfly/v2/pkg/unit"
	"d7y.io/dragonfly/v2/pkg/util/rangeutils"
	"github.com/mitchellh/mapstructure"
	"github.com/pkg/errors"
	"gopkg.in/yaml.v3"
)

type Manager interface {
	Initialize(taskMgr daemon.SeedTaskMgr)

	// ResetRepo reset the storage of task
	ResetRepo(*types.SeedTask) error

	// StatDownloadFile
	StatDownloadFile(taskID string) (*storedriver.StorageInfo, error)

	// WriteDownloadFile
	WriteDownloadFile(taskID string, offset int64, len int64, data io.Reader) error

	// ReadDownloadFile
	ReadDownloadFile(taskID string) (io.ReadCloser, error)

	// CreateUploadLink
	CreateUploadLink(taskID string) error

	// ReadFileMetaData
	ReadFileMetaData(taskID string) (*FileMetaData, error)

	// WriteFileMetaData
	WriteFileMetaData(taskID string, meta *FileMetaData) error

	// WritePieceMetaRecords
	WritePieceMetaRecords(taskID string, metaRecords []*PieceMetaRecord) error

	// AppendPieceMetaData
	AppendPieceMetaData(taskID string, metaRecord *PieceMetaRecord) error

	// ReadPieceMetaRecords
	ReadPieceMetaRecords(taskID string) ([]*PieceMetaRecord, error)

	// DeleteTask
	DeleteTask(taskID string) error
}

// FileMetaData
type FileMetaData struct {
	TaskID           string            `json:"taskId"`
	TaskURL          string            `json:"taskUrl"`
	PieceSize        int32             `json:"pieceSize"`
	SourceFileLen    int64             `json:"sourceFileLen"`
	AccessTime       int64             `json:"accessTime"`
	Interval         int64             `json:"interval"`
	CdnFileLength    int64             `json:"cdnFileLength"`
	SourceRealDigest string            `json:"sourceRealDigest"`
	PieceMd5Sign     string            `json:"pieceMd5Sign"`
	ExpireInfo       map[string]string `json:"expireInfo"`
	Finish           bool              `json:"finish"`
	Success          bool              `json:"success"`
	TotalPieceCount  int32             `json:"totalPieceCount"`
	//PieceMetaDataSign string            `json:"pieceMetaDataSign"`
}

// pieceMetaRecord
type PieceMetaRecord struct {
	PieceNum    int32             `json:"pieceNum"`    // piece Num start from 0
	PieceLen    int32             `json:"pieceLen"`    // 存储到存储介质的真实长度
	Md5         string            `json:"md5"`         // for transported piece content，不是origin source 的 md5，是真是存储到存储介质后的md5（为了读取数据文件时方便校验完整性）
	Range       *rangeutils.Range `json:"range"`       // 下载存储到磁盘的range，不是origin source的range.提供给客户端发送下载请求,for transported piece content
	OriginRange *rangeutils.Range `json:"originRange"` //  piece's real offset in the file
	PieceStyle  types.PieceFormat `json:"pieceStyle"`  // 1: PlainUnspecified
}

const fieldSeparator = ":"

func (record PieceMetaRecord) String() string {
	return fmt.Sprint(record.PieceNum, fieldSeparator, record.PieceLen, fieldSeparator, record.Md5, fieldSeparator, record.Range, fieldSeparator,
		record.OriginRange, fieldSeparator, record.PieceStyle)
}

func ParsePieceMetaRecord(value string) (record *PieceMetaRecord, err error) {
	defer func() {
		if msg := recover(); msg != nil {
			err = errors.Errorf("%v", msg)
		}
	}()
	fields := strings.Split(value, fieldSeparator)
	pieceNum, err := strconv.ParseInt(fields[0], 10, 32)
	if err != nil {
		return nil, errors.Wrapf(err, "invalid pieceNum: %s", fields[0])
	}
	pieceLen, err := strconv.ParseInt(fields[1], 10, 32)
	if err != nil {
		return nil, errors.Wrapf(err, "invalid pieceLen: %s", fields[1])
	}
	md5 := fields[2]
	pieceRange, err := rangeutils.ParseRange(fields[3])
	if err != nil {
		return nil, errors.Wrapf(err, "invalid piece range: %s", fields[3])
	}
	originRange, err := rangeutils.ParseRange(fields[4])
	if err != nil {
		return nil, errors.Wrapf(err, "invalid origin range: %s", fields[4])
	}
	pieceStyle, err := strconv.ParseInt(fields[5], 10, 8)
	if err != nil {
		return nil, errors.Wrapf(err, "invalid pieceStyle: %s", fields[5])
	}
	return &PieceMetaRecord{
		PieceNum:    int32(pieceNum),
		PieceLen:    int32(pieceLen),
		Md5:         md5,
		Range:       pieceRange,
		OriginRange: originRange,
		PieceStyle:  types.PieceFormat(pieceStyle),
	}, nil
}

type managerPlugin struct {
	// name is a unique identifier, you can also name it ID.
	name string
	// instance holds a manger instant which implements the interface of Manager.
	instance Manager
}

func (m *managerPlugin) Type() plugins.PluginType {
	return plugins.StorageManagerPlugin
}

func (m *managerPlugin) Name() string {
	return m.name
}

func (m *managerPlugin) ResetRepo(task *types.SeedTask) error {
	return m.instance.ResetRepo(task)
}

func (m *managerPlugin) StatDownloadFile(path string) (*storedriver.StorageInfo, error) {
	return m.instance.StatDownloadFile(path)
}

func (m *managerPlugin) WriteDownloadFile(taskID string, offset int64, len int64, data io.Reader) error {
	return m.instance.WriteDownloadFile(taskID, offset, len, data)
}

func (m *managerPlugin) ReadDownloadFile(taskID string) (io.ReadCloser, error) {
	return m.instance.ReadDownloadFile(taskID)
}

func (m *managerPlugin) CreateUploadLink(taskID string) error {
	return m.instance.CreateUploadLink(taskID)
}

func (m *managerPlugin) ReadFileMetaData(taskID string) (*FileMetaData, error) {
	return m.instance.ReadFileMetaData(taskID)
}

func (m *managerPlugin) WriteFileMetaData(taskID string, data *FileMetaData) error {
	return m.instance.WriteFileMetaData(taskID, data)
}

func (m *managerPlugin) WritePieceMetaRecords(taskID string, records []*PieceMetaRecord) error {
	return m.instance.WritePieceMetaRecords(taskID, records)
}

func (m *managerPlugin) AppendPieceMetaData(taskID string, record *PieceMetaRecord) error {
	return m.instance.AppendPieceMetaData(taskID, record)
}

func (m *managerPlugin) ReadPieceMetaRecords(taskID string) ([]*PieceMetaRecord, error) {
	return m.instance.ReadPieceMetaRecords(taskID)
}

func (m *managerPlugin) DeleteTask(taskID string) error {
	return m.instance.DeleteTask(taskID)
}

// ManagerBuilder is a function that creates a new storage manager plugin instant with the giving conf.
type ManagerBuilder func(cfg *Config) (Manager, error)

// Register defines an interface to register a storage manager with specified name.
// All storage managers should call this function to register itself to the storage manager factory.
func Register(name string, builder ManagerBuilder) error {
	name = strings.ToLower(name)
	// plugin builder
	var f = func(conf interface{}) (plugins.Plugin, error) {
		cfg := &Config{}
		decoder, err := mapstructure.NewDecoder(&mapstructure.DecoderConfig{
			DecodeHook: mapstructure.ComposeDecodeHookFunc(func(from, to reflect.Type, v interface{}) (interface{}, error) {
				switch to {
				case reflect.TypeOf(unit.B),
					reflect.TypeOf(time.Second):
					b, _ := yaml.Marshal(v)
					p := reflect.New(to)
					if err := yaml.Unmarshal(b, p.Interface()); err != nil {
						return nil, err
					}
					return p.Interface(), nil
				default:
					return v, nil
				}
			}),
			Result: cfg,
		})
		if err != nil {
			return nil, fmt.Errorf("create decoder: %v", err)
		}
		err = decoder.Decode(conf)
		if err != nil {
			return nil, fmt.Errorf("parse config: %v", err)
		}
		return newManagerPlugin(name, builder, cfg)
	}
	return plugins.RegisterPluginBuilder(plugins.StorageManagerPlugin, name, f)
}

func newManagerPlugin(name string, builder ManagerBuilder, cfg *Config) (plugins.Plugin, error) {
	if name == "" || builder == nil {
		return nil, fmt.Errorf("storage manager plugin's name and builder cannot be nil")
	}

	instant, err := builder(cfg)
	if err != nil {
		return nil, fmt.Errorf("init storage manager %s: %v", name, err)
	}

	return &managerPlugin{
		name:     name,
		instance: instant,
	}, nil
}

// Get a storage manager from manager with specified name.
func Get(name string) (Manager, bool) {
	v, ok := plugins.GetPlugin(plugins.StorageManagerPlugin, strings.ToLower(name))
	if !ok {
		return nil, false
	}
	return v.(*managerPlugin).instance, true
}

type Config struct {
	GCInitialDelay time.Duration            `yaml:"gcInitialDelay"`
	GCInterval     time.Duration            `yaml:"gcInterval"`
	DriverConfigs  map[string]*DriverConfig `yaml:"driverConfigs"`
}

type DriverConfig struct {
	GCConfig *GCConfig `yaml:"gcConfig"`
}

// GcConfig
type GCConfig struct {
	YoungGCThreshold  unit.Bytes    `yaml:"youngGCThreshold"`
	FullGCThreshold   unit.Bytes    `yaml:"fullGCThreshold"`
	CleanRatio        int           `yaml:"cleanRatio"`
	IntervalThreshold time.Duration `yaml:"intervalThreshold"`
}

const (
	HybridStorageMode = "hybrid"
	DiskStorageMode   = "disk"
)
