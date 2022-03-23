// Copyright © 2022 Meroxa, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package source

import (
	"fmt"
	"strings"
)

const (
	ConfigKeyURL                    = "url"
	ConfigKeyTable                  = "table"
	ConfigKeyColumns                = "columns"
	ConfigKeyKey                    = "key"
	ConfigKeySnapshotMode           = "snapshot_mode"
	ConfigKeyCDCMode                = "cdc_mode"
	ConfigKeyLogreplPublicationName = "publication_name"
	ConfigKeyLogreplSlotName        = "slot_name"

	DefaultPublicationName = "conduitpub"
	DefaultSlotName        = "conduitslot"
)

type Config struct {
	URL     string
	Table   string
	Columns []string
	Key     string

	// SnapshotMode determines if and when a snapshot is made.
	SnapshotMode SnapshotMode
	// CDCMode determines how the connector should listen to changes.
	CDCMode CDCMode

	// LogreplPublicationName determines the publication name in case the
	// connector uses logical replication to listen to changes (see CDCMode).
	LogreplPublicationName string
	// LogreplSlotName determines the replication slot name in case the
	// connector uses logical replication to listen to changes (see CDCMode).
	LogreplSlotName string
}

type SnapshotMode string

const (
	SnapshotModeInitial SnapshotMode = "initial"
	SnapshotModeNever   SnapshotMode = "never"
)

type CDCMode string

const (
	CDCModeAuto        CDCMode = "auto"
	CDCModeLogrepl     CDCMode = "logrepl"
	CDCModeLongPolling CDCMode = "long_polling"
)

var snapshotModeAll = []SnapshotMode{SnapshotModeInitial, SnapshotModeNever}
var cdcModeAll = []CDCMode{CDCModeAuto, CDCModeLogrepl, CDCModeLongPolling}

func ParseConfig(cfgRaw map[string]string) (Config, error) {
	cfg := Config{
		URL:                    cfgRaw[ConfigKeyURL],
		Table:                  cfgRaw[ConfigKeyTable],
		Columns:                nil, // default
		Key:                    cfgRaw[ConfigKeyKey],
		SnapshotMode:           SnapshotModeInitial,
		CDCMode:                CDCModeAuto,
		LogreplPublicationName: DefaultPublicationName,
		LogreplSlotName:        DefaultSlotName,
	}

	if cfg.URL == "" {
		return Config{}, requiredConfigErr(ConfigKeyURL)
	}
	if cfg.Table == "" {
		return Config{}, requiredConfigErr(ConfigKeyTable)
	}
	if colsRaw := cfgRaw[ConfigKeyColumns]; colsRaw != "" {
		cfg.Columns = strings.Split(colsRaw, ",")
	}
	if modeRaw := cfgRaw[ConfigKeySnapshotMode]; modeRaw != "" {
		if !isSnapshotModeSupported(modeRaw) {
			return Config{}, fmt.Errorf("%q contains unsupported value %q, expected one of %v", ConfigKeySnapshotMode, modeRaw, snapshotModeAll)
		}
		cfg.SnapshotMode = SnapshotMode(modeRaw)
	}
	if modeRaw := cfgRaw[ConfigKeyCDCMode]; modeRaw != "" {
		if !isCDCModeSupported(modeRaw) {
			return Config{}, fmt.Errorf("%q contains unsupported value %q, expected one of %v", ConfigKeyCDCMode, modeRaw, cdcModeAll)
		}
		cfg.CDCMode = CDCMode(modeRaw)
	}
	if cfgRaw[ConfigKeyLogreplPublicationName] != "" {
		cfg.LogreplPublicationName = cfgRaw[ConfigKeyLogreplPublicationName]
	}
	if cfgRaw[ConfigKeyLogreplSlotName] != "" {
		cfg.LogreplSlotName = cfgRaw[ConfigKeyLogreplSlotName]
	}

	return cfg, nil
}

func isSnapshotModeSupported(modeRaw string) bool {
	for _, m := range snapshotModeAll {
		if string(m) == modeRaw {
			return true
		}
	}
	return false
}

func isCDCModeSupported(modeRaw string) bool {
	for _, m := range cdcModeAll {
		if string(m) == modeRaw {
			return true
		}
	}
	return false
}

func requiredConfigErr(name string) error {
	return fmt.Errorf("%q config value must be set", name)
}
