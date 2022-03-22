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
	ConfigKeyURL             = "url"
	ConfigKeyTable           = "table"
	ConfigKeyColumns         = "columns"
	ConfigKeyKey             = "key"
	ConfigKeyMode            = "mode"
	ConfigKeyPublicationName = "publication_name"
	ConfigKeySlotName        = "slot_name"

	DefaultPublicationName = "conduitpub"
	DefaultSlotName        = "conduitslot"
)

type Config struct {
	URL             string
	Table           string
	Columns         []string
	Key             string
	Mode            Mode
	PublicationName string
	SlotName        string
}

type Mode string

const (
	ModeFull     Mode = "full"
	ModeSnapshot Mode = "snapshot"
	ModeCDC      Mode = "cdc"
)

var modeAll = []Mode{ModeFull, ModeSnapshot, ModeCDC}

func ParseConfig(cfgRaw map[string]string) (Config, error) {
	cfg := Config{
		URL:             cfgRaw[ConfigKeyURL],
		Table:           cfgRaw[ConfigKeyTable],
		Columns:         nil, // default
		Key:             cfgRaw[ConfigKeyKey],
		Mode:            ModeFull, // default
		PublicationName: cfgRaw[ConfigKeyPublicationName],
		SlotName:        cfgRaw[ConfigKeySlotName],
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
	if modeRaw := cfgRaw[ConfigKeyMode]; modeRaw != "" {
		if !isModeSupported(modeRaw) {
			return Config{}, fmt.Errorf("%q contains unsupported value %q, expected one of %v", ConfigKeyMode, modeRaw, modeAll)
		}
		cfg.Mode = Mode(modeRaw)
	}
	if cfg.PublicationName == "" {
		cfg.PublicationName = DefaultPublicationName
	}
	if cfg.SlotName == "" {
		cfg.SlotName = DefaultSlotName
	}

	return cfg, nil
}

func isModeSupported(modeRaw string) bool {
	for _, m := range modeAll {
		if string(m) == modeRaw {
			return true
		}
	}
	return false
}

func requiredConfigErr(name string) error {
	return fmt.Errorf("%q config value must be set", name)
}
