// Code generated by paramgen. DO NOT EDIT.
// Source: github.com/ConduitIO/conduit-commons/tree/main/paramgen

package destination

import (
	"github.com/conduitio/conduit-commons/config"
)

func (Config) Parameters() map[string]config.Parameter {
	return map[string]config.Parameter{
		"key": {
			Default:     "",
			Description: "Key represents the column name for the key used to identify and update existing rows.",
			Type:        config.ParameterTypeString,
			Validations: []config.Validation{},
		},
		"table": {
			Default:     "{{ index .Metadata \"opencdc.collection\" }}",
			Description: "Table is used as the target table into which records are inserted.",
			Type:        config.ParameterTypeString,
			Validations: []config.Validation{},
		},
		"url": {
			Default:     "",
			Description: "URL is the connection string for the Postgres database.",
			Type:        config.ParameterTypeString,
			Validations: []config.Validation{
				config.ValidationRequired{},
			},
		},
	}
}
