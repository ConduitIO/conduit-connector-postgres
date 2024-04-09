// Code generated by paramgen. DO NOT EDIT.
// Source: github.com/ConduitIO/conduit-connector-sdk/tree/main/cmd/paramgen

package source

import (
	sdk "github.com/conduitio/conduit-connector-sdk"
)

func (Config) Parameters() map[string]sdk.Parameter {
	return map[string]sdk.Parameter{
		"cdcMode": {
			Default:     "auto",
			Description: "cdcMode determines how the connector should listen to changes.",
			Type:        sdk.ParameterTypeString,
			Validations: []sdk.Validation{
				sdk.ValidationInclusion{List: []string{"auto", "logrepl", "long_polling"}},
			},
		},
		"key": {
			Default:     "",
			Description: "key is a list of key column names per table, e.g.:\"table1:key1,table2:key2\", records should use the key values for their `key` fields.",
			Type:        sdk.ParameterTypeString,
			Validations: []sdk.Validation{},
		},
		"logrepl.publicationName": {
			Default:     "conduitpub",
			Description: "logrepl.publicationName determines the publication name in case the connector uses logical replication to listen to changes (see CDCMode).",
			Type:        sdk.ParameterTypeString,
			Validations: []sdk.Validation{},
		},
		"logrepl.slotName": {
			Default:     "conduitslot",
			Description: "logrepl.slotName determines the replication slot name in case the connector uses logical replication to listen to changes (see CDCMode).",
			Type:        sdk.ParameterTypeString,
			Validations: []sdk.Validation{},
		},
		"snapshotMode": {
			Default:     "initial",
			Description: "snapshotMode is whether the plugin will take a snapshot of the entire table before starting cdc mode.",
			Type:        sdk.ParameterTypeString,
			Validations: []sdk.Validation{
				sdk.ValidationInclusion{List: []string{"initial", "never"}},
			},
		},
		"table": {
			Default:     "",
			Description: "table is a List of table names to read from, separated by a comma, e.g.:\"table1,table2\". Use \"*\" if you'd like to listen to all tables.",
			Type:        sdk.ParameterTypeString,
			Validations: []sdk.Validation{
				sdk.ValidationRequired{},
			},
		},
		"url": {
			Default:     "",
			Description: "url is the connection string for the Postgres database.",
			Type:        sdk.ParameterTypeString,
			Validations: []sdk.Validation{
				sdk.ValidationRequired{},
			},
		},
	}
}
