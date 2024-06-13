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

package postgres

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/conduitio/conduit-commons/csync"
	schema2 "github.com/conduitio/conduit-commons/schema"
	"github.com/conduitio/conduit-connector-postgres/source"
	"github.com/conduitio/conduit-connector-postgres/source/logrepl"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/hamba/avro/v2"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Source is a Postgres source plugin.
type Source struct {
	sdk.UnimplementedSource

	iterator      source.Iterator
	config        source.Config
	pool          *pgxpool.Pool
	tableKeys     map[string]string
	createdSchema schema2.Instance
}

func NewSource() sdk.Source {
	return sdk.SourceWithMiddleware(
		&Source{
			tableKeys: make(map[string]string),
		},
		sdk.DefaultSourceMiddleware()...,
	)
}

func (s *Source) Parameters() map[string]sdk.Parameter {
	return s.config.Parameters()
}

func (s *Source) Configure(_ context.Context, cfg map[string]string) error {
	err := sdk.Util.ParseConfig(cfg, &s.config)
	if err != nil {
		return err
	}

	s.config = s.config.Init()

	return s.config.Validate()
}

func (s *Source) Open(ctx context.Context, pos sdk.Position) error {
	pool, err := pgxpool.New(ctx, s.config.URL)
	if err != nil {
		return fmt.Errorf("failed to create a connection pool to database: %w", err)
	}
	s.pool = pool

	logger := sdk.Logger(ctx)
	if s.readingAllTables() {
		logger.Info().Msg("Detecting all tables...")
		s.config.Tables, err = s.getAllTables(ctx)
		if err != nil {
			return fmt.Errorf("failed to connect to get all tables: %w", err)
		}
		logger.Info().
			Strs("tables", s.config.Tables).
			Int("count", len(s.config.Tables)).
			Msg("Successfully detected tables")
	}

	// ensure we have keys for all tables
	for _, tableName := range s.config.Tables {
		s.tableKeys[tableName], err = s.getPrimaryKey(ctx, tableName)
		if err != nil {
			return fmt.Errorf("failed to find primary key for table %s: %w", tableName, err)
		}
	}

	switch s.config.CDCMode {
	case source.CDCModeAuto:
		// TODO add logic that checks if the DB supports logical replication (since that's the only thing we support at the moment)
		fallthrough
	case source.CDCModeLogrepl:
		i, err := logrepl.NewCombinedIterator(ctx, s.pool, logrepl.Config{
			Position:        pos,
			SlotName:        s.config.LogreplSlotName,
			PublicationName: s.config.LogreplPublicationName,
			Tables:          s.config.Tables,
			TableKeys:       s.tableKeys,
			WithSnapshot:    s.config.SnapshotMode == source.SnapshotModeInitial,
		})
		if err != nil {
			return fmt.Errorf("failed to create logical replication iterator: %w", err)
		}
		s.iterator = i
	default:
		// shouldn't happen, config was validated
		return fmt.Errorf("unsupported CDC mode %q", s.config.CDCMode)
	}

	s.fetchSchema(ctx)
	go func() {
		for {
			time.Sleep(10 * time.Second)
			s.fetchSchema(ctx)
		}
	}()

	return nil
}

func (s *Source) Read(ctx context.Context) (sdk.Record, error) {
	rec, err := s.iterator.Next(ctx)
	if err == nil {
		rec.Metadata["opencdc.schema.name"] = s.createdSchema.Name
		rec.Metadata["opencdc.schema.version"] = strconv.Itoa(s.createdSchema.Version)
	}
	return rec, err
}

func (s *Source) Ack(ctx context.Context, pos sdk.Position) error {
	return s.iterator.Ack(ctx, pos)
}

func (s *Source) Teardown(ctx context.Context) error {
	logger := sdk.Logger(ctx)

	var errs []error
	if s.iterator != nil {
		logger.Debug().Msg("Tearing down iterator...")
		if err := s.iterator.Teardown(ctx); err != nil {
			logger.Warn().Err(err).Msg("Failed to tear down iterator")
			errs = append(errs, fmt.Errorf("failed to tear down iterator: %w", err))
		}
	}
	if s.pool != nil {
		logger.Debug().Msg("Closing connection pool...")
		err := csync.RunTimeout(ctx, s.pool.Close, time.Minute)
		if err != nil {
			errs = append(errs, fmt.Errorf("failed to close DB connection pool: %w", err))
		}
	}
	return errors.Join(errs...)
}

func (s *Source) LifecycleOnDeleted(ctx context.Context, _ map[string]string) error {
	switch s.config.CDCMode {
	case source.CDCModeAuto:
		fallthrough // TODO: Adjust as `auto` changes.
	case source.CDCModeLogrepl:
		if !s.config.LogreplAutoCleanup {
			sdk.Logger(ctx).Warn().Msg("Skipping logrepl auto cleanup")
			return nil
		}

		return logrepl.Cleanup(ctx, logrepl.CleanupConfig{
			URL:             s.config.URL,
			SlotName:        s.config.LogreplSlotName,
			PublicationName: s.config.LogreplPublicationName,
		})
	default:
		return nil
	}
}

func (s *Source) readingAllTables() bool {
	return len(s.config.Tables) == 1 && s.config.Tables[0] == source.AllTablesWildcard
}

func (s *Source) getAllTables(ctx context.Context) ([]string, error) {
	query := "SELECT tablename FROM pg_tables WHERE schemaname = 'public'"

	rows, err := s.pool.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return nil, fmt.Errorf("failed to scan table name: %w", err)
		}
		tables = append(tables, tableName)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows error: %w", err)
	}
	return tables, nil
}

// getPrimaryKey queries the db for the name of the primary key column for a
// table if one exists and returns it.
func (s *Source) getPrimaryKey(ctx context.Context, tableName string) (string, error) {
	query := `SELECT a.attname FROM pg_index i
			JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
			WHERE  i.indrelid = $1::regclass AND i.indisprimary`

	rows, err := s.pool.Query(ctx, query, tableName)
	if err != nil {
		return "", fmt.Errorf("failed to query table keys: %w", err)
	}
	defer rows.Close()

	if !rows.Next() {
		if rows.Err() != nil {
			return "", fmt.Errorf("query failed: %w", rows.Err())
		}
		return "", fmt.Errorf("no table keys found: %w", pgx.ErrNoRows)
	}

	var colName string
	err = rows.Scan(&colName)
	if err != nil {
		return "", fmt.Errorf("failed to scan row: %w", err)
	}

	if rows.Next() {
		// we only support single column primary keys for now
		return "", errors.New("composite keys are not supported")
	}

	return colName, nil
}

func (s *Source) getTableSchema(ctx context.Context) ([]byte, error) {
	// Table name
	tableName := "employees"

	// Query to get column names and data types
	query := `
		SELECT column_name, data_type
		FROM information_schema.columns
		WHERE table_name = $1
	`

	rows, err := s.pool.Query(ctx, query, tableName)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	// Map to store column names and their corresponding Avro types
	columns := make(map[string]avro.Schema)

	for rows.Next() {
		var columnName, dataType string
		if err := rows.Scan(&columnName, &dataType); err != nil {
			log.Fatal(err)
		}

		// Map PostgreSQL data types to Avro data types
		avroType, err := postgresToAvroType(dataType)
		if err != nil {
			log.Fatalf("Unsupported data type: %s", dataType)
		}
		columns[columnName] = avroType
	}

	// Handle any error encountered during iteration
	if err := rows.Err(); err != nil {
		log.Fatal(err)
	}

	// Create Avro schema
	schema, err := createAvroSchema(tableName, columns)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(schema)

	return []byte(schema), nil
}

func (s *Source) fetchSchema(ctx context.Context) {
	sdk.Logger(ctx).Info().Msg("fetching table schema")

	tableSchema, err := s.getTableSchema(ctx)
	if err != nil {
		panic(err)
	}

	schemas, err := sdk.NewSchemaService(ctx)
	if err != nil {
		panic(fmt.Errorf("failed acquiring schema service: %w", err))
	}

	s.createdSchema, err = schemas.Create(ctx, "employees", tableSchema)
	if err != nil {
		panic(err)
	}
}

// Function to map PostgreSQL data types to Avro data types
func postgresToAvroType(pgType string) (avro.Schema, error) {
	switch pgType {
	case "integer", "serial":
		return avro.NewPrimitiveSchema(avro.Int, nil), nil
	case "bigint", "bigserial":
		return avro.NewPrimitiveSchema(avro.Long, nil), nil
	case "real":
		return avro.NewPrimitiveSchema(avro.Float, nil), nil
	case "double precision":
		return avro.NewPrimitiveSchema(avro.Double, nil), nil
	case "boolean":
		return avro.NewPrimitiveSchema(avro.Boolean, nil), nil
	case "character varying", "text", "varchar", "char", "uuid":
		return avro.NewPrimitiveSchema(avro.String, nil), nil
	default:
		return nil, fmt.Errorf("unsupported PostgreSQL type: %s", pgType)
	}
}

// Function to create Avro schema from column definitions
func createAvroSchema(tableName string, columns map[string]avro.Schema) (string, error) {
	fields := make([]*avro.Field, 0, len(columns))

	for name, avroType := range columns {
		field, err := avro.NewField(name, avroType)
		if err != nil {
			return "", err
		}

		fields = append(fields, field)
	}

	recordSchema, err := avro.NewRecordSchema(
		tableName,
		tableName+"_namespace",
		fields,
	)
	if err != nil {
		return "", err
	}

	schema, err := avro.Parse(recordSchema.String())
	if err != nil {
		return "", err
	}

	return schema.String(), nil
}
