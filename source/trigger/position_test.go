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

package trigger

import (
	"errors"
	"reflect"
	"testing"
	"time"

	sdk "github.com/conduitio/conduit-connector-sdk"
)

func TestParseSDKPosition(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   sdk.Position
		want Position
		err  error
	}{
		{
			name: "success_position_is_nil",
			in:   nil,
			want: Position{
				Mode:      ModeSnapshot,
				CreatedAt: time.Now().Format("150405"),
			},
		},
		{
			name: "success_mode_snapshot",
			in: sdk.Position(`{
			   "mode":"snapshot"
			}`),
			want: Position{
				Mode: ModeSnapshot,
			},
		},
		{
			name: "success_mode_cdc",
			in: sdk.Position(`{
			   "mode":"cdc"
			}`),
			want: Position{
				Mode: ModeCDC,
			},
		},
		{
			name: "success_lastProcessedVal_is_float64",
			in: sdk.Position(`{
			   "lastProcessedVal":10
			}`),
			want: Position{
				LastProcessedVal: float64(10),
			},
		},
		{
			name: "success_lastProcessedVal_is_string",
			in: sdk.Position(`{
			   "lastProcessedVal":"abc"
			}`),
			want: Position{
				LastProcessedVal: "abc",
			},
		},
		{
			name: "success_createdAt",
			in: sdk.Position(`{
			   "createdAt":"131415"
			}`),
			want: Position{
				CreatedAt: "131415",
			},
		},
		{
			name: "failure_invalid_position",
			in:   sdk.Position("invalid"),
			err: errors.New("unmarshal sdk.Position into Position: " +
				"invalid character 'i' looking for beginning of value"),
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := ParseSDKPosition(tt.in)
			if err != nil {
				if tt.err == nil {
					t.Errorf("unexpected error: %s", err.Error())

					return
				}

				if err.Error() != tt.err.Error() {
					t.Errorf("unexpected error, got: %s, want: %s", err.Error(), tt.err.Error())

					return
				}

				return
			}

			if got == nil {
				return
			}

			if !reflect.DeepEqual(*got, tt.want) {
				t.Errorf("got: %v, want: %v", *got, tt.want)
			}
		})
	}
}

func TestMarshal(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   Position
		want sdk.Position
	}{
		{
			name: "success_position_is_nil",
			in:   Position{},
			want: sdk.Position(`{"mode":"","lastProcessedVal":null,"createdAt":""}`),
		},
		{
			name: "success_mode_snapshot",
			in: Position{
				Mode: ModeSnapshot,
			},
			want: sdk.Position(`{"mode":"snapshot","lastProcessedVal":null,"createdAt":""}`),
		},
		{
			name: "success_mode_cdc",
			in: Position{
				Mode: ModeCDC,
			},
			want: sdk.Position(`{"mode":"cdc","lastProcessedVal":null,"createdAt":""}`),
		},
		{
			name: "success_lastProcessedVal_is_integer",
			in: Position{
				LastProcessedVal: 10,
			},
			want: sdk.Position(`{"mode":"","lastProcessedVal":10,"createdAt":""}`),
		},
		{
			name: "success_lastProcessedVal_is_string",
			in: Position{
				LastProcessedVal: "abc",
			},
			want: sdk.Position(`{"mode":"","lastProcessedVal":"abc","createdAt":""}`),
		},
		{
			name: "success_createdAt",
			in: Position{
				CreatedAt: "131415",
			},
			want: sdk.Position(`{"mode":"","lastProcessedVal":null,"createdAt":"131415"}`),
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := tt.in.marshal()
			if err != nil {
				t.Errorf("unexpected error: %s", err.Error())

				return
			}

			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("got: %v, want: %v", string(got), string(tt.want))
			}
		})
	}
}
