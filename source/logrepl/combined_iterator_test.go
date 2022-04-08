package logrepl

import (
	"context"
	"reflect"
	"testing"

	sdk "github.com/conduitio/conduit-connector-sdk"
)

func TestCombinedIterator_Next(t *testing.T) {
	type fields struct {
		cdc  *CDCIterator
		snap *SnapshotIterator
	}
	type args struct {
		ctx context.Context
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    sdk.Record
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &CombinedIterator{
				cdc:  tt.fields.cdc,
				snap: tt.fields.snap,
			}
			got, err := c.Next(tt.args.ctx)
			if (err != nil) != tt.wantErr {
				t.Errorf("CombinedIterator.Next() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("CombinedIterator.Next() = %v, want %v", got, tt.want)
			}
		})
	}
}
