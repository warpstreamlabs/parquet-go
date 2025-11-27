package parquet_test

import (
	"bytes"
	"testing"

	"github.com/parquet-go/parquet-go"
)

// TestNullCountAlwaysWritten verifies that NullCount=0 is always written to
// the file statistics, ensuring compatibility with strict Parquet consumers
// like Snowflake that require explicit null counts.
func TestNullCountAlwaysWritten(t *testing.T) {
	type Row struct {
		ID   int64  `parquet:"id"`
		Name string `parquet:"name"`
	}

	rows := []Row{
		{ID: 1, Name: "Alice"},
		{ID: 2, Name: "Bob"},
		{ID: 3, Name: "Charlie"},
	}

	buf := new(bytes.Buffer)
	writer := parquet.NewGenericWriter[Row](buf)

	if _, err := writer.Write(rows); err != nil {
		t.Fatalf("failed to write rows: %v", err)
	}

	if err := writer.Close(); err != nil {
		t.Fatalf("failed to close writer: %v", err)
	}

	// Read the file back
	fileBytes := buf.Bytes()
	reader := bytes.NewReader(fileBytes)
	pf, err := parquet.OpenFile(reader, int64(len(fileBytes)))
	if err != nil {
		t.Fatalf("failed to open parquet file: %v", err)
	}

	// Verify that column chunks have NullCount set to 0
	metadata := pf.Metadata()
	if len(metadata.RowGroups) == 0 {
		t.Fatal("No row groups found")
	}

	for rgIdx, rowGroup := range metadata.RowGroups {
		for colIdx, col := range rowGroup.Columns {
			// NullCount should be 0 for all non-nullable columns
			stats := col.MetaData.Statistics
			nullCount := stats.NullCount
			if nullCount != 0 {
				t.Errorf("RowGroup[%d].Column[%d]: expected NullCount=0, got %d",
					rgIdx, colIdx, nullCount)
			}
			t.Logf("RowGroup[%d].Column[%d]: NullCount=%d ✓", rgIdx, colIdx, nullCount)
		}
	}
}

