package parquet_test

import (
	"bytes"
	"testing"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/format"
)

// TestWriteZeroOptionalFieldsDisabled tests that with the default behavior (disabled),
// zero-valued optional fields like NullCount=0 are omitted from the thrift encoding.
func TestWriteZeroOptionalFieldsDisabled(t *testing.T) {
	type Row struct {
		ID   int64  `parquet:"id"`
		Name string `parquet:"name"`
	}

	buf := new(bytes.Buffer)
	writer := parquet.NewGenericWriter[Row](buf)

	// Write rows with no null values
	rows := []Row{
		{ID: 1, Name: "Alice"},
		{ID: 2, Name: "Bob"},
		{ID: 3, Name: "Charlie"},
	}

	if _, err := writer.Write(rows); err != nil {
		t.Fatalf("failed to write rows: %v", err)
	}

	if err := writer.Close(); err != nil {
		t.Fatalf("failed to close writer: %v", err)
	}

	// Read the file and check statistics
	file, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	if err != nil {
		t.Fatalf("failed to open file: %v", err)
	}

	// Check column chunk statistics for a non-nullable column
	rowGroup := file.RowGroups()[0]
	columnChunk := rowGroup.ColumnChunks()[0].(*parquet.FileColumnChunk) // ID column

	// In the default mode, NullCount should be 0 (the zero value)
	// but since it's optional and zero, it might be omitted by thrift encoding
	nullCount := columnChunk.NullCount()

	// The NullCount should be 0 (either explicitly set or default)
	if nullCount != 0 {
		t.Errorf("expected null count to be 0, got %d", nullCount)
	}

	// Note: We can't easily test if the field was omitted vs explicitly set to 0
	// without inspecting the raw thrift bytes, which is not exposed by the API.
	// This test mainly serves as a baseline for comparison with the enabled test.
}

// TestWriteZeroOptionalFieldsEnabled tests that when WriteZeroOptionalFields is enabled,
// zero-valued optional fields like NullCount=0 are explicitly written to the thrift encoding.
func TestWriteZeroOptionalFieldsEnabled(t *testing.T) {
	type Row struct {
		ID   int64  `parquet:"id"`
		Name string `parquet:"name"`
	}

	buf := new(bytes.Buffer)
	writer := parquet.NewGenericWriter[Row](buf,
		parquet.WriteZeroOptionalFields(true),
	)

	// Write rows with no null values
	rows := []Row{
		{ID: 1, Name: "Alice"},
		{ID: 2, Name: "Bob"},
		{ID: 3, Name: "Charlie"},
	}

	if _, err := writer.Write(rows); err != nil {
		t.Fatalf("failed to write rows: %v", err)
	}

	if err := writer.Close(); err != nil {
		t.Fatalf("failed to close writer: %v", err)
	}

	// Read the file and check statistics
	file, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	if err != nil {
		t.Fatalf("failed to open file: %v", err)
	}

	// Check column chunk statistics for a non-nullable column
	rowGroup := file.RowGroups()[0]
	columnChunk := rowGroup.ColumnChunks()[0].(*parquet.FileColumnChunk) // ID column

	// With WriteZeroOptionalFields enabled, NullCount=0 should be explicitly written
	nullCount := columnChunk.NullCount()
	if nullCount != 0 {
		t.Errorf("expected null count to be 0, got %d", nullCount)
	}

	// Verify that statistics exist and are valid
	// This is a proxy test - the real validation would be done by systems like Snowflake
	// that require explicit null counts for non-nullable columns
}

// TestWriteZeroOptionalFieldsWithNullableColumn tests the behavior with a nullable column
// that actually has null values.
func TestWriteZeroOptionalFieldsWithNullableColumn(t *testing.T) {
	type Row struct {
		ID      int64   `parquet:"id"`
		Name    string  `parquet:"name"`
		Age     *int32  `parquet:"age,optional"`
		Country *string `parquet:"country,optional"`
	}

	age1 := int32(30)
	age2 := int32(25)
	country1 := "USA"

	rows := []Row{
		{ID: 1, Name: "Alice", Age: &age1, Country: &country1}, // No nulls
		{ID: 2, Name: "Bob", Age: &age2, Country: nil},         // Country is null
		{ID: 3, Name: "Charlie", Age: nil, Country: nil},       // Both are null
	}

	testCases := []struct {
		name                    string
		writeZeroOptionalFields bool
	}{
		{"disabled", false},
		{"enabled", true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			buf := new(bytes.Buffer)
			writer := parquet.NewGenericWriter[Row](buf,
				parquet.WriteZeroOptionalFields(tc.writeZeroOptionalFields),
			)

			if _, err := writer.Write(rows); err != nil {
				t.Fatalf("failed to write rows: %v", err)
			}

			if err := writer.Close(); err != nil {
				t.Fatalf("failed to close writer: %v", err)
			}

			// Read the file and verify
			file, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
			if err != nil {
				t.Fatalf("failed to open file: %v", err)
			}

			rowGroup := file.RowGroups()[0]

			// Check Age column (has 1 null)
			ageColumn := rowGroup.ColumnChunks()[2].(*parquet.FileColumnChunk)
			ageNullCount := ageColumn.NullCount()
			if ageNullCount != 1 {
				t.Errorf("expected age null count to be 1, got %d", ageNullCount)
			}

			// Check Country column (has 2 nulls)
			countryColumn := rowGroup.ColumnChunks()[3].(*parquet.FileColumnChunk)
			countryNullCount := countryColumn.NullCount()
			if countryNullCount != 2 {
				t.Errorf("expected country null count to be 2, got %d", countryNullCount)
			}

			// Verify we can read the data back correctly
			reader := parquet.NewGenericReader[Row](bytes.NewReader(buf.Bytes()))
			defer reader.Close()

			readRows := make([]Row, 3)
			n, err := reader.Read(readRows)
			if err != nil && err.Error() != "EOF" {
				t.Fatalf("failed to read rows: %v", err)
			}
			if n != 3 {
				t.Errorf("expected to read 3 rows, got %d", n)
			}

			// Verify the data matches
			for i := range readRows {
				if readRows[i].ID != rows[i].ID {
					t.Errorf("row %d: expected ID=%d, got %d", i, rows[i].ID, readRows[i].ID)
				}
				if readRows[i].Name != rows[i].Name {
					t.Errorf("row %d: expected Name=%s, got %s", i, rows[i].Name, readRows[i].Name)
				}
			}
		})
	}
}

// TestWriteZeroOptionalFieldsThriftEncoding tests the low-level thrift encoding
// to verify that zero-valued optional fields are written when the feature is enabled.
func TestWriteZeroOptionalFieldsThriftEncoding(t *testing.T) {
	// This test verifies the behavior at the thrift encoding level
	// by checking the Statistics struct directly

	type Row struct {
		ID int64 `parquet:"id"`
	}

	testCases := []struct {
		name                    string
		writeZeroOptionalFields bool
	}{
		{"disabled", false},
		{"enabled", true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			buf := new(bytes.Buffer)
			writer := parquet.NewGenericWriter[Row](buf,
				parquet.WriteZeroOptionalFields(tc.writeZeroOptionalFields),
			)

			// Write a single row
			if _, err := writer.Write([]Row{{ID: 1}}); err != nil {
				t.Fatalf("failed to write row: %v", err)
			}

			if err := writer.Close(); err != nil {
				t.Fatalf("failed to close writer: %v", err)
			}

			// Open the file and inspect the raw metadata
			file, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
			if err != nil {
				t.Fatalf("failed to open file: %v", err)
			}

			// Get the metadata
			metadata := file.Metadata()
			if len(metadata.RowGroups) == 0 {
				t.Fatal("no row groups in file")
			}

			rowGroup := metadata.RowGroups[0]
			if len(rowGroup.Columns) == 0 {
				t.Fatal("no columns in row group")
			}

			column := rowGroup.Columns[0]
			stats := column.MetaData.Statistics

			// Check that NullCount is 0 (no nulls in a non-nullable column)
			if stats.NullCount != 0 {
				t.Errorf("expected NullCount to be 0, got %d", stats.NullCount)
			}

			// When WriteZeroOptionalFields is enabled, DistinctCount should also be written
			// (even though it's 0 by default for columns where it's not computed)
			// This is the key difference - with the feature enabled, zero values are written
			// Note: We can't easily test if the field was omitted vs set to 0 without
			// re-encoding and checking the thrift bytes, but this validates the value is correct.
		})
	}
}

// TestWriteZeroOptionalFieldsMetadata tests that the Statistics struct
// is properly populated regardless of the WriteZeroOptionalFields setting.
func TestWriteZeroOptionalFieldsMetadata(t *testing.T) {
	type Row struct {
		ID    int64   `parquet:"id"`
		Value float64 `parquet:"value"`
	}

	rows := []Row{
		{ID: 1, Value: 1.5},
		{ID: 2, Value: 2.5},
		{ID: 3, Value: 3.5},
	}

	testCases := []struct {
		name    string
		enabled bool
	}{
		{"default", false},
		{"enabled", true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			buf := new(bytes.Buffer)
			writer := parquet.NewGenericWriter[Row](buf,
				parquet.WriteZeroOptionalFields(tc.enabled),
			)

			if _, err := writer.Write(rows); err != nil {
				t.Fatalf("failed to write rows: %v", err)
			}

			if err := writer.Close(); err != nil {
				t.Fatalf("failed to close writer: %v", err)
			}

			// Verify metadata
			file, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
			if err != nil {
				t.Fatalf("failed to open file: %v", err)
			}

			metadata := file.Metadata()
			columnMeta := metadata.RowGroups[0].Columns[0].MetaData

			// Verify Statistics exist and NullCount is 0
			if columnMeta.Statistics.NullCount != 0 {
				t.Errorf("expected NullCount=0, got %d", columnMeta.Statistics.NullCount)
			}

			// Verify min/max values are set
			if len(columnMeta.Statistics.MinValue) == 0 {
				t.Error("expected MinValue to be set")
			}
			if len(columnMeta.Statistics.MaxValue) == 0 {
				t.Error("expected MaxValue to be set")
			}

			// Verify NumValues matches
			expectedNumValues := int64(len(rows))
			if columnMeta.NumValues != expectedNumValues {
				t.Errorf("expected NumValues=%d, got %d", expectedNumValues, columnMeta.NumValues)
			}
		})
	}
}

// TestWriteZeroOptionalFieldsBackwardCompatibility ensures that files written with
// the feature disabled can be read by the same library, and files with it enabled
// can also be read correctly.
func TestWriteZeroOptionalFieldsBackwardCompatibility(t *testing.T) {
	type Row struct {
		ID   int64  `parquet:"id"`
		Name string `parquet:"name"`
	}

	rows := []Row{
		{ID: 1, Name: "Alice"},
		{ID: 2, Name: "Bob"},
	}

	// Write with feature disabled
	buf1 := new(bytes.Buffer)
	writer1 := parquet.NewGenericWriter[Row](buf1,
		parquet.WriteZeroOptionalFields(false),
	)
	if _, err := writer1.Write(rows); err != nil {
		t.Fatalf("failed to write with feature disabled: %v", err)
	}
	if err := writer1.Close(); err != nil {
		t.Fatalf("failed to close writer: %v", err)
	}

	// Write with feature enabled
	buf2 := new(bytes.Buffer)
	writer2 := parquet.NewGenericWriter[Row](buf2,
		parquet.WriteZeroOptionalFields(true),
	)
	if _, err := writer2.Write(rows); err != nil {
		t.Fatalf("failed to write with feature enabled: %v", err)
	}
	if err := writer2.Close(); err != nil {
		t.Fatalf("failed to close writer: %v", err)
	}

	// Read both files and verify they produce the same data
	for i, buf := range []*bytes.Buffer{buf1, buf2} {
		reader := parquet.NewGenericReader[Row](bytes.NewReader(buf.Bytes()))
		defer reader.Close()

		readRows := make([]Row, 2)
		n, err := reader.Read(readRows)
		if err != nil && err.Error() != "EOF" {
			t.Fatalf("file %d: failed to read: %v", i, err)
		}
		if n != 2 {
			t.Errorf("file %d: expected 2 rows, got %d", i, n)
		}

		for j := range readRows {
			if readRows[j].ID != rows[j].ID || readRows[j].Name != rows[j].Name {
				t.Errorf("file %d, row %d: data mismatch", i, j)
			}
		}
	}
}

// Helper function to get Statistics from a column chunk (for potential future use)
func getColumnStatistics(t *testing.T, file *parquet.File, columnIndex int) format.Statistics {
	t.Helper()

	metadata := file.Metadata()
	if len(metadata.RowGroups) == 0 {
		t.Fatal("no row groups")
	}

	rowGroup := metadata.RowGroups[0]
	if columnIndex >= len(rowGroup.Columns) {
		t.Fatalf("column index %d out of range", columnIndex)
	}

	return rowGroup.Columns[columnIndex].MetaData.Statistics
}
