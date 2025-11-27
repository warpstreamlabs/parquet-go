package parquet_test

import (
	"bytes"
	"testing"

	"github.com/parquet-go/parquet-go"
)

// TestWriteZeroOptionalFieldsFooterSize compares the footer sizes of files
// written with and without WriteZeroOptionalFields to verify the feature adds bytes.
func TestWriteZeroOptionalFieldsFooterSize(t *testing.T) {
	type Row struct {
		ID   int64  `parquet:"id"`
		Name string `parquet:"name"`
	}

	rows := []Row{
		{ID: 1, Name: "Alice"},
		{ID: 2, Name: "Bob"},
		{ID: 3, Name: "Charlie"},
	}

	// Write file WITHOUT WriteZeroOptionalFields
	buf1 := new(bytes.Buffer)
	writer1 := parquet.NewGenericWriter[Row](buf1,
		parquet.WriteZeroOptionalFields(false),
	)
	if _, err := writer1.Write(rows); err != nil {
		t.Fatalf("failed to write file 1: %v", err)
	}
	if err := writer1.Close(); err != nil {
		t.Fatalf("failed to close writer 1: %v", err)
	}

	// Write file WITH WriteZeroOptionalFields
	buf2 := new(bytes.Buffer)
	writer2 := parquet.NewGenericWriter[Row](buf2,
		parquet.WriteZeroOptionalFields(true),
	)
	if _, err := writer2.Write(rows); err != nil {
		t.Fatalf("failed to write file 2: %v", err)
	}
	if err := writer2.Close(); err != nil {
		t.Fatalf("failed to close writer 2: %v", err)
	}

	file1Bytes := buf1.Bytes()
	file2Bytes := buf2.Bytes()

	t.Logf("File 1 (disabled) size: %d bytes", len(file1Bytes))
	t.Logf("File 2 (enabled) size: %d bytes", len(file2Bytes))

	// Extract footer sizes
	footer1Size := extractFooterSize(file1Bytes)
	footer2Size := extractFooterSize(file2Bytes)

	t.Logf("File 1 footer size: %d bytes", footer1Size)
	t.Logf("File 2 footer size: %d bytes", footer2Size)

	// With WriteZeroOptionalFields enabled, the footer should be larger
	// because it includes zero-valued optional fields like NullCount=0 and DistinctCount=0
	if footer2Size <= footer1Size {
		t.Errorf("Expected file 2 footer (enabled) to be larger than file 1 footer (disabled), got %d <= %d", footer2Size, footer1Size)
		t.Log("⚠️  Feature may not be working - footers are same size")
	} else {
		t.Logf("✓ File 2 footer is %d bytes larger (feature is working)", footer2Size-footer1Size)
	}

	// Also compare total file sizes
	if len(file2Bytes) <= len(file1Bytes) {
		t.Errorf("Expected file 2 (enabled) to be larger than file 1 (disabled), got %d <= %d", len(file2Bytes), len(file1Bytes))
	} else {
		t.Logf("✓ File 2 is %d bytes larger total", len(file2Bytes)-len(file1Bytes))
	}

	// Verify both files can be read and produce the same data
	for i, buf := range []*bytes.Buffer{buf1, buf2} {
		reader := parquet.NewGenericReader[Row](bytes.NewReader(buf.Bytes()))
		defer reader.Close()

		readRows := make([]Row, 3)
		n, err := reader.Read(readRows)
		if err != nil && err.Error() != "EOF" {
			t.Fatalf("file %d: failed to read: %v", i+1, err)
		}
		if n != 3 {
			t.Errorf("file %d: expected 3 rows, got %d", i+1, n)
		}
	}
}

func extractFooterSize(fileBytes []byte) int32 {
	if len(fileBytes) < 12 {
		return 0
	}
	return int32(fileBytes[len(fileBytes)-8]) |
		int32(fileBytes[len(fileBytes)-7])<<8 |
		int32(fileBytes[len(fileBytes)-6])<<16 |
		int32(fileBytes[len(fileBytes)-5])<<24
}
