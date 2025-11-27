package parquet_test

import (
	"bytes"
	"testing"

	"github.com/parquet-go/parquet-go"
)

// TestNullCountFieldSizeValidation validates that files with WriteZeroOptionalFields
// enabled are larger than those without it, proving that zero-valued optional fields
// like NullCount=0 are being written.
func TestNullCountFieldSizeValidation(t *testing.T) {
	type Row struct {
		ID   int64  `parquet:"id"`
		Name string `parquet:"name"`
	}

	rows := []Row{
		{ID: 1, Name: "Alice"},
		{ID: 2, Name: "Bob"},
		{ID: 3, Name: "Charlie"},
	}

	// Write file WITHOUT the feature
	bufWithout := new(bytes.Buffer)
	writerWithout := parquet.NewGenericWriter[Row](bufWithout,
		parquet.WriteZeroOptionalFields(false),
	)
	if _, err := writerWithout.Write(rows); err != nil {
		t.Fatalf("failed to write rows (without feature): %v", err)
	}
	if err := writerWithout.Close(); err != nil {
		t.Fatalf("failed to close writer (without feature): %v", err)
	}

	// Write file WITH the feature
	bufWith := new(bytes.Buffer)
	writerWith := parquet.NewGenericWriter[Row](bufWith,
		parquet.WriteZeroOptionalFields(true),
	)
	if _, err := writerWith.Write(rows); err != nil {
		t.Fatalf("failed to write rows (with feature): %v", err)
	}
	if err := writerWith.Close(); err != nil {
		t.Fatalf("failed to close writer (with feature): %v", err)
	}

	sizeWithout := len(bufWithout.Bytes())
	sizeWith := len(bufWith.Bytes())

	t.Logf("File size WITHOUT WriteZeroOptionalFields: %d bytes", sizeWithout)
	t.Logf("File size WITH WriteZeroOptionalFields:    %d bytes", sizeWith)
	t.Logf("Difference:                                %+d bytes", sizeWith-sizeWithout)

	if sizeWith <= sizeWithout {
		t.Errorf("Expected file WITH WriteZeroOptionalFields to be larger\n"+
			"  without feature: %d bytes\n"+
			"  with feature:    %d bytes",
			sizeWithout, sizeWith)
	}

	// Sanity check: the difference should be reasonable (at least a few bytes per field)
	// With 2 columns and potentially multiple optional fields in Statistics,
	// we expect at least 10-20 bytes difference
	minExpectedDiff := 10
	actualDiff := sizeWith - sizeWithout
	
	if actualDiff < minExpectedDiff {
		t.Errorf("Size difference too small to be meaningful: expected at least %d bytes, got %d",
			minExpectedDiff, actualDiff)
	} else {
		t.Logf("✓ File with WriteZeroOptionalFields is %d bytes larger", actualDiff)
	}
}

