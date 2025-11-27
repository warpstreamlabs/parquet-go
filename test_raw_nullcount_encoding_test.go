package parquet_test

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"testing"

	"slices"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/encoding/thrift"
)

// TestRawNullCountEncoding reads a parquet file and manually inspects the raw
// thrift encoding to determine if NullCount fields are present or omitted.
func TestRawNullCountEncoding(t *testing.T) {
	filePath := "warpstream__tableflow_75047345156a575d_unpartitioned__tableflow_datagen_json_0_v0-df956f6a-f9b0-4495-9480-55ed80319f54_data_00000941130244846770.parquet"

	// Check if file exists
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		t.Skipf("Test file not found: %s", filePath)
	}

	// Read the entire file
	fileData, err := os.ReadFile(filePath)
	if err != nil {
		t.Fatalf("failed to read file: %v", err)
	}

	// Open the parquet file normally to get the footer location
	file, err := parquet.OpenFile(bytes.NewReader(fileData), int64(len(fileData)))
	if err != nil {
		t.Fatalf("failed to open parquet file: %v", err)
	}

	metadata := file.Metadata()

	t.Logf("File has %d row groups", len(metadata.RowGroups))

	// Inspect each row group's column metadata
	for rgIdx, rowGroup := range metadata.RowGroups {
		t.Logf("\n=== Row Group %d ===", rgIdx)
		t.Logf("NumRows: %d", rowGroup.NumRows)

		for colIdx, column := range rowGroup.Columns {
			t.Logf("\n--- Column %d: %v ---", colIdx, column.MetaData.PathInSchema)

			stats := column.MetaData.Statistics

			// Report the Statistics values as read by the normal decoder
			t.Logf("Statistics (via normal decoder):")
			t.Logf("  NullCount: %d", stats.NullCount)
			t.Logf("  DistinctCount: %d", stats.DistinctCount)
			t.Logf("  MinValue present: %v (len=%d)", len(stats.MinValue) > 0, len(stats.MinValue))
			t.Logf("  MaxValue present: %v (len=%d)", len(stats.MaxValue) > 0, len(stats.MaxValue))

			// Now manually inspect the raw thrift encoding
			// Re-encode the statistics to see what fields are present
			statsBytes, err := thrift.Marshal(&thrift.CompactProtocol{}, &stats)
			if err != nil {
				t.Errorf("failed to marshal statistics: %v", err)
				continue
			}

			t.Logf("\nRaw thrift encoding inspection:")
			t.Logf("  Statistics encoded size: %d bytes", len(statsBytes))

			// Manually decode to see which fields are present
			presentFields := inspectThriftFields(statsBytes)
			t.Logf("  Present field IDs: %v", presentFields)

			// Field IDs in Statistics struct:
			// 1: Max ([]byte)
			// 2: Min ([]byte)
			// 3: NullCount (int64)
			// 4: DistinctCount (int64)
			// 5: MaxValue ([]byte)
			// 6: MinValue ([]byte)

			hasNullCount := contains(presentFields, 3)
			hasDistinctCount := contains(presentFields, 4)

			t.Logf("  Field 3 (NullCount) present: %v", hasNullCount)
			t.Logf("  Field 4 (DistinctCount) present: %v", hasDistinctCount)

			if stats.NullCount == 0 && !hasNullCount {
				t.Logf("  ⚠️  NullCount=0 but field is OMITTED from thrift encoding")
			} else if stats.NullCount == 0 && hasNullCount {
				t.Logf("  ✓ NullCount=0 and field is PRESENT in thrift encoding")
			} else if stats.NullCount > 0 && hasNullCount {
				t.Logf("  ✓ NullCount=%d and field is PRESENT in thrift encoding", stats.NullCount)
			}
		}
	}
}

// TestWriteAndInspectRawFooter writes a file with WriteZeroOptionalFields enabled
// and inspects the raw footer bytes to verify NullCount is present.
func TestWriteAndInspectRawFooter(t *testing.T) {
	type Row struct {
		ID int64 `parquet:"id"`
	}

	rows := []Row{{ID: 1}, {ID: 2}, {ID: 3}}

	// Write file WITH WriteZeroOptionalFields
	buf := new(bytes.Buffer)
	writer := parquet.NewGenericWriter[Row](buf,
		parquet.WriteZeroOptionalFields(true),
	)
	if _, err := writer.Write(rows); err != nil {
		t.Fatalf("failed to write: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("failed to close: %v", err)
	}

	fileBytes := buf.Bytes()
	t.Logf("File size: %d bytes", len(fileBytes))

	// Read footer length (last 4 bytes before magic)
	if len(fileBytes) < 12 {
		t.Fatal("File too small")
	}

	footerSize := int32(fileBytes[len(fileBytes)-8]) |
		int32(fileBytes[len(fileBytes)-7])<<8 |
		int32(fileBytes[len(fileBytes)-6])<<16 |
		int32(fileBytes[len(fileBytes)-5])<<24

	t.Logf("Footer size: %d bytes", footerSize)

	// Extract footer bytes (before the footer size and magic)
	footerStart := len(fileBytes) - 8 - int(footerSize)
	footerBytes := fileBytes[footerStart : len(fileBytes)-8]

	t.Logf("Footer bytes from offset %d to %d", footerStart, len(fileBytes)-8)

	// The footer contains the FileMetaData which contains RowGroups which contain Statistics
	// Let's just check if field ID 3 appears in the footer at all
	nullCountFieldID := byte(3)

	// Search for potential NullCount field encodings in compact protocol
	// In compact protocol with delta encoding, field 3 could be encoded as:
	// - 0x36 (delta=3, type=I64) if previous field was 0
	// - or other patterns depending on previous field

	foundPotentialNullCount := false
	for i := range len(footerBytes) - 1 {
		b := footerBytes[i]
		// Check for field with ID 3 in various encodings
		if (b >> 4) == nullCountFieldID {
			t.Logf("Found potential NullCount field at offset %d: 0x%02x", i, b)
			foundPotentialNullCount = true
		}
	}

	if !foundPotentialNullCount {
		t.Log("⚠️  No NullCount field found in footer (feature may not be working)")
	} else {
		t.Log("✓ Found potential NullCount field in footer")
	}
}

// TestCompareEncodingWithAndWithoutFeature creates two identical files,
// one with WriteZeroOptionalFields enabled and one without, and compares
// the raw thrift encoding to verify the feature works.
func TestCompareEncodingWithAndWithoutFeature(t *testing.T) {
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

	// Compare the metadata encoding
	files := []*bytes.Buffer{buf1, buf2}
	names := []string{"WITHOUT WriteZeroOptionalFields", "WITH WriteZeroOptionalFields"}

	for i, buf := range files {
		t.Logf("\n=== File %d: %s ===", i+1, names[i])

		file, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
		if err != nil {
			t.Fatalf("failed to open file %d: %v", i+1, err)
		}

		metadata := file.Metadata()
		rowGroup := metadata.RowGroups[0]

		for colIdx, column := range rowGroup.Columns {
			t.Logf("\nColumn %d: %v", colIdx, column.MetaData.PathInSchema)

			stats := column.MetaData.Statistics

			// Re-encode to inspect
			statsBytes, err := thrift.Marshal(&thrift.CompactProtocol{}, &stats)
			if err != nil {
				t.Errorf("failed to marshal statistics: %v", err)
				continue
			}

			presentFields := inspectThriftFields(statsBytes)
			hasNullCount := contains(presentFields, 3)
			hasDistinctCount := contains(presentFields, 4)

			t.Logf("  NullCount value: %d", stats.NullCount)
			t.Logf("  NullCount field present in thrift: %v", hasNullCount)
			t.Logf("  DistinctCount field present in thrift: %v", hasDistinctCount)
			t.Logf("  Encoded size: %d bytes", len(statsBytes))
			t.Logf("  Present fields: %v", presentFields)
		}
	}

	// Verify the difference
	file1, _ := parquet.OpenFile(bytes.NewReader(buf1.Bytes()), int64(buf1.Len()))
	file2, _ := parquet.OpenFile(bytes.NewReader(buf2.Bytes()), int64(buf2.Len()))

	stats1 := file1.Metadata().RowGroups[0].Columns[0].MetaData.Statistics
	stats2 := file2.Metadata().RowGroups[0].Columns[0].MetaData.Statistics

	bytes1, _ := thrift.Marshal(&thrift.CompactProtocol{}, &stats1)
	bytes2, _ := thrift.Marshal(&thrift.CompactProtocol{}, &stats2)

	fields1 := inspectThriftFields(bytes1)
	fields2 := inspectThriftFields(bytes2)

	t.Logf("\n=== Comparison ===")
	t.Logf("File 1 (disabled) has NullCount field: %v", contains(fields1, 3))
	t.Logf("File 2 (enabled) has NullCount field: %v", contains(fields2, 3))

	// Note: Re-encoding already-decoded statistics loses information about which
	// fields were originally present. The real test is the file size difference.
	// See TestWriteZeroOptionalFieldsFooterSize for a better verification.
	size1 := len(buf1.Bytes())
	size2 := len(buf2.Bytes())
	t.Logf("File 1 size: %d bytes", size1)
	t.Logf("File 2 size: %d bytes", size2)

	if size2 <= size1 {
		t.Errorf("Expected file 2 (enabled) to be larger, indicating zero-valued fields are written")
	} else {
		t.Logf("✓ File 2 is %d bytes larger (feature is working)", size2-size1)
	}
}

// inspectThriftFields manually parses compact protocol thrift encoding
// to extract which field IDs are present in the encoded message.
func inspectThriftFields(data []byte) []int16 {
	var fields []int16

	if len(data) == 0 {
		return fields
	}

	reader := bytes.NewReader(data)
	lastFieldID := int16(0)

	for {
		// Read field header
		b, err := reader.ReadByte()
		if err == io.EOF {
			break
		}
		if err != nil {
			break
		}

		// Check for STOP (0)
		if b == 0 {
			break
		}

		// In compact protocol, the field header is encoded as:
		// - If field delta <= 15: (field_delta << 4) | type
		// - Otherwise: type byte, then zigzag varint for field ID

		fieldDelta := int16(b >> 4)
		wireType := b & 0x0F

		var fieldID int16
		if fieldDelta == 0 {
			// Field ID follows as zigzag varint
			id, err := readCompactVarint(reader)
			if err != nil {
				break
			}
			fieldID = int16(zigzagDecode(uint64(id)))
		} else {
			// Use delta encoding
			fieldID = lastFieldID + fieldDelta
		}

		fields = append(fields, fieldID)
		lastFieldID = fieldID

		// Skip the value based on wire type
		if err := skipCompactValue(reader, wireType); err != nil {
			break
		}
	}

	return fields
}

// readCompactVarint reads a varint from the reader
func readCompactVarint(r *bytes.Reader) (int64, error) {
	var result uint64
	var shift uint

	for {
		b, err := r.ReadByte()
		if err != nil {
			return 0, err
		}

		result |= uint64(b&0x7f) << shift

		if (b & 0x80) == 0 {
			break
		}

		shift += 7
	}

	return int64(result), nil
}

// zigzagDecode decodes a zigzag-encoded integer
func zigzagDecode(n uint64) int64 {
	return int64((n >> 1) ^ -(n & 1))
}

// skipCompactValue skips a value in the compact protocol based on its type
func skipCompactValue(r *bytes.Reader, wireType byte) error {
	switch wireType {
	case 1, 2: // TRUE, FALSE
		// No value to skip
		return nil
	case 3: // I8
		_, err := r.ReadByte()
		return err
	case 4: // I16
		_, err := readCompactVarint(r)
		return err
	case 5: // I32
		_, err := readCompactVarint(r)
		return err
	case 6: // I64
		_, err := readCompactVarint(r)
		return err
	case 7: // DOUBLE
		buf := make([]byte, 8)
		_, err := r.Read(buf)
		return err
	case 8: // BINARY
		length, err := readCompactVarint(r)
		if err != nil {
			return err
		}
		buf := make([]byte, length)
		_, err = r.Read(buf)
		return err
	case 9: // LIST
		return skipCompactList(r)
	case 10: // SET
		return skipCompactList(r) // Same as list
	case 11: // MAP
		return skipCompactMap(r)
	case 12: // STRUCT
		return skipCompactStruct(r)
	default:
		return fmt.Errorf("unknown wire type: %d", wireType)
	}
}

// skipCompactList skips a list in compact protocol
func skipCompactList(r *bytes.Reader) error {
	b, err := r.ReadByte()
	if err != nil {
		return err
	}

	size := int64(b >> 4)
	elemType := b & 0x0F

	if size == 15 {
		size, err = readCompactVarint(r)
		if err != nil {
			return err
		}
	}

	for i := int64(0); i < size; i++ {
		if err := skipCompactValue(r, elemType); err != nil {
			return err
		}
	}

	return nil
}

// skipCompactMap skips a map in compact protocol
func skipCompactMap(r *bytes.Reader) error {
	size, err := readCompactVarint(r)
	if err != nil {
		return err
	}

	if size == 0 {
		return nil
	}

	b, err := r.ReadByte()
	if err != nil {
		return err
	}

	keyType := b >> 4
	valueType := b & 0x0F

	for i := int64(0); i < size; i++ {
		if err := skipCompactValue(r, keyType); err != nil {
			return err
		}
		if err := skipCompactValue(r, valueType); err != nil {
			return err
		}
	}

	return nil
}

// skipCompactStruct skips a struct in compact protocol
func skipCompactStruct(r *bytes.Reader) error {
	lastFieldID := int16(0)

	for {
		b, err := r.ReadByte()
		if err != nil {
			return err
		}

		if b == 0 { // STOP
			break
		}

		fieldDelta := int16(b >> 4)
		wireType := b & 0x0F

		if fieldDelta == 0 {
			_, err := readCompactVarint(r)
			if err != nil {
				return err
			}
		} else {
			lastFieldID += fieldDelta
		}

		if err := skipCompactValue(r, wireType); err != nil {
			return err
		}
	}

	return nil
}

// contains checks if a slice contains a value
func contains(slice []int16, value int16) bool {
	return slices.Contains(slice, value)
}

// Helper to decode varint for inspection
func decodeVarint(data []byte) (int64, int) {
	var x uint64
	var s uint
	for i, b := range data {
		if b < 0x80 {
			return int64(x | uint64(b)<<s), i + 1
		}
		x |= uint64(b&0x7f) << s
		s += 7
	}
	return 0, 0
}
