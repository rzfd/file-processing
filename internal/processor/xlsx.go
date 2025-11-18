package processor

import (
	"bytes"
	"fmt"
	"io"
	"strconv"

	"github.com/rs/zerolog/log"
	"github.com/rzfd/file-processing-system/internal/models"
	"github.com/xuri/excelize/v2"
)

// ProcessXLSX processes an XLSX file and returns processed records
func ProcessXLSX(reader io.Reader) ([]models.ProcessedRecord, error) {
	log.Info().Msg("Starting XLSX processing")

	// Read all data from reader
	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("failed to read XLSX data: %w", err)
	}
	log.Info().
		Int("bytes", len(data)).
		Msg("Read XLSX file data")

	// Open Excel file
	f, err := excelize.OpenReader(bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("failed to open XLSX file: %w", err)
	}
	defer f.Close()

	// Get first sheet
	sheetName := f.GetSheetName(0)
	if sheetName == "" {
		return nil, fmt.Errorf("no sheets found in XLSX file")
	}
	log.Info().
		Str("sheet", sheetName).
		Msg("Processing sheet")

	rows, err := f.GetRows(sheetName)
	if err != nil {
		return nil, fmt.Errorf("failed to get rows: %w", err)
	}

	if len(rows) == 0 {
		return nil, fmt.Errorf("empty sheet")
	}

	// First row is header
	headers := rows[0]
	log.Info().
		Strs("headers", headers).
		Msg("XLSX headers parsed")
	var records []models.ProcessedRecord

	for i := 1; i < len(rows); i++ {
		row := rows[i]
		data := make(map[string]interface{})

		for j, value := range row {
			if j < len(headers) {
				// Try to convert to number if possible
				if num, err := strconv.ParseFloat(value, 64); err == nil {
					data[headers[j]] = num
				} else if value == "true" || value == "false" {
					data[headers[j]] = value == "true"
				} else {
					data[headers[j]] = value
				}
			}
		}

		records = append(records, models.ProcessedRecord{
			RowNumber: i,
			Data:      data,
		})
	}

	log.Info().
		Int("record_count", len(records)).
		Msg("XLSX processing completed")
	return records, nil
}
