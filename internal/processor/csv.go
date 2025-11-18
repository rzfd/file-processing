package processor

import (
	"encoding/csv"
	"fmt"
	"io"
	"strconv"

	"github.com/rs/zerolog/log"
	"github.com/rzfd/file-processing-system/internal/models"
)

// ProcessCSV processes a CSV file and returns processed records
func ProcessCSV(reader io.Reader) ([]models.ProcessedRecord, error) {
	log.Info().Msg("Starting CSV processing")
	csvReader := csv.NewReader(reader)

	// Read header
	headers, err := csvReader.Read()
	if err != nil {
		return nil, fmt.Errorf("failed to read CSV header: %w", err)
	}
	log.Info().
		Strs("headers", headers).
		Msg("CSV headers parsed")

	var records []models.ProcessedRecord
	rowNumber := 1

	for {
		row, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to read CSV row: %w", err)
		}

		// Convert row to map
		data := make(map[string]interface{})
		for i, value := range row {
			if i < len(headers) {
				// Try to convert to number if possible
				if num, err := strconv.ParseFloat(value, 64); err == nil {
					data[headers[i]] = num
				} else if value == "true" || value == "false" {
					data[headers[i]] = value == "true"
				} else {
					data[headers[i]] = value
				}
			}
		}

		records = append(records, models.ProcessedRecord{
			RowNumber: rowNumber,
			Data:      data,
		})

		rowNumber++
	}

	log.Info().
		Int("record_count", len(records)).
		Msg("CSV processing completed")
	return records, nil
}
