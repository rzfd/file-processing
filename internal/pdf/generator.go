package pdf

import (
	"bytes"
	"fmt"
	"time"

	"github.com/jung-kurt/gofpdf/v2"
	"github.com/rs/zerolog/log"
	"github.com/rzfd/file-processing-system/internal/models"
)

// Generator handles PDF generation
type Generator struct{}

// NewGenerator creates a new PDF generator
func NewGenerator() *Generator {
	return &Generator{}
}

// GeneratePDF generates a PDF from items with watermark showing upload date
// Returns PDF as bytes buffer
func (g *Generator) GeneratePDF(items []models.Item, uploadDate time.Time, fileName string) (*bytes.Buffer, error) {
	// A4 landscape: 297mm x 210mm
	pdf := gofpdf.New("L", "mm", "A4", "")
	pdf.SetMargins(10, 10, 10)
	pdf.SetAutoPageBreak(true, 10)

	// Add first page
	pdf.AddPage()

	// Set font
	pdf.SetFont("Arial", "B", 16)
	pdf.Cell(0, 10, fmt.Sprintf("Report: %s", fileName))
	pdf.Ln(15)

	// Add watermark with upload date
	pdf.SetFont("Arial", "", 10)
	uploadDateStr := uploadDate.Format("02 January 2006 15:04:05")
	pdf.SetTextColor(200, 200, 200) // Light gray for watermark
	pdf.SetFont("Arial", "I", 8)

	// Position watermark at bottom right
	pdf.SetXY(250, 195)
	pdf.Cell(0, 5, fmt.Sprintf("Uploaded: %s", uploadDateStr))

	// Reset text color for content
	pdf.SetTextColor(0, 0, 0)
	pdf.SetFont("Arial", "B", 12)

	// Table headers
	headers := []string{"No", "Name", "Nominal"}
	colWidths := []float64{20, 150, 80}

	// Draw header
	pdf.SetFillColor(200, 200, 200)
	pdf.SetFont("Arial", "B", 10)
	for i, header := range headers {
		pdf.CellFormat(colWidths[i], 7, header, "1", 0, "C", true, 0, "")
	}
	pdf.Ln(-1)

	// Draw rows
	pdf.SetFont("Arial", "", 9)
	pdf.SetFillColor(255, 255, 255)

	rowNum := 1
	for _, item := range items {
		// Check if we need a new page
		if pdf.GetY() > 190 {
			pdf.AddPage()
			// Redraw watermark on new page
			pdf.SetTextColor(200, 200, 200)
			pdf.SetFont("Arial", "I", 8)
			pdf.SetXY(250, 195)
			pdf.Cell(0, 5, fmt.Sprintf("Uploaded: %s", uploadDateStr))
			pdf.SetTextColor(0, 0, 0)

			// Redraw header on new page
			pdf.SetFont("Arial", "B", 10)
			pdf.SetFillColor(200, 200, 200)
			for i, header := range headers {
				pdf.CellFormat(colWidths[i], 7, header, "1", 0, "C", true, 0, "")
			}
			pdf.Ln(-1)
			pdf.SetFont("Arial", "", 9)
		}

		// Row number
		pdf.CellFormat(colWidths[0], 6, fmt.Sprintf("%d", rowNum), "1", 0, "C", false, 0, "")

		// Name
		name := ""
		if item.Name != nil {
			name = *item.Name
		}
		pdf.CellFormat(colWidths[1], 6, truncateString(name, 40), "1", 0, "L", false, 0, "")

		// Nominal
		nominal := ""
		if item.Nominal != nil {
			nominal = fmt.Sprintf("%.2f", *item.Nominal)
		}
		pdf.CellFormat(colWidths[2], 6, nominal, "1", 0, "R", false, 0, "")

		pdf.Ln(-1)
		rowNum++
	}

	// Generate PDF bytes
	var buf bytes.Buffer
	err := pdf.Output(&buf)
	if err != nil {
		log.Error().Err(err).Msg("Failed to generate PDF")
		return nil, fmt.Errorf("failed to generate PDF: %w", err)
	}

	log.Info().
		Int("items_count", len(items)).
		Int("pdf_size", buf.Len()).
		Msg("PDF generated successfully")

	return &buf, nil
}

// truncateString truncates a string to max length
func truncateString(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen-3] + "..."
}
