package excelize

import (
	"fmt"
	"strconv"

	"github.com/xuri/efp"
)

// eachCellStringFunc does common value extraction workflow for all each cell
// value function. Passed function implements specific part of required logic.
func (f *File) eachCellStringFunc(sheet string, fn func(x *xlsxWorksheet, c *xlsxC) (bool, error)) error {
	ws, err := f.workSheetReader(sheet)
	if err != nil {
		return err
	}

	ws.mu.Lock()
	defer ws.mu.Unlock()

	for rowIdx := range ws.SheetData.Row {
		rowData := &ws.SheetData.Row[rowIdx]
		for colIdx := range rowData.C {
			colData := &rowData.C[colIdx]
			done, err := fn(ws, colData)
			if err != nil {
				return err
			}
			if done {
				return nil
			}
		}
	}
	return nil
}

// EachCellFormula provides a function to get formula from cell by given
// worksheet name and cell reference in spreadsheet.
func (f *File) EachCellFormula(sheet string, fn func(cell, formula string) bool) error {
	sharedFormulaCache := make(map[int]string)
	colCache := make(map[int]int)
	rowCache := make(map[int]int)
	return f.eachCellStringFunc(sheet, func(x *xlsxWorksheet, c *xlsxC) (bool, error) {
		if c.F == nil {
			return fn(c.R, ""), nil
		}
		if c.F.T == STCellFormulaTypeShared && c.F.Si != nil {
			sfc, cached := sharedFormulaCache[*c.F.Si]
			if !cached {
				sharedFormulaCache[*c.F.Si] = c.F.Content
				colCache[*c.F.Si], rowCache[*c.F.Si], _ = CellNameToCoordinates(c.R)
			} else {
				col, row, _ := CellNameToCoordinates(c.R)
				dCol := col - colCache[*c.F.Si]
				dRow := row - rowCache[*c.F.Si]
				orig := []byte(sfc)
				res, start := parseSharedFormula(dCol, dRow, orig)
				if start < len(orig) {
					res += string(orig[start:])
				}
				return fn(c.R, res), nil
			}
			// return fn(c.R, getSharedFormula(x, *c.F.Si, c.R)), nil
			return fn(c.R, c.F.Content), nil
		}
		return fn(c.R, c.F.Content), nil
	})
}

// EachCellFormulaValue provides a function to get formula and value from cell by given
// worksheet name and cell reference in spreadsheet.
func (f *File) EachCellFormulaValue(sheet string, fn func(cell, formula, value string) bool) error {
	sharedFormulaCache := make(map[int]string)
	colCache := make(map[int]int)
	rowCache := make(map[int]int)
	sst, err := f.sharedStringsReader()
	if err != nil {
		return err
	}

	return f.eachCellStringFunc(sheet, func(x *xlsxWorksheet, c *xlsxC) (bool, error) {
		val, err := c.getValueFrom(f, sst, true)
		if err != nil {
			return false, err
		}

		if c.F == nil {
			return fn(c.R, "", val), nil
		}
		if c.F.T == STCellFormulaTypeShared && c.F.Si != nil {
			sfc, cached := sharedFormulaCache[*c.F.Si]
			if !cached {
				sharedFormulaCache[*c.F.Si] = c.F.Content
				colCache[*c.F.Si], rowCache[*c.F.Si], _ = CellNameToCoordinates(c.R)
			} else {
				col, row, _ := CellNameToCoordinates(c.R)
				dCol := col - colCache[*c.F.Si]
				dRow := row - rowCache[*c.F.Si]
				orig := []byte(sfc)
				res, start := parseSharedFormula(dCol, dRow, orig)
				if start < len(orig) {
					res += string(orig[start:])
				}
				return fn(c.R, res, val), nil
			}
		}
		return fn(c.R, c.F.Content, val), nil
	})
}

type IteratorCellValue struct {
	Formula string
	Value   string
}

func (f *File) IterateRowFormulaValues(sheet string) (Next func() []IteratorCellValue, Close func()) {
	rowChan := make(chan []IteratorCellValue)
	closeChan := make(chan struct{})
	doneChan := make(chan struct{})
	go func() {
		var nextRow []IteratorCellValue
		rowNum := 1
		closed := false
		err := f.EachCellFormulaValue(sheet, func(cell, formula, value string) bool {
			_, row, _ := SplitCellName(cell)
			if row != rowNum {
				select {
				case rowChan <- nextRow:
					nextRow = make([]IteratorCellValue, 0, cap(nextRow))
					rowNum = row
				case <-closeChan:
					closed = true
					return true
				}
			}
			nextRow = append(nextRow, IteratorCellValue{
				Formula: formula,
				Value:   value,
			})
			return false
		})
		if err != nil {
			panic(err)
		}
		if len(nextRow) > 0 && !closed {
			select {
			case rowChan <- nextRow:
			case <-closeChan:
			}
		}
		close(doneChan)
	}()
	Next = func() []IteratorCellValue {
		select {
		case row := <-rowChan:
			return row
		case <-doneChan:
		}
		return nil
	}
	Close = func() {
		select {
		case closeChan <- struct{}{}:
		case <-doneChan:
		}
	}
	return Next, Close
}

func (f *File) SetRowCells(sheet string, row int, values ...string) error {
	rowStr := strconv.Itoa(row)
	for i, val := range values {
		col, err := ColumnNumberToName(i + 1)
		if err != nil {
			return err
		}

		if len(val) > 0 && val[0] == '=' {
			err = f.SetCalcedCellFormula(sheet, col+rowStr, val)
			if err != nil {
				return err
			}
		} else {
			err = f.SetCellValue(sheet, col+rowStr, val)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (f *File) SetCalcedCellFormula(sheet, cell string, formula string, opts ...Options) error {
	var result string
	token, err := f.calcCellFormula(&calcContext{
		entry:             fmt.Sprintf("%s!%s", sheet, cell),
		maxCalcIterations: f.getOptions(opts...).MaxCalcIterations,
		iterations:        make(map[string]uint),
		iterationsCache:   make(map[string]formulaArg),
	}, sheet, cell, formula)
	if err != nil {
		// panic(fmt.Errorf("error parsing formula %s - %w", formula, err))
		result = token.String
		f.SetCellDefault(sheet, cell, result)
		f.SetCellFormula(sheet, cell, formula)
		return err
	}
	result = token.Value()
	err = f.SetCellDefault(sheet, cell, result)
	if err != nil {
		return err
	}
	err = f.SetCellFormula(sheet, cell, formula)
	if err != nil {
		return err
	}
	return nil
}

func (f *File) calcCellFormula(ctx *calcContext, sheet, cell, formula string) (result formulaArg, err error) {
	ps := efp.ExcelParser()
	tokens := ps.Parse(formula)
	if tokens == nil {
		return
	}
	result, err = f.evalInfixExp(ctx, sheet, cell, tokens)
	return
}

// SetSheetBulkStrUnsafe sets a block of strings starting with given cell.
// It does not remove any formulas for performance, so make sure there aren't any
func (f *File) SetSheetBulkStrUnsafe(sheet, cell string, block [][]string) error {
	startCol, startRow, err := CellNameToCoordinates(cell)
	if err != nil {
		return err
	}

	f.mu.Lock()
	ws, err := f.workSheetReader(sheet)
	if err != nil {
		f.mu.Unlock()
		return err
	}
	f.mu.Unlock()
	ws.mu.Lock()
	defer ws.mu.Unlock()

	for i := 0; i < len(block); i++ {
		// fmt.Printf("%d\n", i)
		slice := block[i]
		for j := 0; j < len(slice); j++ {
			value := slice[j]
			col := startCol + j
			row := startRow + i
			ws.prepareSheetXML(col, row)
			c := &ws.SheetData.Row[row-1].C[col-1]

			c.S = ws.prepareCellStyle(col, row, c.S)
			if c.T, c.V, err = f.setCellString(value); err != nil {
				return err
			}
			c.IS = nil
			// err = f.removeFormula(c, ws, sheet)
			// if err != nil {
			// 	return err
			// }
		}
	}
	return nil
}

// SetSheetBulkFloatUnsafe sets a block of floats starting with given cell. nil values are set as empty cells
// It does not remove any formulas for performance, so make sure there aren't any
func (f *File) SetSheetBulkFloatUnsafe(sheet, cell string, block [][]*float64) error {
	startCol, startRow, err := CellNameToCoordinates(cell)
	if err != nil {
		return err
	}

	f.mu.Lock()
	ws, err := f.workSheetReader(sheet)
	if err != nil {
		f.mu.Unlock()
		return err
	}
	f.mu.Unlock()
	ws.mu.Lock()
	defer ws.mu.Unlock()

	for i := 0; i < len(block); i++ {
		// fmt.Printf("%d\n", i)
		slice := block[i]
		for j := 0; j < len(slice); j++ {
			value := slice[j]
			col := startCol + j
			row := startRow + i
			ws.prepareSheetXML(col, row)
			c := &ws.SheetData.Row[row-1].C[col-1]

			c.S = ws.prepareCellStyle(col, row, c.S)
			if value != nil {
				c.T = ""
				c.V = strconv.FormatFloat(*value, 'f', 2, 64)
			} else {
				c.T = ""
				c.V = ""
			}
			c.IS = nil
			// err = f.removeFormula(c, ws, sheet)
			// if err != nil {
			// 	return err
			// }
		}
	}
	return nil
}
