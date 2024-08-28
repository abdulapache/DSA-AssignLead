using OfficeOpenXml;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace WithdarwLead
{
    public class ExcelData
    {
        public static DataTable ConvertExcelToDataTable()
        {
            string excelFilePath = Environment.CurrentDirectory.ToString().Trim() + "\\Book.xlsx";
            DataTable dataTable = new DataTable();

            ExcelPackage.LicenseContext = OfficeOpenXml.LicenseContext.NonCommercial;

            using (ExcelPackage package = new ExcelPackage(new FileInfo(excelFilePath)))
            {
                ExcelWorksheet worksheet = package.Workbook.Worksheets[0];

                // Loop through the cells in the first row to create columns dynamically
                for (int col = worksheet.Dimension.Start.Column; col <= worksheet.Dimension.End.Column; col++)
                {
                    object cellValue = worksheet.Cells[1, col].Value;
                    string columnName = cellValue?.ToString() ?? $"Column{col}";
                    dataTable.Columns.Add(columnName);
                }

                // Start from the second row to read data
                for (int row = worksheet.Dimension.Start.Row + 1; row <= worksheet.Dimension.End.Row; row++)
                {
                    DataRow dataRow = dataTable.NewRow();

                    for (int col = worksheet.Dimension.Start.Column; col <= worksheet.Dimension.End.Column; col++)
                    {
                        object cellValue = worksheet.Cells[row, col].Value;
                        dataRow[col - 1] = cellValue;
                    }

                    dataTable.Rows.Add(dataRow);
                }
            }

            return dataTable;
        }
    }
}
