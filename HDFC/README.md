# HDFC Bank Statement Parser

## **Overview**

This script extracts transactions from **HDFC Bank statement PDFs** and exports them into a structured **Excel (.xlsx)** file.

It supports:

- Automatic client name and account ID extraction  
- Clean handling of multiline transaction descriptions  
- Automatic transaction classification (Credit/Debit)  
- Deduplication of duplicate transactions  
- Smart skipping of reference numbers and summary sections  
- Supports both single PDF and folder-based batch processing  

---

## **Requirements**

- Python 3.8+
- Libraries:
  - `pandas`
  - `pdfplumber`
  - `python-dateutil`
  - `tqdm`
  - `openpyxl`

Install all dependencies with:

```bash
pip install pandas pdfplumber python-dateutil tqdm openpyxl

Usage

Run the script from the command line:
python hdfc4.py input.pdf -o output.xlsx

Options:

input.pdf → Path to a single HDFC Bank statement PDF

input_folder → Folder containing multiple PDFs to process

-o, --output → Output Excel file name (default: HDFC_Transactions.xlsx)

--debug → Enable verbose logging for troubleshooting

Examples

Process a single statement:
python hdfc4.py HDFC_Statement.pdf -o hdfc_statements.xlsx

Process all PDFs in a folder:
python hdfc4.py SAMPLE_PDFS -o hdfc_statements_all.xlsx

Open the resulting Excel file:
Invoke-Item "hfdc_statements.xlsx"

Output

Excel file containing all parsed transactions under the sheet “Transactions”.

Columns include:

Client Name

Bank

Account ID

Date

Currency

Transaction Description

Transaction Type

Withdrawal

Deposit

Balance

Conversion Rate

Notes:

Deduplication prevents duplicate entries across multiple PDFs.

Automatically ignores reference number lines and summary sections.

Descriptions spanning multiple lines are intelligently merged.

Supports both .pdf and .PDF file extensions.

If statement formats change, minor regex pattern updates may be needed.