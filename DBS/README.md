**# DBS Bank Statement Parser**



\## **Overview**

This script extracts transactions from \*\*DBS bank statement PDFs\*\* and saves them into a structured \*\*Excel (.xlsx)\*\* file.  

It includes advanced features such as:

\- Robust header parsing and client name detection  

\- Transaction deduplication  

\- Balance B/F handling with statement month  

\- OCR support for scanned PDFs  

\- Consistent FX conversion rate extraction  



\## **Requirements**

\- Python 3.8+

\- Libraries:

&nbsp; - `pandas`

&nbsp; - `numpy`

&nbsp; - `python-dateutil`

&nbsp; - `openpyxl`

&nbsp; - `PyMuPDF` (`fitz`)

&nbsp; - `pdfplumber`

&nbsp; - `camelot-py`

&nbsp; - `tabula-py`

&nbsp; - `pytesseract` + `opencv-python` (optional, for OCR)



Install with:

```bash

pip install pandas numpy python-dateutil openpyxl pymupdf pdfplumber camelot-py tabula-py pytesseract opencv-python



**Usage**:



Run the script from the command line:



python dbs17.py input.pdf -o output.xlsx



Options



input.pdf → Path to a DBS statement PDF



-o, --output → Output Excel file (default: DBS\_<ClientName>\_Transactions\_<timestamp>.xlsx)



Process a single statement:

python dbs17.py DBS\_Statement.pdf -o dbs\_statements.xlsx



Process all PDFs in a folder:

python dbs17.py SAMPLE\_PDFS -o dbs\_statements\_all.xlsx



To start excel file:
start dbs\_statements.xlsx 

start dbs\_statements\_all.xlsx





**Output**:



Excel file with transactions in DBS Transactions sheet.



Columns include:



* Client Name



* Bank



* Account ID



* Date



* Currency



* Transaction Description



* Transaction Type



* Withdrawal



* Deposit



* Balance



* Conversion Rate



**Notes**:



Deduplication prevents duplicate entries across multiple PDFs.



Balance B/F rows are timestamped with the statement month.



If FX conversion rates are missing in the PDF, default rates are used.



Parsing rules may require updates if DBS changes its statement format.



