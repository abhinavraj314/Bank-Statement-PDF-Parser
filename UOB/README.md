**# UOB Bank Statement Parser**



\## **Overview**

This script extracts transactions from \*\*UOB bank statement PDFs\*\* and exports them into a structured \*\*Excel (.xlsx)\*\* file.  

It supports:

\- Regular account statements

\- FX+ and multi-currency statements

\- Portfolio statements (simple and complex)

\- OCR for scanned PDFs (optional)



\## **Requirements**

\- Python 3.8+

\- Recommended libraries:

&nbsp; - `pandas`

&nbsp; - `numpy`

&nbsp; - `openpyxl`

&nbsp; - `tqdm`

&nbsp; - `dateutil`

&nbsp; - `PyMuPDF` (`fitz`) or `pdfplumber`

&nbsp; - `tabula-py` \*(optional)\*

&nbsp; - `camelot-py` \*(optional)\*

&nbsp; - `pytesseract` + `opencv-python` \*(optional, for OCR)\*

&nbsp; - `loguru` \*(optional, for logging)\*



Install with:

```bash

pip install pandas numpy openpyxl tqdm python-dateutil pymupdf pdfplumber tabula-py camelot-py opencv-python pytesseract loguru



**Usage**:



Basic command:



python uob14.py input.pdf -o output.xlsx



Options:



* input.pdf → Path to a single UOB statement PDF or a folder containing multiple PDFs



* -o, --output → Output Excel file (default: uob\_transactions.xlsx)



* --debug → Enable debug logging



Process a single statement:

python uob14.py UOB\_Statement.pdf -o uob\_statements.xlsx



Process a folder of statements:

python uob14.py SAMPLE\_PDFS -o uob\_statements\_all.xlsx 



To start excel file:
start uob\_statements.xlsx

start uob\_statements\_all.xlsx



**Output**:



Excel file with transactions in UOB Transactions sheet.



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



Summary sheet with:



* Total transactions



* Client name



* Processing method



* Date range



* Currencies used



**Notes**:



* Script automatically detects statement type (regular / portfolio / simple portfolio).



* FX conversion rates are extracted from the PDF if available; otherwise, default rates are used.



* If PDF format changes significantly, parsing logic may require adjustments.



