**# OCBC Bank Statement Parser**



\## **Overview**

This script extracts transactions from \*\*OCBC bank statement PDFs\*\* and outputs them into a structured \*\*Excel (.xlsx)\*\* file.

It supports:

\- \*\*Wealth reports\*\* (multi-account statements with FX rates)

\- \*\*Transaction history statements\*\* (account-level statements)

\- Automatic \*\*deduplication\*\* of transactions across multiple PDFs

\- \*\*Client name caching\*\* between statement types

\- \*\*Conversion rate extraction\*\* from wealth reports



\## Requirements

\- Python 3.8+

\- Libraries:

  - `pandas`

  - `openpyxl`

  - `pdfplumber`

  - `PyPDF2`

  - `python-dateutil`



Install dependencies:

```bash

pip install pandas openpyxl pdfplumber PyPDF2 python-dateutil



\*\***Usage**:\*\*



Run the script on a folder containing OCBC PDF statements:

python ocbc9.py ./statements/ -o ocbc\\\_transactions.xlsx



Options:



input\\\_folder → Folder containing PDF files



-o, --output → Output Excel file (default: ocbc\\\_transactions.xlsx)



Examples

Process a single statement:
python ocbc9.py ./statements/ -o ocbc\_transactions.xlsx



Process all statements in a folder:
python ocbc9.py SAMPLE\_PDFS -o ocbc\_statements\_all.xlsx


**Output**:



Excel file with transactions in Transactions sheet:



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



* Script runs in two passes:



* Process wealth reports first (to extract client names and conversion rates).



* Process transaction history PDFs, using cached names when available.



* Deduplication ensures no duplicate transactions are added.



* Statement month-year is used to assign dates for opening balances.



* If OCBC changes its PDF format, parsing rules may need updates.
