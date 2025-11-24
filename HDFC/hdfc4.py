#!/usr/bin/env python3
"""
HDFC Bank Statement PDF → Excel Transaction Extractor
Extracts transactions from HDFC bank statements and exports to Excel
"""

import os
import re
import sys
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Optional, Tuple
import argparse
import logging

import pdfplumber
import pandas as pd
from tqdm import tqdm
from dateutil import parser as date_parser
import openpyxl
from openpyxl.styles import Font, Alignment

logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)-8s | %(message)s', datefmt='%H:%M:%S')
logger = logging.getLogger(__name__)


class HDFCStatementExtractor:
    def __init__(self, debug: bool = False):
        self.debug = debug
        if debug:
            logger.setLevel(logging.DEBUG)
    
    def extract_from_pdf(self, pdf_path: str) -> List[Dict]:
        logger.info(f"Processing: {pdf_path}")
        try:
            with pdfplumber.open(pdf_path) as pdf:
                first_page = pdf.pages[0]
                text = first_page.extract_text()
                account_info = self._extract_account_info(text)
                logger.debug(f"Account Info: {account_info}")
                
                all_transactions = []
                for page_num, page in enumerate(pdf.pages, 1):
                    page_text = page.extract_text()
                    transactions = self._extract_transactions(page_text, account_info)
                    if transactions:
                        logger.debug(f"Page {page_num}: Found {len(transactions)} transactions")
                        all_transactions.extend(transactions)
                
                logger.info(f"Extracted {len(all_transactions)} transactions from {pdf_path}")
                return all_transactions
        except Exception as e:
            logger.error(f"Error processing {pdf_path}: {e}")
            if self.debug:
                import traceback
                traceback.print_exc()
            return []
    
    def _extract_account_info(self, text: str) -> Dict[str, str]:
        info = {"client_name": "", "account_id": "", "account_type": "", "currency": "INR"}
        
        for pattern in [r"AccountNo\s*:\s*(\d+)", r"Account\s*No\s*:\s*(\d+)"]:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                account_num = re.sub(r'\D', '', match.group(1).strip())
                if len(account_num) >= 10:
                    info["account_id"] = account_num
                    logger.debug(f"Found account number: {account_num}")
                    break
        
        for pattern in [r"MR\s+([A-Z][A-Za-z\s]+?)(?=\s+State\s*:|5AVENUE|\n)", r"MS\s+([A-Z][A-Za-z\s]+?)(?=\s+State\s*:|5AVENUE|\n)"]:
            match = re.search(pattern, text, re.MULTILINE)
            if match:
                name = re.sub(r'\s+', ' ', match.group(1).strip())
                info["client_name"] = "MR " + name if not name.startswith("MR") else name
                logger.debug(f"Found client name: {info['client_name']}")
                break
        
        type_match = re.search(r"AccountType\s*:\s*([^\n]+)", text, re.IGNORECASE)
        if type_match:
            info["account_type"] = type_match.group(1).strip()
        
        currency_match = re.search(r"Currency\s*:\s*([A-Z]{3})", text, re.IGNORECASE)
        if currency_match:
            info["currency"] = currency_match.group(1).strip()
        
        return info
    
    def _extract_transactions(self, text: str, account_info: Dict) -> List[Dict]:
        transactions = []
        lines = text.split('\n')
        
        transaction_start_idx = -1
        for i, line in enumerate(lines):
            if re.search(r"Date.*?Narration.*?Chq.*?Ref.*?No.*?Value.*?Dt.*?Withdrawal.*?Amt.*?Deposit.*?Amt.*?Closing.*?Balance", line, re.IGNORECASE):
                transaction_start_idx = i + 1
                logger.debug(f"Found transaction table at line {i}: {line}")
                break
        
        if transaction_start_idx == -1:
            logger.debug("No transaction table found in this page")
            return transactions
        
        i = transaction_start_idx
        current_transaction = None
        
        while i < len(lines):
            line = lines[i].strip()
            
            if "STATEMENTSUMMARY" in line.replace(" ", "") or "OpeningBalance" in line.replace(" ", ""):
                logger.debug(f"Reached summary section at line {i}, stopping")
                if current_transaction:
                    transactions.append(current_transaction)
                break
            
            if not line:
                i += 1
                continue
            
            date_match = re.match(r'^(\d{2}/\d{2}/\d{2,4})\s+(.+)$', line)
            
            if date_match:
                if current_transaction:
                    transactions.append(current_transaction)
                
                date_str = date_match.group(1)
                rest_of_line = date_match.group(2).strip()
                current_transaction = self._parse_transaction_line(date_str, rest_of_line, lines, i, account_info)
            elif current_transaction and line:
                if any(keyword in line.replace(" ", "").upper() for keyword in ["STATEMENTSUMMARY", "GENERATEDBY", "HDFCBANK"]):
                    if current_transaction:
                        transactions.append(current_transaction)
                        current_transaction = None
                    break
                
                # CHANGED: Skip reference number lines entirely
                # Skip lines that are:
                # 1. Pure numeric reference numbers (10+ digits): IB10121102966209, 0000000000001240
                # 2. Alphanumeric reference codes: IB followed by numbers
                # 3. Lines with only amounts (no description text)
                
                clean_line = line.strip()
                
                # Skip if line is a reference number pattern
                if re.match(r'^[A-Z]{0,2}\d{10,}$', clean_line):
                    # This is a reference number like "IB10121102966209" or "0000000000001240"
                    logger.debug(f"Skipping reference number line: {clean_line}")
                    i += 1
                    continue
                
                # Skip if line contains only two amounts (transaction amount + balance)
                if re.search(r'^\d{1,3}(?:,\d{3})*(?:\.\d{2})?\s+\d{1,3}(?:,\d{3})*(?:\.\d{2})?\s*$', line):
                    logger.debug(f"Skipping amounts-only line: {clean_line}")
                    i += 1
                    continue
                
                # Skip if line is just a single amount
                if re.match(r'^\d{1,3}(?:,\d{3})*(?:\.\d{2})?\s*$', clean_line):
                    logger.debug(f"Skipping single amount line: {clean_line}")
                    i += 1
                    continue
                
                # Include valid continuation text (description parts)
                if clean_line and len(clean_line) > 2 and "Generated" not in clean_line:
                    # Remove ONLY specific reference number patterns
                    # Pattern 1: IB followed by exactly 14 digits
                    clean_line = re.sub(r'\bIB\d{14}\b', '', clean_line)
                    # Pattern 2: All zeros or very repetitive patterns
                    clean_line = re.sub(r'\b0{10,}\d*\b', '', clean_line)
                    
                    clean_line = re.sub(r'\s+', ' ', clean_line).strip()
                    
                    if clean_line:  # Only add if something remains after cleaning
                        current_transaction["Transaction Description"] += " " + clean_line
                        logger.debug(f"Added continuation: {clean_line}")
            
            i += 1
        
        if current_transaction:
            transactions.append(current_transaction)
        
        return transactions
        
    def _parse_transaction_line(self, date_str: str, rest_of_line: str, all_lines: List[str], line_idx: int, account_info: Dict) -> Optional[Dict]:
        try:
            parsed_date = date_parser.parse(date_str, dayfirst=True)
            formatted_date = parsed_date.strftime("%d/%m/%Y")
        except:
            logger.warning(f"Could not parse date: {date_str}")
            return None
        
        amount_pattern = r'\b\d{1,3}(?:,\d{3})*(?:\.\d{2})?\b'
        amounts = re.findall(amount_pattern, rest_of_line)
        
        if not amounts:
            logger.debug(f"No amounts found in: {rest_of_line}")
            return None
        
        balance = amounts[-1]
        balance_idx = rest_of_line.rfind(balance)
        line_without_balance = rest_of_line[:balance_idx].strip()
        
        withdrawal = ""
        deposit = ""
        description_part = ""
        
        if len(amounts) >= 2:
            transaction_amount = amounts[-2]
            amount_idx = line_without_balance.rfind(transaction_amount)
            if amount_idx != -1:
                description_part = line_without_balance[:amount_idx].strip()
            else:
                description_part = line_without_balance
            
            is_credit = self._is_credit_transaction(description_part)
            
            if is_credit:
                deposit = transaction_amount
            else:
                withdrawal = transaction_amount
        else:
            description_part = line_without_balance
        
        description = description_part
        description = re.sub(r'\s+\d{2}/\d{2}/\d{2,4}\s*$', '', description).strip()
        
        # NEW: Remove ONLY standalone reference numbers (IB prefix or all zeros pattern)
        # Keep transaction reference numbers that are part of the description
        # Pattern 1: IB followed by exactly 14 digits (IB10121102966209)
        description = re.sub(r'\bIB\d{14}\b', '', description)
        # Pattern 2: Standalone reference numbers that are all zeros or very repetitive (0000000000001240)
        description = re.sub(r'\b0{10,}\d*\b', '', description)
        
        # Clean up extra whitespace
        description = re.sub(r'\s+', ' ', description).strip()
        
        if not description:
            logger.debug(f"Empty description after cleaning from: {rest_of_line}")
            return None
        
        transaction_type = self._classify_transaction(description)
        conversion_rate = "1" if account_info.get("currency") == "INR" else ""
        
        return {
            "Client Name": account_info.get("client_name", ""),
            "Bank": "HDFC BANK",
            "Account ID": account_info.get("account_id", ""),
            "Date": formatted_date,
            "Currency": account_info.get("currency", "INR"),
            "Transaction Description": description,
            "Transaction Type": transaction_type,
            "Withdrawal": withdrawal,
            "Deposit": deposit,
            "Balance": balance,
            "Conversion Rate": conversion_rate
        }
    def _is_credit_transaction(self, description: str) -> bool:
        credit_keywords = ["FT-CR", "FTCR", "CREDIT", "CR-", "NEFTRETURN", "NEFT RETURN", "INTERESTPAID", "INTEREST PAID", "INTERESTCREDIT", "INTEREST CREDIT", "REFUND", "REVERSAL", "DEPOSIT"]
        description_upper = description.upper()
        for keyword in credit_keywords:
            if keyword in description_upper:
                return True
        debit_keywords = ["FDTHROUGH", "FD THROUGH", "NEFTDR", "NEFT DR", "DEBIT", "DR-", "WITHDRAWAL", "ATM", "POS", "TAXDEDUCTED", "TAX DEDUCTED", "CHARGES", "TPT", "BILLPAY"]
        for keyword in debit_keywords:
            if keyword in description_upper:
                return False
        return False
    
    def _classify_transaction(self, description: str) -> str:
        desc_upper = description.upper()
        if any(keyword in desc_upper for keyword in ["FT-CR", "FTCR", "FT CR"]):
            return "Credit"
        if "NEFTRETURN" in desc_upper or "NEFT RETURN" in desc_upper:
            return "Credit"
        if "INTERESTPAID" in desc_upper or "INTEREST PAID" in desc_upper:
            return "Credit"
        if "FDTHROUGH" in desc_upper or "FD THROUGH" in desc_upper:
            return "Debit"
        if "NEFTDR" in desc_upper or "NEFT DR" in desc_upper:
            return "Debit"
        if "TAXDEDUCTED" in desc_upper or "TAX DEDUCTED" in desc_upper:
            return "Debit"
        if "ATM" in desc_upper:
            return "Debit"
        if "POS" in desc_upper:
            return "Debit"
        if "BILLPAY" in desc_upper:
            return "Debit"
        if "TPT" in desc_upper:
            return "Debit"
        if "CR-" in desc_upper or " CR" in desc_upper:
            return "Credit"
        if "DR-" in desc_upper or " DR" in desc_upper:
            return "Debit"
        return ""
    
    def process_folder(self, folder_path: str) -> List[Dict]:
        folder = Path(folder_path)
        if not folder.exists():
            logger.error(f"Folder not found: {folder_path}")
            return []
        pdf_files = list(folder.glob("*.pdf")) + list(folder.glob("*.PDF"))
        if not pdf_files:
            logger.warning(f"No PDF files found in {folder_path}")
            return []
        logger.info(f"Found {len(pdf_files)} PDF file(s)")
        all_transactions = []
        for pdf_file in tqdm(pdf_files, desc="Processing PDFs"):
            transactions = self.extract_from_pdf(str(pdf_file))
            all_transactions.extend(transactions)
        return all_transactions
    
    def export_to_excel(self, transactions: List[Dict], output_path: str):
        if not transactions:
            logger.warning("No transactions to export")
            return
        df = pd.DataFrame(transactions)
        df_deduplicated = df.drop_duplicates(subset=['Account ID', 'Date', 'Transaction Description', 'Balance'], keep='first')
        duplicates_removed = len(df) - len(df_deduplicated)
        if duplicates_removed > 0:
            logger.info(f"Removed {duplicates_removed} duplicate transaction(s)")
        df = df_deduplicated
        columns = ["Client Name", "Bank", "Account ID", "Date", "Currency", "Transaction Description", "Transaction Type", "Withdrawal", "Deposit", "Balance", "Conversion Rate"]
        df = df[columns]
        df['Account ID'] = df['Account ID'].astype(str)
        df = df.sort_values(['Account ID', 'Date']).reset_index(drop=True)
        logger.info(f"Writing {len(df)} transactions to {output_path}")
        with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name='Transactions', index=False)
            workbook = writer.book
            worksheet = writer.sheets['Transactions']
            for cell in worksheet[1]:
                cell.font = Font(bold=True)
                cell.alignment = Alignment(horizontal='center', vertical='center')
            for row in range(2, len(df) + 2):
                cell = worksheet.cell(row=row, column=3)
                cell.number_format = '@'
            column_widths = {'A': 20, 'B': 15, 'C': 18, 'D': 12, 'E': 10, 'F': 60, 'G': 15, 'H': 15, 'I': 15, 'J': 15, 'K': 15}
            for col_letter, width in column_widths.items():
                worksheet.column_dimensions[col_letter].width = width
        print(f"\n✓ Excel file created successfully: {output_path}")
        logger.info(f"Excel export complete")
        print(f"\nSummary:")
        print(f"  Total transactions: {len(df)}")
        print(f"  Accounts: {df['Account ID'].nunique()}")
        print(f"  Date range: {df['Date'].min()} to {df['Date'].max()}")
        for account_id in df['Account ID'].unique():
            account_txns = df[df['Account ID'] == account_id]
            print(f"  Account {account_id}: {len(account_txns)} transactions")


def main():
    parser = argparse.ArgumentParser(description="Extract transactions from HDFC Bank statement PDFs to Excel")
    parser.add_argument("input", help="Input PDF file or folder containing PDFs")
    parser.add_argument("-o", "--output", default="HDFC_Transactions.xlsx", help="Output Excel file path (default: HDFC_Transactions.xlsx)")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    args = parser.parse_args()
    extractor = HDFCStatementExtractor(debug=args.debug)
    input_path = Path(args.input)
    if input_path.is_file():
        transactions = extractor.extract_from_pdf(str(input_path))
    elif input_path.is_dir():
        transactions = extractor.process_folder(str(input_path))
    else:
        logger.error(f"Input not found: {args.input}")
        return 1
    if not transactions:
        logger.error("No transactions extracted")
        return 1
    extractor.export_to_excel(transactions, args.output)
    print(f"\n✓ Complete! Extracted {len(transactions)} transactions")
    return 0


if __name__ == "__main__":
    sys.exit(main())