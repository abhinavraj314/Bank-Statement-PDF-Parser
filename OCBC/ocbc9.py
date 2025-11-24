#!/usr/bin/env python3
"""
Fixed OCBC Bank Statement PDF Transaction Extractor
Based on proven patterns from DBS and UOB extractors
Handles OCBC wealth reports with proper transaction parsing
"""

import pdfplumber
import PyPDF2
import pandas as pd
import re
import argparse
from pathlib import Path
from datetime import datetime
from dateutil import parser as date_parser
import warnings
import hashlib

warnings.filterwarnings("ignore", category=UserWarning)

class TransactionDeduplicator:
    """Handles transaction deduplication logic"""
    
    def __init__(self):
        self.seen_transactions = set()
        self.transaction_hashes = set()
        
    def create_transaction_signature(self, transaction: dict) -> str:
        """Create a unique signature for a transaction"""
        key_fields = [
            transaction.get('Account ID', '').strip(),
            transaction.get('Date', '').strip(),
            transaction.get('Transaction Description', '').strip().upper(),
            transaction.get('Withdrawal', '').strip(),
            transaction.get('Deposit', '').strip(),
            transaction.get('Balance', '').strip(),
            transaction.get('Currency', '').strip()
        ]
        
        signature = '|'.join(field for field in key_fields if field)
        return signature
    
    def create_transaction_hash(self, transaction: dict) -> str:
        """Create a hash for fast duplicate detection"""
        signature = self.create_transaction_signature(transaction)
        return hashlib.md5(signature.encode()).hexdigest()
    
    def is_duplicate(self, transaction: dict) -> bool:
        """Check if transaction is a duplicate"""
        transaction_hash = self.create_transaction_hash(transaction)
        
        if transaction_hash in self.transaction_hashes:
            return True
        
        signature = self.create_transaction_signature(transaction)
        normalized_sig = re.sub(r'\s+', ' ', signature.strip())
        
        if normalized_sig in self.seen_transactions:
            return True
        
        self.transaction_hashes.add(transaction_hash)
        self.seen_transactions.add(normalized_sig)
        
        return False

class OCBCTransactionExtractor:
    def __init__(self):
        self.conversion_rates = {}
        self.client_name = ""
        self.bank = "OCBC"
        self.report_date = ""
        self.deduplicator = TransactionDeduplicator()
        self.processed_sections = set()
        self.account_name_cache = {}  # Cache account->name mappings

    def log(self, message, level="INFO"):
        """Simple logging function"""
        print(f"  [{level}] {message}")
    
    def extract_text_pdfplumber(self, pdf_path):
        """Extract text using pdfplumber"""
        try:
            with pdfplumber.open(pdf_path) as pdf:
                full_text = ""
                for page in pdf.pages:
                    text = page.extract_text()
                    if text:
                        full_text += text + "\n"
                return full_text
        except Exception as e:
            self.log(f"pdfplumber extraction failed: {e}", "ERROR")
            return None
    
    def extract_text_pypdf2(self, pdf_path):
        """Fallback text extraction using PyPDF2"""
        try:
            with open(pdf_path, 'rb') as file:
                reader = PyPDF2.PdfReader(file)
                full_text = ""
                for page in reader.pages:
                    text = page.extract_text()
                    if text:
                        full_text += text + "\n"
                return full_text
        except Exception as e:
            self.log(f"PyPDF2 extraction failed: {e}", "ERROR")
            return None
    def cache_account_name(self, account_id, client_name):
        """Cache the mapping between account ID and client name"""
        if account_id and client_name:
            # Store both with and without hyphens for flexibility
            clean_account = account_id.replace('-', '')
            self.account_name_cache[clean_account] = client_name
            self.account_name_cache[account_id] = client_name
            self.log(f"Cached name '{client_name}' for account {account_id}")

    def get_cached_name_for_account(self, account_id):
        """Get cached client name for an account ID"""
        if not account_id:
            return None
        
        # Try exact match first
        if account_id in self.account_name_cache:
            return self.account_name_cache[account_id]
        
        # Try without hyphens
        clean_account = account_id.replace('-', '')
        if clean_account in self.account_name_cache:
            return self.account_name_cache[clean_account]
        
        # Try with hyphens if it doesn't have them
        if '-' not in account_id and len(account_id) >= 9:
            # Format: 596054106001 -> 596-054106-001
            formatted_account = f"{account_id[:3]}-{account_id[3:9]}-{account_id[9:]}"
            if formatted_account in self.account_name_cache:
                return self.account_name_cache[formatted_account]
        
        return None
    def extract_conversion_rates(self, text):
        """Extract conversion rates from the PDF text - ENHANCED"""
        rates = {'SGD': 1.0}
        
        if not text:
            return rates
        
        lines = text.split('\n')
        
        # Look for the exchange rate section
        for i, line in enumerate(lines):
            if any(phrase in line.lower() for phrase in ['exchange rate', 'ccy/sgd', 'code ccy']):
                # Look at the next few lines for currency rates
                for j in range(i, min(i + 15, len(lines))):
                    rate_line = lines[j]
                    
                    # Pattern: AUD 0.8352 CHF 1.5816
                    pairs = re.findall(r'([A-Z]{3})\s+([0-9]+\.[0-9]+)', rate_line)
                    for currency, rate in pairs:
                        if currency in ['AUD', 'CHF', 'EUR', 'GBP', 'USD', 'JPY', 'CAD', 'NZD']:
                            rates[currency] = float(rate)
                
                # Also check for vertical format
                if 'USD' in line and re.search(r'[0-9]+\.[0-9]+', line):
                    usd_match = re.search(r'USD\s+([0-9]+\.[0-9]+)', line)
                    if usd_match:
                        rates['USD'] = float(usd_match.group(1))
        
        # Also look for the rate table format in summary section
        in_rate_section = False
        for line in lines:
            if 'Code' in line and 'CCY/SGD' in line:
                in_rate_section = True
                continue
            if in_rate_section:
                # Match currency codes with rates
                rate_match = re.match(r'^([A-Z]{3})\s+([0-9]+\.[0-9]+)', line.strip())
                if rate_match:
                    currency, rate = rate_match.groups()
                    if currency in ['AUD', 'CHF', 'EUR', 'GBP', 'USD', 'JPY', 'CAD', 'NZD']:
                        rates[currency] = float(rate)
                elif line.strip() == '' or 'Valuations' in line:
                    # End of rate section
                    break
        
        # Clean up invalid rates
        rates = {k: v for k, v in rates.items() if v > 0 and k.isalpha() and len(k) == 3}
        
        self.log(f"Extracted conversion rates: {rates}")
        return rates
    
    def extract_client_info(self, text):
        """Extract client name and report date"""
        if not text:
            return
        
        lines = text.split('\n')
        
        # Extract client name
        for line in lines:
            if 'Wealth report for' in line:
                name_match = re.search(r'Wealth report for\s+([A-Z][A-Z\s]+)', line)
                if name_match:
                    self.client_name = name_match.group(1).strip()
                    break
        
        # If not found, look for standalone name lines
        if not self.client_name:
            for line in lines:
                line = line.strip()
                if re.match(r'^[A-Z][A-Z\s]{10,}$', line) and 'SINGAPORE' not in line:
                    self.client_name = line
                    break
        # Extract client name from Transaction History format (if not already found)
        # Extract client name from Transaction History format (if not already found)
        if not self.client_name:
            for line in lines:
                line = line.strip()
                # Skip common header patterns
                if any(skip in line.upper() for skip in [
                    'TRANSACTION HISTORY', 'OCBC', 'PREMIER', 'GLOBAL SAVINGS', 
                    'AVAILABLE BALANCE', 'LEDGER BALANCE', 'VALUE DATE', 'DESCRIPTION',
                    'DEBIT', 'CREDIT', 'ACCOUNT', '360 ACCOUNT'
                ]):
                    continue
                
                # Look for proper name patterns (multiple capital letters with spaces)
                if (re.match(r'^[A-Z][A-Z\s]{8,}$', line) and 
                    len(line.split()) >= 2 and 
                    not re.search(r'\d', line)):
                    self.client_name = line
                    self.log(f"Extracted client name from transaction history: {line}")
                    break
        # Extract report date
        for line in lines:
            date_match = re.search(r'as of\s+(\d{1,2}\s+\w+\s+\d{4})', line)
            if date_match:
                self.report_date = date_match.group(1)
                break
        
        self.log(f"Client: {self.client_name}, Date: {self.report_date}")

    def extract_statement_period_month_year(self, text):
        """Extract statement period and return month-year format for opening balance dating"""
        if not text:
            return None
        
        lines = text.split('\n')
        
        # Look for statement period patterns
        period_patterns = [
            r'(?:for the period|period:|statement period:?)\s+(\d{1,2}\s+\w+\s+\d{4})\s*[-–—]\s*(\d{1,2}\s+\w+\s+\d{4})',
            r'(?:from|period:|statement period:?)\s+(\d{1,2}\s+\w+\s+\d{4})\s+to\s+(\d{1,2}\s+\w+\s+\d{4})',
            r'(\d{1,2}\s+\w+\s+\d{4})\s*[-–—]\s*(\d{1,2}\s+\w+\s+\d{4})',
            r'(\d{1,2}\s+\w+\s+\d{4})\s+to\s+(\d{1,2}\s+\w+\s+\d{4})',
            r'from\s+(\d{1,2}\s+\w+\s+\d{4})',
            r'statement for\s+(\w+\s+\d{4})',
            r'for the period\s+(\d{1,2}\s+\w+\s+\d{4})'
        ]
        
        for line in lines:
            for pattern in period_patterns:
                match = re.search(pattern, line, re.IGNORECASE)
                if match:
                    # Get the start date (first group) for opening balance
                    start_date = match.group(1)
                    
                    # Extract month and year from the start date
                    month_year_match = re.search(r'(\w+)\s+(\d{4})', start_date)
                    if month_year_match:
                        month = month_year_match.group(1)
                        year = month_year_match.group(2)
                        result = f"{month} {year}"
                        self.log(f"Found statement period month-year: {result}")
                        return result
        
        # Fallback: look for any date in the header area (first 50 lines) and extract month-year
        for i, line in enumerate(lines[:50]):
            date_match = re.search(r'(\d{1,2}\s+(\w+)\s+(\d{4}))', line)
            if date_match and not any(skip in line.lower() for skip in ['copyright', 'page', 'printed']):
                month = date_match.group(2)
                year = date_match.group(3)
                fallback_result = f"{month} {year}"
                self.log(f"Using fallback statement month-year: {fallback_result}")
                return fallback_result
        
        return None
    def detect_ocbc_pdf_type(self, text):
        """Detect which type of OCBC PDF this is"""
        if not text:
            return 'unknown'
        
        lines = text.split('\n')
        
        # Check first 50 lines for format indicators
        for line in lines[:50]:
            line = line.strip()
            
            # Transaction History format
            if 'Transaction History' in line:
                return 'transaction_history'
            
            # Wealth report format
            if any(indicator in line for indicator in [
                'Wealth report for', 'Transaction activity', 'Cash and deposits',
                'OCBC PREMIER PRIVATE CLIENT'
            ]):
                return 'wealth_report'
        
        # Fallback detection based on content patterns
        text_upper = text.upper()
        if 'AVAILABLE BALANCE' in text_upper and 'LEDGER BALANCE' in text_upper:
            return 'transaction_history'
        elif 'TRANSACTION ACTIVITY' in text_upper and 'CASH AND DEPOSITS' in text_upper:
            return 'wealth_report'
        
        return 'unknown'
    def parse_transaction_history_format(self, text):
        """Parse the new Transaction History OCBC format"""
        transactions = []
        lines = text.split('\n')
        
        current_account = None
        current_currency = 'SGD'  # Default for this format
        
        # Get default year
        year_matches = re.findall(r'\b(20\d{2})\b', text)
        default_year = int(max(year_matches)) if year_matches else 2025
        
        i = 0
        while i < len(lines):
            line = lines[i].strip()
            
            # Enhanced account number extraction - handle multiple formats
            # Enhanced account number extraction - handle multiple OCBC formats
            account_patterns = [
                r'Account\s+([\d-]+)',                        # Generic "Account 596-054106-001"
                r'Premier Global Savings Account\s+([\d-]+)', # "Premier Global Savings Account 601-264112-201"
                r'Premier EasiSave\s+([\d-]+)',               # "Premier EasiSave 601-291784-001"
                r'360 Account\s+([\d-]+)',                    # "360 Account 596-054106-001"
            ]

            
            account_found = False
            for pattern in account_patterns:
                account_match = re.search(pattern, line)
                if account_match:
                    # Remove hyphens to match existing format
                    current_account = account_match.group(1).replace('-', '')
                    self.log(f"Found transaction history account: {current_account}")
                    account_found = True
                    break
            
            if account_found:
                i += 1
                continue
            
            # Enhanced transaction data line matching - handle multiple formats
            transaction_patterns = [
                r'^(\d{2}/\d{2}/\d{4})\s+(\d{2}/\d{2}/\d{4})\s+(.+)',  # Full format with value date
                r'^(\d{2}/\d{2}/\d{4})\s+(.+)',  # Date only format
            ]

            transaction_found = False
            transaction_date = None
            remaining_content = None
            
            for pattern in transaction_patterns:
                transaction_match = re.match(pattern, line)
                if transaction_match:
                    if len(transaction_match.groups()) == 3:
                        # Full format with value date
                        transaction_date = transaction_match.group(1)
                        value_date = transaction_match.group(2)
                        remaining_content = transaction_match.group(3).strip()
                    else:
                        # Date only format
                        transaction_date = transaction_match.group(1)
                        remaining_content = transaction_match.group(2).strip()
                    
                    transaction_found = True
                    break

            if transaction_found:
                self.log(f"Processing transaction history entry: {transaction_date} - {remaining_content[:50]}...")
                
                # Parse the date (DD/MM/YYYY format)
                try:
                    parsed_date = datetime.strptime(transaction_date, '%d/%m/%Y').strftime('%Y-%m-%d')
                except:
                    self.log(f"Could not parse date: {transaction_date}", "WARNING")
                    i += 1
                    continue
                
                # Parse amounts and description
                # Look for debit/credit amounts at the end of the line
                amounts = re.findall(r'([0-9,]+\.[0-9]{2})', remaining_content)
                
                withdrawal = ""
                deposit = ""
                
                if amounts:
                    # For transaction history format, typically last amount is the transaction amount
                    amount = amounts[-1]
                    
                    # Remove amount from description
                    # Remove amount from description
                    description = re.sub(r'\s*[0-9,]+\.[0-9]{2}\s*$', '', remaining_content).strip()

                    # Look ahead for continuation lines (extra description parts)
                    j = i + 1
                    continuation_lines = []
                    while j < len(lines):
                        next_line = lines[j].strip()
                        # Stop if next line looks like a new transaction or account header
                        if re.match(r'^\d{2}/\d{2}/\d{4}', next_line) or any(keyword in next_line for keyword in ["Account", "Premier", "360 Account"]):
                            break
                        # Stop if next line is empty
                        if not next_line:
                            break
                        continuation_lines.append(next_line)
                        j += 1

                    # Merge continuation lines into description
                    if continuation_lines:
                        description = description + " " + " ".join(continuation_lines)

                    # Clean description
                    description = re.sub(r'\s+', ' ', description).strip()

                    # Determine if it's debit or credit based on description
                    if any(word in description.upper() for word in ['INTEREST CREDIT', 'CREDIT']):
                        deposit = amount
                    else:
                        withdrawal = amount

                    # Move outer loop index forward past continuation lines
                    i = j - 1

                else:
                    description = remaining_content
                
                # Clean description
                description = re.sub(r'\s+', ' ', description).strip()
                
                # Skip if description is too short or meaningless
                if not description or len(description) < 3:
                    i += 1
                    continue
                
                # Get conversion rate
                conversion_rate = self.conversion_rates.get(current_currency, 1.0)

                # Try to get client name from cache based on account ID
                cached_name = self.get_cached_name_for_account(current_account)
                client_name_to_use = cached_name or self.client_name or 'Unknown'
                
                # Don't use "TRANSACTION HISTORY DETAILS" as a valid name
                if client_name_to_use == 'TRANSACTION HISTORY DETAILS':
                    client_name_to_use = 'Unknown'

                transaction = {
                    'Client Name': client_name_to_use,  # Use the resolved name
                    'Bank': self.bank,
                    'Account ID': current_account or 'Unknown',
                    'Date': parsed_date,
                    'Currency': current_currency,
                    'Transaction Description': description,
                    'Transaction Type': self.classify_transaction(description),
                    'Withdrawal': self.clean_amount(withdrawal),
                    'Deposit': self.clean_amount(deposit),
                    'Balance': '',  # Transaction history format doesn't show running balance
                    'Conversion Rate': f"{conversion_rate:.4f}"
                }
                
                # Cache this mapping for future use (only if valid name)
                if current_account and client_name_to_use not in ['Unknown', 'TRANSACTION HISTORY DETAILS']:
                    self.cache_account_name(current_account, client_name_to_use)

                if not self.deduplicator.is_duplicate(transaction):
                    transactions.append(transaction)
                    self.log(f"Valid transaction history entry: {description[:50]}...")
                else:
                    self.log(f"Skipped duplicate transaction: {description[:30]}...")
            
            i += 1
        
        return transactions
    def extract_statement_period(self, text):
        """Extract statement period from PDF for opening balance dating"""
        if not text:
            return None
        
        lines = text.split('\n')
        
        # Look for statement period patterns
        period_patterns = [
            r'(?:From|Period:|Statement period:?)\s+(\d{1,2}\s+\w+\s+\d{4})\s+to\s+(\d{1,2}\s+\w+\s+\d{4})',
            r'(\d{1,2}\s+\w+\s+\d{4})\s+to\s+(\d{1,2}\s+\w+\s+\d{4})',
            r'From\s+(\d{1,2}\s+\w+\s+\d{4})',
            r'Statement for\s+(\w+\s+\d{4})',
            r'For the period\s+(\d{1,2}\s+\w+\s+\d{4})'
        ]
        
        for line in lines:
            for pattern in period_patterns:
                match = re.search(pattern, line, re.IGNORECASE)
                if match:
                    # Return the start date (first group) for opening balance
                    start_date = match.group(1)
                    self.log(f"Found statement period start: {start_date}")
                    return self.parse_date(start_date)
        
        # Fallback: look for any date in the header area (first 50 lines)
        for i, line in enumerate(lines[:50]):
            date_match = re.search(r'(\d{1,2}\s+\w+\s+\d{4})', line)
            if date_match and not any(skip in line.lower() for skip in ['copyright', 'page', 'printed']):
                fallback_date = date_match.group(1)
                self.log(f"Using fallback statement date: {fallback_date}")
                return self.parse_date(fallback_date)
        
        return None

    def parse_date(self, date_str, default_year=2025):
        """Enhanced date parsing for OCBC formats"""
        if not date_str or str(date_str).strip().lower() in ['nan', 'none', '']:
            return None
            
        date_str = str(date_str).strip()
        
        # Add year if missing (OCBC format is often "30 Apr")
        if not re.search(r'\d{4}', date_str):
            date_str = f"{date_str} {default_year}"
        
        # OCBC common patterns
        patterns = [
            (r'(\d{1,2})\s+(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+(\d{4})', 'dd-mmm-yyyy'),
            (r'(\d{1,2})/(\d{1,2})/(\d{4})', 'dd/mm/yyyy'),
            (r'(\d{1,2})-(\d{1,2})-(\d{4})', 'dd-mm-yyyy'),
        ]
        
        month_map = {
            'Jan': '01', 'Feb': '02', 'Mar': '03', 'Apr': '04',
            'May': '05', 'Jun': '06', 'Jul': '07', 'Aug': '08',
            'Sep': '09', 'Oct': '10', 'Nov': '11', 'Dec': '12'
        }
        
        for pattern, format_type in patterns:
            match = re.search(pattern, date_str, re.IGNORECASE)
            if match:
                if format_type == 'dd-mmm-yyyy':
                    day = match.group(1).zfill(2)
                    month = month_map.get(match.group(2), '01')
                    year = match.group(3)
                    return f"{year}-{month}-{day}"
                elif format_type in ['dd/mm/yyyy', 'dd-mm-yyyy']:
                    day = match.group(1).zfill(2)
                    month = match.group(2).zfill(2)
                    year = match.group(3)
                    return f"{year}-{month}-{day}"
        
        # Try dateutil as fallback
        try:
            parsed_date = date_parser.parse(date_str, dayfirst=True)
            if parsed_date:
                return parsed_date.strftime('%Y-%m-%d')
        except:
            pass
        
        return None

    def clean_amount(self, amount_str):
        """Clean and normalize amount strings"""
        if not amount_str or str(amount_str).strip() in ['', 'nan', 'None', '-']:
            return ""
        
        # Remove currency symbols and clean
        cleaned = re.sub(r'[^\d.,()-]', '', str(amount_str))
        # Handle parentheses for negative amounts
        if '(' in str(amount_str) and ')' in str(amount_str):
            cleaned = '-' + cleaned.replace('(', '').replace(')', '')
        
        return cleaned.strip()

    def classify_transaction(self, description):
        """Classify transaction type based on description"""
        if not description:
            return 'Unknown'
        
        desc_lower = description.lower()
        
        if 'interest credit' in desc_lower:
            return 'Interest Credit'
        elif 'fund transfer' in desc_lower:
            return 'Fund Transfer'
        elif any(word in desc_lower for word in ['withdrawal', 'withdraw']):
            return 'Withdrawal'
        elif any(word in desc_lower for word in ['deposit', 'credit']) and 'interest' not in desc_lower:
            return 'Deposit'
        elif any(word in desc_lower for word in ['payment', 'pay']):
            return 'Payment'
        elif any(word in desc_lower for word in ['fee', 'charge']):
            return 'Fee'
        else:
            return 'Other'

    def is_valid_transaction_line(self, line):
        """Check if a line contains a valid transaction (starts with date) or is an opening balance"""
        line = line.strip()
        
        # Skip empty lines
        if not line:
            return False
        
        # Allow opening balance lines
        if re.match(r'^Opening balance\b', line, re.IGNORECASE):
            return True
        
        # Skip other common non-transaction patterns
        skip_patterns = [
            r'^Total withdrawal',
            r'^Total deposits',
            r'^Total interest',
            r'^Ending balance',
            r'^Average balance',
            r'^Some changes',
            r'^REVISION OF',
            r'^From \d+ \w+ \d{4}',
            r'^\d{4} Some changes',
            r'^Important notes',
            r'^What you have',
            r'^Value\s+date',
            r'^Description',
            r'^Withdrawal',
            r'^Deposit',
            r'^Balance',
            r'^\w+ ACCOUNT:',
            r'PREMIER .+ ACCOUNT:',
            r'^\w{3}$',  # Currency codes alone
            r'^Code\s+CCY',
            r'^Total$'
        ]
        
        for pattern in skip_patterns:
            if re.match(pattern, line, re.IGNORECASE):
                return False
        
        # Must start with a date pattern (e.g., "30 Apr", "09 Apr", "31 May")
        date_pattern = r'^(\d{1,2})\s+(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+'
        return bool(re.match(date_pattern, line, re.IGNORECASE))

    def detect_transaction_currency(self, lines, current_line_index):
        """Detect the currency for a transaction by looking at table headers - ENHANCED"""
        # Look backwards from current line to find currency table header
        for back_i in range(current_line_index, max(0, current_line_index - 30), -1):
            line = lines[back_i].strip()
            
            # Check for currency table headers - ENHANCED PATTERNS
            currency_header_patterns = [
                r'^([A-Z]{3})\s+Value\s+date\s+Description',  # "USD Value date Description"
                r'^([A-Z]{3})\s+Description\s+Account',       # "USD Description Account number"  
                r'^([A-Z]{3})\s+Account\s+number',            # "EUR Account number"
                r'^([A-Z]{3})\s+Withdrawal\s+Deposit',        # "USD Withdrawal Deposit Balance"
                r'^([A-Z]{3})\s+.*(?:Withdrawal|Deposit|Balance)', # Currency followed by transaction columns
            ]
            
            for pattern in currency_header_patterns:
                match = re.match(pattern, line, re.IGNORECASE)
                if match:
                    currency = match.group(1).upper()
                    if currency in ['SGD', 'USD', 'EUR', 'GBP', 'AUD', 'CHF', 'JPY', 'CAD', 'NZD']:
                        self.log(f"Detected currency from header at line {back_i}: {currency}")
                        return currency
            
            # Check for standalone currency lines (like just "USD" on a line)
            if re.match(r'^[A-Z]{3}$', line):
                currency = line.upper()
                if currency in ['SGD', 'USD', 'EUR', 'GBP', 'AUD', 'CHF', 'JPY', 'CAD', 'NZD']:
                    # Look ahead to see if this is followed by table headers
                    for ahead_i in range(back_i + 1, min(len(lines), back_i + 5)):
                        next_line = lines[ahead_i].strip()
                        if any(header in next_line for header in ['Value date', 'Description', 'Withdrawal', 'Deposit', 'Balance']):
                            self.log(f"Detected currency from standalone line at {back_i}: {currency}")
                            return currency
            
            # Also check for underlined currency headers (sometimes appear as "USD" with underline chars)
            underlined_match = re.match(r'^([A-Z]{3})\s*[-_=]{3,}', line)
            if underlined_match:
                currency = underlined_match.group(1).upper()
                if currency in ['SGD', 'USD', 'EUR', 'GBP', 'AUD', 'CHF', 'JPY', 'CAD', 'NZD']:
                    self.log(f"Detected underlined currency header: {currency}")
                    return currency
        
        # Default to SGD if no currency header found
        self.log("No currency header found, defaulting to SGD", "WARNING")
        return 'SGD'

    def clean_transaction_description(self, description):
        """Clean transaction description by removing footer/header contamination"""
        if not description:
            return description
        
        # Remove common footer/header patterns
        cleanup_patterns = [
            r'\s+Wealth report for.*$',
            r'\s+OCBC PREMIER.*$', 
            r'\s+Private Client.*$',
            r'\s+Limited Corporation.*$',
            r'\s+Banking.*$',
            r'\s+\d+\s+of\s+\d+.*$',
            r'\s+detimiL.*$'
        ]
        
        cleaned = description
        for pattern in cleanup_patterns:
            cleaned = re.sub(pattern, '', cleaned, flags=re.IGNORECASE)
        
        # Remove extra whitespace
        cleaned = re.sub(r'\s+', ' ', cleaned).strip()
        
        return cleaned
    def parse_transaction_section(self, text):
        """Parse transaction data from OCBC text with enhanced pattern matching"""
        transactions = []
        lines = text.split('\n')
        
        current_account = None
        current_currency = None
        in_transaction_section = False
        
        # Get default year and extract statement period for opening balance
        year_matches = re.findall(r'\b(20\d{2})\b', text)
        default_year = int(max(year_matches)) if year_matches else 2025
        statement_period_month_year = self.extract_statement_period_month_year(text)
        
        i = 0
        while i < len(lines):
            line = lines[i].strip()
            
            # Detect transaction section start
            if 'Transaction activity' in line and 'Cash and deposits' in line:
                in_transaction_section = True
                self.log("Found transaction activity section")
                i += 1
                continue
            
            if not in_transaction_section:
                i += 1
                continue
            
            # Stop at next major section
            if any(section in line for section in ['Important notes', 'What you have', 'Some changes']):
                break
            
            
            # Extract account number from account headers
            account_match = re.search(r'([A-Z\s]+):\s*(\d{12})', line)
            if account_match:
                current_account = account_match.group(2)
                current_currency = None  # Reset currency when new account section starts
                self.log(f"Found account: {current_account}")
                i += 1
                continue

            # Detect currency section headers early
            if re.match(r'^[A-Z]{3}$', line) and line in ['SGD', 'USD', 'EUR', 'GBP', 'AUD', 'CHF', 'JPY', 'CAD', 'NZD']:
                current_currency = line
                self.log(f"Found currency section: {current_currency}")
                i += 1
                continue
            
            # ENHANCED: Only process lines that start with valid transaction dates or are opening balance
            if not self.is_valid_transaction_line(line):
                i += 1
                continue
            
            # Handle Opening Balance lines specially
            if re.match(r'^Opening balance\b', line, re.IGNORECASE):
                self.log(f"Processing opening balance: '{line}'")
                
                # Use statement period month-year for opening balance
                parsed_date = statement_period_month_year
                if not parsed_date:
                    # Fallback to default year/month
                    parsed_date = f"January {default_year}"
                    self.log(f"Using fallback month-year for opening balance: {parsed_date}", "WARNING")
                
                # Extract amounts from opening balance line
                amounts_in_line = re.findall(r'([0-9,]+\.[0-9]{2})', line)
                
                # Look ahead for continuation lines with amounts
                j = i + 1
                while j < len(lines) and j < i + 3:
                    next_line = lines[j].strip()
                    if not next_line or self.is_valid_transaction_line(next_line):
                        break
                    amounts = re.findall(r'([0-9,]+\.[0-9]{2})', next_line)
                    if amounts:
                        amounts_in_line.extend(amounts)
                    j += 1
                
                # Detect currency for opening balance
                transaction_currency = self.detect_transaction_currency(lines, i)                
                # Find current account if not set
                if not current_account:
                    for back_i in range(max(0, i-25), min(len(lines), i+5)):
                        back_line = lines[back_i].strip()
                        acc_match = re.search(r'(\d{12})', back_line)
                        if acc_match:
                            current_account = acc_match.group(1)
                            break
                
                # Create opening balance transaction
                if amounts_in_line:
                    balance_amount = amounts_in_line[-1]  # Last amount is usually the balance
                    conversion_rate = self.conversion_rates.get(transaction_currency, 1.0)
                    
                    transaction = {
                        'Client Name': self.client_name,
                        'Bank': self.bank,
                        'Account ID': current_account or 'Unknown',
                        'Date': parsed_date,
                        'Currency': transaction_currency,
                        'Transaction Description': 'Opening Balance',
                        'Transaction Type': 'Opening Balance',
                        'Withdrawal': '',
                        'Deposit': '',
                        'Balance': self.clean_amount(balance_amount),
                        'Conversion Rate': f"{conversion_rate:.4f}" if conversion_rate else "1.0000"
                    }
                    
                    if not self.deduplicator.is_duplicate(transaction):
                        transactions.append(transaction)
                        self.log(f"Valid opening balance: {balance_amount} {transaction_currency} (Date: {parsed_date})")
                        
                        # Cache the account-name mapping for future use
                        if transaction.get('Account ID') and transaction.get('Client Name'):
                            self.cache_account_name(transaction['Account ID'], transaction['Client Name'])
                
                i = j
                continue
            
            # Extract date and remaining content for regular transactions
            date_match = re.match(r'^(\d{1,2}\s+\w+)\s+(.+)', line)
            if not date_match:
                i += 1
                continue
            
            date_str = date_match.group(1).strip()
            remaining_line = date_match.group(2).strip()
            
            self.log(f"Processing transaction: '{date_str}' - '{remaining_line[:50]}...'")
            
            # Parse date
            parsed_date = self.parse_date(date_str, default_year)
            if not parsed_date:
                self.log(f"Could not parse date: {date_str}", "WARNING")
                i += 1
                continue
            
            # Detect transaction currency
            # Detect transaction currency from table headers
            transaction_currency = self.detect_transaction_currency(lines, i)
            
            if not current_account:
                for back_i in range(max(0, i-25), min(len(lines), i+5)):
                    back_line = lines[back_i].strip()
                    acc_match = re.search(r'(\d{12})', back_line)
                    if acc_match:
                        current_account = acc_match.group(1)
                        break
            
            # Extract description and amounts
            full_description = remaining_line
            withdrawal = ""
            deposit = ""
            balance = ""
            
            # Look for amounts in the current line and following lines
            amounts_in_line = re.findall(r'([0-9,]+\.[0-9]{2})', line)
            
            # Look ahead for continuation lines and amounts
            # Look ahead for continuation lines and amounts
            j = i + 1
            continuation_lines = []

            while j < len(lines) and j < i + 5:  # Reduced from 8 to 5 lines
                next_line = lines[j].strip()
                
                if not next_line:
                    j += 1
                    continue
                
                # Stop if we hit another valid transaction date or section
                if self.is_valid_transaction_line(next_line):
                    break
                if 'Total withdrawal' in next_line or 'Ending balance' in next_line:
                    break
                
                # ENHANCED: Stop if we hit footer/header content
                if any(stop_phrase in next_line for stop_phrase in [
                    'Wealth report for', 'OCBC PREMIER', 'Private Client', 
                    'Limited Corporation', 'Banking', 'of 1', 'detimiL'
                ]):
                    break
                
                # ENHANCED: Stop if line contains typical footer patterns
                if re.search(r'\d+\s+of\s+\d+', next_line) or 'Corporation' in next_line:
                    break
                
                # Look for amounts in continuation line
                amounts = re.findall(r'([0-9,]+\.[0-9]{2})', next_line)
                if amounts:
                    amounts_in_line.extend(amounts)
                
                # ENHANCED: Check if this is a valid continuation with stricter rules
                if (len(next_line) > 3 and 
                    any(char.isalpha() for char in next_line) and 
                    len(next_line) < 80 and  # Reduced from 100 to 80
                    not re.match(r'^(Balance|Total|Withdrawal|Deposit|OCBC|Wealth)', next_line, re.IGNORECASE) and
                    not any(bad_pattern in next_line.upper() for bad_pattern in [
                        'PREMIER', 'PRIVATE CLIENT', 'CORPORATION', 'BANKING', 'WEALTH REPORT'
                    ])):
                    continuation_lines.append(next_line)
                
                j += 1
            
            # Combine description
            if continuation_lines:
                full_description = (full_description + " " + " ".join(continuation_lines)).strip()
            
            full_description = re.sub(r'\s+', ' ', full_description).strip()
            full_description = self.clean_transaction_description(full_description)
            
            # Parse amounts based on transaction context
            if amounts_in_line:
                amounts = [float(amt.replace(',', '')) for amt in amounts_in_line]
                
                # Classify amounts based on description and position
                if 'INTEREST CREDIT' in full_description.upper():
                    if len(amounts) >= 1:
                        deposit = str(amounts[0])
                        if len(amounts) >= 2:
                            balance = str(amounts[-1])
                elif 'FUND TRANSFER' in full_description.upper():
                    if len(amounts) >= 1:
                        withdrawal = str(amounts[0])
                        if len(amounts) >= 2:
                            balance = str(amounts[-1])
                else:
                    # General case: if multiple amounts, last is usually balance
                    if len(amounts) >= 2:
                        # Try to determine if it's withdrawal or deposit
                        if any(word in full_description.upper() for word in ['TRANSFER', 'PAYMENT']):
                            withdrawal = str(amounts[0])
                        else:
                            deposit = str(amounts[0])
                        balance = str(amounts[-1])
                    elif len(amounts) == 1:
                        balance = str(amounts[0])
            
            # Skip if no meaningful description
            if not full_description or len(full_description.strip()) < 2:
                i = j
                continue
            
            # Get conversion rate for the detected currency
            conversion_rate = self.conversion_rates.get(transaction_currency, 1.0)
            
            transaction = {
                'Client Name': self.client_name,
                'Bank': self.bank,
                'Account ID': current_account or 'Unknown',
                'Date': parsed_date,
                'Currency': transaction_currency,
                'Transaction Description': full_description,
                'Transaction Type': self.classify_transaction(full_description),
                'Withdrawal': self.clean_amount(withdrawal),
                'Deposit': self.clean_amount(deposit),
                'Balance': self.clean_amount(balance),
                'Conversion Rate': f"{conversion_rate:.4f}" if conversion_rate else "1.0000"
            }
            
            if not self.deduplicator.is_duplicate(transaction):
                transactions.append(transaction)
                self.log(f"Valid transaction: {date_str} - {full_description[:50]}... (Currency: {transaction_currency}, Rate: {conversion_rate:.4f})")
                
                # Cache the account-name mapping for future use
                if transaction.get('Account ID') and transaction.get('Client Name'):
                    self.cache_account_name(transaction['Account ID'], transaction['Client Name'])
            
            i = j
        
        return transactions
    
    def process_pdf(self, pdf_path):
        """Process a single PDF file"""
        self.log(f"Processing: {pdf_path.name}")
        
        # Reset state
        self.conversion_rates = {}
        self.client_name = ""
        self.report_date = ""
        
        # Try pdfplumber first
        full_text = self.extract_text_pdfplumber(pdf_path)
        
        # Fallback to PyPDF2 if needed
        if not full_text:
            self.log("Trying PyPDF2 fallback...")
            full_text = self.extract_text_pypdf2(pdf_path)
        
        if not full_text:
            self.log("No text extracted from PDF", "ERROR")
            return []
        
        # Extract metadata
        self.conversion_rates = self.extract_conversion_rates(full_text)
        self.extract_client_info(full_text)
        
        # Extract transactions
        # Detect PDF type and extract transactions accordingly
        pdf_type = self.detect_ocbc_pdf_type(full_text)
        self.log(f"Detected OCBC PDF type: {pdf_type}")

        if pdf_type == 'transaction_history':
            transactions = self.parse_transaction_history_format(full_text)
        elif pdf_type == 'wealth_report':
            transactions = self.parse_transaction_section(full_text)
        else:
            self.log("Unknown OCBC PDF format, trying wealth report parser", "WARNING")
            transactions = self.parse_transaction_section(full_text)
        
        self.log(f"Extracted {len(transactions)} valid transactions")
        return transactions
    
    def save_to_excel(self, all_transactions, output_path):
        """Save all transactions to Excel with proper formatting"""
        if not all_transactions:
            self.log("No transactions to save", "WARNING")
            return False
        
        # Prepare DataFrame
        df_data = []
        
        for trans in all_transactions:
            row = {
                'Client Name': trans.get('client_name', ''),
                'Bank': trans.get('bank', ''),
                'Account ID': trans.get('account_id', ''),
                'Date': trans.get('date', ''),
                'Currency': trans.get('currency', ''),
                'Transaction Description': trans.get('description', ''),
                'Transaction Type': self.classify_transaction(trans.get('description', '')),
                'Withdrawal': trans.get('withdrawal', ''),
                'Deposit': trans.get('deposit', ''),
                'Balance': trans.get('balance', ''),
                'Conversion Rate': trans.get('conversion_rate', '1.0000')
            }
            df_data.append(row)
        
        df = pd.DataFrame(df_data)
        
        try:
            # Save to Excel with formatting
            with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
                df.to_excel(writer, sheet_name='Transactions', index=False)
                
                # Get the worksheet to apply formatting
                worksheet = writer.sheets['Transactions']
                
                # Auto-adjust column widths
                for column in worksheet.columns:
                    max_length = 0
                    column_letter = column[0].column_letter
                    
                    for cell in column:
                        try:
                            if len(str(cell.value)) > max_length:
                                max_length = len(str(cell.value))
                        except:
                            pass
                    
                    adjusted_width = min(max_length + 2, 50)
                    worksheet.column_dimensions[column_letter].width = adjusted_width
            
            self.log(f"Successfully saved {len(df)} transactions to {output_path}")
            return True
            
        except Exception as e:
            self.log(f"Error saving Excel file: {e}", "ERROR")
            return False

def main():
    parser = argparse.ArgumentParser(description='Fixed OCBC transaction data extractor')
    parser.add_argument('input_folder', help='Folder containing PDF files')
    parser.add_argument('-o', '--output', default='ocbc_transactions.xlsx', 
                       help='Output Excel file (default: ocbc_transactions.xlsx)')
    parser.add_argument('--quiet', action='store_true', help='Reduce output verbosity')
    
    args = parser.parse_args()
    
    input_path = Path(args.input_folder)
    output_path = Path(args.output)
    
    if not input_path.exists():
        print(f"Error: Folder {input_path} does not exist")
        return 1
    
    # Find PDF files
    pdf_files = list(input_path.glob("*.pdf"))
    if not pdf_files:
        print("No PDF files found in the specified folder")
        return 1
    
    print(f"Found {len(pdf_files)} PDF files")
    
    # Process all PDFs
    all_transactions = []
    extractor = OCBCTransactionExtractor()

    # First pass: Process wealth reports to build name cache
    print("\nFirst pass: Processing wealth reports...")
    for pdf_file in pdf_files:
        try:
            full_text = extractor.extract_text_pdfplumber(pdf_file)
            if not full_text:
                full_text = extractor.extract_text_pypdf2(pdf_file)
            
            if full_text:
                pdf_type = extractor.detect_ocbc_pdf_type(full_text)
                if pdf_type == 'wealth_report':
                    print(f"  Processing wealth report: {pdf_file.name}")
                    transactions = extractor.process_pdf(pdf_file)
                    all_transactions.extend(transactions)
        except Exception as e:
            print(f"Error in first pass processing {pdf_file.name}: {e}")
            continue

    # Second pass: Process transaction history files
    print("\nSecond pass: Processing transaction history...")
    for pdf_file in pdf_files:
        try:
            full_text = extractor.extract_text_pdfplumber(pdf_file)
            if not full_text:
                full_text = extractor.extract_text_pypdf2(pdf_file)
            
            if full_text:
                pdf_type = extractor.detect_ocbc_pdf_type(full_text)
                if pdf_type == 'transaction_history':
                    print(f"  Processing transaction history: {pdf_file.name}")
                    # Reset client_name to allow cache lookup
                    extractor.client_name = ""
                    transactions = extractor.process_pdf(pdf_file)
                    all_transactions.extend(transactions)
        except Exception as e:
            print(f"Error in second pass processing {pdf_file.name}: {e}")
            continue
    
    # Save results
    if all_transactions:
        print(f"\nTotal transactions extracted: {len(all_transactions)}")
        
        # Create DataFrame with proper column names
        df_data = []
        for trans in all_transactions:
            row = {
                'Client Name': trans.get('Client Name', ''),
                'Bank': trans.get('Bank', ''),
                'Account ID': trans.get('Account ID', ''),
                'Date': trans.get('Date', ''),
                'Currency': trans.get('Currency', ''),
                'Transaction Description': trans.get('Transaction Description', ''),
                'Transaction Type': trans.get('Transaction Type', ''),
                'Withdrawal': trans.get('Withdrawal', ''),
                'Deposit': trans.get('Deposit', ''),
                'Balance': trans.get('Balance', ''),
                'Conversion Rate': trans.get('Conversion Rate', '1.0000')
            }
            df_data.append(row)
        
        df = pd.DataFrame(df_data)
        
        # Save to Excel with formatting
        with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name='Transactions', index=False)
            
            # Auto-adjust column widths
            worksheet = writer.sheets['Transactions']
            for column in worksheet.columns:
                max_length = 0
                column_letter = column[0].column_letter
                
                for cell in column:
                    try:
                        if len(str(cell.value)) > max_length:
                            max_length = len(str(cell.value))
                    except:
                        pass
                
                adjusted_width = min(max_length + 2, 50)
                worksheet.column_dimensions[column_letter].width = adjusted_width
        
        print(f"Results saved to: {output_path}")
        return 0
    else:
        print("No transactions found in any PDF files")
        return 1

if __name__ == "__main__":
    exit(main())