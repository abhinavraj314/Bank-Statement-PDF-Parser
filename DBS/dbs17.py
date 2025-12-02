#!/usr/bin/env python3
"""
Enhanced DBS Bank Statement PDF Transaction Extractor
Robust extraction with improved header parsing, client name detection, and consistent rate handling
Based on proven patterns from UOB extractor
"""

import os
import re
import sys
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Any
import warnings
warnings.filterwarnings('ignore')

import pandas as pd
import numpy as np
from dateutil import parser as date_parser
import traceback

# Enhanced PDF processing libraries
try:
    import fitz  # PyMuPDF
    PYMUPDF_AVAILABLE = True
except ImportError:
    PYMUPDF_AVAILABLE = False

try:
    import pdfplumber
    PDFPLUMBER_AVAILABLE = True
except ImportError:
    PDFPLUMBER_AVAILABLE = False

try:
    import camelot
    CAMELOT_AVAILABLE = True
except ImportError:
    CAMELOT_AVAILABLE = False

try:
    import tabula
    TABULA_AVAILABLE = True
except ImportError:
    TABULA_AVAILABLE = False

try:
    import pytesseract
    import cv2
    OCR_AVAILABLE = True
except ImportError:
    OCR_AVAILABLE = False
import hashlib  # Add this to your existing imports

class TransactionDeduplicator:
    """Handles transaction deduplication logic"""
    
    def __init__(self):
        self.seen_transactions = set()
        self.transaction_hashes = set()
        
    def create_transaction_signature(self, transaction: Dict) -> str:
        """Create a unique signature for a transaction"""
        # Use key fields that should be unique for each transaction
        key_fields = [
            transaction.get('Account ID', '').strip(),
            transaction.get('Date', '').strip(),
            transaction.get('Transaction Description', '').strip().upper(),
            transaction.get('Withdrawal', '').strip(),
            transaction.get('Deposit', '').strip(),
            transaction.get('Balance', '').strip(),
            transaction.get('Currency', '').strip()
        ]
        
        # Create signature from non-empty fields
        signature = '|'.join(field for field in key_fields if field)
        return signature
    
    def create_transaction_hash(self, transaction: Dict) -> str:
        """Create a hash for fast duplicate detection"""
        signature = self.create_transaction_signature(transaction)
        return hashlib.md5(signature.encode()).hexdigest()
    
    def is_duplicate(self, transaction: Dict) -> bool:
        """Check if transaction is a duplicate"""
        transaction_hash = self.create_transaction_hash(transaction)
        
        if transaction_hash in self.transaction_hashes:
            return True
        
        # Also check for near-duplicates with slight variations
        signature = self.create_transaction_signature(transaction)
        
        # Normalize signature for comparison
        normalized_sig = re.sub(r'\s+', ' ', signature.strip())
        
        if normalized_sig in self.seen_transactions:
            return True
        
        # Add to tracking sets
        self.transaction_hashes.add(transaction_hash)
        self.seen_transactions.add(normalized_sig)
        
        return False
    
    def get_stats(self) -> Dict:
        """Get deduplication statistics"""
        return {
            'unique_transactions': len(self.transaction_hashes),
            'signatures_tracked': len(self.seen_transactions)
        }
# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class EnhancedDBSExtractor:
    """Enhanced DBS statement extractor with robust header parsing and transaction extraction"""
    
    def __init__(self, use_ocr: bool = False, extraction_method: str = "auto"):
        self.transactions = []
        self.client_name = ""
        self.bank = "DBS"
        self.conversion_rates = {}
        self.account_ids = set()
        self.extraction_log = []
        self.use_ocr = use_ocr and OCR_AVAILABLE
        self.extraction_method = extraction_method
        # Add these after your existing initialization
        self.deduplicator = TransactionDeduplicator()
        self.processed_sections = set()  # Track processed text sections
        self.statement_month = ""  # New: Track the statement month
        # Enhanced patterns for robust extraction
        self.patterns = {
            'date': [
                r'(\d{2}[/\-]\d{2}[/\-]\d{4})',
                r'(\d{1,2}[/\-]\d{1,2}[/\-]\d{4})',
                r'(\d{1,2}\s+(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)(?:\s+\d{4})?)',
            ],
            'amount': [
                r'([\d,]+\.\d{2})',
                r'(\d{1,3}(?:,\d{3})*\.\d{2})'
            ],
            'account_id': [
                r'(\d{3}[-\s]?\d{6}[-\s]?\d)',
                r'Account\s*No[.:]\s*(\d{3}[-\s]?\d{6}[-\s]?\d)',
                r'A/C\s*(?:No[.:])?\s*(\d{3}[-\s]?\d{6}[-\s]?\d)',
                r'(\d{4}[-\s]?\d{8}[-\s]?\d)',
                r'(S-\d{6}-\d)'  # Investment accounts
            ],
            'client_name': [
                # Primary patterns for DBS statements
                r'^([A-Z][A-Z\s&]{8,50})\s*\n.*?(?:SINGAPORE|ROAD|STREET)',
                r'(?:Client|Customer|Account\s*Holder|Name)[:\s]+([A-Z][A-Z\s&]+)',
                r'^([A-Z]+(?:\s+[A-Z]+)+)\s*$',  # All caps name lines
                r'Statement.*?(?:for|to)[:\s]*([A-Z][A-Z\s&]+)',
                r'(SANGANERIA\s+RAVI)',  # Specific pattern
            ],
            'bank': [
                r'(DBS|POSB)(?:\s+Bank)?',
                r'Bank\s*Ltd[:\s]*([A-Z]+)',
            ]
        }
        
        # Currency mapping
        self.currency_mapping = {
            'SINGAPORE DOLLAR': 'SGD', 'UNITED STATES DOLLAR': 'USD',
            'EUROPEAN UNION EURO': 'EUR', 'EURO': 'EUR',
            'STERLING POUND': 'GBP', 'POUND STERLING': 'GBP', 'BRITISH POUND': 'GBP',
            'AUSTRALIAN DOLLAR': 'AUD', 'CANADIAN DOLLAR': 'CAD',
            'JAPANESE YEN': 'JPY', 'HONG KONG DOLLAR': 'HKD'
        }
        
        # Default conversion rates (fallback only)
        self.default_conversion_rates = {
            'SGD': 1.0, 'USD': 1.29525, 'EUR': 1.482284, 
            'GBP': 1.715688, 'AUD': 0.836084, 'CAD': 0.935502,
            'JPY': 0.008663, 'HKD': 0.164975
        }
    def create_section_hash(self, text_section: str, account_id: str, currency: str) -> str:
        """Create a hash for a text section to prevent re-processing"""
        section_key = f"{account_id}|{currency}|{len(text_section)}|{hash(text_section[:200])}"
        return hashlib.md5(section_key.encode()).hexdigest()

    def add_transaction_if_unique(self, transaction: Dict) -> bool:
        """Add transaction only if it's not a duplicate"""
        if self.deduplicator.is_duplicate(transaction):
            logger.debug(f"Skipping duplicate transaction: {transaction.get('Date', '')} - {transaction.get('Transaction Description', '')[:30]}")
            return False
        else:
            self.transactions.append(transaction)
            return True
    def _is_valid_transaction_table(self, df: pd.DataFrame) -> bool:
        """
        Validate if a table is a proper transaction table.
        Only tables with Withdrawal, Deposit, and Balance columns should be processed.
        """
        if df.empty or df.shape[0] < 2:
            return False
        
        # Get all column headers (check first few rows as headers might not be in row 0)
        potential_headers = []
        for i in range(min(3, len(df))):
            row_values = [str(val).upper().strip() for val in df.iloc[i] if pd.notna(val)]
            potential_headers.extend(row_values)
        
        header_text = ' '.join(potential_headers)
        logger.debug(f"Table headers found: {header_text[:100]}...")
        
        # Required transaction table indicators
        required_transaction_columns = ['WITHDRAWAL', 'DEPOSIT', 'BALANCE']
        
        # Check if all required columns are present
        has_required_columns = all(
            any(req_col in header for header in potential_headers) 
            for req_col in required_transaction_columns
        )
        
        if not has_required_columns:
            logger.debug("Table rejected: Missing required transaction columns (Withdrawal, Deposit, Balance)")
            return False
        
        # Exclude non-transaction tables by checking for their specific column patterns
        exclude_patterns = [
            # Fixed Deposit/Investment tables
            ['PRINCIPAL', 'INTEREST', 'RATE'],
            ['INTEREST AMT', 'PERIOD', 'MATURITY'],
            ['DEPOSIT TYPE', 'DEPOSIT AMOUNT', 'TENOR'],
            
            # Portfolio/Investment tables
            ['SECURITY', 'QUANTITY', 'UNIT PRICE'],
            ['PORTFOLIO', 'MARKET VALUE', 'UNREALIZED'],
            
            # Summary/Balance tables without transaction details
            ['TOTAL DEPOSITS', 'TOTAL WITHDRAWALS', 'NET BALANCE'],
            ['OPENING BALANCE', 'CLOSING BALANCE', 'AVERAGE BALANCE'],
            
            # Rate/Fee tables
            ['EXCHANGE RATE', 'CONVERSION', 'CURRENCY RATE'],
            ['SERVICE CHARGES', 'FEES', 'CHARGES']
        ]
        
        # Check if this table matches any exclude pattern
        for exclude_pattern in exclude_patterns:
            if all(
                any(pattern in header for header in potential_headers)
                for pattern in exclude_pattern
            ):
                logger.debug(f"Table rejected: Matches exclude pattern {exclude_pattern}")
                return False
        
        # Additional validation: Look for date patterns (transaction tables should have dates)
        has_date_pattern = any(
            re.search(r'\d{1,2}[/\-]\d{1,2}[/\-]\d{4}', str(cell))
            for row in df.iloc[:10].values  # Check first 10 rows
            for cell in row
            if pd.notna(cell)
        )
        
        if not has_date_pattern:
            logger.debug("Table rejected: No date patterns found")
            return False
        
        logger.debug("Table validated: Valid transaction table")
        return True


    def extract_text_and_tables(self, pdf_path: str) -> Tuple[str, List[pd.DataFrame]]:
        """Extract text and tables using best available method, filtering for valid transaction tables only"""
        text_content = ""
        tables = []
        
        logger.info(f"Extracting from: {os.path.basename(pdf_path)}")
        
        # Text extraction
        try:
            if PDFPLUMBER_AVAILABLE:
                with pdfplumber.open(pdf_path) as pdf:
                    for page_num, page in enumerate(pdf.pages):
                        page_text = page.extract_text() or ""
                        text_content += page_text + "\n"
                        
                        # OCR fallback for sparse pages
                        if self.use_ocr and len(page_text.strip()) < 50:
                            logger.info(f"Trying OCR on sparse page {page_num + 1}")
                            ocr_text = self._ocr_page_pdfplumber(page)
                            text_content += ocr_text + "\n"
            else:
                logger.error("No PDF text extraction library available")
                
        except Exception as e:
            logger.error(f"Text extraction failed: {str(e)}")
        
        # Table extraction with validation
        extracted_tables = []
        
        if CAMELOT_AVAILABLE:
            try:
                camelot_tables = camelot.read_pdf(pdf_path, pages='all', flavor='lattice')
                if not camelot_tables:
                    camelot_tables = camelot.read_pdf(pdf_path, pages='all', flavor='stream')
                
                for table in camelot_tables:
                    extracted_tables.append(table.df)
                        
                logger.info(f"Camelot extracted {len(extracted_tables)} raw tables")
            except Exception as e:
                logger.warning(f"Camelot extraction failed: {str(e)}")
        
        if TABULA_AVAILABLE and not extracted_tables:
            try:
                tabula_tables = tabula.read_pdf(pdf_path, pages='all', multiple_tables=True)
                extracted_tables.extend(tabula_tables)
                        
                logger.info(f"Tabula extracted {len(extracted_tables)} raw tables")
            except Exception as e:
                logger.warning(f"Tabula extraction failed: {str(e)}")
        
        # Filter for valid transaction tables only
        valid_transaction_tables = 0
        for i, table in enumerate(extracted_tables):
            if not table.empty and table.shape[0] > 2:
                if self._is_valid_transaction_table(table):
                    tables.append(table)
                    valid_transaction_tables += 1
                    logger.info(f"Accepted transaction table {i+1}")
                else:
                    logger.info(f"Rejected non-transaction table {i+1}")
        
        logger.info(f"Filtered to {valid_transaction_tables} valid transaction tables out of {len(extracted_tables)} total tables")
        
        return text_content, tables
    def _ocr_page_pdfplumber(self, page) -> str:
        """Perform OCR on a pdfplumber page"""
        if not OCR_AVAILABLE:
            return ""
        
        try:
            # Convert page to image
            img = page.to_image(resolution=300)
            img_array = np.array(img.original)
            
            # Preprocess for better OCR
            gray = cv2.cvtColor(img_array, cv2.COLOR_RGB2GRAY)
            enhanced = cv2.createCLAHE(clipLimit=2.0, tileGridSize=(8,8)).apply(gray)
            
            # OCR
            ocr_text = pytesseract.image_to_string(enhanced, config='--psm 6')
            return ocr_text
            
        except Exception as e:
            logger.warning(f"OCR failed: {e}")
            return ""

    def extract_client_name_enhanced(self, text: str) -> bool:
        """Enhanced client name extraction with multiple strategies"""
        lines = text.split('\n')
        candidates = []
        
        # Strategy 1: Look for labeled client name
        for pattern in self.patterns['client_name']:
            matches = re.finditer(pattern, text, re.MULTILINE | re.IGNORECASE)
            for match in matches:
                if 'SANGANERIA' in match.group(0):
                    self.client_name = "SANGANERIA RAVI"
                    logger.info(f"Found client name (specific pattern): {self.client_name}")
                    return True
                    
                try:
                    candidate = match.group(1).strip()
                except IndexError:
                    candidate = match.group(0).strip()
                    
                if self._is_valid_name(candidate):
                    candidates.append((candidate, 'labeled', match.start()))
        
        # Strategy 2: Look for name near account numbers
        for line_idx, line in enumerate(lines[:20]):  # Check first 20 lines
            if any(re.search(pattern, line) for pattern in self.patterns['account_id']):
                # Check surrounding lines for names
                for offset in [-2, -1, 1, 2]:
                    check_idx = line_idx + offset
                    if 0 <= check_idx < len(lines):
                        check_line = lines[check_idx].strip()
                        if self._is_valid_name(check_line):
                            candidates.append((check_line, 'near_account', check_idx))
        
        # Strategy 3: Look for names in address blocks
        address_pattern = r'([A-Z][A-Z\s&]+)\s*\n.*(?:SINGAPORE|ROAD|STREET|AVENUE|DRIVE)'
        matches = re.finditer(address_pattern, text, re.MULTILINE)
        for match in matches:
            candidate = match.group(1).strip()
            if self._is_valid_name(candidate):
                candidates.append((candidate, 'address_block', match.start()))
        
        # Strategy 4: Look for standalone name lines early in document
        for line_idx, line in enumerate(lines[:15]):
            line = line.strip()
            if self._is_valid_name(line) and len(line) > 10:
                score_boost = 15 - line_idx
                candidates.append((line, 'standalone', score_boost))
        
        # Select best candidate
        if candidates:
            strategy_priority = {'labeled': 10, 'near_account': 8, 'address_block': 6, 'standalone': 4}
            
            def score_candidate(item):
                name, strategy, pos = item
                base_score = strategy_priority.get(strategy, 1)
                length_bonus = min(len(name.split()), 3)
                return base_score + length_bonus
            
            best_candidate = max(candidates, key=score_candidate)
            self.client_name = best_candidate[0]
            logger.info(f"Extracted client name: '{self.client_name}' (method: {best_candidate[1]})")
            return True
        
        # Final fallback
        self.client_name = "UNKNOWN CLIENT"
        self.extraction_log.append("WARNING: Could not extract client name from PDF")
        logger.warning("Could not extract client name")
        return False
    def extract_statement_month(self, text: str) -> str:
        """Extract the statement month and year from the PDF text"""
        # Month patterns to look for
        month_patterns = [
            # Pattern: "Statement as of: 30-APR-2025" (DBS format) - extract both month and year
            r'Statement\s+as\s+of[:\s]+\d{1,2}-([A-Z]{3})-(\d{4})',
            # Pattern: "Statement Date: 31 July 2025" or "Statement for July 2025"
            r'Statement(?:\s+Date)?[:\s]+\d{1,2}\s+(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{4})',
            # Pattern: "July 2025 Statement" 
            r'(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{4})\s+Statement',
            # Pattern: "For the period ... to 31/07/2025" - extract month and year from end date
            r'For\s+the\s+period.*?to\s+\d{1,2}/(\d{1,2})/(\d{4})',
            # Pattern: "Statement Period: 01/07/2025 to 31/07/2025" - extract month and year from dates
            r'Statement\s+Period[:\s]+\d{1,2}/(\d{1,2})/(\d{4})\s+to\s+\d{1,2}/\d{1,2}/\d{4}',
        ]
        
        # Month number to name mapping
        month_numbers = {
            '01': 'January', '02': 'February', '03': 'March', '04': 'April',
            '05': 'May', '06': 'June', '07': 'July', '08': 'August', 
            '09': 'September', '10': 'October', '11': 'November', '12': 'December'
        }
        
        # Month abbreviation to full name mapping (for DBS format)
        month_abbreviations = {
            'JAN': 'January', 'FEB': 'February', 'MAR': 'March', 'APR': 'April',
            'MAY': 'May', 'JUN': 'June', 'JUL': 'July', 'AUG': 'August',
            'SEP': 'September', 'OCT': 'October', 'NOV': 'November', 'DEC': 'December'
        }
        
        for pattern in month_patterns:
            matches = re.finditer(pattern, text, re.IGNORECASE)
            for match in matches:
                try:
                    groups = match.groups()
                    if len(groups) >= 2:
                        month_value = groups[0].upper()
                        year_value = groups[1]
                        
                        # Convert month to full name
                        if month_value.isdigit():
                            # This is a month number, convert to name
                            month_num = month_value.zfill(2)
                            if month_num in month_numbers:
                                month_name = month_numbers[month_num]
                                statement_period = f"{month_name} {year_value}"
                                logger.info(f"Extracted statement period from date: {statement_period}")
                                return statement_period
                        elif month_value in month_abbreviations:
                            # This is a month abbreviation (like APR), convert to full name
                            month_name = month_abbreviations[month_value]
                            statement_period = f"{month_name} {year_value}"
                            logger.info(f"Extracted statement period from abbreviation: {statement_period}")
                            return statement_period
                        else:
                            # This is already a full month name
                            month_name = month_value.title()
                            statement_period = f"{month_name} {year_value}"
                            logger.info(f"Extracted statement period from text: {statement_period}")
                            return statement_period
                except (IndexError, ValueError):
                    continue
        
        # Fallback: Look for any month names with years in the first few lines
        lines = text.split('\n')[:15]  # Check first 15 lines
        for line in lines:
            # Look for month + year patterns
            month_year_pattern = r'(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{4})'
            match = re.search(month_year_pattern, line, re.IGNORECASE)
            if match:
                month_name = match.group(1).title()
                year_value = match.group(2)
                statement_period = f"{month_name} {year_value}"
                logger.info(f"Found statement period in header: {statement_period}")
                return statement_period
        
        logger.warning("Could not extract statement month and year from PDF")
        return ""
    def _is_valid_name(self, text: str) -> bool:
        """Check if text looks like a valid client name"""
        if not text or len(text) < 5:
            return False
        
        # Must be mostly alphabetic with spaces
        if not re.match(r'^[A-Z][A-Z\s&.,-]+$', text):
            return False
        
        # Should have at least 2 words
        words = text.split()
        if len(words) < 2:
            return False
        
        # Exclude common non-name patterns
        exclude_patterns = [
            r'STATEMENT|ACCOUNT|BALANCE|TRANSACTION|SINGAPORE|ROAD|STREET',
            r'PAGE|TOTAL|SUMMARY|REPORT|DATE|CURRENCY|BANK',
            r'^\d+|DBS|POSB|TREASURES|CONSOLIDATED'
        ]
        
        for pattern in exclude_patterns:
            if re.search(pattern, text, re.IGNORECASE):
                return False
        
        # Length constraints
        if len(text) > 50 or len(text) < 8:
            return False
            
        return True

    def extract_account_ids(self, text: str):
        """Extract all account IDs from text"""
        for pattern in self.patterns['account_id']:
            matches = re.finditer(pattern, text)
            for match in matches:
                account_id = match.group(1).replace(' ', '-')  # Normalize format
                self.account_ids.add(account_id)
        
        if self.account_ids:
            logger.info(f"Extracted account IDs: {list(self.account_ids)}")
        else:
            logger.warning("No account IDs found")
            self.extraction_log.append("WARNING: No account IDs found")

    def extract_conversion_rates(self, text: str) -> Dict[str, float]:
        """Extract conversion rates from PDF with enhanced patterns"""
        conversion_rates = {}
        
        # Look for conversion rate sections
        rate_section_patterns = [
            r'Indicative Exchange Rate(.*?)(?=\n\n|\nPage|\nStatement|\Z)',
            r'Additional Information.*?Exchange Rate(.*?)(?=\n\n|\nPage|\nStatement|\Z)',
            r'Exchange rate quoted.*?\n(.*?)(?=\n\n|\nPage|\nStatement|\Z)',
        ]
        
        for pattern in rate_section_patterns:
            match = re.search(pattern, text, re.IGNORECASE | re.DOTALL)
            if match:
                table_text = match.group(1)
                logger.debug(f"Found rate section: {table_text[:200]}...")
                
                # Extract individual rates
                rates = self._parse_conversion_rate_table(table_text)
                if rates:
                    conversion_rates.update(rates)
                    logger.info(f"Extracted {len(rates)} conversion rates from table")
                    break
        
        # Fallback: individual rate patterns
        if not conversion_rates:
            conversion_rates = self._extract_individual_conversion_rates(text)
        
        # Always set SGD as base
        conversion_rates['SGD'] = 1.0
        
        # Store for consistent use
        if conversion_rates:
            logger.info(f"Successfully extracted conversion rates: {conversion_rates}")
        else:
            logger.warning("No conversion rates found, using defaults")
            conversion_rates = self.default_conversion_rates.copy()
        
        return conversion_rates

    def _parse_conversion_rate_table(self, table_text: str) -> Dict[str, float]:
        """Parse conversion rates from table text"""
        rates = {}
        
        # Enhanced rate extraction patterns
        rate_patterns = [
            r'1\s+(AUD|CAD|EUR|GBP|USD|JPY|HKD)\s*=\s*([\d.]+)\s*SGD',
            r'(AUD|CAD|EUR|GBP|USD|JPY|HKD)\s*=\s*([\d.]+)',
            r'1\s*SGD\s*=\s*([\d.]+)\s*(HKD|JPY)',  # Reverse rates
            r'(AUD|CAD|EUR|GBP|USD|JPY|HKD)[\s:]+?([\d.]+)',
        ]
        
        lines = table_text.split('\n')
        
        for line in lines:
            line = line.strip()
            if not line or len(line) < 5:
                continue
                
            # Skip headers
            if any(header in line.upper() for header in ['CURRENCY', 'RATE', 'EXCHANGE', 'SINGAPORE']):
                continue
            
            for pattern in rate_patterns:
                matches = re.findall(pattern, line, re.IGNORECASE)
                for match in matches:
                    try:
                        if len(match) == 2:
                            currency = match[0].upper()
                            rate = float(match[1])
                            
                            # Special handling for reverse rates
                            if 'SGD' in pattern and currency in ['HKD', 'JPY']:
                                rate = 1.0 / rate
                            
                            # Validate rate range
                            if 0.001 <= rate <= 1000 and currency not in rates:
                                rates[currency] = rate
                                logger.debug(f"Extracted rate {currency}: {rate}")
                    except (ValueError, IndexError):
                        continue
        
        return rates

    def _extract_individual_conversion_rates(self, text: str) -> Dict[str, float]:
        """Fallback individual rate extraction"""
        rates = {}
        
        individual_patterns = [
            r'1\s+(AUD|CAD|EUR|GBP|USD|JPY|HKD)\s*=\s*([\d.]+)\s*SGD',
            r'(AUD|CAD|EUR|GBP|USD|JPY|HKD)\s*:\s*([\d.]+)',
        ]
        
        for pattern in individual_patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            for match in matches:
                currency = match[0].upper()
                try:
                    rate = float(match[1])
                    if 0.001 <= rate <= 1000 and currency not in rates:
                        rates[currency] = rate
                        logger.debug(f"Extracted individual rate {currency}: {rate}")
                except (ValueError, IndexError):
                    continue
        
        return rates

    def get_conversion_rate_for_currency(self, currency: str) -> str:
        """Get consistent conversion rate for currency"""
        if currency == 'SGD':
            return '1.0000'
        
        if currency in self.conversion_rates:
            rate = self.conversion_rates[currency]
            return f"{rate:.4f}"
        
        if currency in self.default_conversion_rates:
            rate = self.default_conversion_rates[currency]
            logger.debug(f"Using default rate for {currency}: {rate}")
            return f"{rate:.4f}"
        
        logger.warning(f"No conversion rate found for currency: {currency}")
        return ""

    def extract_multiline_description(self, lines: List[str], start_idx: int) -> Tuple[str, int]:
        """Extract complete multi-line transaction description preserving exact content from PDF"""
        if start_idx >= len(lines):
            return "", start_idx + 1
        
        current_line = lines[start_idx].strip()
        
        # Extract description part after date
        date_patterns = [
            r'^\d{2}/\d{2}/\d{4}\s+(.+)',
            r'^\d{1,2}/\d{1,2}/\d{4}\s+(.+)'
        ]
        
        description_match = None
        for pattern in date_patterns:
            description_match = re.search(pattern, current_line)
            if description_match:
                break
        
        if not description_match:
            return current_line, start_idx + 1
        
        full_line_after_date = description_match.group(1).strip()
        
        # Remove table column contamination at the END first
        contamination_patterns = [
            r'Balance\s+Carried\s+Forward\s+(SGD|USD|EUR|GBP|AUD|CAD|JPY|HKD)\s*$',
            r'Total\s+Balance\s+Carried\s+Forward\s*$',
            r'(SGD|USD|EUR|GBP|AUD|CAD|JPY|HKD)\s*$',  # Standalone currency codes at end
            r'Withdrawal\s*$',
            r'Deposit\s*$', 
            r'Balance\s*$'
        ]
        
        clean_description = full_line_after_date
        
        for pattern in contamination_patterns:
            contamination_match = re.search(pattern, clean_description, re.IGNORECASE)
            if contamination_match:
                clean_description = clean_description[:contamination_match.start()].strip()
                logger.debug(f"Removed contamination: '{contamination_match.group()}'")
        
        # Check if this is an Advice transaction - if so, use specialized cleaning
        if 'ADVICE' in clean_description.upper():
            # For Advice transactions, use the specialized cleaner
            main_description = self.clean_advice_transaction_description(clean_description)
        else:
            # For non-Advice transactions, use the existing logic
            words = clean_description.split()
            
            if len(words) >= 2:
                # Find trailing numeric values (both decimal amounts and whole numbers)
                trailing_numbers = []
                for i in range(len(words) - 1, -1, -1):
                    word = words[i]
                    # Check for decimal amounts OR long numeric strings (reference numbers)
                    if re.match(r'^[\d,]+\.\d{2}$', word) or re.match(r'^\d{8,}$', word):
                        trailing_numbers.append((i, word))
                    else:
                        break
                
                if trailing_numbers:
                    # Reverse to process left-to-right
                    trailing_numbers.reverse()
                    
                    keep_until_index = len(words)
                    
                    for idx, number in trailing_numbers:
                        # Check if this number has currency context (definitely keep)
                        has_currency_context = False
                        if idx > 0:
                            prev_word = words[idx - 1].upper()
                            if re.match(r'^[A-Z]{3}$', prev_word):  # Currency code
                                has_currency_context = True
                        
                        if has_currency_context:
                            continue  # Keep this number
                        
                        # Distinguish between reference numbers and monetary amounts
                        if re.match(r'^[\d,]+\.\d{2}$', number):
                            # This is a decimal amount - likely monetary, remove it
                            try:
                                amount_val = float(number.replace(',', ''))
                                if amount_val >= 1.0:  # Remove monetary amounts >= 1.00
                                    keep_until_index = idx
                                    break
                            except ValueError:
                                continue
                        elif re.match(r'^\d{8,}$', number):
                            # This is a long numeric string - likely a reference number
                            # Keep reference numbers that are part of transaction descriptions
                            # Check context: if it follows transaction keywords, it's likely a reference
                            context_before = ' '.join(words[:idx]).upper()
                            
                            # Reference number indicators
                            reference_contexts = [
                                'ADVICE', 'TRANSFER', 'REMITTANCE', 'TRANSACTION',
                                'REF', 'REFERENCE', 'NO', 'NUMBER'
                            ]
                            
                            has_reference_context = any(ref in context_before for ref in reference_contexts)
                            
                            if not has_reference_context:
                                # If no reference context and it's at the end, might be a balance amount
                                # But be conservative - only remove if very sure
                                if len(number) > 12:  # Very long numbers might be balance amounts
                                    keep_until_index = idx
                                    break
                    
                    # Apply the cutoff
                    if keep_until_index < len(words):
                        clean_description = ' '.join(words[:keep_until_index]).strip()
                        removed_count = len(words) - keep_until_index
                        logger.debug(f"Removed {removed_count} trailing numbers")
            
            main_description = clean_description.strip()
        
        # Look for continuation lines (same logic for all transactions)
        next_idx = start_idx + 1
        additional_parts = []
        
        for i in range(next_idx, min(start_idx + 4, len(lines))):
            if i >= len(lines):
                break
                
            next_line = lines[i].strip()
            
            if not next_line:
                next_idx += 1
                continue
            
            # Stop if we hit another transaction or section marker
            if (re.match(r'^\d{1,2}/\d{1,2}/\d{4}', next_line) or
                any(marker in next_line for marker in ['Total Balance', 'Page', 'Statement'])):
                break
            
            # Check if this looks like a continuation
            if (len(next_line) > 3 and 
                any(char.isalpha() for char in next_line) and 
                len(next_line) < 80 and
                not re.match(r'^(Balance|Total|Withdrawal|Deposit)', next_line, re.IGNORECASE)):
                
                # For continuation lines, apply the same Advice-specific logic if applicable
                if 'ADVICE' in main_description.upper():
                    # For Advice transactions, be more conservative with continuation
                    cont_parts = next_line.split()
                    # Only remove obvious monetary amounts, keep reference numbers
                    keep_until = len(cont_parts)
                    
                    for j in range(len(cont_parts) - 1, -1, -1):
                        word = cont_parts[j]
                        if re.match(r'^[\d,]+\.\d{2}$', word):
                            # Check for currency context
                            has_currency = False
                            if j > 0 and re.match(r'^[A-Z]{3}$', cont_parts[j-1].upper()):
                                has_currency = True
                            
                            if not has_currency:
                                keep_until = j
                            else:
                                break
                        else:
                            break
                    
                    cont_parts = cont_parts[:keep_until]
                    if cont_parts:
                        additional_parts.append(' '.join(cont_parts))
                else:
                    # For non-Advice transactions, use existing logic
                    cont_words = next_line.split()
                    keep_until = len(cont_words)
                    
                    for j in range(len(cont_words) - 1, -1, -1):
                        word = cont_words[j]
                        if re.match(r'^[\d,]+\.\d{2}$', word):
                            # Check for currency context
                            has_currency = False
                            if j > 0 and re.match(r'^[A-Z]{3}$', cont_words[j-1].upper()):
                                has_currency = True
                            
                            if not has_currency:
                                keep_until = j
                            else:
                                break
                        else:
                            break
                    
                    cont_parts = cont_words[:keep_until]
                    if cont_parts:
                        additional_parts.append(' '.join(cont_parts))
                
                next_idx = i + 1
            else:
                break
        
        # Combine all parts
        complete_description = main_description
        if additional_parts:
            complete_description += ' ' + ' '.join(additional_parts)
        
        complete_description = re.sub(r'\s+', ' ', complete_description).strip()
        
        # Special cleanup for Balance B/F
        if complete_description.upper().startswith('BALANCE B/F'):
            complete_description = 'Balance B/F'
        
        # Final validation
        if not complete_description or len(complete_description.strip()) < 2:
            fallback_words = full_line_after_date.split()[:3]
            complete_description = ' '.join(fallback_words) if fallback_words else "Transaction"
        
        return complete_description, next_idx
    def parse_date(self, date_str: str, default_year: int = 2025) -> Optional[str]:
        """Enhanced date parsing"""
        if not date_str or str(date_str).strip().lower() in ['nan', 'none', '']:
            return None
            
        date_str = str(date_str).strip()
        
        # DBS common patterns
        patterns = [
            (r'(\d{1,2})/(\d{1,2})/(\d{4})', 'dd/mm/yyyy'),
            (r'(\d{1,2})-(\d{1,2})-(\d{4})', 'dd-mm-yyyy'),
            (r'(\d{4})-(\d{1,2})-(\d{1,2})', 'yyyy-mm-dd'),
        ]
        
        for pattern, format_type in patterns:
            match = re.search(pattern, date_str)
            if match:
                if format_type == 'dd/mm/yyyy' or format_type == 'dd-mm-yyyy':
                    day = match.group(1).zfill(2)
                    month = match.group(2).zfill(2)
                    year = match.group(3)
                    return f"{year}-{month}-{day}"
                elif format_type == 'yyyy-mm-dd':
                    return f"{match.group(1)}-{match.group(2).zfill(2)}-{match.group(3).zfill(2)}"
        
        # Try dateutil as fallback
        try:
            parsed_date = date_parser.parse(date_str, dayfirst=True)
            if parsed_date:
                return parsed_date.strftime('%Y-%m-%d')
        except:
            pass
        
        return None

    def categorize_transaction(self, description: str) -> str:
        """Enhanced transaction categorization"""
        desc_upper = description.upper()
        
        categories = {
            'Balance Brought Forward': [
                'BALANCE B/F', 'OPENING BALANCE', 'BAL B/F', 
                'BALANCE BROUGHT FORWARD', 'TOTAL BALANCE CARRIED FORWARD'
            ],
            'Debit Card Transaction': ['DEBIT CARD TRANSACTION', 'CARD PURCHASE'],
            'Interest Earned': ['INTEREST EARNED', 'INTEREST CREDIT'],
            'Fund Transfer': ['ADVICE REMITTANCE', 'TRANSFER OF FUNDS'],
            'Cash Rebate': ['CASH REBATE', 'CASHBACK'],
            'Investment': ['INVESTMENT', 'PORTFOLIO'],
            'Deposit': ['DEPOSIT VIA', 'FIXED DEPOSIT'],
            'Other': []
        }
        
        for category, keywords in categories.items():
            if any(keyword in desc_upper for keyword in keywords):
                return category
        
        # Special check for currency-specific Balance B/F
        if 'TOTAL BALANCE CARRIED FORWARD IN' in desc_upper:
            return 'Balance Brought Forward'
        
        return 'Other'
    def clean_amount(self, amount_str: str) -> str:
        """Clean amount string"""
        if not amount_str or str(amount_str).lower() in ['nan', 'none', '']:
            return ""
        
        cleaned = re.sub(r'[^\d.,-]', '', str(amount_str))
        cleaned = cleaned.replace(',', '')
        
        if not cleaned or cleaned in ['-', '.', '']:
            return ""
        
        return cleaned
    def _classify_transaction_amounts(self, description: str, number_values: List[float]) -> Tuple[str, str, str]:
        """
        Enhanced amount classification using balance comparison to determine transaction direction
        Returns: (withdrawal, deposit, balance) as strings
        """
        withdrawal = ""
        deposit = ""
        balance = ""
        
        desc_upper = description.upper()
        
        # Handle no amounts
        if not number_values:
            return withdrawal, deposit, balance
        
        # Special case: Balance B/F only has balance
        if desc_upper.startswith('BALANCE B/F'):
            balance = str(number_values[-1])
            return withdrawal, deposit, balance
        
        # Single amount: always goes to balance (most conservative approach)
        if len(number_values) == 1:
            balance = str(number_values[0])
            return withdrawal, deposit, balance
        
        # Multiple amounts: Last amount is always balance
        balance = str(number_values[-1])
        current_balance = number_values[-1]
        
        # For transactions with 2+ amounts, use intelligent classification
        if len(number_values) >= 2:
            transaction_amount = number_values[0]
            
            # Method 1: Balance comparison logic (most reliable)
            # If we have 3 amounts: [transaction_amount, previous_balance, current_balance]
            if len(number_values) == 3:
                previous_balance = number_values[1]
                balance_change = current_balance - previous_balance
                
                # If balance increased, it's a deposit; if decreased, it's a withdrawal
                if abs(balance_change - transaction_amount) < 0.01:  # Deposit
                    deposit = str(transaction_amount)
                elif abs(balance_change + transaction_amount) < 0.01:  # Withdrawal
                    withdrawal = str(transaction_amount)
                else:
                    # Fallback to contextual analysis
                    # Fallback to contextual analysis with enhanced Advice handling
                    if 'ADVICE' in desc_upper:
                        withdrawal, deposit = self._classify_advice_transaction_direction(desc_upper, number_values)
                    else:
                        withdrawal, deposit = self._classify_by_context(desc_upper, transaction_amount)
            else:
                # Method 2: Enhanced contextual analysis for 2-amount transactions
                if 'ADVICE' in desc_upper:
                    withdrawal, deposit = self._classify_advice_transaction_direction(desc_upper, number_values)
                else:
                    withdrawal, deposit = self._classify_by_context(desc_upper, transaction_amount)
        
        return withdrawal, deposit, balance
    def clean_advice_transaction_description(self, description: str) -> str:
            """
            Specialized cleaning for Advice-related transaction descriptions only.
            Removes ALL monetary amounts (both with and without commas) while preserving
            transaction keywords and reference numbers.
            
            Target result: "Advice120204811818 250725001479" (no amounts at all)
            
            Handles all Advice transaction types:
            - Advice Inward Telegraphic Transfer EM0707003566045I 0016IT1128604
            - Advice 0120204811818
            - Advice120204811818  250725001469
            - Advice Funds Transfer272-582624-5 : I-BANK
            - Advice Remittance Transfer of Funds 0016RF4278491 ULTRA INTERNATIONAL VALUE DATE : 31/07/2025
            """
            if not description or 'ADVICE' not in description.upper():
                return description  # Return unchanged if not an Advice transaction
            
            # Work with the original description
            cleaned = description.strip()
            
            # For Advice transactions, remove ALL monetary amounts
            # Split into words for analysis
            words = cleaned.split()
            
            # Filter out ALL monetary amounts (both with and without commas)
            filtered_words = []
            removed_count = 0
            
            for word in words:
                # TARGET FOR REMOVAL: ANY decimal number (with or without commas)
                if re.match(r'^[\d,]+\.\d{2}', word):
                    # This is any monetary amount - remove it completely
                    removed_count += 1
                    logger.debug(f"Removing monetary amount: '{word}'")
                    continue
                
                # TARGET FOR REMOVAL: Large integers with commas (like 175,000)
                elif re.match(r'^[\d,]+', word) and ',' in word and len(word) > 4:
                    # This is a comma-containing integer - likely a balance amount
                    removed_count += 1
                    logger.debug(f"Removing comma-containing integer: '{word}'")
                    continue
                
                # PRESERVE: Currency codes (but they usually come with amounts, so might be rare)
                elif re.match(r'^[A-Z]{3}', word) and word in ['SGD', 'USD', 'EUR', 'GBP', 'AUD', 'CAD', 'JPY', 'HKD']:
                    filtered_words.append(word)
                
                # PRESERVE: Reference numbers without commas (like 0120204811818, 250725001479)
                elif re.match(r'^\d+', word) and ',' not in word:
                    filtered_words.append(word)
                
                # PRESERVE: Alphanumeric codes (like 0016RF4278491, EM0707003566045I)
                elif re.match(r'^[A-Z0-9]+', word):
                    filtered_words.append(word)
                
                # PRESERVE: Account patterns (like 272-582624-5)
                elif re.match(r'^\d{3}-\d{6}-\d', word):
                    filtered_words.append(word)
                
                # PRESERVE: Date patterns (like 31/07/2025)
                elif re.match(r'^\d{1,2}/\d{1,2}/\d{4}', word):
                    filtered_words.append(word)
                
                # PRESERVE: Text with special characters (like I-BANK)
                elif re.match(r'^[A-Z]+-[A-Z]+', word):
                    filtered_words.append(word)
                
                # PRESERVE: All other words (regular text, transaction keywords, symbols, etc.)
                else:
                    filtered_words.append(word)
            
            # Reconstruct the description
            cleaned = ' '.join(filtered_words).strip()
            
            if removed_count > 0:
                logger.debug(f"Advice transaction cleaned: removed {removed_count} monetary amounts")
            
            # Clean up any double spaces
            cleaned = re.sub(r'\s+', ' ', cleaned).strip()
            
            return cleaned
    def _classify_advice_remittance_transfer(self, description: str, number_values: List[float]) -> Tuple[str, str]:
        """
        Simplified classification for 'Advice Remittance Transfer of Funds' transactions.
        Uses simple balance comparison: if balance increases = deposit, if decreases = withdrawal.
        """
        desc_upper = description.upper()
        
        # Default return values
        withdrawal = ""
        deposit = ""
        
        if not number_values:
            return withdrawal, deposit
        
        transaction_amount = number_values[0]
        amount_str = str(transaction_amount)
        
        logger.debug(f"Classifying Advice Remittance Transfer: '{desc_upper[:50]}...' with amounts: {number_values}")
        
        # Add detailed debug logging
        if len(number_values) >= 2:
            logger.debug(f"Transaction amount: {transaction_amount}")
            if len(number_values) >= 3:
                logger.debug(f"All balance amounts: {number_values[1:]}")
            else:
                logger.debug(f"Only one balance amount: {number_values[1]}")
        
        # SIMPLIFIED LOGIC: Look for two balance amounts and compare them
        if len(number_values) >= 3:
            # We have [transaction_amount, balance1, balance2, ...]
            # Find the two balance amounts (usually the largest values)
            balance_amounts = number_values[1:]
            
            # Sort to find the two largest amounts (likely the balances)
            sorted_balances = sorted(balance_amounts, reverse=True)
            largest_balance = sorted_balances[0]
            second_largest = sorted_balances[1]
            
            logger.debug(f"Comparing balances: {second_largest} vs {largest_balance}")
            
            # Simple rule: If largest > second_largest, balance increased = DEPOSIT
            # If largest < second_largest, balance decreased = WITHDRAWAL
            
            balance_difference = largest_balance - second_largest
            logger.debug(f"Balance difference: {balance_difference}")
            
            if balance_difference > 0:
                # Balance increased = DEPOSIT
                deposit = amount_str
                logger.info(f"Advice Remittance Transfer classified as DEPOSIT: balance increased by {balance_difference}")
                return withdrawal, deposit
            elif balance_difference < 0:
                # Balance decreased = WITHDRAWAL
                withdrawal = amount_str
                logger.info(f"Advice Remittance Transfer classified as WITHDRAWAL: balance decreased by {abs(balance_difference)}")
                return withdrawal, deposit
            else:
                # No change in balance - shouldn't happen, fallback to pattern analysis
                logger.warning(f"No balance change detected, using pattern fallback")
        
        elif len(number_values) == 2:
            # Only transaction amount and one balance - can't compare, use patterns
            logger.debug(f"Only 2 amounts, using pattern analysis")
        
        # PATTERN-BASED FALLBACK: RF code + VALUE DATE usually indicates incoming
        if 'RF' in desc_upper and 'VALUE DATE' in desc_upper:
            deposit = amount_str
            logger.info(f"Advice Remittance Transfer classified as DEPOSIT: RF + VALUE DATE pattern")
            return withdrawal, deposit
        
        # AMOUNT-BASED FALLBACK: Large amounts more likely to be incoming international transfers
        if transaction_amount >= 100000:
            deposit = amount_str
            logger.info(f"Advice Remittance Transfer classified as DEPOSIT: large amount heuristic")
            return withdrawal, deposit
        else:
            withdrawal = amount_str
            logger.info(f"Advice Remittance Transfer classified as WITHDRAWAL: small amount heuristic")
            return withdrawal, deposit
    # Replace your existing _classify_advice_transaction_direction method with this corrected version:
    def _classify_advice_transaction_direction(self, description: str, number_values: List[float]) -> Tuple[str, str]:
        """
        Enhanced classification specifically for Advice transactions.
        Delegates 'Advice Remittance Transfer of Funds' to specialized handler.
        """
        desc_upper = description.upper()
        
        # Check if this is specifically an "Advice Remittance Transfer of Funds" transaction
        if 'ADVICE REMITTANCE TRANSFER OF FUNDS' in desc_upper:
            logger.debug("Delegating to specialized Advice Remittance Transfer handler")
            return self._classify_advice_remittance_transfer(description, number_values)
        
        # For all other Advice transactions, use the original logic
        # Default return values
        withdrawal = ""
        deposit = ""
        
        if not number_values:
            return withdrawal, deposit
        
        transaction_amount = number_values[0]
        amount_str = str(transaction_amount)
        
        # Look for explicit directional indicators in the description
        outgoing_indicators = [
            'OUTWARD', 'SENT', 'DEBIT', 'TO BENEFICIARY', 'OUTGOING', 
            'PAYMENT TO', 'TRANSFER TO', 'SENT TO', 'PAY TO'
        ]
        
        incoming_indicators = [
            'INWARD', 'RECEIVED', 'FROM', 'CREDIT', 'INCOMING', 
            'RECEIVED FROM', 'TRANSFER FROM', 'REMITTANCE FROM'
        ]
        
        # Check for explicit directional indicators
        has_outgoing = any(indicator in desc_upper for indicator in outgoing_indicators)
        has_incoming = any(indicator in desc_upper for indicator in incoming_indicators)
        
        if has_outgoing:
            withdrawal = amount_str
        elif has_incoming:
            deposit = amount_str
        else:
            # Enhanced heuristics for other Advice transactions
            
            # Look for patterns that suggest direction
            if 'ULTRA INTERNATIONAL' in desc_upper or 'BENEFICIARY' in desc_upper:
                # These often indicate outgoing international transfers
                withdrawal = amount_str
            elif 'VALUE DATE' in desc_upper and 'RF' in desc_upper:
                # RF codes with value dates are often incoming remittances
                deposit = amount_str
            else:
                # For ambiguous cases, use balance comparison if available
                if len(number_values) >= 3:
                    # We have [amount, previous_balance, current_balance]
                    previous_balance = number_values[1] 
                    current_balance = number_values[2]
                    balance_change = current_balance - previous_balance
                    
                    # If balance increased by the transaction amount, it's a deposit
                    if abs(balance_change - transaction_amount) < 0.01:
                        deposit = amount_str
                    # If balance decreased by the transaction amount, it's a withdrawal
                    elif abs(balance_change + transaction_amount) < 0.01:
                        withdrawal = amount_str
                    else:
                        # Most "Advice" transactions appear to be outgoing (withdrawals)
                        withdrawal = amount_str
                else:
                    # Default to withdrawal for ambiguous cases
                    withdrawal = amount_str
        
        return withdrawal, deposit
    def _classify_by_context(self, desc_upper: str, transaction_amount: float) -> Tuple[str, str]:
        """
        Enhanced contextual classification for transaction direction
        Returns: (withdrawal, deposit) as strings
        """
        withdrawal = ""
        deposit = ""
        amount_str = str(transaction_amount)
        
        # Strong deposit indicators (money coming in)
        strong_deposit_hints = [
            'INTEREST EARNED', 'INTEREST CREDIT', 'CASH REBATE', 'CASHBACK',
            'DEPOSIT VIA', 'SALARY', 'DIVIDEND', 'BONUS', 'REFUND',
            'INWARD REMITTANCE', 'CREDIT TRANSFER', 'RECEIVED FROM',
            'INTEREST'  # Added - Interest transactions are deposits
        ]
        
        # Strong withdrawal indicators (money going out)
        strong_withdrawal_hints = [
            'DEBIT CARD TRANSACTION', 'WITHDRAWAL', 'PAYMENT TO', 'PURCHASE',
            'FEE', 'CHARGE', 'ATM', 'OUTWARD REMITTANCE', 'TRANSFER TO'
        ]
        
        # Check for strong indicators first
        if any(hint in desc_upper for hint in strong_deposit_hints):
            deposit = amount_str
            return withdrawal, deposit
        elif any(hint in desc_upper for hint in strong_withdrawal_hints):
            withdrawal = amount_str
            return withdrawal, deposit
        
        # MODIFIED: Use specialized handler for Advice transactions
        if 'ADVICE REMITTANCE' in desc_upper or 'REMITTANCE TRANSFER' in desc_upper or 'ADVICE' in desc_upper:
            # Use the enhanced Advice transaction classifier
            return self._classify_advice_transaction_direction(desc_upper, [transaction_amount])
        
        # Check for DR/CR notation
        elif any(indicator in desc_upper for indicator in ['DR', 'DEBIT', 'MISC DR']):
            withdrawal = amount_str
        elif any(indicator in desc_upper for indicator in ['CR', 'CREDIT']):
            deposit = amount_str
        
        # General transfer logic
        elif 'TRANSFER' in desc_upper:
            if any(word in desc_upper for word in ['INWARD', 'RECEIVED', 'CREDIT', 'INCOMING']):
                deposit = amount_str
            else:
                withdrawal = amount_str  # Default assumption for transfers
        
        # Final fallback: analyze transaction type patterns
        else:
            # If description suggests money coming in
            if any(pattern in desc_upper for pattern in [
                'DEPOSIT', 'CREDIT', 'EARNING', 'REBATE', 'REFUND', 'BONUS'
            ]):
                deposit = amount_str
            # Default to withdrawal for unclassified transactions
            else:
                withdrawal = amount_str
        
        return withdrawal, deposit

    def _is_parseable_account(self, text_section: str, account_id: str) -> bool:
        """Determine if an account should be parsed based on context - ENHANCED for DBS"""
        if not account_id:
            return False
        
        # Look for the account in the text section
        account_contexts = []
        lines = text_section.split('\n')
        
        account_found_in_text = False
        for i, line in enumerate(lines):
            if account_id in line:
                account_found_in_text = True
                # Get context around the account mention
                context_start = max(0, i - 5)  # Increased context window
                context_end = min(len(lines), i + 15)  # Increased context window
                context = ' '.join(lines[context_start:context_end]).upper()
                account_contexts.append((line.strip(), context, i))
        
        if not account_found_in_text:
            logger.debug(f"Account {account_id} not found in text")
            return False
        
        logger.debug(f"Account {account_id} found in {len(account_contexts)} contexts")
        
        for line_content, context, line_idx in account_contexts:
            # Skip if it's just a reference listing (like "272-582624-5 : I-BANK")
            if re.match(r'^\s*\d{3}-\d{6}-\d\s*:\s*[A-Z-]+\s*$', line_content.strip()):
                logger.debug(f"Skipping reference account listing: {line_content.strip()}")
                continue
            
            # ENHANCED: Be more lenient - accept if account appears in statement context
            # Look for DBS-specific statement indicators
            dbs_statement_indicators = [
                'DBS EMULTI-CURRENCY', 'ACCOUNT NO', 'TREASURES', 'AUTOSAVE',
                'STATEMENT', 'BALANCE', 'SINGAPORE DOLLAR', 'CURRENCY:',
                'WITHDRAWAL', 'DEPOSIT', 'TRANSACTION'
            ]
            
            has_dbs_context = any(
                indicator in context for indicator in dbs_statement_indicators
            )
            
            if has_dbs_context:
                logger.debug(f"Account {account_id} found in DBS statement context")
                return True
            
            # Original transaction context check (keep as fallback)
            transaction_indicators = [
                'TRANSACTION DETAILS', 'CURRENCY:', 'BALANCE CARRIED FORWARD',
                'DEBIT CARD', 'TRANSFER', 'DEPOSIT', 'WITHDRAWAL',
                r'\d{1,2}/\d{1,2}/\d{4}',  # Date pattern
                'BALANCE B/F', 'INTEREST EARNED', 'BALANCE BROUGHT FORWARD'
            ]
            
            has_transaction_context = any(
                re.search(indicator, context) for indicator in transaction_indicators
            )
            
            if has_transaction_context:
                logger.debug(f"Account {account_id} found in transaction context")
                return True
            else:
                logger.debug(f"Account {account_id} lacks transaction context in: {context[:100]}")
        
        logger.debug(f"Account {account_id} not found in parseable context")
        return False

    def parse_transaction_section(self, text: str, default_year: int) -> List[Dict]:
        """Parse transaction sections from DBS statement text - ENHANCED WITH DEDUPLICATION"""
        transactions = []
        lines = text.split('\n')
        current_account = None
        current_currency = None
        in_transaction_section = False
        
        # Create section hash to prevent re-processing same content
        section_hash = self.create_section_hash(text, '', '')
        if section_hash in self.processed_sections:
            logger.info("Section already processed, skipping")
            return []
        
        i = 0
        while i < len(lines):
            line = lines[i].strip()
            if not line:
                i += 1
                continue
            
            # Enhanced account detection - multiple patterns with improved page handling
            account_patterns = [
                r'Account No\.?\s*(\d{3}-\d{6}-\d)',
                r'DBS eMulti-Currency Autosave Account\s+Account No\.\s*(\d{3}-\d{6}-\d)',
                r'Account\s+(\d{3}-\d{6}-\d)',
                r'(\d{3}-\d{6}-\d)'  # Standalone account numbers
            ]
            
            account_found = False
            for pattern in account_patterns:
                account_match = re.search(pattern, line)
                if account_match:
                    potential_account = account_match.group(1)
                    
                    # Check if this account should be parsed using context analysis
                    if self._is_parseable_account(text, potential_account):
                        # Only switch to new account if it's different from current
                        if current_account != potential_account:
                            current_account = potential_account
                            logger.info(f"Found parseable account: {current_account}")
                        else:
                            logger.debug(f"Account {current_account} already active")
                        
                        in_transaction_section = True
                        account_found = True
                    else:
                        logger.info(f"Skipping non-parseable account: {potential_account}")
                        # Don't reset current_account if we're just seeing a reference
                        if not current_account:  # Only reset if no active account
                            in_transaction_section = False
                    break
            
            if account_found:
                i += 1
                continue
            
            # Enhanced currency detection with persistence across pages
            currency_patterns = [
                r'CURRENCY:\s*([A-Z\s]+)',
                r'Balance Carried Forward\s+([A-Z]{3})',
                r'Total Balance Carried Forward in ([A-Z]{3}):',
            ]
            
            currency_found = False
            for pattern in currency_patterns:
                currency_match = re.search(pattern, line)
                if currency_match:
                    currency_name = currency_match.group(1).strip()
                    new_currency = self.currency_mapping.get(currency_name, currency_name)
                    
                    # Clean up currency codes
                    if len(new_currency) > 3:
                        for code in ['SGD', 'USD', 'EUR', 'GBP', 'AUD', 'CAD', 'JPY', 'HKD']:
                            if code in new_currency:
                                new_currency = code
                                break
                    
                    # Only update currency if it's different from current
                    if new_currency != current_currency:
                        current_currency = new_currency
                        logger.debug(f"Currency changed to: {current_currency}")
                    else:
                        logger.debug(f"Currency remains: {current_currency}")
                    
                    currency_found = True
                    break
            
            if currency_found:
                i += 1
                continue
            
            # Investment account detection - but check if parseable
            inv_match = re.search(r'Account No\.\s*(S-\d{6}-\d)', line)
            if inv_match:
                potential_account = inv_match.group(1)
                if self._is_parseable_account(text, potential_account):
                    current_account = potential_account
                    current_currency = 'SGD'  # Default for investment accounts
                    in_transaction_section = True
                    logger.info(f"Found parseable investment account: {current_account}")
                else:
                    logger.info(f"Skipping non-parseable investment account: {potential_account}")
                i += 1
                continue
            
            # Transaction processing - ENHANCED
            # Transaction processing - ENHANCED WITH TABLE CONTEXT VALIDATION
            if current_account and current_currency and in_transaction_section:
                # Multiple date patterns for DBS transactions
                date_patterns = [
                    r'^\d{2}/\d{2}/\d{4}',  # DD/MM/YYYY
                    r'^\d{1,2}/\d{1,2}/\d{4}',  # D/M/YYYY or DD/M/YYYY
                ]
                
                is_transaction_line = any(re.match(pattern, line) for pattern in date_patterns)
                
                # Simple Balance B/F detection (for table rows without dates)
                is_balance_bf_line = (
                    'Balance Brought Forward' in line or 
                    'BALANCE B/F' in line.upper() or
                    'Balance B/F' in line
                )
                
                if is_transaction_line:
                    # ENHANCED: Validate this is actually from a transaction table, not fixed deposit
                    # Check for fixed deposit indicators that should be excluded
                    fixed_deposit_indicators = [
                        'DEPOSIT VIA INTERNET', 'BANKING', 'S/N:', 'SERIAL NUMBER',
                        r'\d+/\d+\s+Banking', r'DBS Bank Ltd S/N:', r'\d{4}-\d{4}-\d{4}',
                        'TERM DEPOSIT', 'FIXED DEPOSIT', 'FD', 'DEPOSIT CERTIFICATE'
                    ]
                    
                    is_fixed_deposit = any(
                        re.search(indicator, line, re.IGNORECASE) for indicator in fixed_deposit_indicators
                    )
                    
                    if is_fixed_deposit:
                        logger.debug(f"Skipping fixed deposit line: {line[:100]}")
                        i += 1
                        continue
                    
                    # Additional validation: Check if line has proper transaction table structure
                    # Transaction lines should have amounts in proper positions
                    amounts = re.findall(r'[\d,]+\.\d{2}', line)
                    
                    # Skip if no amounts found (not a proper transaction row)
                    if not amounts:
                        logger.debug(f"Skipping line with no amounts: {line[:50]}")
                        i += 1
                        continue
                    
                    # Skip if this looks like a date range (fixed deposit pattern)
                    if re.search(r'\d{2}/\d{2}/\d{4}\s*-\s*\d{2}/\d{2}/\d{4}', line):
                        logger.debug(f"Skipping date range line: {line[:50]}")
                        i += 1
                        continue
                    
                    # Extract complete description using enhanced method
                    complete_description, next_i = self.extract_multiline_description(lines, i)
                    
                    # Skip if description is too short or invalid
                    if len(complete_description.strip()) < 3:
                        i = next_i
                        continue
                    
                    # Parse date more robustly
                    date_match = re.match(r'^(\d{1,2}/\d{1,2}/\d{4})', line)
                    if not date_match:
                        i = next_i
                        continue
                        
                    parsed_date = self.parse_date(date_match.group(1), default_year)
                    if not parsed_date:
                        logger.warning(f"Could not parse date from: {date_match.group(1)}")
                        i = next_i
                        continue
                    
                    # Enhanced amount extraction - get ALL amounts from the original transaction line
                    number_values = []
                    for amt_str in amounts:
                        try:
                            clean_amount = amt_str.replace(',', '')
                            val = float(clean_amount)
                            number_values.append(val)
                        except ValueError:
                            continue

                    # Apply flexible amount classification
                    withdrawal, deposit, balance = self._classify_transaction_amounts(
                        complete_description, number_values
                    )
                    
                    # Get conversion rate
                    conversion_rate = self.get_conversion_rate_for_currency(current_currency)
                    
                    transaction = {
                        'Client Name': self.client_name,
                        'Bank': self.bank,
                        'Account ID': current_account,
                        'Date': parsed_date,
                        'Currency': current_currency,
                        'Transaction Description': complete_description,
                        'Transaction Type': self.categorize_transaction(complete_description),
                        'Withdrawal': withdrawal,
                        'Deposit': deposit,
                        'Balance': balance,
                        'Conversion Rate': conversion_rate
                    }
                    
                    if self.add_transaction_if_unique(transaction):
                        transactions.append(transaction)
                        logger.debug(f"Added unique transaction for account {current_account}: {parsed_date} - {complete_description[:50]}")
                    i = next_i
                    continue
                
                # Handle Balance B/F table rows (no date, but from transaction table)
                elif is_balance_bf_line:
                    # Extract amounts from Balance B/F line
                    amounts = re.findall(r'[\d,]+\.\d{2}', line)
                    if amounts:
                        try:
                            # Use the last amount as balance
                            balance_amount = float(amounts[-1].replace(',', ''))
                            
                            # Create Balance B/F transaction with statement month
                            transaction = {
                                'Client Name': self.client_name,
                                'Bank': self.bank,
                                'Account ID': current_account,
                                'Date': self.statement_month,  # Use statement month instead of empty string
                                'Currency': current_currency,
                                'Transaction Description': 'Balance B/F',
                                'Transaction Type': 'Balance Brought Forward',
                                'Withdrawal': '',
                                'Deposit': '',
                                'Balance': str(balance_amount),
                                'Conversion Rate': self.get_conversion_rate_for_currency(current_currency)
                            }
                            
                            if self.add_transaction_if_unique(transaction):
                                transactions.append(transaction)
                                logger.debug(f"Added Balance B/F for account {current_account}: {balance_amount}")
                        
                        except (ValueError, IndexError):
                            logger.warning(f"Could not parse Balance B/F amount from: {line}")
                    
                    i += 1
                    continue
                
                # Section end detection - be more specific but preserve currency across pages
                elif any(end_marker in line for end_marker in ['Total Balance Carried Forward', 'End of Transaction']):
                    logger.debug(f"End of section detected for account {current_account}: {line}")
                    in_transaction_section = False
                    current_account = None
                    current_currency = None
                # Page break detection - maintain currency but reset account context
                elif 'Page:' in line or 'Page ' in line:
                    logger.debug(f"Page break detected: {line}")
                    # Keep currency but reset account context to allow re-detection
                    if current_currency:
                        logger.debug(f"Maintaining currency {current_currency} across page break")
                    in_transaction_section = False
                    current_account = None
                    # current_currency is preserved for next page
            
            i += 1
        
        # Mark this section as processed
        self.processed_sections.add(section_hash)
        return transactions
    def extract_from_pdf(self, pdf_path: str) -> List[Dict]:
        """Main extraction method"""
        logger.info(f"Processing: {os.path.basename(pdf_path)}")
        # Reset for new PDF
        self.transactions = []
        self.deduplicator = TransactionDeduplicator()
        self.processed_sections = set()
        try:
            # Extract text and tables
            text_content, tables = self.extract_text_and_tables(pdf_path)
            
            # Extract header information
            if not self.client_name:
                self.extract_client_name_enhanced(text_content)
            
            self.extract_account_ids(text_content)
            
            # Extract conversion rates consistently
            self.conversion_rates = self.extract_conversion_rates(text_content)
            # Extract statement month for Balance B/F transactions
            self.statement_month = self.extract_statement_month(text_content)
            
            # Determine default year
            year_matches = re.findall(r'\b(20\d{2})\b', text_content)
            default_year = int(max(year_matches)) if year_matches else 2025
            
            # Parse transactions
            transactions = self.parse_transaction_section(text_content, default_year)
            
            # Replace:
            # logger.info(f"Extracted {len(transactions)} transactions")

            # With:
            # Final deduplication stats
            dedup_stats = self.deduplicator.get_stats()
            logger.info(f"Extracted {len(transactions)} unique transactions")
            logger.info(f"Deduplication stats: {dedup_stats}")
            
            # Validate conversion rates
            self._validate_conversion_rates(transactions)
            
            return transactions
            
        except Exception as e:
            logger.error(f"Error processing {pdf_path}: {str(e)}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            return []

    def _validate_conversion_rates(self, transactions: List[Dict]):
        """Validate conversion rate consistency"""
        currency_rates = {}
        
        for tx in transactions:
            currency = tx.get('Currency', '')
            rate = tx.get('Conversion Rate', '')
            
            if currency and rate:
                if currency in currency_rates:
                    if currency_rates[currency] != rate:
                        logger.warning(f"Inconsistent rate for {currency}: expected {currency_rates[currency]}, got {rate}")
                else:
                    currency_rates[currency] = rate
        
        logger.info(f"Conversion rate validation complete: {currency_rates}")

    def save_to_excel(self, output_path: str = None) -> str:
        """Save transactions to Excel with enhanced formatting"""
        if not self.transactions:
            raise ValueError("No transactions to save")
        
        if not output_path:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            client_clean = re.sub(r'[^\w\s]', '', self.client_name).replace(' ', '_')
            output_path = f"DBS_{client_clean}_Transactions_{timestamp}.xlsx"
        
        # Create DataFrame
        df = pd.DataFrame(self.transactions)
        
        # Ensure correct column order
        columns = [
            'Client Name', 'Bank', 'Account ID', 'Date', 'Currency', 
            'Transaction Description', 'Transaction Type', 'Withdrawal', 
            'Deposit', 'Balance', 'Conversion Rate'
        ]
        
        # Reorder and fill missing columns
        for col in columns:
            if col not in df.columns:
                df[col] = ''
        
        df = df[columns]
        
        # Clean data
        df['Date'] = pd.to_datetime(df['Date'], errors='coerce').dt.strftime('%Y-%m-%d')
        
        # Save with formatting
        with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name='DBS Transactions', index=False)
            
            # Format columns
            worksheet = writer.sheets['DBS Transactions']
            for column in worksheet.columns:
                max_length = 0
                column_name = column[0].column_letter
                for cell in column:
                    try:
                        if len(str(cell.value)) > max_length:
                            max_length = len(str(cell.value))
                    except:
                        pass
                adjusted_width = min(max_length + 2, 70)
                worksheet.column_dimensions[column_name].width = adjusted_width
        
        logger.info(f"Excel file saved: {output_path}")
        return output_path


def process_single_pdf(pdf_path: str) -> Tuple[List[Dict], str]:
    """Process a single PDF file"""
    extractor = EnhancedDBSExtractor()
    transactions = extractor.extract_from_pdf(pdf_path)
    return transactions, extractor.client_name


def process_folder(folder_path: str, output_file: str = None) -> str:
    """Process all PDF files in a folder with global deduplication"""
    folder = Path(folder_path)
    
    if not folder.exists():
        raise ValueError(f"Folder '{folder_path}' not found")
    
    pdf_files = list(folder.glob("*.pdf")) + list(folder.glob("*.PDF"))
    if not pdf_files:
        raise ValueError(f"No PDF files found in '{folder_path}'")
    
    print(f"Found {len(pdf_files)} PDF files to process")
    
    all_transactions = []
    processed_files = 0
    failed_files = []
    
    # Global deduplicator across all files
    global_deduplicator = TransactionDeduplicator()
    
    for pdf_file in pdf_files:
        try:
            print(f"Processing: {pdf_file.name}...")
            transactions, client_name = process_single_pdf(str(pdf_file))
            
            if transactions:
                # Apply global deduplication
                unique_transactions = []
                for tx in transactions:
                    tx['Source File'] = pdf_file.name
                    if not global_deduplicator.is_duplicate(tx):
                        unique_transactions.append(tx)
                
                all_transactions.extend(unique_transactions)
                processed_files += 1
                print(f"  Added {len(unique_transactions)} unique transactions from {pdf_file.name} (filtered from {len(transactions)})")
            else:
                print(f"  No transactions found in {pdf_file.name}")
                failed_files.append(pdf_file.name)
                
        except Exception as e:
            print(f"  Failed to process {pdf_file.name}: {str(e)}")
            failed_files.append(pdf_file.name)
    
    # Get final deduplication stats (AFTER the loop)
    dedup_stats = global_deduplicator.get_stats()
    
    if not all_transactions:
        raise ValueError("No transactions extracted from any PDF files")
    
    # Generate output filename
    if not output_file:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = f"DBS_Combined_Statements_{timestamp}.xlsx"
    
    # Sort by date
    all_transactions.sort(key=lambda x: x.get('Date', ''))
    
    # Create DataFrame
    df = pd.DataFrame(all_transactions)
    
    columns = [
        'Client Name', 'Bank', 'Account ID', 'Date', 'Currency', 
        'Transaction Description', 'Transaction Type', 'Withdrawal', 
        'Deposit', 'Balance', 'Conversion Rate', 'Source File'
    ]
    
    for col in columns:
        if col not in df.columns:
            df[col] = ''
    
    df = df[columns]
    
    # Save to Excel
    with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
        df.to_excel(writer, sheet_name='DBS Transactions', index=False)
        
        worksheet = writer.sheets['DBS Transactions']
        for column in worksheet.columns:
            max_length = 0
            column_name = column[0].column_letter
            for cell in column:
                try:
                    if len(str(cell.value)) > max_length:
                        max_length = len(str(cell.value))
                except:
                    pass
            adjusted_width = min(max_length + 2, 70)
            worksheet.column_dimensions[column_name].width = adjusted_width
    
    # Print summary (AFTER the loop)
    print(f"\nProcessing completed!")
    print(f"Unique transactions: {len(all_transactions)}")
    print(f"Excel file: {output_file}")
    print(f"Files processed: {processed_files}/{len(pdf_files)}")
    print(f"Deduplication: {dedup_stats['unique_transactions']} unique signatures tracked")
    
    if failed_files:
        print(f"Failed files: {', '.join(failed_files)}")
    
    # Summary statistics
    currencies = set(tx['Currency'] for tx in all_transactions if tx['Currency'])
    accounts = set(tx['Account ID'] for tx in all_transactions if tx['Account ID'])
    clients = set(tx['Client Name'] for tx in all_transactions if tx['Client Name'])
    
    print(f"\nData Summary:")
    print(f"   Clients: {len(clients)} ({', '.join(sorted(clients))})")
    print(f"   Accounts: {len(accounts)}")
    print(f"   Currencies: {len(currencies)} ({', '.join(sorted(currencies))})")
    if all_transactions:
        dates = [tx['Date'] for tx in all_transactions if tx['Date']]
        if dates:
            print(f"   Date range: {min(dates)} to {max(dates)}")
    
    return output_file


def main():
    """Main function"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Enhanced DBS bank statement PDF extractor",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Process single PDF
  python enhanced_dbs_extractor.py statement.pdf
  
  # Process folder
  python enhanced_dbs_extractor.py dbs_pdfs/
  
  # Process with custom output
  python enhanced_dbs_extractor.py dbs_pdfs/ -o dbs_statements.xlsx
        """
    )
    
    parser.add_argument('input', help='PDF file or folder containing PDFs')
    parser.add_argument('-o', '--output', help='Output Excel filename (optional)')
    parser.add_argument('--debug', action='store_true', help='Enable debug logging')
    
    args = parser.parse_args()
    
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
    
    try:
        input_path = Path(args.input)
        
        if input_path.is_file() and input_path.suffix.lower() == '.pdf':
            print(f"Processing DBS statement: {input_path}")
            
            extractor = EnhancedDBSExtractor()
            transactions = extractor.extract_from_pdf(str(input_path))
            
            if not transactions:
                print("No transactions found in the PDF")
                return
            
            # Save to Excel
            output_file = extractor.save_to_excel(args.output)
            dedup_stats = extractor.deduplicator.get_stats()

            print(f"\nExtraction completed!")  # Remove emoji
            print(f"Total unique transactions: {len(transactions)}")  # Change "Total" to "Total unique"
            print(f"Excel file: {output_file}")  # Remove emoji
            print(f"Client: {extractor.client_name}")  # Remove emoji
            print(f"Deduplication: {dedup_stats['unique_transactions']} unique signatures tracked")
            
            # Statistics
            currencies = set(tx['Currency'] for tx in transactions if tx['Currency'])
            accounts = set(tx['Account ID'] for tx in transactions if tx['Account ID'])
            
            print(f"\n📋 Summary:")
            print(f"   Accounts: {len(accounts)} ({', '.join(sorted(accounts))})")
            print(f"   Currencies: {len(currencies)} ({', '.join(sorted(currencies))})")
            dates = [tx['Date'] for tx in transactions if tx['Date']]
            if dates:
                print(f"   Date range: {min(dates)} to {max(dates)}")
                
        elif input_path.is_dir():
            print(f"Processing PDF folder: {input_path}")
            output_file = process_folder(str(input_path), args.output)
            
        else:
            print(f"Error: '{args.input}' is not a valid file or directory")
            sys.exit(1)
            
    except Exception as e:
        logger.error(f"Processing failed: {str(e)}")
        print(f"❌ Error: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()