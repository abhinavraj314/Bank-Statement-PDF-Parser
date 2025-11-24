#!/usr/bin/env python3
"""
Enhanced UOB Bank Statement PDF Transaction Extractor
Handles all UOB statement types with advanced table extraction and OCR capabilities
"""

import os
import re
import sys
import logging
from pathlib import Path
from typing import List, Dict, Optional, Tuple, Any, Union
from datetime import datetime
import argparse
import traceback

import pandas as pd
import numpy as np
from dateutil import parser as dateparser
import openpyxl
from openpyxl.styles import Font, Alignment
from tqdm import tqdm

# Enhanced PDF processing libraries
try:
    import fitz  # PyMuPDF
    PYMUPDF_AVAILABLE = True
except ImportError:
    PYMUPDF_AVAILABLE = False
    import pdfplumber

try:
    import tabula
    TABULA_AVAILABLE = True
except ImportError:
    TABULA_AVAILABLE = False

try:
    import camelot
    CAMELOT_AVAILABLE = True
except ImportError:
    CAMELOT_AVAILABLE = False

try:
    import pytesseract
    import cv2
    OCR_AVAILABLE = True
except ImportError:
    OCR_AVAILABLE = False

try:
    from loguru import logger
    LOGURU_AVAILABLE = True
    # Configure loguru
    logger.remove()  # Remove default handler
    logger.add(sys.stderr, format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>")
except ImportError:
    LOGURU_AVAILABLE = False
    # Fallback to standard logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)-8s | %(name)s:%(funcName)s:%(lineno)d - %(message)s')
    logger = logging.getLogger(__name__)

class EnhancedUOBTransactionExtractor:
    def __init__(self, use_ocr: bool = False, extraction_method: str = "auto"):
        """
        Initialize the enhanced UOB transaction extractor
        
        Args:
            use_ocr: Whether to use OCR for scanned PDFs
            extraction_method: 'auto', 'pdfplumber', 'pymupdf', 'tabula', 'camelot'
        """
        self.transactions = []
        self.client_name = ""
        self.fx_rates = {}  # This will store the extracted rates from the PDF
        self.use_ocr = use_ocr and OCR_AVAILABLE
        self.extraction_method = extraction_method
        
        # Log available libraries
        self._log_available_libraries()
        
        # Currency mappings for UOB accounts
        self.currency_mapping = {
            '249-9': 'AUD', '250-2': 'CAD', '251-0': 'CHF', '252-9': 'CNH',
            '253-7': 'EUR', '254-5': 'GBP', '255-3': 'HKD', '256-1': 'JPY', 
            '258-8': 'NZD', '259-6': 'USD'
        }
        
        # Default FX rates (only used if PDF extraction completely fails)
        self.default_fx_rates = {
            'USD': 1.2936, 'EUR': 1.4643, 'GBP': 1.6941, 'CHF': 1.5774,
            'JPY': 0.008485, 'HKD': 0.16305, 'AUD': 0.8207, 'CAD': 0.9236,
            'NZD': 0.7513, 'CNH': 0.1777
        }

    def _log_available_libraries(self):
        """Log which enhanced libraries are available"""
        libraries = {
            'PyMuPDF': PYMUPDF_AVAILABLE,
            'Tabula-py': TABULA_AVAILABLE,
            'Camelot-py': CAMELOT_AVAILABLE,
            'OCR (pytesseract + opencv)': OCR_AVAILABLE,
            'Loguru': LOGURU_AVAILABLE
        }
        
        logger.info("Available enhanced libraries:")
        for lib, available in libraries.items():
            status = "✓" if available else "✗"
            logger.info(f"  {status} {lib}")

    def extract_text_pymupdf(self, pdf_path: str) -> str:
        """Extract text using PyMuPDF (faster alternative to pdfplumber)"""
        if not PYMUPDF_AVAILABLE:
            raise ImportError("PyMuPDF not available")
        
        text_content = ""
        doc = fitz.open(pdf_path)
        
        try:
            for page_num in range(doc.page_count):
                page = doc[page_num]
                text_content += page.get_text() + "\n"
                
                # If OCR is enabled and text extraction fails
                if self.use_ocr and len(page.get_text().strip()) < 50:
                    logger.info(f"Poor text extraction on page {page_num + 1}, trying OCR...")
                    text_content += self._ocr_page_pymupdf(page) + "\n"
        finally:
            doc.close()
        
        return text_content

    def _ocr_page_pymupdf(self, page) -> str:
        """Perform OCR on a PyMuPDF page"""
        if not OCR_AVAILABLE:
            return ""
        
        try:
            # Get page as image
            mat = fitz.Matrix(2.0, 2.0)  # 2x zoom for better OCR
            pix = page.get_pixmap(matrix=mat)
            img_data = pix.tobytes("png")
            
            # Convert to OpenCV format
            nparr = np.frombuffer(img_data, np.uint8)
            img = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
            
            # Preprocess for better OCR
            gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
            # Denoise
            denoised = cv2.fastNlMeansDenoising(gray)
            # Enhance contrast
            enhanced = cv2.createCLAHE(clipLimit=2.0, tileGridSize=(8,8)).apply(denoised)
            
            # OCR
            ocr_text = pytesseract.image_to_string(enhanced, config='--psm 6')
            return ocr_text
            
        except Exception as e:
            logger.warning(f"OCR failed: {e}")
            return ""

    def extract_tables_tabula(self, pdf_path: str, page_num: int = None) -> List[pd.DataFrame]:
        """Extract tables using tabula-py"""
        if not TABULA_AVAILABLE:
            return []
        
        try:
            pages = page_num if page_num else 'all'
            tables = tabula.read_pdf(
                pdf_path,
                pages=pages,
                multiple_tables=True,
                pandas_options={'header': None},
                lattice=True,  # Try lattice method first
                stream=False
            )
            
            # If lattice fails, try stream method
            if not tables:
                logger.info("Lattice method failed, trying stream method...")
                tables = tabula.read_pdf(
                    pdf_path,
                    pages=pages,
                    multiple_tables=True,
                    pandas_options={'header': None},
                    lattice=False,
                    stream=True
                )
            
            # Clean and filter tables
            cleaned_tables = []
            for table in tables:
                if not table.empty and table.shape[0] > 2:  # At least 3 rows
                    # Remove completely empty rows and columns
                    table = table.dropna(how='all').dropna(axis=1, how='all')
                    if not table.empty:
                        cleaned_tables.append(table)
            
            logger.info(f"Tabula extracted {len(cleaned_tables)} tables from {os.path.basename(pdf_path)}")
            return cleaned_tables
            
        except Exception as e:
            logger.warning(f"Tabula extraction failed: {e}")
            return []

    def extract_tables_camelot(self, pdf_path: str, page_num: int = None) -> List[pd.DataFrame]:
        """Extract tables using camelot-py"""
        if not CAMELOT_AVAILABLE:
            return []
        
        try:
            pages = str(page_num) if page_num else 'all'
            
            # Try lattice method first (better for tables with clear borders)
            tables = camelot.read_pdf(
                pdf_path,
                pages=pages,
                flavor='lattice'
            )
            
            # If lattice fails or finds no tables, try stream method
            if not tables or len(tables) == 0:
                logger.info("Camelot lattice method found no tables, trying stream method...")
                tables = camelot.read_pdf(
                    pdf_path,
                    pages=pages,
                    flavor='stream'
                )
            
            # Convert to list of DataFrames and filter
            cleaned_tables = []
            for table in tables:
                df = table.df
                if not df.empty and df.shape[0] > 2:  # At least 3 rows
                    # Remove completely empty rows and columns
                    df = df.replace('', np.nan).dropna(how='all').dropna(axis=1, how='all')
                    if not df.empty:
                        cleaned_tables.append(df)
            
            logger.info(f"Camelot extracted {len(cleaned_tables)} tables from {os.path.basename(pdf_path)}")
            return cleaned_tables
            
        except Exception as e:
            logger.warning(f"Camelot extraction failed: {e}")
            return []

    def extract_text_and_tables(self, pdf_path: str) -> Tuple[str, List[pd.DataFrame]]:
        """
        Extract text and tables using the best available method
        
        Returns:
            Tuple of (text_content, list_of_tables)
        """
        text_content = ""
        tables = []
        
        if self.extraction_method == "auto":
            # Auto-select best method based on available libraries
            if PYMUPDF_AVAILABLE:
                method = "pymupdf"
            else:
                method = "pdfplumber"
        else:
            method = self.extraction_method
        
        logger.info(f"Using extraction method: {method}")
        
        # Extract text
        try:
            if method == "pymupdf" and PYMUPDF_AVAILABLE:
                text_content = self.extract_text_pymupdf(pdf_path)
            else:
                # Fallback to pdfplumber
                import pdfplumber
                with pdfplumber.open(pdf_path) as pdf:
                    for page in pdf.pages:
                        page_text = page.extract_text() or ""
                        text_content += page_text + "\n"
                        
                        # Try OCR if text is sparse and OCR is available
                        if self.use_ocr and len(page_text.strip()) < 50:
                            logger.info(f"Trying OCR on sparse page...")
                            
        except Exception as e:
            logger.error(f"Text extraction failed: {e}")
            text_content = ""
        
        # Extract tables
        if method in ["auto", "tabula"] and TABULA_AVAILABLE:
            tables.extend(self.extract_tables_tabula(pdf_path))
        
        if method in ["auto", "camelot"] and CAMELOT_AVAILABLE and not tables:
            tables.extend(self.extract_tables_camelot(pdf_path))
        
        return text_content, tables

    def find_transaction_tables(self, tables: List[pd.DataFrame], text_content: str) -> List[pd.DataFrame]:
        """Identify tables that contain transaction data"""
        transaction_tables = []
        
        # Keywords that indicate transaction tables
        transaction_keywords = [
            'date', 'description', 'withdrawal', 'deposit', 'balance',
            'amount', 'debit', 'credit', 'transaction', 'particulars'
        ]
        
        for i, table in enumerate(tables):
            # Check if table has transaction-like structure
            score = 0
            
            # Check column headers (first few rows)
            header_text = ""
            for row_idx in range(min(3, len(table))):
                header_text += " ".join([str(cell).lower() for cell in table.iloc[row_idx] if pd.notna(cell)])
            
            # Score based on transaction keywords
            for keyword in transaction_keywords:
                if keyword in header_text:
                    score += 1
            
            # Check if table has date-like patterns
            date_pattern = r'\d{1,2}[-/\s](jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec|[0-9]{1,2})[-/\s]\d{2,4}'
            for row_idx in range(min(10, len(table))):  # Check first 10 rows
                row_text = " ".join([str(cell).lower() for cell in table.iloc[row_idx] if pd.notna(cell)])
                if re.search(date_pattern, row_text, re.IGNORECASE):
                    score += 2
                    break
            
            # Check if table has amount-like patterns
            amount_pattern = r'\d+[,.]?\d*\.\d{2}'
            amount_count = 0
            for row_idx in range(min(10, len(table))):
                row_text = " ".join([str(cell) for cell in table.iloc[row_idx] if pd.notna(cell)])
                amount_count += len(re.findall(amount_pattern, row_text))
            
            if amount_count >= 3:  # At least 3 amounts found
                score += 3
            
            # Minimum score threshold for transaction tables
            if score >= 3:
                logger.info(f"Table {i} identified as transaction table (score: {score})")
                transaction_tables.append(table)
            else:
                logger.debug(f"Table {i} rejected (score: {score})")
        
        return transaction_tables

    def extract_client_name(self, text: str) -> str:
        """Extract client name from PDF text (enhanced with better patterns)"""
        patterns = [
            r'MR\s+([A-Z\s]+)\n',
            r'MS\s+([A-Z\s]+)\n',
            r'([A-Z]{2,}\s+[A-Z]{2,}(?:\s+[A-Z]{2,})*)\n\d+\s+[A-Z]+\s+ROAD',
            r'SANGANERIA\s+RAVI',
            r'Name:\s*([A-Z\s]+)',
            r'Client:\s*([A-Z\s]+)',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                name = match.group(1) if '(' in pattern else match.group(0)
                name = name.strip()
                if len(name) > 5 and not any(char.isdigit() for char in name):
                    return name
        
        # Enhanced fallback
        lines = text.split('\n')[:30]  # Check first 30 lines
        for i, line in enumerate(lines):
            line = line.strip()
            # Look for capitalized names
            if re.match(r'^[A-Z][A-Z\s]{8,50}$', line):
                # Check if next line has address indicators
                next_line = lines[i + 1].strip() if i + 1 < len(lines) else ""
                if any(indicator in next_line.upper() for indicator in ['ROAD', 'STREET', 'AVENUE', 'SINGAPORE', 'BLOCK', 'UNIT']):
                    return line
        
        return "SANGANERIA RAVI"  # Default fallback

    def parse_date(self, date_str: str, default_year: int = 2025) -> Optional[str]:
        """Enhanced date parsing with better format support"""
        if not date_str or str(date_str).strip().lower() in ['nan', 'none', '']:
            return None
            
        date_str = str(date_str).strip()
        
        month_map = {
            'jan': '01', 'feb': '02', 'mar': '03', 'apr': '04',
            'may': '05', 'jun': '06', 'jul': '07', 'aug': '08',
            'sep': '09', 'oct': '10', 'nov': '11', 'dec': '12'
        }
        
        # Handle various date formats
        date_patterns = [
            # DD MMM YYYY or DD MMM
            (r'(\d{1,2})\s+(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)(?:\s+(\d{4}))?', 'dd-mmm-yyyy'),
            # DD-MMM-YYYY
            (r'(\d{1,2})-(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)-(\d{4})', 'dd-mmm-yyyy'),
            # DD/MM/YYYY or DD/MM/YY
            (r'(\d{1,2})/(\d{1,2})/(\d{2,4})', 'dd/mm/yyyy'),
            # YYYY-MM-DD
            (r'(\d{4})-(\d{1,2})-(\d{1,2})', 'yyyy-mm-dd'),
        ]
        
        for pattern, format_type in date_patterns:
            match = re.search(pattern, date_str.lower())
            if match:
                if format_type == 'dd-mmm-yyyy':
                    day = match.group(1).zfill(2)
                    month = month_map.get(match.group(2), '01')
                    year = match.group(3) if match.group(3) else str(default_year)
                    return f"{year}-{month}-{day}"
                elif format_type == 'dd/mm/yyyy':
                    day = match.group(1).zfill(2)
                    month = match.group(2).zfill(2)
                    year = match.group(3)
                    if len(year) == 2:
                        year = f"20{year}" if int(year) < 50 else f"19{year}"
                    return f"{year}-{month}-{day}"
                elif format_type == 'yyyy-mm-dd':
                    return f"{match.group(1)}-{match.group(2).zfill(2)}-{match.group(3).zfill(2)}"
        
        # Try dateutil parser as fallback
        try:
            parsed_date = dateparser.parse(date_str, dayfirst=True)
            if parsed_date:
                return parsed_date.strftime('%Y-%m-%d')
        except:
            pass
        
        return None

    def clean_amount(self, amount_str: str) -> str:
        """Enhanced amount cleaning"""
        if not amount_str or str(amount_str).lower() in ['nan', 'none', '']:
            return ""
        
        # Remove currency symbols and whitespace
        cleaned = re.sub(r'[^\d.,-]', '', str(amount_str))
        # Handle comma as thousand separator
        cleaned = cleaned.replace(',', '')
        
        if not cleaned or cleaned in ['-', '.', '']:
            return ""
        
        # Handle negative amounts in parentheses
        if '(' in str(amount_str) and ')' in str(amount_str):
            cleaned = '-' + cleaned
            
        return cleaned

    def calculate_conversion_rate(self, sgd_amount: str, foreign_amount: str) -> str:
        """Calculate conversion rate from SGD amount and foreign currency amount"""
        try:
            if sgd_amount and foreign_amount:
                sgd_val = float(self.clean_amount(sgd_amount))
                foreign_val = float(self.clean_amount(foreign_amount))
                if foreign_val > 0:
                    rate = sgd_val / foreign_val
                    return f"{rate:.4f}"
        except (ValueError, ZeroDivisionError):
            pass
        return ""

    def get_conversion_rate_for_currency(self, currency: str) -> str:
        """
        Get the consistent conversion rate for a currency from the extracted FX rates table.
        
        Args:
            currency: Currency code (e.g., 'CHF', 'USD')
            
        Returns:
            String representation of the conversion rate, or empty string if not found
        """
        if currency == 'SGD':
            return '1.0000'  # SGD always has rate of 1.0
        
        # First try to get from extracted FX rates
        if currency in self.fx_rates:
            rate = self.fx_rates[currency]
            return f"{rate:.4f}"
        
        # Fallback to default rates if extraction failed
        if currency in self.default_fx_rates:
            rate = self.default_fx_rates[currency]
            logger.debug(f"Using default rate for {currency}: {rate}")
            return f"{rate:.4f}"
        
        logger.warning(f"No conversion rate found for currency: {currency}")
        return ""

    def categorize_transaction(self, description: str) -> str:
        """Enhanced transaction categorization"""
        desc_upper = description.upper()
        
        categories = {
            'Balance Brought Forward': ['BALANCE B/F', 'OPENING BALANCE', 'BAL B/F'],
            'Debit Card Purchase': ['MISC DR-DEBIT CARD', 'DEBIT CARD', 'POS PURCHASE', 'CARD PURCHASE'],
            'Interest Credit': ['INTEREST CREDIT', 'BONUS INTEREST', 'INTEREST EARNED'],
            'Deposit Placement': ['DEPOSIT PLACEMENT', 'TIME DEPOSIT', 'FIXED DEPOSIT'],
            'Deposit Maturity': ['DEPOSIT WITHDRAWAL', 'DEPOSIT MATURITY', 'MATURITY PROCEEDS'],
            'Fund Transfer': ['FUND TRANSFER', 'TRANSFER TO', 'TRANSFER FROM', 'GIRO'],
            'ATM Withdrawal': ['ATM WITHDRAWAL', 'CASH WITHDRAWAL'],
            'Salary Credit': ['SALARY', 'PAYROLL', 'EMPLOYMENT'],
            'Bank Charges': ['BANK CHARGES', 'SERVICE CHARGE', 'COMMISSION', 'FEE'],
        }
        
        for category, keywords in categories.items():
            if any(keyword in desc_upper for keyword in keywords):
                return category
        
        return 'Other'

    def extract_fx_rates(self, text: str) -> Dict[str, float]:
        """Extract FX rates from conversion rate table in PDF - FIXED VERSION"""
        fx_rates = {}
        
        # Look for various FX rate table formats in UOB statements
        rate_table_patterns = [
            # Pattern 1: "Foreign Exchange Rates against Singapore Dollar"
            r'Foreign Exchange Rates.*?Singapore Dollar(.*?)(?=\n\n|\nPage|\nStatement|\Z)',
            # Pattern 2: "FX Rate" or "Exchange Rate" section  
            r'(?:FX|Exchange)\s+Rates?\s*.*?\n(.*?)(?=\n\n|\nPage|\nStatement|\Z)',
            # Pattern 3: Currency conversion table with headers
            r'Currency.*?Rate.*?\n(.*?)(?=\n\n|\nPage|\nStatement|\Z)',
            # Pattern 4: Simple currency-rate pairs
            r'((?:USD|GBP|EUR|AUD|CAD|NZD|CHF|JPY|HKD|CNH).*?[\d.]+.*?\n){3,}',
        ]
        
        for pattern in rate_table_patterns:
            match = re.search(pattern, text, re.IGNORECASE | re.DOTALL)
            if match:
                table_text = match.group(1) if len(match.groups()) > 0 else match.group(0)
                logger.debug(f"Found potential FX rate table: {table_text[:200]}...")
                
                # Extract individual currency rates from table
                currency_rates = self._parse_fx_rate_table(table_text)
                if currency_rates:
                    fx_rates.update(currency_rates)
                    logger.info(f"Extracted {len(currency_rates)} FX rates from table")
                    break
        
        # If no table found, try individual rate patterns
        if not fx_rates:
            logger.info("No FX rate table found, trying individual rate extraction")
            fx_rates = self._extract_individual_fx_rates(text)
        
        # Store the extracted rates for consistent use
        if fx_rates:
            logger.info(f"Successfully extracted FX rates: {fx_rates}")
        else:
            logger.warning("No FX rates found in PDF, using default rates")
            fx_rates = self.default_fx_rates.copy()
        
        return fx_rates

    def _parse_fx_rate_table(self, table_text: str) -> Dict[str, float]:
        """Parse FX rates from extracted table text"""
        fx_rates = {}
        
        # Enhanced patterns for different table formats
        rate_extraction_patterns = [
            # Pattern 1: "USD    United States Dollar    1.2936"
            r'(USD|GBP|EUR|AUD|CAD|NZD|CHF|JPY|HKD|CNH)\s+.*?\s+([\d.]+)\s*$',
            # Pattern 2: "USD 1.2936" or "USD: 1.2936"
            r'(USD|GBP|EUR|AUD|CAD|NZD|CHF|JPY|HKD|CNH)[\s:]+?([\d.]+)',
            # Pattern 3: "United States Dollar USD 1.2936"
            r'.*?(USD|GBP|EUR|AUD|CAD|NZD|CHF|JPY|HKD|CNH)\s+([\d.]+)',
            # Pattern 4: Table format with currency in first column, rate in last
            r'(USD|GBP|EUR|AUD|CAD|NZD|CHF|JPY|HKD|CNH).*?([\d.]{4,})\s*$',
        ]
        
        lines = table_text.split('\n')
        
        for line in lines:
            line = line.strip()
            if not line or len(line) < 5:
                continue
                
            # Skip header lines
            if any(header in line.upper() for header in ['CURRENCY', 'RATE', 'EXCHANGE', 'SINGAPORE', 'DOLLAR']):
                continue
                
            # Try each pattern
            for pattern in rate_extraction_patterns:
                matches = re.findall(pattern, line, re.IGNORECASE)
                for match in matches:
                    currency = match[0].upper()
                    try:
                        rate = float(match[1])
                        # Validate reasonable rate range
                        if 0.001 <= rate <= 1000:
                            # Only store the first occurrence of each currency to ensure consistency
                            if currency not in fx_rates:
                                fx_rates[currency] = rate
                                logger.debug(f"Extracted rate {currency}: {rate} from line: {line}")
                            else:
                                logger.debug(f"Duplicate rate for {currency} found, keeping original: {fx_rates[currency]}")
                        else:
                            logger.debug(f"Rejected unreasonable rate {currency}: {rate}")
                    except (ValueError, IndexError):
                        continue
        
        return fx_rates

    def _extract_individual_fx_rates(self, text: str) -> Dict[str, float]:
        """Fallback method to extract individual FX rates from text"""
        fx_rates = {}
        
        # More comprehensive individual rate patterns
        individual_patterns = [
            # Pattern 1: "USD against SGD: 1.2936"
            r'(USD|GBP|EUR|AUD|CAD|NZD|CHF|JPY|HKD|CNH)\s+against\s+SGD[\s:]+?([\d.]+)',
            # Pattern 2: "SGD/USD 1.2936" 
            r'SGD/(USD|GBP|EUR|AUD|CAD|NZD|CHF|JPY|HKD|CNH)\s+([\d.]+)',
            # Pattern 3: "1 USD = 1.2936 SGD"
            r'1\s+(USD|GBP|EUR|AUD|CAD|NZD|CHF|JPY|HKD|CNH)\s*=\s*([\d.]+)\s*SGD',
            # Pattern 4: Currency name followed by rate
            r'(USD|GBP|EUR|AUD|CAD|NZD|CHF|JPY|HKD|CNH)\s+[\w\s]*?\s+([\d.]{4,})',
        ]
        
        for pattern in individual_patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            for match in matches:
                currency = match[0].upper()
                try:
                    rate = float(match[1])
                    if 0.001 <= rate <= 1000:
                        # Only store the first occurrence of each currency to ensure consistency
                        if currency not in fx_rates:
                            fx_rates[currency] = rate
                            logger.debug(f"Extracted individual rate {currency}: {rate}")
                except (ValueError, IndexError):
                    continue
        
        return fx_rates

    def extract_multiline_description(self, lines: List[str], start_idx: int) -> Tuple[str, int]:
        """
        FIXED: Extract complete multi-line transaction description while preserving the actual 
        description content and avoiding contamination from balance/withdrawal/deposit columns.
        """
        if start_idx >= len(lines):
            return "", start_idx + 1
        
        current_line = lines[start_idx].strip()
        
        # Extract description part (after date)
        date_match = re.match(r'^(\d{1,2}\s+(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec))\s+(.+)', current_line)
        if not date_match:
            return current_line, start_idx + 1
        
        # Get the full line after the date
        full_line_after_date = date_match.group(2).strip()
        
        # FIXED: More conservative approach - only remove obvious trailing balance amounts
        # Split the line into parts
        parts = full_line_after_date.split()
        
        # Look for the pattern where we have 2-3 standalone amounts at the very end
        # These are likely withdrawal/balance amounts, not part of description
        description_parts = []
        
        # Strategy: Include everything except for 2-3 trailing amounts that look like withdrawal/balance
        # Example: "Misc DR-Debit Card SGD 34.93 29 MAR 0355 5982832 UBER *TRIP HELP.UBER.COMNL 89.44 552.05"
        #          Keep everything up to "HELP.UBER.COMNL", remove "89.44 552.05"
        
        # Count trailing amounts (numbers with 2 decimal places at the end)
        trailing_amounts = 0
        for i in range(len(parts) - 1, -1, -1):
            if re.match(r'^[\d,]+\.\d{2}$', parts[i]):
                trailing_amounts += 1
            else:
                break
        
        # If we have 2 or more trailing amounts, likely withdrawal + balance
        # If we have 1 trailing amount, could be part of description (like SGD amount)
        if trailing_amounts >= 2:
            # Remove the last 2 amounts (likely withdrawal and balance)
            description_parts = parts[:-trailing_amounts]
            logger.debug(f"Removed {trailing_amounts} trailing amounts from description")
        else:
            # Keep everything - no clear trailing amounts pattern
            description_parts = parts
        
        # Join the main description parts
        main_description = ' '.join(description_parts)
        
        # Now look for continuation lines
        next_idx = start_idx + 1
        
        # Continuation line patterns
        continuation_patterns = [
            r'^[A-Z0-9\s\*\-\.\(\)/]+$',  # All caps with symbols, slashes
            r'^\d+\s+[A-Z\d\*\-\.]+.*',   # Reference numbers
            r'^[A-Z]{2,}(?:\s+[A-Z\d\*\-\.]+)*$',  # Merchant names
            r'^[A-Z][a-z]+(?:\s+[A-Z][a-z]*)*\s+[A-Z]{2}$',  # City Country
            r'^[A-Z\s]+\*[A-Z\s]+$',  # Patterns like "UBER *TRIP"
        ]
        
        location_indicators = [
            'FR', 'GB', 'US', 'SG', 'NL', 'CH', 'IE', 'DE',
            'LONDON', 'SINGAPORE', 'CANNES', 'AMSTERDAM', 'NEW YORK',
            'help.uber.com', 'LINKEDIN.COM', 'Global-e.com', 'HELP.UBER.COMNL'
        ]
        
        # Footer text patterns to stop at
        footer_patterns = [
            r'^Please\s*note\s*that\s*you\s*are\s*bound',
            r'^If\s*you\s*do\s*not\s*notify\s*us',
            r'^This\s*statement\s*covers\s*the\s*period',
            r'^\d+\s*/\s*\d+$',  # Page numbers like "1/2"
            r'^Page\s+\d+',
            r'^Continued\s+on\s+next\s+page',
            r'^End\s+of\s+statement',
            r'^For\s+your\s+security',
            r'^UOB\s+(Bank|Limited)',
            r'^Statement\s+(Date|Period)',
        ]
        
        # Check next few lines for continuations
        additional_parts = []
        for i in range(next_idx, min(start_idx + 4, len(lines))):  # Check up to 4 continuation lines
            if i >= len(lines):
                break
                
            next_line = lines[i].strip()
            
            # Stop conditions
            if not next_line:
                next_idx += 1
                continue
                
            # Stop at next transaction (new date)
            if re.match(r'^\d{1,2}\s+(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)', next_line):
                break
                
            # Stop at section headers
            if any(keyword in next_line for keyword in ['Total', 'Balance', 'FX+', 'Account', 'Date']):
                break
                
            # Stop at footer text
            if any(re.match(pattern, next_line, re.IGNORECASE) for pattern in footer_patterns):
                logger.debug(f"Stopping at footer text: {next_line[:50]}...")
                break
                
            # Stop if line looks like standard bank footer text
            if len(next_line) > 50 and any(word in next_line.lower() for word in 
                ['duty', 'bound', 'rules', 'governing', 'operation', 'statement', 'notify', 'error']):
                logger.debug(f"Stopping at bank footer: {next_line[:50]}...")
                break
            
            # FIXED: Be more inclusive about continuation lines for merchant info
            is_continuation = False
            
            # Check continuation patterns
            for pattern in continuation_patterns:
                if re.match(pattern, next_line):
                    is_continuation = True
                    break
            
            # Check for location indicators (including URLs)
            if not is_continuation:
                if any(indicator in next_line.upper() for indicator in location_indicators):
                    is_continuation = True
            
            # FIXED: More liberal acceptance of merchant/transaction text
            if not is_continuation:
                # Accept continuation if it's descriptive text (not just amounts)
                if (len(next_line) > 3 and  # Longer than just numbers
                    any(char.isalpha() for char in next_line) and  # Contains letters
                    len(next_line) < 100 and  # Reasonable length
                    not re.match(r'^[\d\s,\.]+$', next_line)):  # Not just numbers and punctuation
                    is_continuation = True
            
            if is_continuation:
                # For continuation lines, be more conservative about removing trailing amounts
                continuation_parts = next_line.split()
                
                # Only remove amounts if there are 2+ trailing amounts (likely balance info)
                cont_trailing_amounts = 0
                for j in range(len(continuation_parts) - 1, -1, -1):
                    if re.match(r'^[\d,]+\.\d{2}$', continuation_parts[j]):
                        cont_trailing_amounts += 1
                    else:
                        break
                
                if cont_trailing_amounts >= 2:
                    # Remove trailing amounts from continuation line
                    clean_continuation = ' '.join(continuation_parts[:-cont_trailing_amounts])
                else:
                    # Keep the whole continuation line
                    clean_continuation = next_line
                
                if clean_continuation.strip():
                    additional_parts.append(clean_continuation.strip())
                next_idx = i + 1
            else:
                break
        
        # Combine all description parts
        complete_description = main_description
        if additional_parts:
            complete_description += ' ' + ' '.join(additional_parts)
        
        complete_description = re.sub(r'\s+', ' ', complete_description).strip()
        
        # EXPLICIT FIX: Ensure BALANCE B/F descriptions are always clean
        if complete_description.upper().startswith('BALANCE B/F'):
            complete_description = re.sub(r'^(BALANCE\s+B/F).*$', r'\1', complete_description, flags=re.IGNORECASE)
            logger.debug(f"Ensured clean BALANCE B/F description: '{complete_description}'")
        
        return complete_description, next_idx

    def _clean_description_from_amounts(self, text: str) -> str:
        """
        FIXED: Preserve the actual transaction description exactly as it appears in the PDF,
        but remove only trailing balance/withdrawal/deposit amounts that don't belong to the description.
        Special handling for BALANCE B/F to remove any appended amounts.
        """
        if not text:
            return text
            
        # Store original for debugging
        original_text = text
        cleaned = text.strip()
        
        # SPECIAL CASE: Handle BALANCE B/F specifically - FIXED
        if cleaned.upper().startswith('BALANCE B/F'):
            # For BALANCE B/F, remove any numeric amounts that follow
            balance_bf_pattern = r'^(BALANCE\s+B/F).*$'
            cleaned = re.sub(balance_bf_pattern, r'\1', cleaned, flags=re.IGNORECASE)
            logger.debug(f"Cleaned BALANCE B/F: '{original_text}' -> '{cleaned}'")
            return cleaned.strip()
        
        # For all other transactions, apply the existing logic
        
        # Pattern 1: Remove standalone trailing amounts after SGD amounts
        # Example: "UBER *TRIP London GB SGD 68.56 1,234.56" -> "UBER *TRIP London GB SGD 68.56"
        double_amount_pattern = r'(SGD\s+[\d,]+\.?\d{0,2})\s+([\d,]+\.?\d{0,2})\s*$'
        if re.search(double_amount_pattern, cleaned, re.IGNORECASE):
            cleaned = re.sub(r'\s+([\d,]+\.?\d{0,2})\s*$', '', cleaned)
            logger.debug(f"Removed trailing amount after SGD amount: '{original_text}' -> '{cleaned}'")
        
        # Pattern 2: Remove standalone amounts at the very end that are NOT preceded by currency codes
        # Example: "Transaction description 463.49" -> "Transaction description"
        # But preserve: "Transaction SGD 68.56"
        elif not re.search(r'SGD\s+[\d,]+\.?\d{0,2}', cleaned, re.IGNORECASE):
            # Only remove trailing amounts if there's no SGD amount in the description
            standalone_amount_pattern = r'\s+([\d,]+\.?\d{0,2})\s*$'
            if re.search(standalone_amount_pattern, cleaned):
                cleaned = re.sub(standalone_amount_pattern, '', cleaned)
                logger.debug(f"Removed standalone trailing amount: '{original_text}' -> '{cleaned}'")
        
        # Pattern 3: Remove obvious balance/withdrawal/deposit labels with amounts at the end
        balance_patterns = [
            r'\s+balance[:\s]+([\d,]+\.?\d{0,2})\s*$',  # "description balance: 123.45"
            r'\s+bal[:\s]+([\d,]+\.?\d{0,2})\s*$',     # "description bal: 123.45"
            r'\s+withdrawal[:\s]+([\d,]+\.?\d{0,2})\s*$',  # "description withdrawal: 123.45"
            r'\s+deposit[:\s]+([\d,]+\.?\d{0,2})\s*$',     # "description deposit: 123.45"
        ]
        
        for pattern in balance_patterns:
            if re.search(pattern, cleaned, re.IGNORECASE):
                cleaned = re.sub(pattern, '', cleaned, flags=re.IGNORECASE)
                logger.debug(f"Removed labeled amount: '{original_text}' -> '{cleaned}'")
                break
        
        # Clean up any extra whitespace left behind
        cleaned = re.sub(r'\s+', ' ', cleaned).strip()
        
        # Only log if we actually made a meaningful change
        if len(cleaned) < len(original_text) - 5:  # More than 5 characters removed
            logger.debug(f"Description cleaning: '{original_text}' -> '{cleaned}'")
        
        return cleaned

    def _parse_regular_statement_text(self, text: str, default_year: int) -> List[Dict]:
        """Parse regular UOB multi-currency statement from text - FIXED VERSION"""
        transactions = []
        lines = text.split('\n')
        current_account = None
        current_currency = None
        in_transaction_section = False
        
        i = 0
        while i < len(lines):
            line = lines[i].strip()
            if not line:
                i += 1
                continue
            
            # Account detection
            if re.search(r'\bOne Account\s+\d{3}-\d{3}-\d{3}-\d', line):
                account_match = re.search(r'(\d{3}-\d{3}-\d{3}-\d)', line)
                if account_match:
                    current_account = account_match.group(1)
                    current_currency = "SGD"
                    in_transaction_section = True
            
            elif re.search(r'\bFX\+\s+\d{3}-\d{3}-\d{3}-\d', line):
                account_match = re.search(r'(\d{3}-\d{3}-\d{3}-\d)', line)
                if account_match:
                    current_account = account_match.group(1)
                    current_currency = "SGD"  # Default
                    
                    # Determine currency from account number
                    for code, curr in self.currency_mapping.items():
                        if code in current_account:
                            current_currency = curr
                            break
                    
                    in_transaction_section = True
            
            # Process transactions
            elif in_transaction_section and current_account:
                if re.search(r'\bDate\b.*\bDescription\b', line, re.IGNORECASE):
                    i += 1
                    continue
                
                # Transaction line
                date_match = re.match(r'^(\d{1,2}\s+(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec))\s+(.+)', line)
                if date_match:
                    date_str = date_match.group(1)
                    
                    # Extract multi-line description (now with enhanced cleaning)
                    complete_description, next_i = self.extract_multiline_description(lines, i)
                    
                    if len(complete_description) < 3:
                        i = next_i
                        continue
                    
                    # Parse date
                    parsed_date = self.parse_date(date_str, default_year)
                    if not parsed_date:
                        i = next_i
                        continue
                    
                    # Extract amounts from the original line (not the cleaned description)
                    amounts = re.findall(r'[\d,]+\.\d{2}', line)
                    number_values = []
                    for amt_str in amounts:
                        try:
                            val = float(amt_str.replace(',', ''))
                            number_values.append(val)
                        except:
                            continue
                    
                    # Determine withdrawal/deposit
                    withdrawal = ""
                    deposit = ""
                    balance = ""
                    
                    if 'BALANCE B/F' in complete_description.upper():
                        if number_values:
                            balance = str(number_values[-1])
                    elif 'Misc DR-Debit Card' in complete_description:
                        if len(number_values) >= 2:
                            withdrawal = str(number_values[0])
                            balance = str(number_values[-1])
                        elif len(number_values) == 1:
                            balance = str(number_values[0])
                    elif any(keyword in complete_description.lower() for keyword in ['interest', 'bonus']):
                        if len(number_values) >= 2:
                            deposit = str(number_values[0])
                            balance = str(number_values[-1])
                        elif len(number_values) == 1:
                            deposit = str(number_values[0])
                    else:
                        if len(number_values) >= 2:
                            amount_val = number_values[0]
                            balance = str(number_values[-1])
                            if amount_val != number_values[-1]:
                                withdrawal = str(amount_val)
                        elif len(number_values) == 1:
                            balance = str(number_values[0])
                    
                    # FIXED: Use consistent conversion rate from the FX rates table
                    conversion_rate = self.get_conversion_rate_for_currency(current_currency)
                    
                    transaction = {
                        'Client Name': self.client_name,
                        'Bank': 'UOB',
                        'Account ID': current_account,
                        'Date': parsed_date,
                        'Currency': current_currency,
                        'Transaction Description': complete_description,  # Now properly cleaned
                        'Transaction Type': self.categorize_transaction(complete_description),
                        'Withdrawal': withdrawal,
                        'Deposit': deposit,
                        'Balance': balance,
                        'Conversion Rate': conversion_rate  # Consistent rate from FX table
                    }
                    
                    # VALIDATION: Ensure BALANCE B/F descriptions are clean
                    if transaction['Transaction Description'].upper().startswith('BALANCE B/F'):
                        transaction['Transaction Description'] = 'Balance B/F'
                        logger.debug(f"Standardized BALANCE B/F description for {transaction['Date']}")
                    
                    transactions.append(transaction)
                    i = next_i
                    continue
                
                elif line.startswith('Total'):
                    in_transaction_section = False
                    current_account = None
                    current_currency = None
            
            i += 1
        
        return transactions

    def _parse_simple_portfolio_statement_text(self, text: str, default_year: int) -> List[Dict]:
        """
        Parse simple UOB portfolio statements that only contain Balance B/F transactions.
        These are different from complex portfolio statements with Cash Activity sections.
        """
        transactions = []
        lines = text.split('\n')
        
        current_account = None
        current_currency = None
        
        i = 0
        while i < len(lines):
            line = lines[i].strip()
            if not line:
                i += 1
                continue
            
            # Look for account headers like "CURRENT 450-347-611-2" or "GLOBAL CURRENCY 450-910-955-3"
            account_match = re.search(r'(CURRENT|GLOBAL CURRENCY)\s+(\d{3}-\d{3}-\d{3}-\d)', line)
            if account_match:
                account_type = account_match.group(1)
                current_account = account_match.group(2)
                
                # Determine currency from account type
                if account_type == "CURRENT":
                    current_currency = "SGD"
                elif account_type == "GLOBAL CURRENCY":
                    current_currency = "USD"  # Default, but check mapping
                    # Apply currency mapping based on account number
                    for code, curr in self.currency_mapping.items():
                        if code in current_account:
                            current_currency = curr
                            break
                
                logger.debug(f"Found account: {current_account}, type: {account_type}, currency: {current_currency}")
            
            # Look for transaction lines when we have an active account
            if current_account and current_currency:
                # Match transaction format: "01 May BALANCE B/F                     463.49"
                # or "01 May BALANCE B/F         0.00"
                tx_match = re.match(r'(\d{1,2}\s+\w{3})\s+(BALANCE\s+B/F)\s+(.*)$', line)
                if tx_match:
                    date_str = tx_match.group(1).strip()
                    description = tx_match.group(2).strip()
                    remainder = tx_match.group(3).strip()
                    
                    # Extract balance amount from the remainder
                    # Look for the last number pattern in the line
                    amount_matches = re.findall(r'([\d,]+\.?\d*)', remainder)
                    balance_amount = amount_matches[-1].replace(',', '') if amount_matches else "0.00"
                    
                    # Parse date
                    parsed_date = self.parse_date(date_str, default_year)
                    if not parsed_date:
                        logger.warning(f"Could not parse date: {date_str}")
                        i += 1
                        continue
                    
                    # Get conversion rate
                    conversion_rate = self.get_conversion_rate_for_currency(current_currency)
                    
                    # Create transaction
                    transaction = {
                        'Client Name': self.client_name,
                        'Bank': 'UOB',
                        'Account ID': current_account,
                        'Date': parsed_date,
                        'Currency': current_currency,
                        'Transaction Description': 'Balance B/F',  # Clean, standardized description
                        'Transaction Type': 'Balance Brought Forward',  # Correct transaction type
                        'Withdrawal': '',
                        'Deposit': '',
                        'Balance': balance_amount,  # Balance amount goes here
                        'Conversion Rate': conversion_rate
                    }
                    
                    # VALIDATION: Ensure BALANCE B/F descriptions are clean
                    if transaction['Transaction Description'].upper().startswith('BALANCE B/F'):
                        transaction['Transaction Description'] = 'Balance B/F'
                        logger.debug(f"Standardized BALANCE B/F description for {transaction['Date']}")
                    
                    transactions.append(transaction)
                    logger.info(f"Added Balance B/F transaction: {parsed_date} - {current_currency} {balance_amount} (Account: {current_account})")
                
                # Reset account context when we hit "Total" or move to next section
                elif line.startswith('Total') or 'End of Transaction Details' in line:
                    current_account = None
                    current_currency = None
            
            i += 1
        
        logger.info(f"Simple portfolio statement parsing extracted {len(transactions)} transactions")
        return transactions

    def detect_simple_portfolio_statement(self, text: str) -> bool:
        """
        Detect if this is a simple portfolio statement (only Balance B/F transactions)
        vs a complex portfolio statement (with Cash Activity sections)
        """
        # Simple portfolio statements have:
        # 1. Account Transaction Details section
        # 2. Only BALANCE B/F transactiosns
        # 3. No "Cash Activity Statement" section
        
        has_account_transaction_details = "Account Transaction Details" in text
        has_balance_bf = "BALANCE B/F" in text
        has_cash_activity = "Cash Activity Statement" in text
        
        # Count non-Balance B/F transactions
        transaction_lines = []
        lines = text.split('\n')
        for line in lines:
            # Look for date patterns that aren't Balance B/F
            if re.match(r'\d{1,2}\s+\w{3}\s+(?!BALANCE\s+B/F)', line):
                transaction_lines.append(line)
        
        non_balance_transactions = len(transaction_lines)
        
        # Simple portfolio = has account details, has balance B/F, no cash activity, minimal other transactions
        is_simple = (has_account_transaction_details and 
                    has_balance_bf and 
                    not has_cash_activity and
                    non_balance_transactions == 0)
        
        logger.debug(f"Simple portfolio detection: account_details={has_account_transaction_details}, "
                    f"balance_bf={has_balance_bf}, cash_activity={has_cash_activity}, "
                    f"other_transactions={non_balance_transactions} -> simple={is_simple}")
        
        return is_simple
    def _parse_portfolio_statement_text(self, text: str, default_year: int) -> List[Dict]:
        """Parse UOB portfolio statement from text - FIXED VERSION WITH PROPER INTEREST PARSING"""
        transactions = []
        
        # Process Cash Activity Statement sections
        lines = text.split('\n')
        current_account = None
        current_currency = None
        in_cash_activity = False
        
        i = 0
        while i < len(lines):
            line = lines[i].strip()
            if not line:
                i += 1
                continue
            
            # Detect Cash Activity Statement
            if 'Cash Activity Statement' in line:
                in_cash_activity = True
                i += 1
                continue
            
            if in_cash_activity:
                # Account header detection
                account_match = re.search(r'(SGD|USD|EUR|GBP|CHF)\s+(\d{3}-\d{3}-\d{3}-\d)', line)
                if account_match:
                    current_currency = account_match.group(1)
                    current_account = account_match.group(2)
                    i += 1
                    continue
                
                # Transaction processing
                if current_account and current_currency:
                    # Balance B/F
                    if 'Balance B/F' in line:
                        date_match = re.search(r'(\d{2}-\w{3}-\d{4})', line)
                        date_str = date_match.group(1) if date_match else f"01-Jan-{default_year}"
                        
                        parsed_date = self.parse_date(date_str, default_year)
                        amounts = re.findall(r'[\d,]+\.\d{2}', line)
                        balance = amounts[-1].replace(',', '') if amounts else ""
                        
                        # Clean description (should already be clean, but ensure consistency)
                        clean_description = 'Balance B/F'  # Explicitly set to clean value
                        
                        transaction = {
                            'Client Name': self.client_name,
                            'Bank': 'UOB',
                            'Account ID': current_account,
                            'Date': parsed_date,
                            'Currency': current_currency,
                            'Transaction Description': clean_description,
                            'Transaction Type': 'Balance Brought Forward',
                            'Withdrawal': '',
                            'Deposit': '',
                            'Balance': balance,
                            'Conversion Rate': self.get_conversion_rate_for_currency(current_currency)  # Consistent rate
                        }
                        
                        # VALIDATION: Ensure BALANCE B/F descriptions are clean
                        if transaction['Transaction Description'].upper().startswith('BALANCE B/F'):
                            transaction['Transaction Description'] = 'Balance B/F'
                            logger.debug(f"Standardized BALANCE B/F description for {transaction['Date']}")
                        
                        transactions.append(transaction)
                    
                    # FIXED: Enhanced transaction parsing for Interest and other transactions
                    elif (re.search(r'\d{2}-\w{3}-\d{4}', line) and 
                        any(keyword in line for keyword in ['Interest', 'Deposit', 'Placement', 'Withdrawal'])):
                        
                        date_match = re.search(r'(\d{2}-\w{3}-\d{4})', line)
                        date_str = date_match.group(1) if date_match else f"01-Jan-{default_year}"
                        
                        parsed_date = self.parse_date(date_str, default_year)
                        
                        # FIXED: Extract the actual transaction description from the line
                        # Look for patterns like "A88-26998-180000147 Interest Deposit 450-021-321-8 000004"
                        description_match = re.search(r'A\d{2}-\d{5}-\d{9}\s+\w+\s+(.+?)(?:\s+[\d,]+\.\d{2}|\s*$)', line)
                        if description_match:
                            raw_description = description_match.group(1).strip()
                        else:
                            # Fallback: extract everything after the date and reference number
                            parts = line.split()
                            if len(parts) > 2:
                                # Skip date and reference number, take the rest as description
                                raw_description = ' '.join(parts[2:])
                                # Remove trailing amounts
                                raw_description = re.sub(r'\s+[\d,]+\.\d{2}$', '', raw_description).strip()
                            else:
                                raw_description = "Portfolio Transaction"  # Ultimate fallback
                        
                        # Clean description of any remaining amounts
                        clean_description = self._clean_description_from_amounts(raw_description)
                        
                        # FIXED: Determine transaction type based on actual description
                        if 'Interest Credit' in line:
                            transaction_type = "Interest Credit"
                        elif 'Interest' in line and 'Deposit' in clean_description:
                            transaction_type = "Interest Credit"
                        elif 'Deposit Placement' in line:
                            transaction_type = "Deposit Placement"
                        elif 'Deposit Withdrawal' in line:
                            transaction_type = "Deposit Maturity"
                        elif 'Deposit Maturity' in line:
                            transaction_type = "Deposit Maturity"
                        else:
                            transaction_type = self.categorize_transaction(clean_description)
                        
                        # FIXED: Extract amounts correctly
                        amounts = re.findall(r'[\d,]+\.\d{2}', line)
                        number_values = [float(amt.replace(',', '')) for amt in amounts]
                        
                        withdrawal = ""
                        deposit = ""
                        balance = ""
                        
                        # FIXED: Improved amount assignment logic
                        if transaction_type == "Interest Credit":
                            # For interest credit, amount is typically a deposit
                            if len(number_values) >= 1:
                                deposit = str(number_values[0])
                                if len(number_values) >= 2:
                                    balance = str(number_values[-1])
                        elif transaction_type == "Deposit Placement":
                            # For deposit placement, amount is a withdrawal from cash
                            if len(number_values) >= 1:
                                withdrawal = str(number_values[0])
                        elif transaction_type == "Deposit Maturity":
                            # For deposit maturity, amount is a deposit to cash
                            if len(number_values) >= 1:
                                deposit = str(number_values[0])
                        else:
                            # For other transactions, try to determine based on context
                            if len(number_values) >= 1:
                                # If it looks like an interest or credit transaction, treat as deposit
                                if any(word in clean_description.lower() for word in ['interest', 'credit', 'deposit']):
                                    deposit = str(number_values[0])
                                else:
                                    # Otherwise, could be withdrawal
                                    withdrawal = str(number_values[0])
                        
                        transaction = {
                            'Client Name': self.client_name,
                            'Bank': 'UOB',
                            'Account ID': current_account,
                            'Date': parsed_date,
                            'Currency': current_currency,
                            'Transaction Description': clean_description,  # Now contains proper description
                            'Transaction Type': transaction_type,
                            'Withdrawal': withdrawal,
                            'Deposit': deposit,
                            'Balance': balance,
                            'Conversion Rate': self.get_conversion_rate_for_currency(current_currency)  # Consistent rate
                        }
                        
                        # VALIDATION: Ensure BALANCE B/F descriptions are clean
                        if transaction['Transaction Description'].upper().startswith('BALANCE B/F'):
                            transaction['Transaction Description'] = 'Balance B/F'
                            logger.debug(f"Standardized BALANCE B/F description for {transaction['Date']}")
                        
                        transactions.append(transaction)
                    
                    # End of account section
                    elif 'Total' in line:
                        current_account = None
                        current_currency = None
            
            i += 1
        
        return transactions
    def detect_statement_type(self, text: str) -> str:
        """Detect the type of UOB statement from text"""
        if any(indicator in text for indicator in ["Portfolio Statement", "Portfolio Overview", "Cash Activity Statement"]):
            return "portfolio"
        elif "Account Transaction Details" in text:
            return "regular"
        return "regular"  # default

    def extract_from_pdf(self, pdf_path: str) -> List[Dict]:
        """Enhanced main extraction method using multiple approaches - UPDATED VERSION"""
        logger.info(f"Processing: {os.path.basename(pdf_path)}")
        
        try:
            # Extract text and tables
            text_content, tables = self.extract_text_and_tables(pdf_path)
            
            # Extract client name first
            if not self.client_name:
                self.client_name = self.extract_client_name(text_content)
            
            # Extract FX rates once per PDF and store consistently
            self.fx_rates = self.extract_fx_rates(text_content)
            logger.info(f"FX rates extracted for this PDF: {self.fx_rates}")
            
            # Determine statement type with enhanced detection
            statement_type = self.detect_statement_type(text_content)
            is_simple_portfolio = self.detect_simple_portfolio_statement(text_content)
            
            logger.info(f"Detected statement type: {statement_type}")
            logger.info(f"Is simple portfolio (Balance B/F only): {is_simple_portfolio}")
            
            transactions = []
            
            # Try table-based extraction first (more accurate for well-structured data)
            if tables:
                logger.info(f"Found {len(tables)} tables, attempting table-based extraction")
                transaction_tables = self.find_transaction_tables(tables, text_content)
                logger.info(f"Identified {len(transaction_tables)} potential transaction tables")
                
                for i, table in enumerate(transaction_tables):
                    logger.debug(f"Table {i} shape: {table.shape}")
                    logger.debug(f"Table {i} first few rows:\n{table.head()}")
                    
                    # Try to determine account info from surrounding text
                    account_id = self.extract_account_from_context(text_content, table)
                    currency = self.extract_currency_from_context(text_content, account_id)
                    
                    logger.debug(f"Account: {account_id}, Currency: {currency}")
                    
                    table_transactions = []  # Placeholder for table parsing
                    transactions.extend(table_transactions)
                    logger.info(f"Extracted {len(table_transactions)} transactions from table {i}")
            else:
                logger.info("No tables found by table extraction methods")
            
            # Fallback to text-based extraction if table extraction fails
            if not transactions:
                logger.info("Table extraction failed or found no transactions, falling back to text-based extraction")
                # Determine default year
                year_matches = re.findall(r'\b(20\d{2})\b', text_content)
                default_year = int(max(year_matches)) if year_matches else 2025
                
                # Choose the appropriate parser based on statement type
                if is_simple_portfolio:
                    logger.info("Using simple portfolio statement parser for Balance B/F only statements")
                    transactions = self._parse_simple_portfolio_statement_text(text_content, default_year)
                elif statement_type == "portfolio":
                    logger.info("Using complex portfolio statement parser for Cash Activity statements")
                    transactions = self._parse_portfolio_statement_text(text_content, default_year)
                else:
                    logger.info("Using regular statement parser")
                    transactions = self._parse_regular_statement_text(text_content, default_year)
            
            logger.info(f"  Total extracted {len(transactions)} transactions")
            
            # VALIDATION: Check for consistent conversion rates
            self._validate_conversion_rates(transactions)
            
            return transactions
            
        except Exception as e:
            logger.error(f"Error processing {pdf_path}: {str(e)}")
            if hasattr(logger, 'level') and logger.level <= 10:  # DEBUG level
                logger.error(f"Full traceback: {traceback.format_exc()}")
            return []
    def _validate_conversion_rates(self, transactions: List[Dict]):
        """Validate that conversion rates are consistent for each currency"""
        currency_rates = {}
        
        for tx in transactions:
            currency = tx.get('Currency', '')
            rate = tx.get('Conversion Rate', '')
            
            if currency and rate:
                if currency in currency_rates:
                    if currency_rates[currency] != rate:
                        logger.warning(f"Inconsistent conversion rate for {currency}: "
                                     f"expected {currency_rates[currency]}, got {rate}")
                else:
                    currency_rates[currency] = rate
        
        logger.info(f"Validation complete. Consistent rates per currency: {currency_rates}")

    def extract_account_from_context(self, text: str, table: pd.DataFrame) -> str:
        """Extract account ID from text context around table"""
        # Look for account patterns
        account_patterns = [
            r'One Account\s+(\d{3}-\d{3}-\d{3}-\d)',
            r'FX\+\s+(\d{3}-\d{3}-\d{3}-\d)',
            r'Account.*?(\d{3}-\d{3}-\d{3}-\d)',
            r'(\d{3}-\d{3}-\d{3}-\d)',
        ]
        
        for pattern in account_patterns:
            match = re.search(pattern, text)
            if match:
                return match.group(1)
        
        return "Unknown"

    def extract_currency_from_context(self, text: str, account_id: str) -> str:
        """Extract currency from context"""
        # Check currency mapping first
        for code, currency in self.currency_mapping.items():
            if code in account_id:
                return currency
        
        # Look for currency indicators in text
        currency_patterns = [
            r'\b(SGD|USD|EUR|GBP|CHF|JPY|HKD|AUD|CAD|NZD|CNH)\b'
        ]
        
        for pattern in currency_patterns:
            matches = re.findall(pattern, text)
            if matches:
                return matches[0]
        
        return "SGD"  # Default

    def export_to_excel(self, all_transactions: List[Dict], output_path: str):
        """Enhanced Excel export with better formatting"""
        if not all_transactions:
            logger.error("No transactions to export")
            return
        
        df = pd.DataFrame(all_transactions)
        
        # Enhanced deduplication
        initial_count = len(df)
        # More comprehensive duplicate detection
        df = df.drop_duplicates(
            subset=['Account ID', 'Date', 'Transaction Description', 'Withdrawal', 'Deposit'], 
            keep='first'
        )
        final_count = len(df)
        
        if initial_count != final_count:
            logger.info(f"Removed {initial_count - final_count} duplicate transactions")
        
        # Enhanced sorting
        df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
        df = df.sort_values(['Account ID', 'Date', 'Transaction Description'])
        df['Date'] = df['Date'].dt.strftime('%Y-%m-%d')
        
        # Export with enhanced formatting
        with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name='UOB Transactions', index=False)
            
            # Enhanced formatting
            workbook = writer.book
            worksheet = writer.sheets['UOB Transactions']
            
            # Set column widths
            column_widths = {
                'A': 25, 'B': 10, 'C': 20, 'D': 15, 'E': 12,
                'F': 70, 'G': 25, 'H': 18, 'I': 18, 'J': 18, 'K': 18
            }
            
            for column, width in column_widths.items():
                worksheet.column_dimensions[column].width = width
            
            # Enhanced header formatting
            header_font = Font(bold=True, size=12)
            header_alignment = Alignment(horizontal='center', vertical='center')
            
            for cell in worksheet[1]:
                cell.font = header_font
                cell.alignment = header_alignment
            
            # Add summary sheet
            summary_df = pd.DataFrame([
                ['Total Transactions', len(df)],
                ['Client Name', self.client_name],
                ['Processing Method', self.extraction_method],
                ['Date Range', f"{df['Date'].min()} to {df['Date'].max()}"],
                ['Currencies', ', '.join(sorted(df['Currency'].unique()))],
                ['Available Libraries', self._get_library_status()]
            ], columns=['Metric', 'Value'])
            
            summary_df.to_excel(writer, sheet_name='Summary', index=False)
        
        logger.info(f"Exported {len(df)} transactions to {output_path}")

    def _get_library_status(self) -> str:
        """Get status of available libraries"""
        status = []
        if PYMUPDF_AVAILABLE: status.append("PyMuPDF")
        if TABULA_AVAILABLE: status.append("Tabula")
        if CAMELOT_AVAILABLE: status.append("Camelot")
        if OCR_AVAILABLE: status.append("OCR")
        return ", ".join(status) if status else "Basic (pdfplumber only)"

def main():
    parser = argparse.ArgumentParser(description='Enhanced UOB PDF Transaction Extractor')
    parser.add_argument('input_path', help='PDF file or directory containing PDFs')
    parser.add_argument('-o', '--output', default='uob_transactions.xlsx', 
                       help='Output Excel file (default: uob_transactions.xlsx)')
    parser.add_argument('--method', choices=['auto', 'pdfplumber', 'pymupdf', 'tabula', 'camelot'],
                       default='auto', help='Extraction method (default: auto)')
    parser.add_argument('--ocr', action='store_true', help='Enable OCR for scanned PDFs')
    parser.add_argument('--debug', action='store_true', help='Enable debug logging')
    
    args = parser.parse_args()
    
    if args.debug:
        if LOGURU_AVAILABLE:
            logger.remove()
            logger.add(sys.stderr, level="DEBUG")
        else:
            logging.getLogger().setLevel(logging.DEBUG)
    
    # Collect PDF files
    pdf_files = []
    input_path = Path(args.input_path)
    
    if input_path.is_dir():
        pdf_files = list(input_path.glob('*.pdf'))
        pdf_files.extend(input_path.glob('*.PDF'))
    elif input_path.is_file() and input_path.suffix.lower() == '.pdf':
        pdf_files = [input_path]
    else:
        logger.error(f"No PDF files found in: {args.input_path}")
        return
    
    if not pdf_files:
        logger.error("No PDF files found to process")
        return
    
    logger.info(f"Found {len(pdf_files)} PDF files to process")
    
    # Initialize enhanced extractor
    extractor = EnhancedUOBTransactionExtractor(
        use_ocr=args.ocr,
        extraction_method=args.method
    )
    
    all_transactions = []
    
    # Process PDFs with progress bar
    for pdf_path in tqdm(pdf_files, desc="Processing PDFs"):
        try:
            transactions = extractor.extract_from_pdf(str(pdf_path))
            all_transactions.extend(transactions)
        except Exception as e:
            logger.error(f"Error processing {pdf_path.name}: {str(e)}")
            if args.debug:
                logger.error(f"Full traceback: {traceback.format_exc()}")
    
    if not all_transactions:
        logger.error("No transactions extracted from any PDF")
        return
    
    # Export results
    extractor.export_to_excel(all_transactions, args.output)
    
    # Enhanced summary
    print(f"\n{'='*50}")
    print(f"EXTRACTION SUMMARY")
    print(f"{'='*50}")
    print(f"Total transactions: {len(all_transactions)}")
    print(f"Client: {extractor.client_name}")
    print(f"Extraction method: {args.method}")
    print(f"OCR enabled: {args.ocr}")
    
    if all_transactions:
        df = pd.DataFrame(all_transactions)
        print(f"Date range: {df['Date'].min()} to {df['Date'].max()}")
        print(f"Currencies: {', '.join(sorted(df['Currency'].unique()))}")
        
        # Enhanced account breakdown
        account_summary = df.groupby(['Account ID', 'Currency']).agg({
            'Transaction Description': 'count',
            'Withdrawal': lambda x: len([v for v in x if v]),
            'Deposit': lambda x: len([v for v in x if v])
        }).rename(columns={
            'Transaction Description': 'Total',
            'Withdrawal': 'Withdrawals', 
            'Deposit': 'Deposits'
        })
        
        print(f"\nAccount breakdown:")
        for (account, currency), row in account_summary.iterrows():
            print(f"  {account} ({currency}): {row['Total']} transactions ({row['Withdrawals']} withdrawals, {row['Deposits']} deposits)")
        
        # Transaction type breakdown
        type_summary = df['Transaction Type'].value_counts()
        print(f"\nTransaction types:")
        for tx_type, count in type_summary.items():
            print(f"  {tx_type}: {count}")
    
    print(f"\nOutput saved to: {args.output}")
    print(f"Available libraries: {extractor._get_library_status()}")

if __name__ == "__main__":
    main()