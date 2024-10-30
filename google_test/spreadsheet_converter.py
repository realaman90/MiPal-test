from typing import Dict, List, Optional
import logging
from datetime import datetime
import os
from dotenv import load_dotenv
import openai
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import tempfile
import json
import pandas as pd
import csv
from neo4j_test.user_store import UserStore

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SpreadsheetConverter:
    def __init__(self):
        """Initialize connections and services"""
        # OpenAI setup
        self.openai_api_key = os.getenv("OPENAI_API_KEY")
        if not self.openai_api_key:
            raise ValueError("OPENAI_API_KEY environment variable is required")
        openai.api_key = self.openai_api_key
        
        # Initialize UserStore for Google credentials
        self.user_store = UserStore()

    def close(self):
        """Close connections"""
        self.user_store.close()

    def _get_google_services(self, user_id: str):
        """Initialize Google services for a user"""
        try:
            # Get credentials from Neo4j using UserStore
            credentials = self.user_store.get_google_credentials(user_id)
            if not credentials:
                raise ValueError(f"No valid Google credentials found for user {user_id}")
            
            # Build services
            drive_service = build('drive', 'v3', credentials=credentials)
            sheets_service = build('sheets', 'v4', credentials=credentials)
            
            return drive_service, sheets_service
            
        except Exception as e:
            logger.error(f"Error getting Google services: {e}")
            raise

    def _extract_sheet_data(self, sheets_service, spreadsheet_id: str) -> List[Dict]:
        """Extract data from all sheets in a spreadsheet"""
        try:
            # Get spreadsheet metadata
            spreadsheet = sheets_service.spreadsheets().get(
                spreadsheetId=spreadsheet_id
            ).execute()
            
            sheets_data = []
            
            # Process each sheet
            for sheet in spreadsheet.get('sheets', []):
                try:
                    sheet_title = sheet['properties']['title']
                    range_name = f"'{sheet_title}'"
                    
                    # Get sheet data
                    result = sheets_service.spreadsheets().values().get(
                        spreadsheetId=spreadsheet_id,
                        range=range_name
                    ).execute()
                    
                    rows = result.get('values', [])
                    if rows:
                        # Handle empty or malformed headers
                        headers = rows[0] if rows else []
                        if not headers:
                            logger.warning(f"No headers found in sheet: {sheet_title}")
                            continue
                        
                        # Create DataFrame with explicit column names
                        if len(rows) > 1:
                            data = rows[1:]  # Data rows
                            # Pad rows to match header length
                            max_cols = len(headers)
                            padded_data = [row + [''] * (max_cols - len(row)) for row in data]
                            df = pd.DataFrame(padded_data, columns=headers)
                        else:
                            # Create empty DataFrame with headers if no data
                            df = pd.DataFrame(columns=headers)
                        
                        sheets_data.append({
                            'title': sheet_title,
                            'data': df
                        })
                        logger.info(f"Successfully processed sheet: {sheet_title}")
                    
                except Exception as e:
                    logger.error(f"Error processing sheet {sheet_title}: {e}")
                    continue
            
            return sheets_data
            
        except Exception as e:
            logger.error(f"Error extracting sheet data: {e}")
            raise

    def convert_to_csv(self, user_id: str, spreadsheet_id: str, output_dir: str = "converted_data") -> List[str]:
        """Convert spreadsheet to CSV files"""
        try:
            # Create output directory
            os.makedirs(output_dir, exist_ok=True)
            
            # Get services
            _, sheets_service = self._get_google_services(user_id)
            
            # Extract data
            sheets_data = self._extract_sheet_data(sheets_service, spreadsheet_id)
            
            output_files = []
            for sheet in sheets_data:
                # Create filename
                filename = f"{spreadsheet_id}_{sheet['title']}.csv"
                filepath = os.path.join(output_dir, filename)
                
                # Save to CSV
                sheet['data'].to_csv(filepath, index=False)
                output_files.append(filepath)
                
                logger.info(f"Created CSV file: {filepath}")
            
            return output_files
            
        except Exception as e:
            logger.error(f"Error converting to CSV: {e}")
            return []

    def convert_to_json(self, user_id: str, spreadsheet_id: str, output_dir: str = "converted_data") -> List[str]:
        """Convert spreadsheet to JSON files"""
        try:
            # Create output directory
            os.makedirs(output_dir, exist_ok=True)
            
            # Get services
            _, sheets_service = self._get_google_services(user_id)
            
            # Extract data
            sheets_data = self._extract_sheet_data(sheets_service, spreadsheet_id)
            
            output_files = []
            for sheet in sheets_data:
                # Create filename
                filename = f"{spreadsheet_id}_{sheet['title']}.json"
                filepath = os.path.join(output_dir, filename)
                
                # Convert to JSON
                json_data = {
                    'sheet_title': sheet['title'],
                    'data': sheet['data'].to_dict(orient='records'),
                    'columns': sheet['data'].columns.tolist(),
                    'metadata': {
                        'row_count': len(sheet['data']),
                        'column_count': len(sheet['data'].columns),
                        'converted_at': datetime.now().isoformat()
                    }
                }
                
                # Save to JSON file
                with open(filepath, 'w') as f:
                    json.dump(json_data, f, indent=2)
                
                output_files.append(filepath)
                logger.info(f"Created JSON file: {filepath}")
            
            return output_files
            
        except Exception as e:
            logger.error(f"Error converting to JSON: {e}")
            return []

    def analyze_structure(self, user_id: str, spreadsheet_id: str) -> Dict:
        """Analyze spreadsheet structure and content"""
        try:
            # Get services
            _, sheets_service = self._get_google_services(user_id)
            
            # Get spreadsheet metadata
            try:
                spreadsheet = sheets_service.spreadsheets().get(
                    spreadsheetId=spreadsheet_id
                ).execute()
            except Exception as e:
                logger.error(f"Error accessing spreadsheet: {e}")
                return {
                    'error': f"Could not access spreadsheet: {str(e)}",
                    'total_sheets': 0,
                    'sheets': []
                }
            
            analysis = {
                'spreadsheet_id': spreadsheet_id,
                'total_sheets': len(spreadsheet.get('sheets', [])),
                'sheets': []
            }
            
            for sheet in spreadsheet.get('sheets', []):
                try:
                    sheet_title = sheet['properties']['title']
                    range_name = f"'{sheet_title}'"
                    
                    # Get sheet data
                    result = sheets_service.spreadsheets().values().get(
                        spreadsheetId=spreadsheet_id,
                        range=range_name
                    ).execute()
                    
                    rows = result.get('values', [])
                    if rows:
                        headers = rows[0] if rows else []
                        if not headers:
                            continue
                        
                        # Create DataFrame
                        if len(rows) > 1:
                            data = rows[1:]
                            max_cols = len(headers)
                            padded_data = [row + [''] * (max_cols - len(row)) for row in data]
                            df = pd.DataFrame(padded_data, columns=headers)
                        else:
                            df = pd.DataFrame(columns=headers)
                        
                        sheet_analysis = {
                            'title': sheet_title,
                            'rows': len(df),
                            'columns': len(df.columns),
                            'column_info': []
                        }
                        
                        # Analyze each column
                        for column in df.columns:
                            col_info = {
                                'name': column,
                                'type': str(df[column].dtypes),
                                'unique_values': df[column].nunique(),
                                'null_count': df[column].isnull().sum(),
                                'sample_values': df[column].dropna().head(3).tolist()
                            }
                            sheet_analysis['column_info'].append(col_info)
                        
                        analysis['sheets'].append(sheet_analysis)
                    
                except Exception as e:
                    logger.error(f"Error analyzing sheet {sheet_title}: {e}")
                    continue
            
            return analysis
            
        except Exception as e:
            logger.error(f"Error analyzing structure: {e}")
            return {
                'error': str(e),
                'total_sheets': 0,
                'sheets': []
            }

def main():
    """Test spreadsheet conversion"""
    try:
        converter = SpreadsheetConverter()
        test_user_id = "dev"  # Use your test user ID
        
        # Test spreadsheet ID
        test_spreadsheet_id = "1Ld2afMCl-9HM8Eb9S_jvrn02V4iFcZLJh0DobOh00eY"  # Replace with actual spreadsheet ID
        
        # First analyze structure
        print("\nAnalyzing spreadsheet structure...")
        analysis = converter.analyze_structure(test_user_id, test_spreadsheet_id)
        
        if 'error' in analysis:
            print(f"\nError analyzing spreadsheet: {analysis['error']}")
            return
        
        print("\nSpreadsheet Analysis:")
        print(f"Total sheets: {analysis['total_sheets']}")
        
        if analysis['total_sheets'] == 0:
            print("No sheets found in spreadsheet")
            return
            
        for sheet in analysis['sheets']:
            print(f"\nSheet: {sheet['title']}")
            print(f"Rows: {sheet['rows']}")
            print(f"Columns: {sheet['columns']}")
            print("\nColumns:")
            for col in sheet['column_info']:
                print(f"- {col['name']} ({col['type']})")
                print(f"  Unique values: {col['unique_values']}")
                print(f"  Null count: {col['null_count']}")
                print(f"  Sample values: {col['sample_values']}")
        
        # Convert to CSV
        if analysis['total_sheets'] > 0:
            print("\nConverting to CSV...")
            csv_files = converter.convert_to_csv(test_user_id, test_spreadsheet_id)
            print(f"Created CSV files: {csv_files}")
            
            # Convert to JSON
            print("\nConverting to JSON...")
            json_files = converter.convert_to_json(test_user_id, test_spreadsheet_id)
            print(f"Created JSON files: {json_files}")
        
    except Exception as e:
        logger.error(f"Error in main: {e}")
        raise
    
    finally:
        converter.close()

if __name__ == "__main__":
    main() 