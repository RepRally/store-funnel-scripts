import random
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
import gspread
import time

DRIVE_ID = "18bSzmcB1jUJhB6JM1vRi6sEYI0z0ZsjY"
SERVICE_ACCOUNT_FILE = 'rich-compiler-462615-a3-49279f5b5e7d.json'

# read all files in the drive
drive_service = build('drive', 'v3', credentials=Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE))
sheets_service = build('sheets', 'v4', credentials=Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE))

# list all files in the drive
files = []
page_token = None

while True:
    response = drive_service.files().list(
        q=f"'{DRIVE_ID}' in parents",
        fields="nextPageToken, files(id, name)",
        pageToken=page_token
    ).execute()

    files.extend(response.get('files', []))
    page_token = response.get('nextPageToken', None)

    if page_token is None:
        break

# Now `files` contains all files under the specified parent
print(f"Total files found: {len(files)}")

# get the contact list sheet
gc = gspread.service_account(filename=SERVICE_ACCOUNT_FILE)
contact_list_sheet = gc.open("Contact list").sheet1

name_to_email = {}
for row in contact_list_sheet.get_all_values()[1:]:
    seller_name = row[0]
    seller_email = row[4]
    print(seller_name, seller_email)
    name_to_email[seller_name] = seller_email

print(name_to_email)

def clean_and_format_columns_as_currency(sheet_id, sheet_name="Sheet1"):
    """Clean apostrophes from columns C-G and format as currency in the specified sheet"""
    try:
        # Get sheet metadata to find the correct sheet ID
        sheet_metadata = sheets_service.spreadsheets().get(spreadsheetId=sheet_id).execute()
        sheets = sheet_metadata.get('sheets', '')
        
        # Find the sheet ID (different from spreadsheet ID)
        target_sheet_id = None
        for sheet in sheets:
            if sheet['properties']['title'] == sheet_name:
                target_sheet_id = sheet['properties']['sheetId']
                break
        
        if target_sheet_id is None:
            # If sheet_name not found, use the first sheet
            target_sheet_id = sheets[0]['properties']['sheetId']
            sheet_name = sheets[0]['properties']['title']
            print(f"Using first sheet: {sheet_name}")
        
        # Get data from columns C-G to clean
        range_name = f"{sheet_name}!C:G"
        sheet_data = sheets_service.spreadsheets().values().get(
            spreadsheetId=sheet_id,
            range=range_name
        ).execute()
        
        values = sheet_data.get('values', [])
        if not values:
            print(f"No data found in columns C-G of sheet {sheet_id}")
            return False
        
        # Clean the data by removing apostrophes and converting to numbers
        cleaned_values = []
        for row in values:
            cleaned_row = []
            for cell in row:
                if isinstance(cell, str) and cell.startswith("'"):
                    # Remove the apostrophe and try to convert to number
                    cleaned_cell = cell[1:]  # Remove first character (apostrophe)
                    try:
                        # Try to convert to float to ensure it's a valid number
                        float(cleaned_cell)
                        cleaned_row.append(cleaned_cell)
                    except ValueError:
                        # If it's not a valid number, keep original
                        cleaned_row.append(cell)
                else:
                    cleaned_row.append(cell)
            cleaned_values.append(cleaned_row)
        
        # Update the sheet with cleaned values
        update_body = {
            'values': cleaned_values
        }
        
        sheets_service.spreadsheets().values().update(
            spreadsheetId=sheet_id,
            range=range_name,
            valueInputOption='USER_ENTERED',  # This will interpret the values as numbers
            body=update_body
        ).execute()
        
        print(f"Cleaned apostrophes from columns C-G in sheet {sheet_id}")
        
        # Now format columns C-G as currency
        request_body = {
            'requests': [
                {
                    'repeatCell': {
                        'range': {
                            'sheetId': target_sheet_id,
                            'startRowIndex': 0,
                            'endRowIndex': len(values),
                            'startColumnIndex': 2,  # Column C (0-indexed)
                            'endColumnIndex': 7     # Column G (exclusive, so 7 means up to column G)
                        },
                        'cell': {
                            'userEnteredFormat': {
                                'numberFormat': {
                                    'type': 'CURRENCY',
                                    'pattern': '"$"#,##0.00'
                                }
                            }
                        },
                        'fields': 'userEnteredFormat.numberFormat'
                    }
                }
            ]
        }
        
        # Execute the formatting request
        result = sheets_service.spreadsheets().batchUpdate(
            spreadsheetId=sheet_id,
            body=request_body
        ).execute()
        
        print(f"Successfully cleaned and formatted columns C-G as currency in sheet {sheet_id}")
        return True
        
    except Exception as e:
        print(f"Error processing sheet {sheet_id}: {str(e)}")
        return False

# for each file, format columns 3-7 as currency
successful_updates = 0
failed_updates = 0

for file in files:
    file_id = file['id']
    file_name = file['name']
    
    print(f"Processing file: {file_name} (ID: {file_id})")
    
    # Check if it's a Google Sheets file
    try:
        file_metadata = drive_service.files().get(fileId=file_id).execute()
        if file_metadata.get('mimeType') == 'application/vnd.google-apps.spreadsheet':
            if clean_and_format_columns_as_currency(file_id):
                successful_updates += 1
            else:
                failed_updates += 1
            
            # Add a small delay to avoid hitting API rate limits
            time.sleep(1)
        else:
            print(f"Skipping {file_name} - not a Google Sheets file")
    except Exception as e:
        print(f"Error processing file {file_name}: {str(e)}")
        failed_updates += 1

print(f"\nFormatting complete!")
print(f"Successfully updated: {successful_updates} files")
print(f"Failed to update: {failed_updates} files")