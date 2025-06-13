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
        
        # Check if row 2 already has totals (to avoid processing files that were already successful)
        check_range = f"{sheet_name}!B2:B2"
        try:
            check_data = sheets_service.spreadsheets().values().get(
                spreadsheetId=sheet_id,
                range=check_range
            ).execute()
            
            check_values = check_data.get('values', [])
            if check_values and len(check_values[0]) > 0 and check_values[0][0] == "TOTAL":
                print(f"Sheet {sheet_id} already has totals row - skipping")
                return True  # Return True because it's already processed successfully
        except Exception as e:
            # If we can't check, proceed with processing
            print(f"Could not check for existing totals in {sheet_id}, proceeding with processing")
        
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
        
        # Insert a new row at position 2 (right after header) for totals
        insert_row_request = {
            'requests': [
                {
                    'insertDimension': {
                        'range': {
                            'sheetId': target_sheet_id,
                            'dimension': 'ROWS',
                            'startIndex': 1,  # Insert at row 2 (0-indexed, so 1 = row 2)
                            'endIndex': 2     # Insert 1 row
                        },
                        'inheritFromBefore': False
                    }
                }
            ]
        }
        
        sheets_service.spreadsheets().batchUpdate(
            spreadsheetId=sheet_id,
            body=insert_row_request
        ).execute()
        
        print(f"Inserted totals row at position 2 in sheet {sheet_id}")
        
        # Create formulas for totals (columns C-G) - now data starts from row 3
        total_formulas = [
            f"=SUM(C3:C{len(values) + 2})",  # Column C total (+2 because we inserted a row and data now starts at row 3)
            f"=SUM(D3:D{len(values) + 2})",  # Column D total
            f"=SUM(E3:E{len(values) + 2})",  # Column E total
            f"=SUM(F3:F{len(values) + 2})",  # Column F total
            f"=SUM(G3:G{len(values) + 2})"   # Column G total
        ]
        
        # Add "TOTAL" label in column B and formulas in columns C-G at row 2
        totals_range = f"{sheet_name}!B2:G2"
        totals_values = [["TOTAL"] + total_formulas]
        
        totals_body = {
            'values': totals_values
        }
        
        sheets_service.spreadsheets().values().update(
            spreadsheetId=sheet_id,
            range=totals_range,
            valueInputOption='USER_ENTERED',  # This will interpret formulas
            body=totals_body
        ).execute()
        
        print(f"Added totals formulas to row 2 in sheet {sheet_id}")
        
        # Now format columns C-G as currency (including the new totals row)
        request_body = {
            'requests': [
                {
                    'repeatCell': {
                        'range': {
                            'sheetId': target_sheet_id,
                            'startRowIndex': 0,
                            'endRowIndex': len(values) + 1,  # +1 to include the totals row
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
        
        # Apply bold formatting and golden yellow background to the entire totals row
        format_totals_request = {
            'requests': [
                {
                    'repeatCell': {
                        'range': {
                            'sheetId': target_sheet_id,
                            'startRowIndex': 1,  # Row 2 (0-indexed)
                            'endRowIndex': 2,    # Just row 2
                            'startColumnIndex': 0,  # Start from column A
                            'endColumnIndex': 10    # Go to column J to ensure full row coverage
                        },
                        'cell': {
                            'userEnteredFormat': {
                                'backgroundColor': {
                                    'red': 1.0,      # Golden yellow color
                                    'green': 0.85,
                                    'blue': 0.4
                                },
                                'textFormat': {
                                    'bold': True
                                }
                            }
                        },
                        'fields': 'userEnteredFormat.backgroundColor,userEnteredFormat.textFormat.bold'
                    }
                }
            ]
        }
        
        sheets_service.spreadsheets().batchUpdate(
            spreadsheetId=sheet_id,
            body=format_totals_request
        ).execute()
        
        print(f"Applied golden yellow highlighting and bold formatting to totals row in sheet {sheet_id}")
        
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

    # print(file_id)

    # if file_id != '1jiIPkjOupLQdM5HdZ0H1YzJNDZ0pdxOgpVatMD8fwpA' and file_id != '1TRWja7K4RLypTVwwo8vfPVxk464l-xL5jSGUikK2rMc':
    #     continue
    
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