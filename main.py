import gspread
from google.oauth2.service_account import Credentials
from datetime import date
from googleapiclient.discovery import build
import time
import random

def create_sheet_with_retry(gc, title, max_retries=5, initial_delay=1):
    """Create a sheet with exponential backoff retry logic"""
    delay = initial_delay
    for attempt in range(max_retries):
        try:
            return gc.create(title)
        except gspread.exceptions.APIError as e:
            if "rate limit" in str(e).lower() and attempt < max_retries - 1:
                # Add some jitter to the delay
                jitter = random.uniform(0, 0.1 * delay)
                sleep_time = delay + jitter
                print(f"Rate limit hit, waiting {sleep_time:.2f} seconds before retry...")
                time.sleep(sleep_time)
                delay *= 2  # Exponential backoff
            else:
                raise

# Setup credentials and client
SERVICE_ACCOUNT_FILE = 'rich-compiler-462615-a3-49279f5b5e7d.json'
SCOPES = ["https://www.googleapis.com/auth/spreadsheets",
          "https://www.googleapis.com/auth/drive"]
creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
gc = gspread.authorize(creds)

# Create Drive API service
drive_service = build('drive', 'v3', credentials=creds)
sheets_service = build('sheets', 'v4', credentials=creds)

# Print all available spreadsheets with their IDs
print("Available spreadsheets:")
spreadsheets = gc.list_spreadsheet_files()
for spreadsheet in spreadsheets:
    pass
    # print(f"Title: {spreadsheet.title}, ID: {spreadsheet.id}")

# Load source spreadsheet
try:
    source_sheet = gc.open("Store Funnel")
    worksheet = source_sheet.sheet1
    data = worksheet.get_all_values()
except gspread.exceptions.SpreadsheetNotFound:
    print("\nError: Could not find spreadsheet named 'Store Funnel'")
    print("Please check the exact name of your spreadsheet and make sure it matches exactly.")
    exit(1)

# Extract header and group data by a column (e.g., column 1)
header, *rows = data
split_column_index = 0  # change this depending on which column to split by
grouped_data = {}

# Get a seller to stores mapping
seller_to_stores = {}

for row in rows:
    data = dict(zip(header, row))
    seller_id = data['Seller ID']
    if (seller_id, data['Seller Name']) not in seller_to_stores:
        seller_to_stores[(seller_id, data['Seller Name'])] = []
    seller_to_stores[(seller_id, data['Seller Name'])].append(row)  # Store the original row instead of the dict

print(f"Processing {len(seller_to_stores)} sellers...")

# Create a drive folder for each seller
folder_metadata = {
    'name': f"Store Funnel by Seller - {date.today().strftime('%Y-%m-%d-%H')}",
    'mimeType': 'application/vnd.google-apps.folder'
}
folder = drive_service.files().create(body=folder_metadata, fields='id').execute()
folder_id = folder.get('id')

# share with nathan.chiu@reprally.com
user_permission = {
    'type': 'user',
    'role': 'writer',
    'emailAddress': 'nathan.chiu@reprally.com'
}
drive_service.permissions().create(fileId=folder_id, body=user_permission, fields='id').execute()

# verify that the folder is created
print(f"Created folder with ID: {folder_id}")
# list folders
folders = drive_service.files().list(q=f"mimeType='application/vnd.google-apps.folder' and name='{folder_metadata['name']}'", fields='files(id, name)').execute()
print(folders)

# Create a new sheet for each seller
for i, (seller, stores) in enumerate(seller_to_stores.items(), 1):
    seller_id, seller_name = seller
    # if seller_id != "26261":  # test only on 26261
    #     continue

    print(f"\nProcessing seller {i}/{len(seller_to_stores)}: {seller_name}")
    
    # Add delay between API calls
    if i > 1:
        delay = random.uniform(1, 2)  # Random delay between 1-2 seconds
        print(f"Waiting {delay:.2f} seconds before creating next sheet...")
        time.sleep(delay)
    
    try:
        new_sheet = create_sheet_with_retry(gc, f"{seller_name} - {date.today().strftime('%Y-%m-%d')}")
        new_worksheet = new_sheet.sheet1
        
        print(f"Adding {len(stores)} rows to {seller_name}")

        # Format the data as a 2D array
        # dont need last three columns
        # updated headers
        headers = header[:-3]
        
        # Check if "Total" already exists in headers
        if "Total" in headers:
            print(f"⚠️ Skipping {seller_name} - already has Total field")
            # Delete the sheet since we don't need it
            drive_service.files().delete(fileId=new_sheet.id).execute()
            continue
        
        headers.append("Total")
        headers.append("Task Confirmation")
        headers.append("When you can visit the store")
        headers.append("Notes")

        # Prepare rows with new columns
        rows_to_insert = []
        for row in stores:
            row_to_insert = row[:-3]  # Remove last 3 columns
            row_to_insert.append("")  # Add empty total
            row_to_insert.extend(["", "", ""])  # Add empty values for new columns
            rows_to_insert.append(row_to_insert)

        # Insert the data first
        data_to_insert = [headers] + rows_to_insert
        new_worksheet.insert_rows(data_to_insert)

        # Get the sheet ID from the spreadsheet
        sheet_id = new_sheet.worksheet('Sheet1')._properties['sheetId']

        # Prepare the batch update request
        checkbox_col_index = len(headers) - 2   # Task Confirmation column, first of last 3
        dropdown_col_index = len(headers) - 1  # When you can visit column, second of last 3
        # notes_col_index = len(headers)  # Notes column, third of last 3

        # Define your multiple choice options
        visit_time_options = [
            "Before June 15th",
            "June 15th - June 20th",
            "June 21st - June 25th",
            "June 26th - June 30th",
            "Other"
        ]

        batch_update_request = {
            'requests': [
                {
                    'setDataValidation': {
                        'range': {
                            'sheetId': sheet_id,
                            'startRowIndex': 1,  # Row 2 (0-based, skip header)
                            'endRowIndex': len(stores) + 1,  # End after all data rows
                            'startColumnIndex': checkbox_col_index - 1,  # Convert to 0-based
                            'endColumnIndex': checkbox_col_index
                        },
                        'rule': {
                            'condition': {
                                'type': 'BOOLEAN'
                            }
                        }
                    }
                },
                {
                    'setDataValidation': {
                        'range': {
                            'sheetId': sheet_id,
                            'startRowIndex': 1,  # Row 2 (0-based, skip header)
                            'endRowIndex': len(stores) + 1,  # End after all data rows
                            'startColumnIndex': dropdown_col_index - 1,  # Convert to 0-based
                            'endColumnIndex': dropdown_col_index
                        },
                        'rule': {
                            'condition': {
                                'type': 'ONE_OF_LIST',
                                'values': [{'userEnteredValue': option} for option in visit_time_options]
                            },
                            'showCustomUi': True,
                            'strict': True
                        }
                    }
                },
                # Freeze the header row
                {
                    'updateSheetProperties': {
                        'properties': {
                            'sheetId': sheet_id,
                            'gridProperties': {
                                'frozenRowCount': 1
                            }
                        },
                        'fields': 'gridProperties.frozenRowCount'
                    }
                },
                # Format header row
                {
                    'repeatCell': {
                        'range': {
                            'sheetId': sheet_id,
                            'startRowIndex': 0,
                            'endRowIndex': 1,
                            'startColumnIndex': 0,
                            'endColumnIndex': len(headers)
                        },
                        'cell': {
                            'userEnteredFormat': {
                                'backgroundColor': {
                                    'red': 0.2,
                                    'green': 0.2,
                                    'blue': 0.2
                                },
                                'textFormat': {
                                    'bold': True,
                                    'foregroundColor': {
                                        'red': 1.0,
                                        'green': 1.0,
                                        'blue': 1.0
                                    }
                                },
                                'horizontalAlignment': 'CENTER'
                            }
                        },
                        'fields': 'userEnteredFormat(backgroundColor,textFormat,horizontalAlignment)'
                    }
                },
                # Add alternating row colors
                {
                    'addBanding': {
                        'bandedRange': {
                            'range': {
                                'sheetId': sheet_id,
                                'startRowIndex': 1,
                                'endRowIndex': len(stores) + 1,
                                'startColumnIndex': 0,
                                'endColumnIndex': len(headers)
                            },
                            'rowProperties': {
                                'headerColor': {
                                    'red': 0.95,
                                    'green': 0.95,
                                    'blue': 0.95
                                },
                                'firstBandColor': {
                                    'red': 1.0,
                                    'green': 1.0,
                                    'blue': 1.0
                                },
                                'secondBandColor': {
                                    'red': 0.95,
                                    'green': 0.95,
                                    'blue': 0.95
                                }
                            }
                        }
                    }
                },
                # Set column widths
                {
                    'updateDimensionProperties': {
                        'range': {
                            'sheetId': sheet_id,
                            'dimension': 'COLUMNS',
                            'startIndex': 0,
                            'endIndex': len(headers)
                        },
                        'properties': {
                            'pixelSize': 150  # Set all columns to 150 pixels wide
                        },
                        'fields': 'pixelSize'
                    }
                }
            ]
        }

        # Execute the batch update
        sheets_service.spreadsheets().batchUpdate(
            spreadsheetId=new_sheet.id,
            body=batch_update_request
        ).execute()

        # Move the file to the folder
        file = drive_service.files().update(
            fileId=new_sheet.id,
            addParents=folder_id,
            fields='id, parents'
        ).execute()
        
        print(f"✅ Successfully created and moved sheet for {seller_name}")
        
    except Exception as e:
        print(f"❌ Error processing {seller_name}: {str(e)}")
        continue

print("\n✅ All sheets processed!")

# share drive with "nathan.chiu@reprally.com"
# drive_folder.share("nathan.chiu@reprally.com", perm_type="user", role="writer")

# Create new sheets and write data
# summary_links = []
# for key, group_rows in grouped_data.items():
#     new_sheet = gc.create(f"{key} - Split Sheet")
#     new_worksheet = new_sheet.sheet1
#     new_worksheet.insert_rows([header] + group_rows)

#     # Share with yourself if needed
#     # new_sheet.share("your_email@example.com", perm_type="user", role="writer")

#     # Collect link
#     sheet_url = f"https://docs.google.com/spreadsheets/d/{new_sheet.id}"
#     summary_links.append([key, sheet_url])

# # Optional: Create summary sheet
# summary_sheet = gc.create("Split Summary Sheet")
# summary_ws = summary_sheet.sheet1
# summary_ws.insert_rows([["Key", "Sheet URL"]] + summary_links)

# print("✅ Sheets created and summary sheet written.")

print("✅ Sheets created and organized in folder successfully.")