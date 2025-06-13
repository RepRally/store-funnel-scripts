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
                jitter = random.uniform(0, 0.1 * delay)
                sleep_time = delay + jitter
                print(f"Rate limit hit, waiting {sleep_time:.2f} seconds before retry...")
                time.sleep(sleep_time)
                delay *= 2  # Exponential backoff
            else:
                raise

def create_bonuses_sheet(new_sheet, level: int):
    """Create the Bonuses sheet with GMV bonus lookup table"""
    try:
        bonuses_sheet = new_sheet.worksheet('Bonuses')
        bonuses_sheet.clear()
    except gspread.exceptions.WorksheetNotFound:
        bonuses_sheet = new_sheet.add_worksheet(title='Bonuses', rows=100, cols=10)
    
    base_bonus_table = [
        ["Monthly GMV Bonuses (Sprint Goals)", "1", "2", "3", "4", "5"],
        ["Total Monthly GMV", "Bonus", "Bonus", "Bonus", "Bonus", "Bonus"],
        [1000, 25, 20, "", "", ""],
        [3000, 50, 40, 50, "", ""],
        [5000, 100, 60, 75, 50, ""],
        [10000, 150, 100, 100, 75, 75],
        [20000, 150, 150, 150, 100, 100],
        [30000, 200, 200, 150, 150, 200],
        [40000, "", "", 200, 150, ""],
        [50000, "", "", 250, 200, 250],
        [60000, "", "", 250, 250, ""],
        [70000, "", "", "", 250, 250],
        [80000, "", "", "", 400, 300],
        [90000, "", "", "", "", 300],
        [100000, "", "", "", "", ""],
        [120000, "", "", "", "", 400],
        [140000, "", "", "", "", ""],
        [150000, "", "", "", "", 500],
        [160000, "", "", "", "", ""],
        [200000, "", "", "", "", 800]
    ]

    bonus_table = []
    for row in base_bonus_table:
        # always get the first col
        bonus_table.append([row[0]])
        for i in range(1, len(row)):
            if level == i:
                bonus_table[i].append(row[i])
            else:
                bonus_table[i].append("")

    print(bonus_table)

    bonuses_sheet.update(range_name='A1', values=bonus_table, value_input_option='USER_ENTERED')
    
    try:
        bonuses_sheet.format('A3:A20', {'numberFormat': {'type': 'CURRENCY', 'pattern': '$#,##0'}})
        bonuses_sheet.format('B3:F20', {'numberFormat': {'type': 'CURRENCY', 'pattern': '[$+$]#,##0'}})
    except:
        pass
    
    return bonuses_sheet

# Setup credentials and client
SERVICE_ACCOUNT_FILE = 'rich-compiler-462615-a3-49279f5b5e7d.json'
SCOPES = ["https://www.googleapis.com/auth/spreadsheets",
          "https://www.googleapis.com/auth/drive"]
creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
gc = gspread.authorize(creds)

# Create Drive API service
drive_service = build('drive', 'v3', credentials=creds)
sheets_service = build('sheets', 'v4', credentials=creds)

# Load source spreadsheet
try:
    source_sheet = gc.open("Store Funnel")
    worksheet = source_sheet.sheet1
    data = worksheet.get_all_values()
except gspread.exceptions.SpreadsheetNotFound:
    print("\nError: Could not find spreadsheet named 'Store Funnel'")
    print("Please check the exact name of your spreadsheet and make sure it matches exactly.")
    exit(1)

# Extract header and group data by seller
header, *rows = data
seller_to_stores = {}

for row in rows:
    data_dict = dict(zip(header, row))
    seller_id = data_dict['Seller ID']
    if (seller_id, data_dict['Seller Name']) not in seller_to_stores:
        seller_to_stores[(seller_id, data_dict['Seller Name'])] = []
    seller_to_stores[(seller_id, data_dict['Seller Name'])].append(row)

print(f"Processing {len(seller_to_stores)} sellers...")

# Create a drive folder for each seller
folder_metadata = {
    'name': f"Store Funnel by Seller - {date.today().strftime('%Y-%m-%d-%H')}",
    'mimeType': 'application/vnd.google-apps.folder'
}
folder = drive_service.files().create(body=folder_metadata, fields='id').execute()
folder_id = folder.get('id')

# Share with nathan.chiu@reprally.com
user_permission = {
    'type': 'user',
    'role': 'writer',
    'emailAddress': 'nathan.chiu@reprally.com'
}
drive_service.permissions().create(fileId=folder_id, body=user_permission, fields='id').execute()

print(f"Created folder with ID: {folder_id}")

# Create a new sheet for each seller
for i, (seller, stores) in enumerate(seller_to_stores.items(), 1):
    seller_id, seller_name = seller

    print(f"\nProcessing seller {i}/{len(seller_to_stores)}: {seller_name}")
    
    # Add delay between API calls
    if i > 1:
        delay = random.uniform(1, 2)
        time.sleep(delay)
    
    try:
        new_sheet = create_sheet_with_retry(gc, f"{seller_name} - {date.today().strftime('%Y-%m-%d')}")
        new_worksheet = new_sheet.sheet1
        
        print(f"Adding {len(stores)} rows to {seller_name}")

        # Prepare headers (remove last 3 columns, add new ones)
        headers = header[:-3]
        headers.extend(["Task Confirmation", "When you can visit the store", "Notes"])

        # Prepare rows with new columns
        rows_to_insert = []
        for row in stores:
            row_to_insert = row[:-3]  # Remove last 3 columns
            row_to_insert.extend(["", "", ""])  # Add empty values for new columns
            rows_to_insert.append(row_to_insert)

        # Create Bonuses sheet first
        create_bonuses_sheet(new_sheet)

        # Create summary box data
        data_start_row = 12
        target_col = "C"
        task_confirmation_col = chr(ord('A') + len(headers) - 3)
        store_bonuses_col = "F"
        goal_completion_col = "G"

        summary_data = [
            ["Metric", "All Stores", "Confirmed Stores", "Earnings left on table"],
            ["Total Gmv", 
             f"=SUM({target_col}{data_start_row}:{target_col}{data_start_row + len(stores) - 1})",
             f"=SUMIF({task_confirmation_col}{data_start_row}:{task_confirmation_col}{data_start_row + len(stores) - 1}, TRUE, {target_col}{data_start_row}:{target_col}{data_start_row + len(stores) - 1})",
             f"=B2-C2"],
            ["Amount of Stores",
             f"=COUNT({target_col}{data_start_row}:{target_col}{data_start_row + len(stores) - 1})",
             f"=COUNTIF({task_confirmation_col}{data_start_row}:{task_confirmation_col}{data_start_row + len(stores) - 1},TRUE)",
             f"=B3-C3"],
            ["", "", "", ""],
            ["Minimum Total Earnings", f"=SUM(B6:B9)", f"=SUM(C6:C9)", f"=B5-C5"],
            ["Commission", f"=10%*B2", f"=10%*C2", f"=B6-C6"],
            ["Portfolio Bonuses", 
             f"=SUM(FILTER(Bonuses!D3:D20,B2>=Bonuses!A3:A20))",
             f"=SUM(FILTER(Bonuses!D3:D20,C2>=Bonuses!A3:A20))",
             f"=B7-C7"],
            ["Goal Completion Bonuses",
             f"=SUM({store_bonuses_col}{data_start_row}:{store_bonuses_col}{data_start_row + len(stores) - 1})",
             f"=SUMIF({task_confirmation_col}{data_start_row}:{task_confirmation_col}{data_start_row + len(stores) - 1}, TRUE,{store_bonuses_col}{data_start_row}:{store_bonuses_col}{data_start_row + len(stores) - 1})",
             f"=B8-C8"],
            ["Goal Completion Boosters",
             f"=SUM({goal_completion_col}{data_start_row}:{goal_completion_col}{data_start_row + len(stores) - 1})-B8",
             f"=SUMIF({task_confirmation_col}{data_start_row}:{task_confirmation_col}{data_start_row + len(stores) - 1}, TRUE,{goal_completion_col}{data_start_row}:{goal_completion_col}{data_start_row + len(stores) - 1})-C8",
             f"=B9-C9"]
        ]
        
        # Add empty row and data table
        summary_data.append(["", "", "", ""])
        summary_data.append(headers)
        summary_data.extend(rows_to_insert)

        # Insert all data
        new_worksheet.clear()
        new_worksheet.update(range_name='A1', values=summary_data, value_input_option='USER_ENTERED')

        # Get the sheet ID for formatting
        sheet_id = new_sheet.worksheet('Sheet1')._properties['sheetId']
        checkbox_col_index = len(headers) - 2
        dropdown_col_index = len(headers) - 1

        visit_time_options = [
            "Before June 15th",
            "June 15th - June 20th",
            "June 21st - June 25th",
            "June 26th - June 30th",
            "Other"
        ]

        batch_update_request = {
            'requests': [
                # Format summary header
                {
                    'repeatCell': {
                        'range': {
                            'sheetId': sheet_id,
                            'startRowIndex': 0,
                            'endRowIndex': 1,
                            'startColumnIndex': 0,
                            'endColumnIndex': 4
                        },
                        'cell': {
                            'userEnteredFormat': {
                                'backgroundColor': {'red': 0.298, 'green': 0.686, 'blue': 0.314},
                                'textFormat': {
                                    'bold': True,
                                    'foregroundColor': {'red': 1.0, 'green': 1.0, 'blue': 1.0}
                                },
                                'horizontalAlignment': 'CENTER'
                            }
                        },
                        'fields': 'userEnteredFormat(backgroundColor,textFormat,horizontalAlignment)'
                    }
                },
                # Summary alternating colors
                {
                    'addBanding': {
                        'bandedRange': {
                            'range': {
                                'sheetId': sheet_id,
                                'startRowIndex': 1,
                                'endRowIndex': 9,
                                'startColumnIndex': 0,
                                'endColumnIndex': 4
                            },
                            'rowProperties': {
                                'headerColor': {'red': 0.95, 'green': 0.95, 'blue': 0.95},
                                'firstBandColor': {'red': 1.0, 'green': 1.0, 'blue': 1.0},
                                'secondBandColor': {'red': 0.95, 'green': 0.95, 'blue': 0.95}
                            }
                        }
                    }
                },
                # Highlight Minimum Total Earnings row
                {
                    'repeatCell': {
                        'range': {
                            'sheetId': sheet_id,
                            'startRowIndex': 4,
                            'endRowIndex': 5,
                            'startColumnIndex': 1,
                            'endColumnIndex': 4
                        },
                        'cell': {
                            'userEnteredFormat': {
                                'textFormat': {'bold': True},
                                'numberFormat': {'type': 'CURRENCY', 'pattern': '$#,##0'}
                            }
                        },
                        'fields': 'userEnteredFormat(textFormat,numberFormat)'
                    }
                },
                # Format data table header
                {
                    'repeatCell': {
                        'range': {
                            'sheetId': sheet_id,
                            'startRowIndex': 10,
                            'endRowIndex': 11,
                            'startColumnIndex': 0,
                            'endColumnIndex': len(headers)
                        },
                        'cell': {
                            'userEnteredFormat': {
                                'backgroundColor': {'red': 0.2, 'green': 0.2, 'blue': 0.2},
                                'textFormat': {
                                    'bold': True,
                                    'foregroundColor': {'red': 1.0, 'green': 1.0, 'blue': 1.0}
                                },
                                'horizontalAlignment': 'CENTER'
                            }
                        },
                        'fields': 'userEnteredFormat(backgroundColor,textFormat,horizontalAlignment)'
                    }
                },
                # Data alternating colors
                {
                    'addBanding': {
                        'bandedRange': {
                            'range': {
                                'sheetId': sheet_id,
                                'startRowIndex': 11,
                                'endRowIndex': 11 + len(stores),
                                'startColumnIndex': 0,
                                'endColumnIndex': len(headers)
                            },
                            'rowProperties': {
                                'headerColor': {'red': 0.95, 'green': 0.95, 'blue': 0.95},
                                'firstBandColor': {'red': 1.0, 'green': 1.0, 'blue': 1.0},
                                'secondBandColor': {'red': 0.95, 'green': 0.95, 'blue': 0.95}
                            }
                        }
                    }
                },
                # Checkbox validation
                {
                    'setDataValidation': {
                        'range': {
                            'sheetId': sheet_id,
                            'startRowIndex': 11,
                            'endRowIndex': 11 + len(stores),
                            'startColumnIndex': checkbox_col_index - 1,
                            'endColumnIndex': checkbox_col_index
                        },
                        'rule': {'condition': {'type': 'BOOLEAN'}}
                    }
                },
                # Dropdown validation
                {
                    'setDataValidation': {
                        'range': {
                            'sheetId': sheet_id,
                            'startRowIndex': 11,
                            'endRowIndex': 11 + len(stores),
                            'startColumnIndex': dropdown_col_index - 1,
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
                # Freeze rows
                {
                    'updateSheetProperties': {
                        'properties': {
                            'sheetId': sheet_id,
                            'gridProperties': {'frozenRowCount': 11}
                        },
                        'fields': 'gridProperties.frozenRowCount'
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
                        'properties': {'pixelSize': 150},
                        'fields': 'pixelSize'
                    }
                }
            ]
        }

        # Execute formatting
        sheets_service.spreadsheets().batchUpdate(
            spreadsheetId=new_sheet.id,
            body=batch_update_request
        ).execute()

        # Move file to folder
        drive_service.files().update(
            fileId=new_sheet.id,
            addParents=folder_id,
            fields='id, parents'
        ).execute()
        
        print(f"✅ Successfully created and moved sheet for {seller_name}")
        
    except Exception as e:
        print(f"❌ Error processing {seller_name}: {str(e)}")
        continue

print("\n✅ All sheets processed!")
print("✅ Sheets created and organized in folder successfully.")