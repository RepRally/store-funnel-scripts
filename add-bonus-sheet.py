import gspread
from google.oauth2.service_account import Credentials
from datetime import date
from googleapiclient.discovery import build
import time
import random

from get_all_files import get_all_files, get_store_funnel, get_store_funnel_by_sellers, sheets_service, gc

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
        try:
            # if the worksheet "Bonuses" exists, return
            if new_sheet.worksheet("Bonuses"):
                return False
            
            # # move the sheet to the end
            # new_sheet.move_to_end()
            
            # create new sheet bonuses
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
            if level == "":
                level = 1
            if row[int(level)] != "":
                bonus_table.append([row[0], row[int(level)]])

        try:
            bonuses_sheet.update(range_name='A1', values=bonus_table, value_input_option='USER_ENTERED')
        except Exception as e:
            if "429" in str(e):
                print("Rate limit hit, waiting 60 seconds before retrying...")
                time.sleep(30)  # Wait for 30 seconds
                return False
            else:
                print(f"Error updating bonuses sheet: {e}")
                return False
        
        try:
            # format the sheet  
            bonuses_sheet.format('A3:A20', {'numberFormat': {'type': 'CURRENCY', 'pattern': '$#,##0'}})
            bonuses_sheet.format('B3:F20', {'numberFormat': {'type': 'CURRENCY', 'pattern': '[$+$]#,##0'}})
            bonuses_sheet.format('A3:A20', {'backgroundColor': {'red': 0.9, 'green': 1, 'blue': 0.9}})
            bonuses_sheet.format('B3:F20', {'backgroundColor': {'red': 0.9, 'green': 1, 'blue': 0.9}})
            # make column A longer
            requests = [{
                'updateDimensionProperties': {
                    'range': {
                        'sheetId': 0,  # First sheet (0-indexed)
                        'dimension': 'COLUMNS',
                        'startIndex': 0,  # Column A (0-indexed)
                        'endIndex': 1     # Only column A
                    },
                    'properties': {
                        'pixelSize': 200  # Set width to 200 pixels
                    },
                    'fields': 'pixelSize'
                }
            }]
            sheets_service.spreadsheets().batchUpdate(spreadsheetId=bonuses_sheet.id, body={'requests': requests}).execute()

        except:
            pass
    except Exception as e:
        if "429" in str(e):
            print("Rate limit hit, waiting 60 seconds before retrying...")
            time.sleep(10)  # Wait for 60 seconds
            # retry
            return create_bonuses_sheet(new_sheet, level)
        else:
            print(f"Error updating bonuses sheet: {e}")
            return False
    
    return True


def get_user_level(sellers_by_id: dict[str, dict], seller_name: str):
    # get the user level
    return sellers_by_id[seller_name]["Current Seller Level"]


print("\n✅ All sheets processed!")
print("✅ Sheets created and organized in folder successfully.")

files = get_all_files()
sellers_by_id = get_store_funnel_by_sellers()

# reverse
for file in reversed(files):
    # if file['id'] != '1cupgrkDNxenh6lYSmCQPN6MYz5spwIXavSmOTDHB9u4':
    #     continue
    print(file['name'])
    # get name
    name = file['name'].split(" - ")[0]
    try:
        sheet = gc.open(file['name'])
    except gspread.exceptions.APIError as e:
        # Check if it's a rate limit error (429)
        if "429" in str(e):
            print("Rate limit hit, waiting 60 seconds before retrying...")
            time.sleep(10)  # Wait for 60 seconds
            continue
        else:
            print(f"Error opening {file['name']}: {e}")
            continue
    except Exception as e:
        print(f"Unexpected error opening {file['name']}: {e}")
        continue
    
    user_level = get_user_level(sellers_by_id, name)

    print(user_level)
    
    # create the bonuses sheet
    # created = create_bonuses_sheet(sheet, user_level)
    already_exists = []
    needs_creation = []
    print(file['id'])
    try:
        sheet.worksheet("Bonuses")
        already_exists.append(file['id'])
        print("Bonuses sheet already exists for ", file['name'], file['id'])
    except:
        # needs_creation.append(file['id'])
        #print("Bonus sheet does not exist")
        create_bonuses_sheet(sheet, user_level)
    
    
    # throttle
    
    delay = random.uniform(0,1)  # Random delay between 1-2 seconds
    print(f"Waiting {delay:.2f} seconds before creating next sheet...")
    time.sleep(delay)
    
print(f"Already exists: {len(already_exists)}")
print(f"Needs creation: {len(needs_creation)}")
print(already_exists)
print(needs_creation)

print("\n✅ All sheets processed!")
print("✅ Sheets created and organized in folder successfully.")