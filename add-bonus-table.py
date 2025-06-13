import time
import traceback
from gspread import Spreadsheet, Worksheet
import gspread
from get_all_files import get_all_files, get_store_funnel_by_sellers, gc, sheets_service


def get_bonus_table(drive_service, sheets_service, new_sheet: Spreadsheet):
    # get the bonus table
    try:
        sheet: Worksheet = new_sheet.worksheet("Sheet1")
    except Exception as e:
        return False, True, f"Sheet1 not found: {str(e)}"
    
    retry_count = 0
    while retry_count < 100:
        try:
            sheet.update_title("Potential Earnings")
            break
        except Exception as e:
            print(f"Error updating title: {str(e)}")
            time.sleep(10)
            retry_count += 1
            if retry_count == 100:
                return False, True, f"Error updating title: {str(e)}"
        
    
    try:
        # Create summary box data
        summary_data = [
            ["Metric", "All Stores", "Confirmed Stores", "Earnings left on table"],
            ["Total Gmv", 
             f"=SUM(C13:C)",
             f"=SUMIF(S13:S, TRUE, C13:C)",
             f"=B2-C2"],
            ["Amount of Stores",
             f"=COUNTA(A13:A)",
             f"=COUNTIF(S13:S, TRUE)",
             f"=B3-C3"],
            ["", "", "", ""],
            ["Minimum Total Earnings", f"=SUM(B6:B8)", f"=SUM(C6:C9)", f"=B5-C5"],
            ["Commission", f"=10%*B2", f"=10%*C2", f"=B6-C6"],
            ["Portfolio Bonuses", 
             f"=SUM(FILTER(Bonuses!B3:B20,B2>=Bonuses!A3:A20))",
             f"=SUM(FILTER(Bonuses!B3:B20,C2>=Bonuses!A3:A20))",
             f"=B7-C7"],
            ["Goal Completion Bonuses",
             f"=SUM(F13:F)",
             f"=SUMIF(S13:S, TRUE, F13:F)",
             f"=B8-C8"],
            ["Goal Completion Boosters",
             f"=SUM(G13:G)",
             f"=SUMIF(S13:S, TRUE, G13:G)-C8",
             f"=B9-C9"]
        ]
        # move the existing data down by inserting 10 rows above)
        # Insert empty rows at row 1
        num_rows_to_insert = len(summary_data) + 1
        empty_rows = [[""] * 4 for _ in range(num_rows_to_insert)]  # 4 columns wide
        sheet.insert_rows(empty_rows, row=1)

        # add a header row        
        # make sure top row is green with white text
        sheet.format('A1:U1', {'backgroundColor': {'red': 0.298, 'green': 0.686, 'blue': 0.314}, 'textFormat': {'foregroundColor': {'red': 1.0, 'green': 1.0, 'blue': 1.0}}})
        
        # make sure the subsequent rows are alternating colors, and text is black
        for i in range(2, len(summary_data) + 1):
            sheet.format(f'A{i}:U{i}', {'backgroundColor': {'red': 0.95, 'green': 0.95, 'blue': 0.95} if i % 2 == 0 else {'red': 1.0, 'green': 1.0, 'blue': 1.0}})
            sheet.format(f'A{i}:U{i}', {'textFormat': {'foregroundColor': {'red': 0.0, 'green': 0.0, 'blue': 0.0}}})
        
        sheet.update(range_name='A1', values=summary_data, value_input_option='USER_ENTERED')

        # print(f"✅ Updated {new_sheet.title}")   
        return True, False, None
    except Exception as e:
        print(f"❌ Error processing {new_sheet.title}: {str(e)}") # print line number
        traceback.print_exc()
        return False, False, str(e)


def process_file(file, error_count, skipped_count, success_count):
    sheet = gc.open(file['name'])

    success, skipped, error = get_bonus_table(gc, sheets_service, sheet)

    if success:
        print(f"✅ Updated {file['name']}")
        success_count += 1
    if skipped:
        #print(f"❌ Skipped {file['name']}")
        skipped_count += 1
    if error:
        #print(f"❌ Error {file['name']}: {error}")
        error_count += 1
    
        return success, skipped, error

files = get_all_files()
# sellers_by_id = get_store_funnel_by_sellers()

skipped_count = 0
error_count = 0
success_count = 0
files.sort(key=lambda x: x['name'])
for file in files[150:200]:
    # if file['id'] != '1cupgrkDNxenh6lYSmCQPN6MYz5spwIXavSmOTDHB9u4':
    #     continue
    # print(file['name'])
    # get name
    name = file['name'].split(" - ")[0]
    try:
        print(f"Processing {file['name']}")
        process_file(file, error_count, skipped_count, success_count)
    except gspread.exceptions.APIError as e:
        # Check if it's a rate limit error (429)
        if "429" in str(e):
            print("Rate limit hit, waiting 60 seconds before retrying...")
            time.sleep(10)  # Wait for 60 seconds
            process_file(file, error_count, skipped_count, success_count)
        else:
            print(f"Error opening {file['name']}: {e}")
            continue

print(f"Total: {success_count + skipped_count + error_count}")
print(f"✅ Success: {success_count}")
print(f"Skipped: {skipped_count}")
print(f"❌ Error: {error_count}")
