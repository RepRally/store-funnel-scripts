from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
import gspread

DRIVE_ID = "18bSzmcB1jUJhB6JM1vRi6sEYI0z0ZsjY"
SERVICE_ACCOUNT_FILE = 'rich-compiler-462615-a3-49279f5b5e7d.json'


# read all files in the drive
drive_service = build('drive', 'v3', credentials=Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE))
sheets_service = build('sheets', 'v4', credentials=Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE))

# list all files in the drive
# this only returns 100, i need to get all files
# files = drive_service.files().list(q=f"parents='{DRIVE_ID}'", fields='files(id, name)').execute()
# print(files)
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


# get store funnel master sheet
gc = gspread.service_account(filename=SERVICE_ACCOUNT_FILE)
try:
    source_sheet = gc.open("Store Funnel")
    worksheet = source_sheet.sheet1
    data = worksheet.get_all_values()
except gspread.exceptions.SpreadsheetNotFound:
    print("\nError: Could not find spreadsheet named 'Store Funnel'")
    print("Please check the exact name of your spreadsheet and make sure it matches exactly.")
    # exit(1)


# create a store seller name to id , phone number , regional manager mapping, error on dupliaces
store_seller_name_to_id = {}
store_seller_name_to_phone = {}
store_seller_name_to_regional_manager = {}
store_seller_name_to_link = {}
store_seller_name_to_email = {}

# get header row for store funnel master sheet
# print(data[0:2])
header = data[0]
# print(header)

# print("files")
# print(files)

for row in data[1:]:
    # zip row data with header
    row_data = dict(zip(header, row))
    # print(row_data)

    # check if the row is empty
    if row_data['Seller Name'] in store_seller_name_to_id:
        # check if the id is the same
        if store_seller_name_to_id[row_data['Seller Name']] != row_data['Seller ID']:
            print(f"Error: Duplicate store seller name: {row[0]}")
            # exit(1)
            continue
    else:
        # add the row to the mapping
        store_seller_name_to_id[row_data['Seller Name']] = row_data['Seller ID']
        store_seller_name_to_phone[row_data['Seller Name']] = row_data['Seller Phone']
        store_seller_name_to_regional_manager[row_data['Seller Name']] = row_data['RM Region']
        store_seller_name_to_email[row_data['Seller Name']] = row_data['Seller Email']

    for file in files:
        # print(file['name'], file['id'])
        if file['name'].startswith(row_data['Seller Name']):
            store_seller_name_to_link[row_data['Seller Name']] = "https://docs.google.com/spreadsheets/d/" + file['id'] + "/edit?gid=0&usp=sharing"
            break


    # if row_data['Seller Name'] not in store_seller_name_to_link:
    #     print(f"No link found for {row_data['Seller Name']}")
    #     print(file['name'], row_data['Seller Name'] )
    #     exit(1)
    #     # continue

# print(store_seller_name_to_id)
# print(store_seller_name_to_phone)
# print(store_seller_name_to_regional_manager)
# print(store_seller_name_to_link)
# print(store_seller_name_to_email)

# row needs to be seller rep_name, seller_ID, link, phone number, regional_manager
# rep_name = 
row = []
print("Gotten files: ", len(files))
for seller_name in store_seller_name_to_id:
    try:
        row.append(seller_name)
        row.append(store_seller_name_to_id[seller_name])
        row.append(store_seller_name_to_link[seller_name])
        row.append(store_seller_name_to_phone[seller_name])
        row.append(store_seller_name_to_regional_manager[seller_name])
        # print(row)
        row = []
    except Exception as e:
        print("Error: ", e)
        continue


print("Creating new sheet")
# write row to a new sheet
new_sheet = gc.create("Contact list draft")
# contact list headers
contact_list_header = ["Seller Name", "Link", "Seller ID", "Seller Phone", "Seller Email", "RM Region"]
# write header to a new sheet
new_sheet.sheet1.update([contact_list_header])


# iterate over seller names and write rows to a new sheet
rows = []
for seller_name in store_seller_name_to_id:
    try:
        print(seller_name)
        rows.append([seller_name, store_seller_name_to_link[seller_name], store_seller_name_to_id[seller_name], store_seller_name_to_phone[seller_name],store_seller_name_to_email[seller_name], store_seller_name_to_regional_manager[seller_name]])
        
    except Exception as e:
        print(e)
        print("Error at the end of the script: ", e)
        # exit(1)

new_sheet.sheet1.insert_rows([contact_list_header] + rows)
drive_service.files().update(fileId=new_sheet.id, body={'name': 'Contact list'}).execute()
print(new_sheet.id)

# share sheet with nathan
new_sheet.share("nathan.chiu@reprally.com", perm_type="user", role="writer")
