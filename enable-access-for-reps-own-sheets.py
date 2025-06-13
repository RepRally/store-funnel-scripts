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


# for each file, enable their email access
for file in files:
    # print(file['name'])
    # seller_email = name_to_email[file['name'].split(" - ")[0]]
    # # enable their email access
    # drive_service.permissions().create(
    #     fileId=file['id'],
    #     body={'role': 'writer', 'type': 'user', 'emailAddress': seller_email}
    # ).execute()
    # delay = random.uniform(1, 2)  # Random delay between 1-2 seconds
    # print(f"Waiting {delay:.2f} seconds before creating next sheet...")
    # time.sleep(delay)

    # enable public access
    drive_service.permissions().create(
        fileId=file['id'],
        body={'role': 'writer', 'type': 'anyone'}
    ).execute()
    
    # # get the sheet
    # sheet = gc.open_by_key(file['id'])
    # sheet.share("nathan.chiu@reprally.com", perm_type="user", role="writer")
    