import random
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
import gspread
import time

DRIVE_ID = "18bSzmcB1jUJhB6JM1vRi6sEYI0z0ZsjY"
SERVICE_ACCOUNT_FILE = 'rich-compiler-462615-a3-49279f5b5e7d.json'
SCOPES = ["https://www.googleapis.com/auth/spreadsheets",
          "https://www.googleapis.com/auth/drive"]

# read all files in the drive
drive_service = build('drive', 'v3', credentials=Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE))
sheets_service = build('sheets', 'v4', credentials=Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE))


creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
gc = gspread.authorize(creds)

def get_all_files():
    # list all files in the drive
    files = []
    page_token = None
    MAX_LOOP_COUNT = 10000000
    loop_count = 0

    while True:
        loop_count += 1
        if loop_count > MAX_LOOP_COUNT:
            break

        response = drive_service.files().list(
            q=f"'{DRIVE_ID}' in parents",
            fields="nextPageToken, files(id, name)",
            pageToken=page_token
        ).execute()

        files.extend(response.get('files', []))
        page_token = response.get('nextPageToken', None)

        if page_token is None:
            print(f"Gotten {len(files)} files")
            return files

    return files

def get_store_funnel():
    # get the store funnel sheet
    sheet = gc.open("Store Funnel")
    # print(sheet)
    return sheet


def get_store_funnel_by_sellers() -> dict[str, dict]:
    sheet = get_store_funnel()
    seller_by_id = {}

    # get header row
    header_row = sheet.get_worksheet(0).get_all_values()[0]
    # print(header_row)

    # get data rows
    data_rows = sheet.get_worksheet(0).get_all_values()[1:]
    # print(data_rows)

    # create a dict of seller_id to row
    for row in data_rows:
        row_dict = dict(zip(header_row, row))
        seller_by_id[row_dict['Seller Name']] = row_dict
    return seller_by_id
