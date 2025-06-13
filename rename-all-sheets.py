from get_all_files import get_all_files, drive_service
from googleapiclient.discovery import build

sheets_service = build('sheets', 'v4', credentials=drive_service._credentials)

files = get_all_files()

for file in files:
    print(file['name'])
    # rename the file
    drive_service.files().update(fileId=file['id'], body={'name': file['name']}).execute()
    # rename the sheet
    sheets_service.spreadsheets().batchUpdate(spreadsheetId=file['id'], body={'requests': [{'updateSheetProperties': {'properties': {'sheetId': 0, 'title': 'Potential earnings'}, 'fields': 'title'}}]}).execute()


