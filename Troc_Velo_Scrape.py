import requests
import time
import json
import sys
import io
import os
import pandas as pd
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
from google.oauth2.service_account import Credentials
from datetime import datetime


#API credentials and user information to pull the data we have already scraped
# from troc-velo in previous days. 
SCOPES = ['https://www.googleapis.com/auth/drive']
creds = Credentials.from_service_account_file('/Users/cpowers/Downloads/troc-velo-google-credentials.json', scopes=SCOPES)
service = build('drive', 'v3', credentials=creds)


#Requesting the new data from the troc-velo api, which limits per call pulls to
# 10000 rows of data. The data pull by defaults gives the 10000 most recently 
# updated bike announcements. A large portion of this code is implementing a 
# system of requests that update announcements from last call and adds new
# announcments to the dataframe.
base_url = "https://api.troc-velo.com/api/products"
params = {
    'category': 'u1',             # Category parameter
    'sorting': 'relevance:desc',
    'itemsPerPage': 300
}
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Accept': 'application/json'
}

#Google API information for where the final dataset sits.
file_name = 'troc_velo_announcements_base.csv'
folder_id = '1F1iiGkN3cqw9GMAdryA-cJGvYjJILy2J'

#Pulls the csv file from the Google Drive folder for our team
query = f"'{folder_id}' in parents and name='{file_name}' and mimeType='text/csv'"
results = service.files().list(q=query, fields="files(id, name)").execute()
files = results.get('files', [])

results = service.files().list(
    q=f"'{folder_id}' in parents",
    fields="files(id, name)",
    supportsAllDrives=True
).execute()

files = results.get('files', [])

file_id = files[0]['id']
print(f"Found file: {file_name} (ID: {file_id})")

#IO pull of the csv file and stores the old data in a file_stream to reconvert
# to csv. We chose this method instead of locally downloading the csv file 
# and uploading it to this code to reduce the local memory load on our computers.
request = service.files().get_media(fileId=file_id)
file_stream = io.BytesIO()
downloader = MediaIoBaseDownload(file_stream, request)

done = False
while not done:
    status, done = downloader.next_chunk()
    print(f"Download progress: {int(status.progress() * 100)}%")

# Load the CSV content into a Pandas DataFrame
file_stream.seek(0)
df = pd.read_csv(file_stream)

#The troc-velo requires pageiation for each request, or the request will time
# out. So we demand 300 rows per request and iterate through the pages to comb
# all 100000 announcements.
for page in range(1, 34):  

    params['page'] = page

    response = requests.get(base_url, headers=headers, params=params)

    if response.status_code == 200:
        data = response.json()
        if isinstance(data, list):
            #We parse the json file of all the announcements the API call. We
            #only want to keep a subsection of the data necessary for unique ids,
            #data cleaning, pricing, and geographic location.
            for announce in data:
                user = announce.get('user', {})
                country = user.get('country', {})
                announcement = pd.DataFrame([{
                    "title" : str(announce.get('title', 'N/A')),  
                    "price" : float(announce.get('price', 'NaN') / 100), 
                    "postcode" : str(user.get('postcode', 'N/A')), 
                    "city" : str(user.get('city','N/A')),
                    "country" : str(country.get('name', 'N/A')),
                    "brand" : int(announce.get('brand','NaN')),
                    "id" : int(announce.get('id', "NaN")),
                    "user_id" : int(user.get('id','NaN')),
                    "publish_date" : str(announce.get('publishAt', 'N/A')),
                    'update_date' : str(announce.get('updatedAt','N/A'))
                    }])
                announcement['date_pulled'] = datetime.today().strftime('%Y-%m-%d')
                announcement['url_pulled'] = base_url
                if not df['id'].eq(announcement['id'].iloc[0]).any():
                    df = pd.concat([df, announcement], ignore_index=True)
                if announcement['update_date'][0] > df.loc[df['id'] == announcement['id'][0], 'update_date'].iloc[0]:
                    df = df.set_index('id')
                    announcement = announcement.set_index('id')
                    df.update(announcement)
                    df = df.reset_index()
        else:
            print("Unexpected JSON format. Skipping this page.")

    #We sleep the requests for a standard 2 second break in order to not
    #overwhelm the troc-velo server.
    time.sleep(2)

#We are only looking at announcements in France
df = df[df['country']=="France"]
df.drop(columns=df.columns[df.columns.str.contains('^Unnamed')], inplace = True)
df.to_csv('/Users/cpowers/Desktop/DEPP/Data_Scraping/TrocVelo/troc_velo_announcements.csv')

#We place the updated csv to our common google drive folder, located through
# the metadata. We upload the csv file saved locally to the Google Drive Folder.
file_metadata = {
    'name': 'troc_velo_announcements.csv',  
    'parents': [folder_id]  
}
media = MediaFileUpload('/Users/cpowers/Desktop/DEPP/Data_Scraping/TrocVelo/troc_velo_announcements.csv', mimetype='text/csv', resumable=True)

request = service.files().create(body=file_metadata, media_body=media)

# Upload the file in chunks
response = None
while response is None:
    status, response = request.next_chunk()
    if status:
        print(f"Uploaded {int(status.progress() * 100)}%")
