import requests
import time
import json
import sys
import io
import os
import pandas as pd
import geopandas as gpd
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
from google.oauth2.service_account import Credentials
from datetime import datetime
from unidecode import unidecode

#This french accent mapping is used to string match the names of communes 
#across the two data sets from INSEE and TrocVélo. It is normalized to be
#able to map it across a dataframe
normalMap = {'À': 'A', 'Â': 'A',
             'à': 'a', 'â': 'a',
             'È': 'E', 'É': 'E', 'Ê': 'E', 'Ë': 'E',
             'è': 'e', 'é': 'e', 'ê': 'e', 'ë': 'e',
             'Î': 'I', 'Ï': 'I',
             'î': 'i', 'ï': 'i',
             'Ô': 'O',
             'ô': 'o',
             'Ù': 'U', 'Û': 'U', 'Ü': 'U',
             'ù': 'u', 'û': 'u', 'ü': 'u',
             'Ç': 'C', 'ç': 'c',}
normalize = str.maketrans(normalMap)

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
df.to_csv('/Users/cpowers/Desktop/DEPP/In_Progress/Data_Scraping/TrocVelo/troc_velo_announcements.csv')

gdf = gpd.read_file('/Users/cpowers/Desktop/DEPP/In_Progress/Data_Scraping/TrocVelo/france-20240601.geojson')
gdf = gdf[gdf.is_valid]
gdf.head(5)

#We place the updated csv to our common google drive folder, located through
# the metadata. We upload the csv file saved locally to the Google Drive Folder.
file_metadata = {
    'name': 'troc_velo_announcements.csv',  
    'parents': [folder_id]  
}
media = MediaFileUpload('/Users/cpowers/Desktop/DEPP/In_Progress/Data_Scraping/TrocVelo/troc_velo_announcements.csv', mimetype='text/csv', resumable=True)

request = service.files().create(body=file_metadata, media_body=media)

# Upload the file in chunks
response = None
while response is None:
    status, response = request.next_chunk()
    if status:
        print(f"Uploaded {int(status.progress() * 100)}%")

#uploads the INSEE population, Geospatial, and department level data
geo_df = pd.read_csv("/Users/cpowers/Desktop/DEPP/In_Progress/Data_Scraping/TrocVelo/correspondance-code-insee-code-postal.csv", sep=";")

#Merging the two datasets, which require a lot of cleaning, since the naming
#conventions are not the same across the datasets and each postal code does not
#correspond to departement. 
df2 = df.copy()
df2.info

#all of geo_df communes are in uppercase. Since this comes from INSEE, we follow
#that standard
df2['city_mrg'] = df2['city'].str.upper()

#Lyon, Marseille, and Paris have special cases where the TrocVelo data keeps
# the arrondissement and city in the same string. The commune name needs to
# be seperated. Saint cities also have an issue where spelling is ST or SNT
df2.loc[df2['city_mrg'].str.contains(r'\bLYON(?:-\d+E|)\b', case=False, na=False), 'city_mrg'] = 'LYON'
df2.loc[df2['city_mrg'].str.contains(r'\bMARSEILLE(?:-\d+E|)\b', case=False, na=False), 'city_mrg'] = 'MARSEILLE'
df2.loc[df2['city_mrg'].str.contains(r'\bPARIS(?:-\w+|)\b', case=False, na=False), 'city_mrg'] = 'PARIS'
df2['city_mrg'] = df2['city_mrg'].apply(unidecode)
df2['city_mrg'] = df2['city_mrg'].str.replace('-',' ')
df2['city_mrg'] = df2['city_mrg'].str.replace(r'\bST\b |\bSTE\b ', 'SAINT ', regex=True)

#There are unifying string adjustments to make, mainly data structure issues
#like string classification and removal of unnecssary list objects.
geo_df['Code Postal'] = geo_df['Code Postal'].astype(str)
geo_df['Code Postal'] = geo_df['Code Postal'].str.split('/')
geo_df = geo_df.explode('Code Postal', ignore_index=True)
geo_df.loc[geo_df['Commune'].str.contains(r'\bLYON\b(?:--.*)?', case=False, na=False), 'Commune'] = 'LYON'
geo_df.loc[geo_df['Commune'].str.contains(r'\bMARSEILLE\b(?:--.*)?', case=False, na=False), 'Commune'] = 'MARSEILLE'
geo_df.loc[geo_df['Commune'].str.contains(r'\bPARIS\b(?:--.*)?', case=False, na=False), 'Commune'] = 'PARIS'
geo_df['Commune_mrg'] = geo_df['Commune'].str.replace('-',' ')

#Finally merging the two datasets on two condition clauses, since code postal
# and commune name match best to departement level data. There are repeat communes
# in different departements and postal code does not match directly with
# Department codes.
final_df_in = pd.merge(geo_df,df2,left_on=["Code Postal","Commune_mrg"], right_on=['postcode','city_mrg'], how='inner')

#Exporting the merged data set
final_df_in.to_csv('/Users/cpowers/Desktop/DEPP/In_Progress/Data_Scraping/TrocVelo/cities_in.csv',index=False)


df = pd.read_csv("/Users/cpowers/Desktop/DEPP/In_Progress/Data_Scraping/TrocVelo/troc_velo_announcements.csv")  # Replace with the actual file path
df = df.dropna(subset=["Code INSEE"])
df['Code INSEE'] = df['Code INSEE'].astype(str)
df['Code Postal'] = df['Code Postal'].apply(
    lambda x: str(int(float(x))) if str(x).replace(".0", "").isdigit() else x
)
df["Code Département"] = df["Code Département"].apply(
    lambda x: str(int(float(x))) if str(x).replace(".0", "").isdigit() else x

df_insee = df.groupby("Code INSEE", as_index=False).agg(
    total_population=("Population", "mean"),
    total_unique_users=("user_id", "nunique"),
    total_announcements=("id", "count")
)
print(df_insee)

# Step 3: Group by "Code Département" (Department level)
df_departement = df_insee.merge(df[["Code INSEE", "Code Département"]].drop_duplicates(), on="Code INSEE")
print(df_departement)
df_test = df_departement[df_departement['Code INSEE'] == '01004']
print(df_test)
df_departement = df_departement.groupby("Code Département", as_index=False).agg(
    total_population=("total_population", "sum"),
    total_unique_users=("total_unique_users", "sum"),
    total_announcements=("total_announcements", "sum")
)

# Display final result
print(df_departement['Code Département'].unique())
print(df_departement.describe())
print(df_departement.loc[df_departement["total_announcements"].idxmax()])
