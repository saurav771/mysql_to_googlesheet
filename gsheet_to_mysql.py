try:
    import mysql.connector as ms
except:
    try:
        import MySQLdb as ms 
    except:
        import pymysql as ms
import pandas as pd
import numpy as np
import os
import sys
import os.path
import smtplib
import smtplib,email,email.encoders,email.mime.text,email.mime.base
import email.utils
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import pickle as pkl
import requests
import json
import time
import schedule
import datetime
from time import sleep
from datetime import datetime, timedelta
import sys; 
from importlib import reload
reload(sys); 
pd.set_option('display.precision', 2)
dt1 = datetime.now().strftime('%Y-%m-%d')
pd.options.display.float_format = '{:.0f}'.format
yesterday = '{dt.year}-{dt.month}-{dt.day}'.format(dt = datetime.now() - timedelta(days = 1))




def credsLogin(SCOPES): #credentials to connect to  the google sheets api 
    try:    
        creds = None
        if os.path.exists('token.pickle'):
            print("Reading GCP service account Credentials ...")
            with open('token.pickle', 'rb') as token:
                creds = pkl.load(token)
        if not creds or not creds.valid:
            print("Creating GCP service account Credentials ...")
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file('gsheetautomation/src/credentials.json', SCOPES) #Enter the path to your service account credentails.json file
                creds = flow.run_local_server(port=0)
            with open('token.pickle', 'wb') as token:
                # pickle.dump(creds, token)
                pkl.dump(creds, token , protocol = 2)
        return creds
    except Exception as e:
        print("There is an Exeption in credsLogin Function : ",e)

def readGoogleSheet(creds,SPREADSHEET_ID,RANGE_NAME): #google sheet read function to read the sheet 
    try:   
        print("Reading Sheet...")
        service = build('sheets', 'v4', credentials=creds)
        sheet = service.spreadsheets()
        result = sheet.values().get(spreadsheetId=SPREADSHEET_ID,range=RANGE_NAME).execute()
        values = result.get('values', [])
        final_df = pd.DataFrame(values)
        return final_df
    except Exception as e:
        print("There is an Exeption in readGoogleSheet Function : ",e)


def writeGoogleSheet(creds,SPREADSHEET_ID,RANGE_NAME,body): #writes the data into the specified google sheets 
    try:
        print("Fetching Data...")
        service = build('sheets','v4', credentials=creds)
        result = service.spreadsheets().values().update(spreadsheetId=SPREADSHEET_ID,range=RANGE_NAME,valueInputOption='USER_ENTERED',body=body)
        result.execute()
        return result
    except Exception as e:
        print("There is an Exeption in writeGoogleSheet Function : ",e)


def clearGoogleSheet(creds,SPREADSHEET_ID,RANGE_NAME,body): #clears the data in the given google sheet range
    try:
        print("Clearing Sheet Content...")
        service = build('sheets','v4', credentials=creds)
        result = service.spreadsheets().values().clear(spreadsheetId=SPREADSHEET_ID,range=RANGE_NAME,body=body)
        result.execute()
        return result
    except Exception as e:
        print("There is an Exeption in clearGoogleSheet Function : ",e)

def appendGoogleSheet(creds,SPREADSHEET_ID,RANGE_NAME,body): 
    try:
        print("\n\nAppending Data......")
        service = build('sheets','v4', credentials=creds)
        result = service.spreadsheets().values().append(spreadsheetId=SPREADSHEET_ID,range=RANGE_NAME,valueInputOption='USER_ENTERED',insertDataOption='INSERT_ROWS',body=body)
        result.execute()
        return result
    except Exception as e:
        print("There is an Exeption in appendGoogleSheet Function : ",e)


def connnectDB(query,DBname,encode = False): #db connection credentials to fetch the data
    print("\nVerifying DB Credentials....\n")
    if DBname == 'Enter the DB Name':
        conn = ms.connect(host = '----------',user = '--------',passwd = '----------',db = DBname)
    if DBname != 'Enter the DB Name' and DBname != 'Enter the DB Name':
        conn = ms.connect(host = '------------',user = '-------',passwd = '---------',db = DBname)
    if DBname == 'Enter the DB Name':
        conn = ms.connect(host = '-----------',user = '-------',passwd ='--------',db = DBname)
    if not encode:
        return(pd.read_sql(query,conn))
    else:
        data = pd.read_sql(query,conn)
        return (pd.DataFrame(([[str(data[j][i]) for j in list(data.columns)] for i in range(len(data))]),columns = list(data.columns))) 



SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

# STEP 1
# Google Sheet ID

SPREADSHEET_ID = 'Enter the id to your sheet where you want your data to be written'


creds = credsLogin(SCOPES)

print ("\n\n---------------------Connection Estabulished!-------------------\n\n")

# STEP 2
# Put your SQL query here :

sql = """
Enter your query here to fetch the data
"""

# STEP 3
# Specify Database Name Below:

result = connnectDB(sql,'name of your database schema',True)


print (result.head())

# exit()



values=[[str(result[j][i]) for j in result.columns] for i in range(result.shape[0])]


# STEP 4
# Specify the TAB Name & Range

RANGE_NAME= """data!A2:H"""#Enter the Range of your google sheet where you want the data to be written
body = {}

# To clear google sheet

clearGoogleSheet(creds,SPREADSHEET_ID,RANGE_NAME,body)

main_body = {'values':values}

# To write google sheet

writeGoogleSheet(creds,SPREADSHEET_ID,RANGE_NAME,main_body)

appendGoogleSheet(creds,SPREADSHEET_ID,RANGE_NAME,body)
print("\n--------------Execution Completed--------------\n")



