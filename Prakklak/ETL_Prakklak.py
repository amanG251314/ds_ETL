#Importing Libraries
import os
import numpy as np
from datetime import datetime
import pandas as pd
from datetime import date
from datetime import timedelta
import requests
from datetime import datetime
from dateutil import parser
import google.auth.transport.requests
import google.oauth2.id_token
from google.cloud import bigquery
from datetime import datetime, timedelta,timezone
from google.cloud import storage
from dotenv import load_dotenv
load_dotenv()
#os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="C:/Users/apatk/Documents/service_keys/aman_dev.json"

def ETL():
    #Environment Variables
    projectName=os.getenv('projectName')
    tableName=os.getenv('tableName')
    pashuKundaliUrl=os.getenv('pashuKundaliUrl')
    akaashGangaUrl=os.getenv('akaashGangaUrl')
    bcsURL=os.getenv('bcsURL')
    bucketName=os.getenv('bucketName')
    fileSaveLoc=os.getenv('fileSaveLoc')

    #Getting date of yesterday
    d= datetime.now().date()-timedelta(1)
    month=d.month
    year=d.year
    day=d.day
    if month<10:
        month=str(0)+str(month)
    if day<10:
        day=str(0)+str(day)
    date=str(year)+"-"+str(month)+"-"+str(day)
    date='2022-12-24'

    query="""
    SELECT * FROM {} WHERE dateOfsale='{}'""".format(tableName,date)


    # If you don't specify credentials when constructing the client, the
    # client library will look for credentials in the environment.
    client = bigquery.Client(project=projectName)

    query_job = client.query(query)
    results = query_job.result()

    df_finance=results.to_dataframe()

    #Loading PashuKundali Data

    def getAnimalIdFromTag(animalTag, audience=pashuKundaliUrl):
        auth_req = google.auth.transport.requests.Request()
        id_token = google.oauth2.id_token.fetch_id_token(auth_req, audience)

        headers = {
        'Authorization' : 'Bearer ' +  id_token
        }
        response = requests.get(audience + '/animal/tag/' + animalTag, headers = headers)

        return response.json()[0]['id']

    def getAnimalDataFromId(animalId, audience=pashuKundaliUrl):
        auth_req = google.auth.transport.requests.Request()
        id_token = google.oauth2.id_token.fetch_id_token(auth_req, audience)

        headers = {
        'Authorization' : 'Bearer ' +  id_token
        }
        response = requests.get(audience + '/animal/' + animalId, headers = headers)

        return response.json()

    def getMilkYield(animalId,audience=akaashGangaUrl):
        startDate = '2022-01-01'
        endDate = '2025-06-01'
        params = {
            'startDate' : startDate,
            'endDate' : endDate,
            'take' : 100,
            'skip' : 0
        }
        auth_req = google.auth.transport.requests.Request()
        id_token = google.oauth2.id_token.fetch_id_token(auth_req, audience)
        headers = {
            'Authorization' : 'Bearer ' +  id_token
        }
        response = requests.get(audience + '/milk-yield-record/' + animalId, params = params, headers = headers)
        return response.json()

    def getLastnMilking(animalId,n=6):
        result=getMilkYield(animalId)
        MY=pd.DataFrame(result)
        MY['milkingDate']=MY['milkingTime'].apply(lambda x:parser.parse(x).date())
        MY['weightValue']=MY['weightValue'].astype(float)
        return MY['weightValue'][-n:]

    def getBCS(imageUrl,bcsURL=bcsURL):
        data={"image":str(imageUrl)}
        response=requests.post(bcsURL,json=data)
        return response.json()['goodBcsProb']


    #Loading and Processing
    colsAnimal=['height', 'midSectionLength','widthFromBack','weight','girth','lactationNo','calfGender','parturationDate','bcsAI','bcsURL']
    colsFinance=['buffaloTag','procurementPrice','dateOfSale','salesPrice']
    df=pd.DataFrame()
    for i,row in df_finance.iterrows():
        try:
            animalId=getAnimalIdFromTag(row['buffaloTag'])
        except:
            continue
            
        dataFinance=list(row[colsFinance])
        animalData=getAnimalDataFromId(animalId)
        
        try:
            height=animalData['measurements']['height']['value']
        except:
            height=np.nan
        try:
            midSectionLength=animalData['measurements']['midSectionLength']['value']
        except:
            midSectionLength=np.nan
        try:
            widthFromBack=animalData['measurements']['widthFromBack']['value']
        except:
            widthFromBack=np.nan
        try:
            weight=animalData['measurements']['weight']['value']
        except:
            weight=np.nan
        try:
            girth=animalData['measurements']['girth']['value']
        except:
            girth=np.nan
        try:
            lactationNo=animalData['lactationNo']
        except:
            lactationNo=np.nan
        # try:
        #     parturationDate=animalData['dob']
        #     parturationDate=datetime.strptime(parturationDate, "%Y-%m-%dT%H:%M:%S.%fZ").date()
        # except:
        #     parturationDate=np.nan
        try:
            childId=animalData['childReferences'][0].split('/')[1]
            childData=getAnimalDataFromId(childId)
            calfGender=childData['gender']
        except:
            calfGender=np.nan
        
        try:
            parturationDate=childData['dob']
            parturationDate=datetime.strptime(parturationDate, "%Y-%m-%dT%H:%M:%S.%fZ").date()
        except:
            parturationDate=np.nan
        
        try:
            bcsPhotoUrl=[med for med in animalData['media'] if med['of']=='back'][0]['url']
            bcsAI=getBCS(bcsPhotoUrl)
        except:
            bcsAI=np.nan
            try:
                bcsPhotoUrl=[med for med in animalData['media'] if med['of']=='back'][0]['url']
            except:
                bcsPhotoUrl=np.nan

        try:
            milkYeild=getLastnMilking(animalId)
            milkYeild=np.mean(milkYeild)
        except:
            try:
                milkYeild=getLastnMilking(animalId)
                milkYeild=np.mean(milkYeild)
            except:
                milkYeild=np.nan
        animalData=[height,midSectionLength,widthFromBack,weight,girth,lactationNo,calfGender,parturationDate,bcsAI,bcsPhotoUrl]
        df.loc[i,colsFinance+colsAnimal]=dataFinance+animalData
        df.loc[i,'milkYeild']=milkYeild*2
    if len(df)==0:
        df.loc[0,colsFinance+colsAnimal+['milkYeild']]=[np.nan for i in range(len(colsFinance+colsAnimal+['milkYeild']))]


    #pushing to google cloud storage
    client=storage.Client(project=projectName)
    print(projectName)

    # The bucket on GCS in which to write the CSV file
    bucket = client.bucket(bucketName)
    # The name assigned to the CSV file on GCS
    blob = bucket.blob(fileSaveLoc)
    blob.upload_from_string(df.to_csv(index=False), 'text/csv')
