# dbBludger -- Upload to Transcend
import os, sys, yaml, json
import pandas as pd
import datetime as dt
from sqlalchemy.engine import create_engine

#load config:
with open('config.yaml', 'r') as file:
    configyaml = yaml.load(file.read()) # , Loader=yaml.FullLoader) #<-- will need eventually
coafolders = {}
for fo in configyaml['folders']:
    coafolders.update(fo)
coatranscend = {}
for sy in configyaml['transcend']:
    coatranscend.update(sy)

#TODO: approot needs to be commandline driven, or current location if not provided.
# define app-root path:
approot = os.path.join('.',coafolders['output'],'2020-01-21_175223')

# define upload manifest:
with open(os.path.join(approot, 'upload-manifest.json'),'r') as manfile:
    manjson = manfile.read()
    manifest = json.loads(manjson)

# loop thru all TRANSCEND systems and execute manifest
for name, connstring in coatranscend.items():
    conn = create_engine(connstring) # <------------------------------- Connect to the database

    for entry in manifest['entries']:

        # open CSV file for reading
        dfcsv = pd.read_csv(os.path.join(approot, entry['file']))

        for col in dfcsv.columns:
            if col[:8] == 'Unnamed:':
                print(col)
                dfcsv = dfcsv.drop(columns=[col])
        print(dfcsv)

        if len(entry['table'].split('.')) == 1:
            db = 'adlste_coa'
            tbl = entry['table']
        else:
            db = entry['table'].split('.')[0]
            tbl = entry['table'].split('.')[1]

        # APPEND data set to table
        dfcsv.to_sql(tbl, conn, schema=db, if_exists='append', index=False)

        # CALL any specified SPs:
        if str(entry['call']).strip() != "":
            conn.execute('call %s ;' %str(entry['call']) )

#TODO: explicitly close connection
