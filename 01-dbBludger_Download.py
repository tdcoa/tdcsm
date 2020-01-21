import  os, sys, io, yaml
import pandas as pd
import requests

### Load config
with open('config.yaml', 'r') as file:
    configyaml = yaml.load(file.read()) # , Loader=yaml.FullLoader) #<-- will need eventually

### Create required subfolders
coafolders = {}
for fo in configyaml['folders']:
    coafolders.update(fo)

for nm, subfo in coafolders.items():
    if not os.path.exists(subfo):
        os.mkdir(subfo)

### define the files to download (preserve names)
githost = 'https://raw.githubusercontent.com/tdcoa/usage/master/'
gitfiles=configyaml['download-files']

for gitfile in gitfiles:
    giturl = githost+gitfile
    filecontent = requests.get(giturl).content
    file = open(os.path.join('.', coafolders['download'].strip(), gitfile), 'w+')
    file.write(filecontent.decode('utf-8'))
    file.close

    if gitfile=='motd.txt' or gitfile[-3:]=='.py':
        os.replace(os.path.join('.', coafolders['download'].strip(), gitfile), os.path.join('.', gitfile))

print('done!')
