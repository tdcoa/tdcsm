import os, sys, yaml, errno
from shutil import copyfile
import pandas as pd

#TODO: Make this a commandline arg
coadir = 'coa_default'

#load config:
with open('config.yaml', 'r') as file:
    configyaml = yaml.load(file.read()) # , Loader=yaml.FullLoader) #<-- will need eventually
coafolders = {}
for fo in configyaml['folders']:
    coafolders.update(fo)

# clear out any pre-existing files in run folder:
for file in os.listdir(os.path.join('.', coafolders['run'])):
    if file[:1] != '.':
        os.remove(os.path.join('.', coafolders['run'],file))

#load .coa.sql files into the _run directory, with all replacements done (ready to run sql)
for coafile in os.listdir(coadir):
    if coafile[:1] != '.':
        if coafile[-8:]=='.coa.sql':  # if SQL, do substitutions
            with open(os.path.join(coadir, coafile), 'r') as coasqlfile:           # read from template
                coasqls = coasqlfile.read()

            with open(os.path.join(coafolders['run'],coafile),'w') as runsqlfile:  # write to _run file

                #light formatting of supplied sql
                sqls = coasqls.split(';')
                for sql in sqls:
                    while '\n\n' in sql:
                        sql = sql.replace('\n\n','\n').strip()
                    sql = sql.strip() + '\n;\n\n'

                    if sql != '\n;\n\n':  # exclude null statements (only ; and newlines)

                        # do substitutions first... (allows for substitution in {{loop}} command)
                        for setting in configyaml['substitutions']:
                            for find,replace in setting.items():
                                sql = sql.replace('{%s}' %str(find),str(replace))

                        # if there are any {{loop: commands, go get the csv
                        if '/*{{loop:' in sql:

                            # parse csv file name from {{loop:csvfilename}} string
                            csvfile = sql[(sql.find('/*{{loop:') + len('/*{{loop:')):sql.find('}}*/')].strip()

                            # can we find the file?
                            if os.path.isfile(os.path.join(coafolders['run'],csvfile)): # csv file found!  let's open:
                                df = pd.read_csv(os.path.join(coafolders['run'],csvfile))

                                for index, row in df.iterrows():  # one row = one sql written to file
                                    tempsql = sql
                                    for col in df.columns:
                                        tempsql = tempsql.replace(str('{%s}' %col), str(row[col]))
                                    tempsql = tempsql.replace(csvfile,' csv row %i out of %i' %(index+1, len(df)))
                                    tempsql = tempsql.replace('/*{{loop:','/*{{')
                                    runsqlfile.write(tempsql)

                            else:  # file not found, raise error
                                raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), csvfile)

                        else:  # not a loop, just write file as-is (post-replace)
                            runsqlfile.write(sql)

        else:  # if not a .coa.sql file... just copy
            copyfile(os.path.join('.', coadir, coafile), os.path.join('.', coafolders['run'], coafile))

print("Done!!!")
