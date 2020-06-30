from tdcsm.tdcoa import tdcoa
import os

coa = tdcoa(approot=os.getcwd())
# coa = tdcoa(approot=r'C:\Deepan\Data_Science\tdcsm\tdcoa_demo')

options = """
Select a step:

1) Download Files
2) Prepare SQL
3) Execute Run
4) Upload to Transcend
"""

while True:

    x = 0
    while x < 1 or x > 5:
        try:
            x = int(input(options))
        except ValueError as e:
            print('\nInput needs to be a number between 1 and 4')

    # 1) DOWNLOAD FILES
    if int(x) == 1:
        coa.download_files()

    # 2) PREPARE SQL
    elif int(x) == 2:
        coa.prepare_sql()

    # 3) EXECUTE SQL
    elif int(x) == 3:
        coa.execute_run()

    # 4) UPLOAD TO TRANSCEND
    elif int(x) == 4:
        coa.upload_to_transcend()
        break

