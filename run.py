from tdcsm.tdcoa import tdcoa

coa = tdcoa(approot='../coa/Test', secrets='../!secrets.yaml')

options = """
Select a step:

0) Reload Configs
1) Download Files
2) Prepare SQL
3) Execute Run
4) Upload to Transcend
5) Exit
"""

while True:

    x = -1
    while x < 0 or x > 6:
        try:
            x = int(input(options))
        except ValueError as e:
            print('\nInput needs to be a number between 1 and 4')

    # 0) RELOAD_Config
    if int(x) == 0:
        coa.reload_config()

    # 1) DOWNLOAD FILES
    elif int(x) == 1:
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

    # 4) UPLOAD TO TRANSCEND
    elif int(x) == 5:
        break
