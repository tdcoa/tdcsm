from tdcsm.tdcoa import tdcoa

coa = tdcoa(approot=r'C:\Deepan\Data_Science\tdcsm\tdcoa_demo')

options = """
Select a step:

0) Reload Configs
1) Download Files
2) Prepare SQL
3) Execute Run
4) Upload to Transcend
5) Make Customer Files
6) Collect Data
7) Process Data
"""

while True:

    x = -1
    while x < 0 or x > 7:
        try:
            x = int(input(options))
        except ValueError as e:
            print('\nInput needs to be a number between 1 and 4')

    # 0) RELOAD_Config
    if int(x) == 0:
        coa.reload_config()

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

        # 5) Make Manual Files
    elif int(x) == 5:
        coa.make_customer_files()

    elif int(x) == 6:
        coa.collect_data()

    elif int(x) == 7:
        coa.process_data()

