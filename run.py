from tdcsm.tdcoa import tdcoa

coa = tdcoa(r'C:\Users\nd186026\Documents\tdcsm_demo')

options = """
Select a step:

1) Setup
2) Download Files
3) Prepare SQL
4) Execute Run
5) Upload to Transcend
"""

while True:
    x = input(options)

    try:
        # 1) SETUP
        if int(x) == 1:
            pass

        # 2) DOWNLOAD FILES
        elif int(x) == 2:
            coa.download_files()

        # 3) PREPARE SQL
        elif int(x) == 3:
            coa.prepare_sql()

        # 4) EXECUTE SQL
        elif int(x) == 4:
            coa.execute_run()

        # 5) UPLOAD TO TRANSCEND
        elif int(x) == 5:
            coa.upload_to_transcend()
            break

    except ValueError:
        pass

