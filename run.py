from tdcsm.tdcoa import tdcoa

coa = tdcoa(r'C:\Users\nd186026\Documents\tdcsm_demo')

options = """
Select a step:

1) Download Files
2) Prepare SQL
3) Execute Run
4) Upload to Transcend
"""

while True:
    x = input(options)

    try:
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

    except ValueError:
        pass

