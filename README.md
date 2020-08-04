HELP on tdcsm (Teradata CSM Tools)

tdcoa: Teradata Consumption Analytics
------------------------------
This library will perform 4 descrete steps:
  (1) DOWNLOAD sql and csv from github respositories,
  (2) PREPARE sql locally, including variable substitutions and csv merging,
  (3) EXECUTE the prepared sql against customer site ids, and export any
      indicated data sets to csv, then finally
  (4) UPLOAD specifically indicated csv to Transcend, and call named stored procs
      to merge uploaded temp tables with final target tables.

Each step is designed to be autonomous and can be run independently, assuming
all dependencies are met.
This allows CSMs to download and prepare SQL first, insepct and make manual
changes to reflect the particular needs of their customer, all on their local PC.
Once happy with the script, they can move the process over and execute on a
customer-owned laptop, where the process should pick-up seemlessly. Continuing
on, the CSM could execute on the customer's TD system, export CSVs, and move
back to the CSM's laptop where results can be uploaded to Transcend.

Sample Usage:
1  from tdcoa import tdcoa
2  coa = tdcoa()

3  coa.download_files()
4  coa.prepare_sql()
5  coa.execute_run()
6  coa.upload_to_transcend()

what stuff does, by line (python 3.6+):
Line 1 = import the class.  Pretty standard python stuff.
Line 2 = instantiate the class.  This will also setup the local running environment
         in the same directory as your calling script (by default, but you can supply
         an alternate path as a parameter).  If they are missing, it will also create
         default files such as secrets.yaml and config.yaml, which are critical for
         the process.  It will NOT overwrite existing files.
         ** if you are running for the first time, it is recommended you run just
             line 1 & 2 first, and modify secrets.yaml and config.yaml as needed.
             This makes subsequent tests more interesting, as credentials will work.
Line 3 = download any missing files or sql from GitHub. URL and file inventory are
         both stored in the config.yaml.  the 0000.*.sql files are example files.
         While line 3 only needs to be run once, sql *will* be overwritten with
         newer content, so it is recommended you update periodically.
Line 4 = iterates all .coa.sql and .csv files in in the 'sql' folder and preapres
         the sql for later execution. This step includes several sustitution
         steps: from secrets.yaml (never printed in logs), from config.yaml in
         the substitution section, and from any embedded /*{{loop:myfile.csv}}
         command. In the last case, the csv is opened and the process generates
         one new sql per row in the file, substituting {{column_name}} with the
         value for that row. All of these sql scripts are written out to the
         'run' directory as defined in the config.yaml, awaiting the next step.
Line 5 = iterates thru all site id connection strings first, then thru  all sql
         files found in the 'run' directory *in alpha order*, and executes the sql.
         all substitutions are done in the previous step, so besides secrets.yaml
         replacements and certain runtime values like {{siteid}}, the sql will be
         exactly what is run.  There are three special commands, formatted in
         comments:
           /*{{save:MyFile.csv}}*/ = save the sql results to the named csv
           /*{{load:db.Tablename}}*/ = load the above csv to the Transcend table
           /*{{call:db.StoredProc}}*/ = call the stored proc after above load
         The first special command will execute during step 5, exporting data from
         each siteid.   Note that it is possible to use {{substitution}} in these
         commands, so /*{{save:{{siteid}}_export.csv}}*/ is valid, and will
         produce a different csv for each site id you run against.  Without this,
         the process might run against many siteids, but each would overwrite the
         same filename.
         The last two special commands, load and call, are intended to run against
         transcend, and so in step five are only written to 'upload_manifest.json'
         and saved in the same 'output' folder, awaiting the next and final step.
Line 6 = for each line in the 'upload_manifest.json', perform the requested load
         and subsequent stored proc call.  The intention is to upload csv files
         into Transcend Global Temporary Tables, then initiate a stored procedure
         to complete whatever cleansing, validation, and data movement is required
         to merge into the final target table.  This keeps most of the business
         logic as close to the data as possible.


DEBUGGING:

Missing tdcoa -- if you get an error stating you're missing tdcoa, then first, I'm
unclear on how you're reading this text.  That aside, open up a command prompt and type:
pip install tdcoa
if that gives you errors, then maybe this wasn't meant to be. Call Stephen Hilton.
