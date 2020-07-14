from tdcsm.tdcoa import tdcoa
import time, os, argparse

### Allow AppRoot and Secrets location to be passed in via named params
parser = argparse.ArgumentParser()
parser.add_argument('--approot', help='directory of the approot (working directory) in which to house all COA collateral')
parser.add_argument('--secrets', help='location to your secrets.yaml file RELATIVE TO APPROOT directory above')
args = vars(parser.parse_args())

print('args provided:\n%s' %str(args))
if (args['approot'] is None) or (not os.path.exists(args['approot'])):
    approot  =  os.getcwd()
else:
    approot = args['approot']

if (args['secrets'] is None) or (not os.path.exists(os.path.join(approot, args['secrets']))):
    secrets  =  ''
else:
    secrets = args['secrets']


print('approot (working) Directory: %s' %approot)
if secrets != '': print('secrets.yaml: %s\n(relative to approot)' %secrets)

print('='*30)
print('INSTANTIATING TDCOA, PLEASE WAIT...')
print('='*30)

if secrets == '':
    c = tdcoa(approot=approot)
else:
    c = tdcoa(approot=approot, secrets=secrets)

def printdict(dicttoprint={}, name='', lvl=0, secretdict={}):
    if lvl==0: print('\n%s\n%s\n%s' %('-'*30, str(name).upper(), '-'*30))
    for n,v in dicttoprint.items():
        if isinstance(v, dict):
            if lvl==0: print('')
            print(stripsecrets('%s%s:'  %(' '*lvl, n), secretdict))
            printdict(v, lvl=lvl+2, secretdict=secretdict)
        else:
            print(stripsecrets('%s%s%s'  %(' '*lvl, str(n+':').ljust(20), str(v)), secretdict))

def stripsecrets(msg='', secretdict={}):
    for nm, secret in secretdict.items():
        if secret in msg:
            msg = msg.replace(secret, '%s%s%s' % (secret[:1], '*' * (len(secret) - 2), secret[-1:]))
    return str(msg)



options = """
--------------------------------
COA version: %s
AppRoot: %s
--------------------------------

SELECT AN ACTION:
======================
1)  Download Files
2)  Prepare SQL
3)  Execute Run
4)  Upload to Transcend
5)  Make Customer Files
9)  Run R,1,2,3,4

--- other options (not cap sensitive)
R)  Reload Configs
C)  Print Config from memory
S)  Print Systems from memory
!)  Print Secrets from memory (careful!)
F)  Print FileSets from memory
K)  Toggle skip_dbs flag (for testing)
G)  Launch GUI
Q)  Quit
""" %(c.version, os.path.abspath(c.approot))

skip_dbs = c.skip_dbs

while True:
    try:
        x = str(input(options)[:1]).lower()
    except ValueError as e:
        print('\nInvalid Entry: %s\nTry again' %x)

    if   x == '1': c.download_files()
    elif x == '2': c.prepare_sql()
    elif x == '3': c.execute_run()
    elif x == '4': c.upload_to_transcend()
    elif x == '5': c.make_customer_files()
    elif x == '9':
        c.reload_config()
        c.skip_dbs = skip_dbs
        c.download_files()
        c.prepare_sql()
        c.execute_run()
        c.upload_to_transcend()
    elif x == 'r':
        c.reload_config()
        c.skip_dbs = skip_dbs
    elif x == 'c':
        printdict(c.substitutions, 'substitutions', 0, c.secrets)
        printdict(c.settings, 'settings', 0, c.secrets)
        printdict(c.transcend, 'transcend', 0, c.secrets)
    elif x == 's': printdict(c.systems, 'systems', 0, c.secrets)
    elif x == 'f': printdict(c.filesets, 'filesets', 0, c.secrets)
    elif x == '!': printdict(c.secrets, 'secrets')
    elif x == 'q': break
    elif x == 'g':
        from tdcsm.tdgui import coa
        g = coa(approot=approot, secrets=secrets)
    elif x == 'k':
        c.skip_dbs = not c.skip_dbs
        skip_dbs = c.skip_dbs
        if skip_dbs:
            msg = 'skip_dbs==True, DBS Connections PROHIBITED\nall processes will ONLY ENULATE dbs activity.'
        else:
            msg = 'skip_dbs==False, DBS connections ALLOWED\nall processes will connect to the dbs as normal.'
        print('\n\n%s\n%s\n%s' %('+='*16, msg,'=+'*16) )
        time.sleep(1)

    else:
        print('\nInvalid Entry: %s\nTry again' %x)
        time.sleep(1)
