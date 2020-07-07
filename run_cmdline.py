from tdcsm.tdcoa import tdcoa
import time

approot = '../coa/1Dev'
secrets  = '../!secrets.yaml'
print('Dev Folder: %s' %approot)
print('secrets.yaml: %s\n(relative to Dev Folder)' %secrets)

print('='*30)
print('INSTANTIATING TDCOA, PLEASE WAIT...')
print('='*30)

c = tdcoa(approot=approot, secrets=secrets)


def printdict(dicttoprint={}, name='', lvl=0):
    if lvl==0: print('\n%s\n%s\n%s' %('-'*30, str(name).upper(), '-'*30))
    for n,v in dicttoprint.items():
        if isinstance(v, dict):
            if lvl==0: print('')
            print('%s%s:'  %(' '*lvl, n))
            printdict(v, lvl=lvl+2)
        else:
            print('%s%s%s'  %(' '*lvl, str(n+':').ljust(20), str(v)) )



options = """
COA version: %s

SELECT AN ACTION:
======================
1)  Download Files
2)  Prepare SQL
3)  Execute Run
4)  Upload to Transcend

--- other options ---
(not cap sensitive)
R)  Reload Configs
C)  Print Config from memory
S)  Print Systems from memory
F)  Print FileSets from memory
G)  Launch GUI
Q)  Quit
""" %c.version

while True:
    try:
        x = str(input(options)[:1]).lower()
    except ValueError as e:
        print('\nInvalid Entry: %s\nTry again' %x)

    if x == '1': c.download_files()
    elif x == '2': c.prepare_sql()
    elif x == '3': c.execute_run()
    elif x == '4': c.upload_to_transcend()
    elif x == 'r': c.reload_config()
    elif x == 'c':
        printdict(c.substitutions, 'substitutions')
        printdict(c.settings, 'settings')
        printdict(c.transcend, 'transcend')
    elif x == 's': printdict(c.systems, 'systems')
    elif x == 'f': printdict(c.filesets, 'filesets')
    elif x == 'q': break
    elif x == 'g':
        from tdcsm.tdgui import coa
        g = coa(approot=approot, secrets=secrets)

    else:
        print('\nInvalid Entry: %s\nTry again' %x)
        time.sleep(1)
