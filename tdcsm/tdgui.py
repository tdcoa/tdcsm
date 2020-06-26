import subprocess, os


class coa():

    missing_file = False
    approot_path = '.'
    approot_var = ''
    config_var = ''
    secrets_var = ''
    systems_var = ''
    coa = ''
    pathdelim = ''

    debug = False

    version = "0.1.0.4"

    def __init__(self, approot=''):
        print('GUI for TDCOA started')
        if approot == '':
            self.approot_path = os.getcwd()
        else:
            self.approot_path = approot
        print('application root folder:\n\t%s' %self.approot_path)
        self.pathdelim = os.path.join('123','')[3:]
        self.run()

    def openconfig(self, filepath):
        pth = os.path.join(self.approot_var.get(), filepath)
        print('opening', '"%s"' %pth)
        subprocess.call(['open', pth])

    def __get_filename(self, file):
        if file =='filesets.yaml':
            filenames = ['1_download%s%s' %(self.pathdelim, file), '%s' %file]
        else:
            filenames = ['..%s!%s' %(self.pathdelim, file), '%s' %file]
        for filename in filenames:
            if os.path.exists(filename): return filename

        self.missing_file=True
        return filename

    def __get_lastrun_folder(self, lastrunfile='.last_run_output_path.txt'):
        valid_locations = ['.'
                          ,'4_output'
                          ,str(self.approot_var.get())]
        for loc in valid_locations:
            if os.path.exists(os.path.join(loc, lastrunfile)):
                lastrunfile = os.path.join(loc, lastrunfile)
                print('FOUND', lastrunfile)
                with open(lastrunfile,'r') as fh:
                    outputfo = str(fh.read())
                    print(outputfo)
                    for loc2 in valid_locations:
                        if os.path.exists(os.path.join(loc2, outputfo)):
                            return outputfo
                    print('NOT FOUND:', outputfo)
                    return 'None'
        print('NOT FOUND:', lastrunfile)
        return 'None'


    def __open_file_explorer(self,folderpath):
        try:
            os.startfile(folderpath)  # windows
            return True
        except:
            pass
        try:
            subprocess.Popen(["open", folderpath])  # mac
            return True
        except:
            pass
        try:
            subprocess.Popen(["xdg-open", folderpath])  # Linux
            return True
        except:
            pass
        return False


    def __toggle_all_chk(self):
        if  self.chk_download_files_var.get()      ==1 \
        and self.chk_prepare_sql_var.get()         ==1 \
        and self.chk_execute_run_var.get()         ==1 \
        and self.chk_upload_to_transcend_var.get() ==1:
            # if all boxes checked, then uncheck:
            self.chk_all_var.set(0)
            self.chk_download_files_var.set(0)
            self.chk_prepare_sql_var.set(0)
            self.chk_execute_run_var.set(0)
            self.chk_upload_to_transcend_var.set(0)
        else:   # otherwise, check all:
            self.chk_all_var.set(1)
            self.chk_download_files_var.set(1)
            self.chk_prepare_sql_var.set(1)
            self.chk_execute_run_var.set(1)
            self.chk_upload_to_transcend_var.set(1)

    def close(*args):
        print("GUI Closed")
        exit()


    def __run_coa(self, steps=''):
        print('GUI executing steps', steps)
        coa = self.coa
        if 'all' in steps: steps = 'rdpeu'
        if 'chk' in steps:
            steps = 'i'
            if self.chk_download_files_var.get():      steps = steps + 'd'
            if self.chk_prepare_sql_var.get():         steps = steps + 'p'
            if self.chk_execute_run_var.get():         steps = steps + 'e'
            if self.chk_upload_to_transcend_var.get(): steps = steps + 'u'

        if any(x in steps for x in ['i','r','d','p','e','u']):
            #instantiate if not already
            if coa == '' or 'i' in steps:
                if self.debug:
                    from tdcoa import tdcoa
                else:
                    from tdcsm.tdcoa import tdcoa
                coa = tdcoa(approot = self.approot_var.get(),
                            config  = self.config_var.get(),
                            systems = self.systems_var.get(),
                            secrets = self.secrets_var.get())
            if 'r' in steps: coa.reload_config()
            if 'd' in steps: coa.download_files()
            if 'p' in steps:
                coa.copy_download_to_sql()
                coa.prepare_sql()
            if 'e' in steps:
                # coa.execute_run() # <-- stupid langGO error  >:^(
                cmd = "from tdcsm.tdcoa import tdcoa; c=tdcoa(approot='%s', config='%s', systems='%s', secrets='%s'); c.execute_run()" %(self.approot_var.get(), self.config_var.get(), self.systems_var.get(), self.secrets_var.get())
                os.system('python -c "%s"' %cmd)
                self.output_var.set(self.__get_lastrun_folder())
            if 'u' in steps:
                # coa.upload_to_transcend() # <-- and again  >:^(
                outputpath = os.path.join(self.approot_var.get(), self.output_var.get())
                cmd = "from tdcsm.tdcoa import tdcoa; c=tdcoa(approot='%s', config='%s', systems='%s', secrets='%s'); c.upload_to_transcend('%s')" %(self.approot_var.get(), self.config_var.get(), self.systems_var.get(), self.secrets_var.get(), outputpath)
                os.system('python -c "%s"' %cmd)
        # update last output folder
        self.output_var.set(self.__get_lastrun_folder())



    def run(self):
        #import subprocess, os
        import tkinter as tk
        from tkinter import ttk

        print('GUI setup')
        root = tk.Tk()
        root.title("TD Consumption Analytics - GUI Helper v%s" %self.version)

        mainframe = ttk.Frame(root, padding="3 3 12 12")
        mainframe.grid(column=0, row=0, sticky=(tk.N, tk.W, tk.E, tk.S))
        root.columnconfigure(0, weight=1)
        root.rowconfigure(0, weight=1)
        col2_width = 40

        self.approot_var = tk.StringVar()
        self.approot_var.set(self.approot_path)
        self.config_var = tk.StringVar()
        self.config_var.set(self.__get_filename('config.yaml'))
        self.secrets_var = tk.StringVar()
        self.secrets_var.set(self.__get_filename('secrets.yaml'))
        self.systems_var = tk.StringVar()
        self.systems_var.set(self.__get_filename('source_systems.yaml'))
        self.filesets_var = tk.StringVar()
        self.filesets_var.set(self.__get_filename('filesets.yaml'))
        self.output_var = tk.StringVar()
        self.output_var.set(self.__get_lastrun_folder())


        if self.missing_file:
            self.__run_coa('i')
            self.missing_file=False

        self.chk_all_var                 = tk.IntVar(value=0)
        self.chk_download_files_var      = tk.IntVar(value=1)
        self.chk_prepare_sql_var         = tk.IntVar(value=1)
        self.chk_execute_run_var         = tk.IntVar(value=1)
        self.chk_upload_to_transcend_var = tk.IntVar(value=0)

        print('GUI define elements: paths')

        #row 1
        irow = 1
        ttk.Label(mainframe, text='TD Consumption Analytics - Execution Helper').grid(column=1,row=irow, columnspan=3, sticky=tk.W)

        #row: app root
        irow +=1
        ttk.Label(mainframe, text='App Root Directory:').grid(column=1, row=irow, sticky=tk.E)
        approot_entry = ttk.Entry(mainframe, width=col2_width, textvariable=self.approot_var).grid(column=2, row=irow, sticky=(tk.W, tk.E))
        #ttk.Button(mainframe, text="set", command=lambda:self.openconfig(config.get()), width=3).grid(column=3, row=irow, sticky=tk.W)

        #row: config
        irow +=1
        ttk.Label(mainframe, text='config file:').grid(column=1, row=irow, sticky=tk.E)
        config_entry = ttk.Entry(mainframe, width=col2_width, textvariable=self.config_var).grid(column=2, row=irow, sticky=(tk.W, tk.E))
        ttk.Button(mainframe, text="open", command=lambda:self.openconfig(self.config_var.get()), width=5).grid(column=3, row=irow, sticky=tk.W)

        #row: secrets
        irow +=1
        ttk.Label(mainframe, text='secrets file:').grid(column=1, row=irow, sticky=tk.E)
        secrets_entry = ttk.Entry(mainframe, width=col2_width, textvariable=self.secrets_var).grid(column=2, row=irow, sticky=(tk.W, tk.E))
        ttk.Button(mainframe, text="open", command=lambda:self.openconfig(self.secrets_var.get()), width=5).grid(column=3, row=irow, sticky=tk.W)

        #row: source systems
        irow +=1
        ttk.Label(mainframe, text='systems file:').grid(column=1, row=irow, sticky=tk.E)
        systems_entry = ttk.Entry(mainframe, width=col2_width, textvariable=self.systems_var).grid(column=2, row=irow, sticky=(tk.W, tk.E))
        ttk.Button(mainframe, text="open", command=lambda:self.openconfig(self.systems_var.get()), width=5).grid(column=3, row=irow, sticky=tk.W)

        #row: FileSets
        irow +=1
        ttk.Label(mainframe, text='file sets:').grid(column=1, row=irow, sticky=tk.E)
        systems_entry = ttk.Entry(mainframe, width=col2_width, textvariable=self.filesets_var).grid(column=2, row=irow, sticky=(tk.W, tk.E))
        ttk.Button(mainframe, text="open", command=lambda:self.openconfig(  self.filesets_var.get()), width=5).grid(column=3, row=irow, sticky=tk.W)


        #row: load configs
        irow +=1
        ttk.Button(mainframe, text="  Reload Config Files  ", command=lambda:self.__run_coa('r')).grid(column=2, row=irow, sticky=tk.W)

        #row: break
        irow +=2
        ttk.Label(mainframe, text=' ').grid(column=1, row=irow, sticky=tk.E)


        # -------------
        print('GUI define elements: buttons')

        #row: headeer
        irow +=2
        ttk.Checkbutton(mainframe, text="Check All", command=self.__toggle_all_chk, variable=self.chk_all_var).grid(column=1, row=irow, sticky=tk.E)
        ttk.Label(mainframe, text='                        Click below to run individual steps').grid(column=2, row=irow, sticky=(tk.W, tk.E))
        ttk.Label(mainframe, text='Internet Domain:').grid(column=3, row=irow, sticky=tk.W)

        #row: download_files
        irow +=1
        ttk.Button(mainframe, text="Download Files", command=lambda: self.__run_coa('d'), width=col2_width).grid(column=2, row=irow, sticky=tk.W)
        self.chk_download_files = ttk.Checkbutton(mainframe, text=" ", variable=self.chk_download_files_var).grid(column=1, row=irow, sticky=tk.E)
        ttk.Label(mainframe, text='Public').grid(column=3, row=irow, sticky=tk.W)

        #row: prepare_sql
        irow +=1
        ttk.Button(mainframe, text="Prepare_SQL", command=lambda:self.__run_coa('p'), width=col2_width).grid(column=2, row=irow, sticky=tk.W)
        self.chk_prepare_sql = ttk.Checkbutton(mainframe, text=" ", variable=self.chk_prepare_sql_var).grid(column=1, row=irow, sticky=tk.E)
        ttk.Label(mainframe, text='Public').grid(column=3, row=irow, sticky=tk.W)

        #row: execute_run
        irow +=1
        ttk.Button(mainframe, text="Execute_Run", command=lambda:self.__run_coa('e'), width=col2_width).grid(column=2, row=irow, sticky=tk.W)
        self.chk_execute_run = ttk.Checkbutton(mainframe, text=" ", variable=self.chk_execute_run_var).grid(column=1, row=irow, sticky=tk.E)
        ttk.Label(mainframe, text='Customer System').grid(column=3, row=irow, sticky=tk.W)

        #row: Output
        irow +=1
        ttk.Label(mainframe, text='Last Output Folder:').grid(column=1, row=irow, sticky=tk.E)
        output_entry = ttk.Entry(mainframe, width=int(col2_width-10), textvariable=self.output_var).grid(column=2, row=irow, sticky=tk.W)
        outputpath = os.path.join(self.approot_var.get(), self.output_var.get())
        ttk.Button(mainframe, text="open", command=lambda:self.__open_file_explorer(outputpath), width=5).grid(column=3, row=irow, sticky=tk.W)

        #row: upload_to_transcend
        irow +=1
        ttk.Button(mainframe, text="Upload_to_Transcend", command=lambda:self.__run_coa('u'), width=col2_width).grid(column=2, row=irow, sticky=tk.W)
        self.chk_upload_to_transcend = ttk.Checkbutton(mainframe, text=" ", variable=self.chk_upload_to_transcend_var).grid(column=1, row=irow, sticky=tk.E)
        ttk.Label(mainframe, text='Teradata VPN').grid(column=3, row=irow, sticky=tk.W)

        #row: run checked
        irow +=1
        ttk.Button(mainframe, text="Run Checked", command=lambda:self.__run_coa('chk'), width=10).grid(column=1, row=irow, sticky=tk.E)

        #row: run checked
        irow +=1
        ttk.Button(mainframe, text="Run All", command=lambda:self.__run_coa('all'), width=10).grid(column=1, row=irow, sticky=tk.E)

        #row: Close
        irow +=1
        ttk.Button(mainframe, text="Close", command=lambda:self.close(), width=10).grid(column=3, row=irow, sticky=tk.E)

        for child in mainframe.winfo_children(): child.grid_configure(padx=5, pady=5)

        root.bind('<Escape>', self.close)

        root.mainloop()



    def __subframe_control_and_open(self, name, cnt, open_path):
        root = self.guiroot
