class Logger:

    def __init__(self):

        # log settings
        self.logs = []
        self.logspace = 30
        self.bufferlogs = True
        self.configyaml = ''
        self.printlog = True
        self.show_full_sql = False
        self.logpath = ''
        self.secrets = {}

    def log(self, msgleft='', msgright='', header=False, error=False, warning=False):
        delim = '' if msgright == '' else ':'
        msg = '%s%s' % (str(msgleft + delim).ljust(self.logspace), msgright)

        if error:
            msg = '%s\nERROR:\n%s\n%s' % ('-=' * self.logspace, msg, '-=' * self.logspace)
        if warning:
            msg = '%s\nWARNING:\n%s\n%s' % ('-' * self.logspace, msg, '-' * self.logspace)

        # prevent secrets from appearing in log:
        for nm, secret in self.secrets.items():
            if secret in msg:
                msg = msg.replace(secret, '%s%s%s' % (secret[:1], '*' * (len(secret) - 2), secret[-1:]))

        if header:
            msg = '\n\n%s\n%s\n%s' % ('=' * 40, msg.upper(), '-' * 40)

        if self.bufferlogs:
            self.logs.append(msg)
            if self.printlog:
                print(msg)

        else:  # no buffer
            if len(self.logs) == 0:
                if self.printlog:
                    print(msg)
                # self.__writelog(msg)
                with open(self.logpath, 'a') as logfile:
                    logfile.write(msg + '\n')

            else:
                if self.printlog:
                    print(msg)
                for log in self.logs:
                    # self.__writelog(log)
                    with open(self.logpath, 'a') as logfile:
                        logfile.write(log + '\n')
                    self.logs = []
                # self.__writelog(msg)
                with open(self.logpath, 'a') as logfile:
                    logfile.write(msg + '\n')

    @property
    def secrets(self):
        return self._secrets

    @secrets.setter
    def secrets(self, val):
        self._secrets = val

    @property
    def bufferlogs(self):
        return self._bufferlogs

    @bufferlogs.setter
    def bufferlogs(self, val):
        self._bufferlogs = val
