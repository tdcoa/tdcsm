
class tdviz():
    import os
    import numpy as np
    import pandas as pd



    def get_siteid(df, default='unknown'):
        """does case insensitive match on column names for SiteID.
        If found, will return string from the first row.
        If not found, will return default value."""
        rtn=''
        print('looking for SiteID in dataframe...')
        for col in df.columns:
            if col.lower().replace('_','')=='siteid':
                rtn = df[col].iloc[0]
                break
        if rtn=='':
            rtn=default
            print('did not find, using default',default)
        else:
            print('found SiteID in dataframe', rtn)
        return rtn



    def cleanse_df(df, indexlist=[], datalist=[], inplace=True):
        """basic cleaning of dataframe, including:
        - removing all unnamed columns
        - changing all 'object' datatypes to 'string'
        - optionally changing index, if specified in indexlist
        - optionally sort by indexlist, if provided
        - optionally removing all columns not found in indexlist or datalist
            (both must be specified)"""
        print('cleansing dataset...')
        if inplace:
            rtn = df
        else:
            rtn = df.copy(deep=True)

        idx=[]
        dat=[]
        keep=[]
        delete=[]
        for itm in indexlist: idx.append(itm.lower())
        for itm in datalist:  dat.append(itm.lower())
        keep.extend(idx)
        keep.extend(dat)

        msg=''
        logdate=''
        loghour=''
        for col in rtn.columns:
            msg='no change for %s' %col
            if rtn[col].dtypes == 'object':
                rtn[col] = rtn[col].astype(str)
                msg='changed datatype to string: %s' %col
            if col.lower() in idx:
                idx.remove(col.lower())
                idx.append(col)
            if col.lower()=='loghour':
                rtn[col] = rtn[col].astype(int).astype(str).apply(lambda x: x.zfill(2))
                loghour=col
                msg='zero-fill column: %s' %col
            if col.lower()=='logdate':
                logdate=col
            if col[:8] == 'Unnamed:' or (len(dat)!=0 and col.lower() not in keep):
                msg='dropped column: %s' %col
                delete.append(col)
            print(msg)

        if len(idx)!=0:
            rtn.set_index(idx, inplace=True).sort_values(by=idx)

        if logdate!='' and loghour!='':
            rtn.insert(0,'LogTime', df[logdate].astype(str) + ' h' + df[loghour])

        for col in delete:
            rtn.drop(columns=[col], inplace=True)

        return rtn
