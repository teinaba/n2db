#! /usr/bin/env python

# import modules
# --------------
import n2gdrive
import n2gspread
import os
import time
import numpy
import pandas
import configparser
from datetime import datetime


class n2db(object):
    gs = None
    drive = None
    abspath = None
    cnfpath = None
    jsonfile = None
    settingsfile = None

    def __init__(self):
        self.set_abspath()
        self.set_cnfpath()
        pass

    def authorize(self, json='credentials.json', settings='settings.yaml'):
        """
        Set Authorized OAuth2.0 account.
        --------------------------------

        :param json: < json files to authorize : str : ex.) 'credentials.json'>
        :return:
        """
        # set files --
        self.jsonfile = os.path.join(self.abspath, json)
        self.settingsfile = os.path.join(self.abspath, settings)
        # authorize --
        self.gs = n2gspread.Authorize(json=self.jsonfile)
        self.drive = n2gdrive.Authorize(settings=self.settingsfile)
        return

    def refresh(self):
        """
        Refresh access token.
        ---------------------

        :return:
        """
        self.drive = n2gdrive.Authorize(settings=self.settingsfile)
        self.gs.refresh()
        return

    def set_abspath(self):
        abspath = os.path.dirname(os.path.abspath(__file__))
        self.abspath = abspath
        return

    def set_cnfpath(self):
        joined = os.path.join(self.abspath, 'config')
        self.cnfpath = os.path.normpath(joined)
        return

    def INSERT(self, pjt, table, data, wks_num=1):
        # data type check --
        if type(data) == numpy.ndarray:
            darr = data
            dlist = data.tolist()
            pass
        elif type(data) == list:
            darr = numpy.array(data)
            dlist = data
        else: raise TypeError('\n\nExpected Type : "numpy.ndarray" or "list"\n')

        # get date list --
        index = darr[:, 0]
        datelist, dtindex = IndexManager().get_datelist(index=index)

        # upload data by date --
        s = pandas.Series(data=dlist, index=dtindex)
        for date in datelist:
            d = list(s[date])  # pandas.Series is not recognized as list(array).
            self.insert(pjt=pjt, table=table, date=date, data=d, wks_num=wks_num)
        return

    def insert(self, pjt, table, date, data, wks_num=1):
        """
        Insert data to the sheet 'Table-yymmdd' according to timestamp.
        It is not taken into account that if there are multiple dates in data-index.
        ---------------------------------------------------------------------------

        :param table: < Table name : str >
        :param date: < timestamp of data : str : fmt = 'YYYY-mm-dd' >
        :param data: < Monitor data : array (list??) >
        :param wks_num: The number of worksheet to insert data.
        :return:
        """
        t = datetime.strptime(date, '%Y-%m-%d')
        year = str(t.year)
        month = str(t.month)
        sheettitle = '{}-{}'.format(table, t.strftime('%Y%m%d'))

        # check sheet existence and create sheet
        # --------------------------------------
        # check sheet --
        if self.drive.exists(title=sheettitle) is False:
            # check month dir --
            monthID = self.get_monthID(pjt=pjt, table=table, year=year, month=month)
            if monthID is None:
                # check year dir --
                yearID = self.get_yearID(pjt=pjt, table=table, year=year)
                if yearID is None:
                    self.create_yeardir(pjt=pjt, table=table, year=year)
                monthID = self.create_monthdir(pjt=pjt, table=table, year=year, month=month)
            sheet = self.create_sheet(sheettitle=sheettitle, parents=monthID)
        else:
            sheet = self.drive.search(msg="title = '{}' and trashed=False".format(sheettitle))[0]
            # TODO!! If Multiple sheet ??

        # append data
        # -----------
        self.gs.append(sheet=sheet['id'], wks_num=wks_num, data=data)
        return

    def SELECT(self, table, start, end):
        # get date list as DatetimeIndex --
        datelist = pandas.date_range(start=start, end=end, freq='D')

        # get data by date --
        data = []
        for t in datelist:
            date = t.strftime('%Y%m%d')
            daydata = self.select(table=table, date=date)
            data.extend(daydata)
        return data

    def select(self, table, date, wks_num=1):
        sheettitle = '{}-{}'.format(table, date)
        sheet = self.drive.search(msg="title = '{}' and trashed=False".format(sheettitle))[0]

        # if specified Spreadsheet doesn't exist.
        # ---------------------------------------
        if not sheet:
            print('\n'
                  ' No SpreadSheet: {}\n'.format(sheettitle))
            return []
        # ---------------------------------------

        wks = self.gs.load(sheet=sheet['id'], wks_num=wks_num)
        data = self.gs.get_all_values(wks=wks)
        return data

    def CREATE_PROJECT(self, pjt):
        # set path --
        dbcnf = os.path.join(self.cnfpath, 'n2db.cnf')
        rootID = ConfigManager().get_value(file=dbcnf, section='Info', key='rootid')
        # create project directory in drive
        pjtdir = self.drive.create_folder(title=pjt, parents=rootID)

        # add Project name to n2db.cnf --
        num_of_pjts = ConfigManager().get_num_of_pjts(dbcnf=dbcnf)
        key = 'project{}'.format(num_of_pjts+1)
        ConfigManager().write_value(file=dbcnf, section='Project', key=key, value=pjt)

        # create Project info in n2db.cnf --
        dic = {'id': pjtdir['id']}
        ConfigManager().write_section(file=dbcnf, section=pjt, dic=dic)
        dbcnfID = ConfigManager().get_value(file=dbcnf, section='Info', key='cnfid')
        # -- upload
        self.drive.upload_local_file(filepath=dbcnf, id=dbcnfID)

        # create Project config file --
        info = {'cnfid': '', 'project': pjt, 'pjtid': pjtdir['id']}
        # -- create in local
        pjtcnf = ConfigManager().create_pjt_cnf(cnfpath=self.cnfpath, pjt=pjt, info=info)
        # -- upload to drive
        d_pjtcnf = self.drive.create_file(title=pjt+'.cnf', mimeType='txt', parents=rootID, content=pjtcnf)
        # -- add config-ID to local config
        ConfigManager().write_value(file=pjtcnf, section='Info', key='cnfid', value=d_pjtcnf['id'])
        # -- re-upload
        self.drive.upload_local_file(filepath=pjtcnf, id=d_pjtcnf['id'])
        return d_pjtcnf

    def CREATE_TABLE(self, pjt, table, description=''):
        pjtcnf = os.path.normpath(os.path.join(self.cnfpath, pjt, pjt+'.cnf'))

        # add Table name to project.cnf --
        num_of_tables = ConfigManager().get_num_of_tables(pjtcnf=pjtcnf)
        key = 'table{}'.format(num_of_tables+1)
        ConfigManager().write_value(file=pjtcnf, section='Table', key=key, value=table)

        # create table directory --
        pjtID = self.get_pjtID(pjt=pjt)
        tbldir = self.drive.create_folder(title=table, parents=pjtID)

        # create Table info in project config file --
        dic = {'id': tbldir['id'], 'description': description}
        ConfigManager().write_section(file=pjtcnf, section=table, dic=dic)
        pjtcnfID = ConfigManager().get_value(file=pjtcnf, section='Info', key='cnfid')
        # -- upload
        self.drive.upload_local_file(filepath=pjtcnf, id=pjtcnfID)

        # create Table config file --
        info = {'cnfid': '', 'project': pjt, 'table': table}
        # -- create in local
        tblcnf = ConfigManager().create_table_cnf(cnfpath=self.cnfpath, pjt=pjt, info=info, table=table)
        # -- upload to drive
        d_tblcnf = self.drive.create_file(title=table+'.cnf', mimeType='txt', parents=pjtID, content=tblcnf)
        # -- add config-ID to local config
        ConfigManager().write_value(file=tblcnf, section='Info', key='cnfid', value=d_tblcnf['id'])
        # -- re-upload
        self.drive.upload_local_file(filepath=tblcnf, id=d_tblcnf['id'])
        return d_tblcnf

    def SHOW_CREATE_TABLE(self, table):
        pass

    def UPDATE(self):
        pass

    def DELETE(self):
        pass

    def DROP_TABLE(self, table):
        pass

    def update_cnf(self, file):
        cnfid = ConfigManager.get_value(file=file, section='Info', key='cnfid')
        self.drive.upload_local_file(id=cnfid, filepath=file)
        return

    def create_monthdir(self, pjt, table, year, month):
        yearID = self.get_yearID(pjt=pjt, table=table, year=year)
        if yearID is None:
            yearID = self.create_yeardir()
        monthdir = self.drive.create_folder(title=month, parents=yearID)
        # add file-ID to config file --
        file = os.path.join(self.cnfpath, pjt, table+'.cnf')
        tblcnfID = ConfigManager().get_value(file=file, section='Info', key='cnfid')
        ConfigManager().write_value(file=file, section='Month', key='{}-{}'.format(year, month),
                                    value=monthdir['id'], save=True)
        # -- upload
        self.drive.upload_local_file(filepath=file, id=tblcnfID)
        return monthdir['id']

    def create_yeardir(self, pjt, table, year):
        tableID = self.get_tableID(pjt=pjt, table=table)
        yeardir = self.drive.create_folder(title=year, parents=tableID)
        # add file-ID to config file --
        file = os.path.join(self.cnfpath, pjt, table+'.cnf')
        tblcnfID = ConfigManager().get_value(file=file, section='Info', key='cnfid')
        ConfigManager().write_value(file=file, section='Year', key=year, value=yeardir['id'], save=True)
        # -- upload
        self.drive.upload_local_file(filepath=file, id=tblcnfID)
        return yeardir['id']

    def get_sheetID(self, table, timestamp):
        msg = '"title={}-{}" and trashed=False'.format(table, timestamp)
        flist = self.drive.search(msg=msg)

    def get_monthID(self, pjt, table, year, month):
        file = os.path.join(self.cnfpath, pjt, table+'.cnf')
        # try to get monthID
        # ------------------
        try:
            monthID = ConfigManager().get_value(file=file, section='Month', key='{}-{}'.format(year, month))
        except:
            monthID = None
        return monthID

    def get_yearID(self, pjt, table, year):
        file = os.path.join(self.cnfpath, pjt, table+'.cnf')
        # try to get yearID
        # -----------------
        try:
            yearID = ConfigManager().get_value(file=file, section='Year', key='{}'.format(year))
        except:
            yearID = None
        return yearID

    def get_tableID(self, pjt, table):
        file = os.path.normpath(os.path.join(self.cnfpath, pjt, pjt+'.cnf'))
        # try to get table ID
        # -------------------
        try:
            tableID = ConfigManager().get_value(file=file, section='{}'.format(table), key='id')
        except:
            tableID = None
        return tableID

    def get_pjtID(self, pjt):
        file = os.path.join(self.cnfpath, 'n2db.cnf')
        # try to get pjt ID
        # -----------------
        try:
            pjtID = ConfigManager().get_value(file=file, section=pjt, key='id')
        except:
            print('================================\n'
                  ' No such a Project : {}         \n'
                  '================================\n'.format(pjt))
            pjtID = None
        return pjtID

    def create_sheet(self, sheettitle, parents):
        sheet = self.drive.create_file(title=sheettitle, mimeType='sheet', parents=parents)
        return sheet


class IndexManager(object):
    timestamp_fmt = '%Y-%m-%d %H:%M:%S'

    def __init__(self):
        pass

    def get_datelist(self, index):
        # convert timestamp to DateTimeIndex --
        dtindex = pandas.to_datetime(index)
        # list up dates --
        datelist = []
        for ts in dtindex:
            date = ts.strftime('%Y-%m-%d')
            if not date in datelist: datelist.append(date)
        return datelist, dtindex

    def timestamp_to_date(self, timestamp):
        t = datetime.strptime(timestamp, self.timestamp_fmt)
        date = datetime.strftime(t, '%Y%m%d')
        return date, t


class ConfigManager(object):

    def __init__(self):
        pass

    def read(self, file):
        config = configparser.ConfigParser()
        config.read(file)
        return config

    def get_value(self, file, section, key):
        config = self.read(file=file)
        value = config.get(section=section, option=key)
        return value

    def write_value(self, file, section, key, value, save=True):
        config = self.read(file=file)
        config.set(section=section, option=key, value=value)

        # save file
        # ---------
        if save is True:
            with open(file, 'w') as configfile:
                config.write(configfile)
        return

    def write_section(self, file, section, dic, save=True):
        config = self.read(file=file)
        config[section] = dic

        # save file
        # ---------
        if save is True:
            with open(file, 'w') as configfile:
                config.write(configfile)
        return

    def get_num_of_pjts(self, dbcnf):
        config = self.read(file=dbcnf)
        keys = config.options(section='Project')
        return len(keys)

    def get_num_of_tables(self, pjtcnf):
        config = self.read(file=pjtcnf)
        keys = config.options(section='Table')
        return len(keys)

    def create_db_cnf(self, cnfpath, rootID):
        dbcnf = os.path.normpath(os.path.join(cnfpath, 'n2db.cnf'))
        config = configparser.ConfigParser()
        timestamp = time.strftime('%Y-%m-%d')

        # contents --
        info = {'cnfid': '',
                'rootid': rootID,
                'created': timestamp}
        # write contents --
        config['Info'] = info
        config['Project'] = {}
        # save file --
        with open(dbcnf, 'w') as configfile:
            config.write(configfile)
        return dbcnf

    def create_pjt_cnf(self, cnfpath, pjt, info):
        pjtpath = os.path.normpath(os.path.join(cnfpath, pjt))
        # create Project directory --
        try:
            os.mkdir(pjtpath)
        except FileExistsError:
            pass
        # create config file --
        pjtcnf = os.path.normpath(os.path.join(pjtpath, '{}.cnf'.format(pjt)))
        config = configparser.ConfigParser()
        # write contents --
        config['Info'] = info
        config['Table'] = {}
        # save file --
        with open(pjtcnf, 'w') as configfile:
            config.write(configfile)
        return pjtcnf

    def create_table_cnf(self, cnfpath, pjt, table, info, year={}, month={}, column={}):
        tblcnf = os.path.normpath(os.path.join(cnfpath, pjt, '{}.cnf'.format(table)))
        config = configparser.ConfigParser()

        # write contents --
        config['Info'] = info
        config['Year'] = year
        config['Month'] = month
        config['Column'] = column

        # save file --
        with open(tblcnf, 'w') as configfile:
            config.write(configfile)
        return tblcnf

# History
# -------
# 2017/12/04 written by T.Inaba
# 2017/12/05 T.Inaba: test operation.
# 2017/12/06 T.Inaba: create SELECT method. modified set_abspath and authorize.
