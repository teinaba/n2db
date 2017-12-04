#! /usr/bin/env python

# import modules
# --------------
from . import n2gdrive
from . import n2gspread
import os
import numpy
import pandas
import configparser
from datetime import datetime



class n2db(object):
    gs = None
    drive = None
    abspath = None
    cnfpath = None

    def __init__(self):
        self.set_abspath()
        self.set_cnfpath()
        pass

    def authorize(self, json):
        self.drive = n2gdrive.Authorize()
        self.gs = n2gspread.Authorize(json=json)
        return

    def set_abspath(self):
        abspath = os.path.dirname(os.path.abspath(__name__))
        self.abspath = abspath
        return

    def set_cnfpath(self):
        joined = os.path.join(self.abspath, 'config')
        self.cnfpath = os.path.normpath(joined)
        return

    def INSERT(self, table, timestamp, data, wks_num=1):
        # data type check --
        if type(data) == numpy.ndarray: pass
        elif type(data) == list: data = numpy.array(data)

        # get date list --
        index = data[:, 0]
        datelist = IndexManager().get_datelist(index=index)

        # upload data by date --
        s = pandas.Series(data=data, index=index)
        for date in datelist:
            self.insert(table=table, date=date, data=s[date], wks_num=wks_num)
        pass

    def SELECT(self, table):
        return

    def CREATE_PROJECT(self, pjt):
        # set path and create project directory in drive --
        dbcnf = os.path.join(self.cnfpath, 'n2db.cnf')
        rootID = ConfigManager().get_value(file=dbcnf, section='Info', key='rootID')
        pjtdir = self.drive.create_folder(title=pjt, parents=rootID)

        # add Project name to n2db.cnf --
        num_of_pjts = ConfigManager().get_num_of_pjts(dbcnf=dbcnf)
        key = 'project{}'.format(num_of_pjts+1)
        ConfigManager().write_value(file=dbcnf, section='Project', key=key, value=pjt)

        # create Project info in n2db.cnf --
        dic = {'id': pjtdir['id']}
        ConfigManager().write_section(file=dbcnf, section=pjt, dic=dic)

        # create Project config file --
        info = {'cnfid': '', 'project': pjt, 'pjtid': pjtdir['id']}
        # -- create in local
        pjtcnf = ConfigManager().create_pjt_cnf(cnfpath=self.cnfpath, pjt=pjt, info=info)
        # -- upload to drive
        d_pjtcnf = self.drive.create_file(title=pjt+'.cnf', mimeType='txt', parents=rootID, content=pjtcnf)
        ConfigManager().write_value(file=pjtcnf, section='Info', key='cnfid', value=d_pjtcnf['id'])
        return

    def CREATE_TABLE(self, pjt, table, description=''):
        # set path and create table directory in drive --
        pjtcnf = os.path.join(self.cnfpath, pjt+'.cnf')
        pjtID = self.get_pjtID(pjt=pjt)
        tbldir = self.drive.create_folder(title=table, parents=pjtID)

        # add Table name to project.cnf --
        num_of_tables = ConfigManager().get_num_of_tables(pjtcnf=pjtcnf)
        key = 'table{}'.format(num_of_tables+1)
        ConfigManager.write_value(file=pjtcnf, section='Table', key=key, value=table)

        # create Table info in project config file --
        dic = {'id': tbldir['id'], 'description': description}
        ConfigManager().write_section(file=pjtcnf, section=table, dic=dic)

        # create Table config file --
        info = {'cnfid': '', 'project': pjt, 'table': table}
        # -- create in local
        tblcnf = ConfigManager().create_table_cnf(cnfpath=self.cnfpath, pjt=pjt, info=info, table=table)
        # -- upload to drive
        d_tblcnf = self.drive.create_file(title=table+'.cnf', mimeType='txt', parents=pjtID, content=tblcnf)
        ConfigManager().write_value(file=tblcnf, section='Info', key='cnfid', value=d_tblcnf['id'])
        return

    def CREATE_SHEET(self, table, monthid):
        # Search parent file ID
        # ---------------------


        # Create sheet
        # ------------
        self.drive.create_file()

    def SHOW_CREATE_TABLE(self, table):
        pass

    def UPDATE(self):
        pass

    def DELETE(self):
        pass

    def DROP_TABLE(self, table):
        pass

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
        sheetname = '{}-{}'.format(table, t.strftime('%Y%m%d'))

        # Check sheet existence and create sheet
        # --------------------------------------
        # check sheet --
        if not self.drive.exists(title=sheetname):
            # check month dir --
            monthID = self.get_monthID(pjt=pjt, table=table, year=t.year, month=t.month)
            if not monthID:
                # check year dir --
                yearID = self.get_yearID(pjt=pjt, table=table, year=t.year)
                if not yearID:
                    self.create_yeardir(pjt=pjt, table=table, year=t.year)
                    pass
                self.create_monthdir(pjt=pjt, table=table, year=t.year, month=t.month)
                pass
            sheet = self.create_sheet(sheetname=sheetname, parents=monthID)
            pass
        else: sheet = self.drive.search(msg="title = '{}' and trashed=False".format(sheetname))

        # append data
        # -----------
        self.gs.append(sheet=sheet['id'], wks_num=wks_num, data=data)
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
        ConfigManager.write_value(file=file, section='Month', key='{}-{}'.format(year, month),
                                  value=monthdir['id'], save=True)
        return monthdir['id']

    def create_yeardir(self, pjt, table, year):
        tableID = self.get_tableID(pjt=pjt, table=table)
        yeardir = self.drive.create_folder(title=year, parents=tableID)
        # add file-ID to config file --
        file = os.path.join(self.cnfpath, pjt, table+'.cnf')
        ConfigManager.write_value(file=file, section='Year', key=year, value=yeardir['id'], save=True)
        return yeardir['id']

    def get_sheetID(self, table, timestamp):
        msg = '"title={}-{}" and trashed=False'.format(table, timestamp)
        flist = self.drive.search(msg=msg)


    def get_monthID(self, pjt, table, year, month):
        file = os.path.join(self.cnfpath, pjt, table+'.cnf')
        # try to get monthID
        # ------------------
        try:
            monthID = ConfigManager.get_value(file=file, section='Month', key='{}-{}'.format(year, month))
        except:
            monthID = None
        return monthID

    def get_yearID(self, pjt, table, year):
        file = os.path.join(self.cnfpath, pjt, table+'.cnf')
        # try to get yearID
        # -----------------
        try:
            yearID = ConfigManager.get_value(file=file, section='Year', key='{}'.format(year))
        except:
            yearID = None
        return yearID

    def get_tableID(self, pjt, table):
        file = os.path.join(self.cnfpath, pjt+'.cnf')
        # try to get table ID
        # -------------------
        try:
            tableID = ConfigManager.get_value(file=file, section='{}', key='{id}'.format(table))
        except:
            tableID = None
        return tableID

    def get_pjtID(self, pjt):
        file = os.path.join(self.cnfpath, 'n2db.cnf')
        # try to get pjt ID
        # -----------------
        try:
            pjtID = ConfigManager.get_value(file=file, section=pjt, key='id')
        except:
            print('================================'
                  ' No such a Project : {}'
                  '================================'.format(pjt))
            pjtID = None
        return pjtID

    def create_sheet(self, sheetname, parents):
        sheet = self.drive.create_file(title=sheetname, mimeType='sheet', parents=parents)
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
        return datelist

    def split_data(self, data):
        pass

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

    def create_pjt_cnf(self, cnfpath, pjt, info, table={}):
        pjtdir = os.path.join(cnfpath, pjt)
        if not os.path.isdir(pjtdir): os.mkdir(pjtdir)
        file = pjtdir + '{}.cnf'.format(pjt)
        config = configparser.ConfigParser()

        # write contents --
        config['Info'] = info
        config['Table'] = table

        # save file --
        with open(file, 'w') as configfile:
            config.write(configfile)
        return file

    def create_table_cnf(self, cnfpath, pjt, table, info, year={}, month={}, column={}):
        tbldir = os.path.join(cnfpath, pjt, table)
        if not os.path.isdir(tbldir): os.mkdir(tbldir)
        file = tbldir + '{}.cnf'.format(pjt)
        config = configparser.ConfigParser()

        # write contents --
        config['Info'] = info
        config['Year'] = year
        config['Month'] = month
        config['Column'] = column

        # save file --
        with open(file, 'w') as configfile:
            config.write(configfile)
        return file


# History
# -------
# 2017/12/04 written by T.Inaba
