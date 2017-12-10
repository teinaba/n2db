#! /usr/bin/env python

# import modules
# --------------
import gspread
from oauth2client.file import Storage
import numpy
import pandas
import httplib2

class n2gspread(object):
    gs = None

    def __init__(self):
        pass

    def authorize(self, json='credentials.json'):
        """
        Authorize with json file.
        -------------------------

        :param json: < The path to json file : str >
        :return:
        """
        credentials = Storage(json).get()
        self.gs = gspread.authorize(credentials)
        return

    def authorize2(self, credentials):
        self.gs = gspread.authorize(credentials=credentials)
        return

    def login(self):
        self.gs.login()
        return

    def refresh(self):
        """
        Refresh access_token.
        ---------------------

        :return:
        """
        http = httplib2.Http()
        self.gs.auth.refresh(http)
        self.gs.session.add_header('Authorization', "Bearer " + self.gs.auth.access_token)
        return


    def load(self, sheet, wks_num=1):
        """
        Get gspread.worksheet class instance from 'file-ID' and 'the number of worksheet'.
        ----------------------------------------------------------------------------------

        :param sheet: < file ID : str >
        :param wks_num: < the number of worksheet : int >
        :return: < wks : gspread.worksheet class instance >
        """
        self.login()
        wks = self.gs.open_by_key(key=sheet).get_worksheet(index=wks_num-1)
        return wks

    def append_rows(self, wks, data, nrow):
        ref_row = wks.row_count
        self.login()
        wks.add_rows(nrow)
        data_width = len(data[0])

        # check data width --
        self.login()
        if wks.col_count < data_width:
            self.login()
            wks.resize(cols=data_width)
            pass

        # append  data --
        self.login()
        cell_list = wks.range(ref_row+1, 1, ref_row+nrow, data_width)
        for row in range(nrow):
            for col, value in enumerate(data[row], start=0):
                num = data_width * row + col
                cell = cell_list[num]
                cell.value = value
                cell_list[num] = cell
        self.login()
        wks.update_cells(cell_list=cell_list)
        return

    def append_rows_old(self, wks, data, nrow):
        """
        Append rows to end of the worksheet.
        ------------------------------------

        :param wks:  < gspread.worksheet class instance >
        :param data: < monitor data : array or list >
        :param nrow: < number of rows : int >
        :return:
        """
        for row in range(nrow):
            wks.append_row(data[row])
        return

    def append(self, sheet, data, wks_num=1):
        """
        The Master Function of appending data.
        --------------------------------------

        :param sheet: < File ID : str >
        :param wks_num: < the number of worksheet : int >
        :param data: < monitor data : array or list >
        :return:
        """
        nrow = len(data)
        wks = self.load(sheet=sheet, wks_num=wks_num)
        blank = self.blank_check(wks)
        if blank:
            dcsv = self.array_to_csvstr(data=data)
            self.login()
            self.gs.import_csv(file_id=sheet, data=dcsv)
            # refresh token. without this, RequestError is caused. --
            wks = self.load(sheet=sheet, wks_num=wks_num)
            self.login()
            wks.resize(rows=nrow)
        else:
            self.append_rows(wks=wks, data=data, nrow=nrow)
        return

    def size(self, wks):
        row = wks.row_count()
        col = wks.col_count()
        size = [row, col]
        return size

    def blank_check(self, wks):
        """
        Check whether the worksheet is blank file or not.
        -------------------------------------------------

        :param wks: < gspread.worksheet class instance >
        :return blank: < Blank file or not : True or False >
        """
        self.login()
        cell_A1 = wks.acell('A1')
        if cell_A1.value == '': blank = True
        else: blank = False
        return blank

    def array_to_csvstr(self, data):
        """
        Convert data to csv-string format. For gs.import_csv method.
        ------------------------------------------------------------

        :param data: < monitor data : array or list >
        :return csv_str: < data (csv-string format) : str >
        :return nrow (ncol): < A number of rows(cols) of data : int >
        """
        d = numpy.array(data)
        nrow, ncol = numpy.shape(d)
        csv_str = ''
        for row in range(nrow):
            ikuta = ','.join(map(str, d[row, :]))
            csv_str += (ikuta + '\n')
        return csv_str

    def get_all_values(self, wks):
        """
        Get all data in worksheet and return data as string list.
        ---------------------------------------------------------

        :param wks: < gspread.worksheet class instance >
        :return strdata: < all data in worksheet : Type Str list>

        Example data: strdata = [['2017-12-06 2:11:00', '2', '3', '4', '5', '6'],
                                 ['2017-12-06 3:11:00', '2', '3', '4', '5', '6'],
                                 ['2017-12-06 2:11:00', '2', '3', '4', '5', '6']]
        """
        strdata = wks.get_all_values()
        return strdata



def Authorize(json='credentials.json'):
    gs = n2gspread()
    gs.authorize(json=json)
    return gs

def Authorize2(credentials):
    gs = n2gspread()
    gs.authorize2(credentials=credentials)
    return gs


# History
# -------
# 2017/11/30 written by T.Inaba
# 2017/12/04 T.Inaba: ver.0.1.1
# 2017/12/05 T.Inaba: test operation.
# 2017/12/06 T.Inaba: create get_all_value method. modified keyword of authorize.
