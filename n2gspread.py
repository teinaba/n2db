#! /usr/bin/env python

# import modules
# --------------
import gspread
from oauth2client.file import Storage
import numpy


class n2gspread(object):
    gs = None

    def __init__(self, json):
        self.authorize(json=json)
        pass

    def authorize(self, json=None):
        """
        Authorize with json file.
        -------------------------

        :param json: < The path to json file : str >
        :return:
        """
        credentials = Storage(json).get()
        self.gs = gspread.authorize(credentials)
        return

    def load(self, sheet, wks_num):
        """
        Get gspread.worksheet class instance from 'file-ID' and 'the number of worksheet'.
        ----------------------------------------------------------------------------------

        :param sheet: < file ID : str >
        :param wks_num: < the number of worksheet : int >
        :return: < wks : gspread.worksheet class instance >
        """
        wks = self.gs.open_by_key(key=sheet).get_worksheet(index=wks_num)
        return wks

    def append_rows(self, wks, data, nrow):
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

    def append(self, sheet, wks_num, data):
        """
        The Master Function of appending data.
        --------------------------------------

        :param sheet: < File ID : str >
        :param wks_num: < the number of worksheet : int >
        :param data: < monitor data : array or list >
        :return:
        """
        wks = self.load(sheet=sheet, wks_num=wks_num)
        blank = self.blank_check(wks)
        if blank:
            dcsv, nrow, ncol = self.array_to_csvstr(data=data)
            self.gs.import_csv(file_id=sheet, data=dcsv)
            wks.resize(rows=nrow, cols=ncol)
        else: self.append_rows(wks=wks, data=data)
        return

    def size(self, wks):
        row = wks.row_count()
        col = wks.col_count()
        size = [row, col]
        return size

    def blank_check(self, wks):
        """
        Check whether the worksheet is blank file or not.
        --------------------------------------------------

        :param wks: < gspread.worksheet class instance >
        :return blank: < Blank file or not : True or False >
        """
        cell_A1 = wks.acell('A1')
        if cell_A1.value == '': blank = True
        else: blank = False
        return blank

    def blank_check2(self, wks, row, col):
        values = wks.row_values(row=row)  # TODO: row-1 ??
        if values == [None]*col: blank = True
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
            csv_str += ikuta
        return csv_str, nrow, ncol

    def load_data(self):
        return


def Authorize(json):
    gs = n2gspread(json=json)
    return gs


# History
# -------
# 2017/11/30 written by T.Inaba
# 2017/12/04 T.Inaba: ver.0.1.1
