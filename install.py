#! /usr/bin/env python

# import modules
# --------------
import os
from . import n2database
from . import n2gdrive


drive = n2gdrive.Authorize()
db = n2database.N2db()
db.authorize(json='credentials_n2db.json')


# Functions
# ---------
def mkdir_root():
    """
    Create root directory(='NECST_FILE_IO').
    ----------------------------------------

    :return: Folder id of 'NECST_FILE_IO'
    """
    root = drive.create_folder(title='NECST_FILE_IO_Test', parents=None)
    return root['id']

def mkdir_monitor(rootID):
    """
    Create Monitor directory(='NECST_FILE_IO/Monitor').
    ---------------------------------------------------

    :param rootID: < The File ID of root dir : str >
    :return: Folder id of 'Monitor'
    """
    mon = drive.create_folder(title='Monitor', parents=rootID)
    return mon['id']

def mkdir_pjt(monID):
    """
    Create Project directory(='NECST_FILE_IO/Monitor/Project').
    -----------------------------------------------------------

    :param monID: < The File ID of Monitor dir : str >
    :return: Folder id of each 'Project'.
    """
    rx = drive.create_folder(title='NASCORX', parents=monID)
    tele = drive.create_folder(title='Telescope', parents=monID)
    gene = drive.create_folder(title='Generator', parents=monID)
    return rx['id'], tele['id'], gene['id']

def mkdir_rx(rxID):
    """
    Create NASCORX Table directory(='NECST_FILE_IO/Monitor/NASCORX/Table').
    -----------------------------------------------------------------------

    :param rxID: < The File ID of NASCORX dir : str >
    :return: Folder id of each 'Table'.
    """
    sis = drive.create_folder(title='SIS', parents=rxID)
    cryo = drive.create_folder(title='Cryo', parents=rxID)
    IF = drive.create_folder(title='IF', parents=rxID)
    amb = drive.create_folder(title='Amb', parents=rxID)
    return sis['id'], cryo['id'], IF['id'], amb['id']

def mkdir_tele(teleID):
    return

def mkdir_gene(geneID):
    return

# Main
# ----
def run():
    # create database directory @drive --
    rootID = mkdir_root()
    print('\nCREATE FOLDER @Drive: NECST_FILE_IO\n'
          '    ID: {}\n'.format(rootID))
    monID = mkdir_monitor(rootID=rootID)
    print('\nCREATE FOLDER @Drive: Monitor\n'
          '    ID: {}\n'.format(monID))

    # create config directory @local --
    scriptname = os.path.dirname(os.path.abspath(__name__))
    cnfpath = os.path.normpath(os.path.join(scriptname, 'config'))
    try :
        os.mkdir(cnfpath)
    except:
        FileExistsError
    print('\nMKDIR @Local: config directory\n'
          '    PATH: {}\n'.format(cnfpath))

    # create database config @local --
    dbcnf = n2database.ConfigManager().create_db_cnf(cnfpath=cnfpath, rootID=monID)
    print('\nCREATE FILE @Local: DataBase config\n'
          '    FILE: {}\n'.format(dbcnf))
    # -- upload to drive
    d_dbcnf = drive.create_file(title='n2db.cnf', mimeType='txt', parents=monID, content=dbcnf)
    print('\nUPLOAD FILE @Drive: DataBase config\n'
          '    Parents: {}\n'
          '    FOLDER ID: {}\n'.format(d_dbcnf['parents'][0]['id'], d_dbcnf['id']))
    # -- add config-ID to config file
    n2database.ConfigManager().write_value(file=dbcnf, section='Info', key='cnfid', value=d_dbcnf['id'])
    drive.upload_local_file(filepath=dbcnf, id=d_dbcnf['id'])

    # create project --
    # -- NASCORX
    pjtcnf = db.CREATE_PROJECT(pjt='NASCORX')
    print('\nCREATE PROJECT: {}\n'
          '    FOLDER ID: {}\n'.format(pjtcnf['title'], pjtcnf['id']))
    tblcnf = db.CREATE_TABLE(pjt='NASCORX', table='SIS', description='THIS-is-Test-Program')
    print('\nCREATE TABLE: {}\n'
          '    FOLDER ID: {}\n'.format(tblcnf['title'], tblcnf['id']))
    tblcnf2 = db.CREATE_TABLE(pjt='NASCORX', table='Amb', description='THIS-is-Test-Program')
    print('\nCREATE TABLE: {}\n'
          '    FOLDER ID: {}\n'.format(tblcnf2['title'], tblcnf2['id']))
    # -- Telescope
    pjtcnf2 = db.CREATE_PROJECT(pjt='Telescope')
    print('\nCREATE PROJECT: {}\n'
          '    FOLDER ID: {}'.format(pjtcnf2['title'], pjtcnf2['id']))
    tblcnf2 = db.CREATE_TABLE(pjt='Telescope', table='AzEl', description='AzEl')
    print('\nCREATE TABLE: {}\n'
          '    FOLDER ID: {}\n'.format(tblcnf2['title'], tblcnf2['id']))


    print('\n\n-- FINISH INSTALL --')
    return


if __name__ == '__main__':
    run()


# History
# -------
# 2017/11/30 written by T.Inaba
