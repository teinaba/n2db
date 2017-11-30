#! /usr/bin/env python

# import modules
# --------------
from . import n2gdrive

drive = n2gdrive.n2gdrive()

# Functions
# ---------
def mkdir_root():
    """
    Create root directory(='NECST_FILE_IO').
    ----------------------------------------

    :return: Folder id of 'NECST_FILE_IO'
    """
    root = drive.create_folder(title='NECST_FILE_IO', parents=None)
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
rootID = mkdir_root()
monID = mkdir_monitor(rootID=rootID)
rxID, teleID, geneID = mkdir_pjt(monID=monID)
mkdir_rx(rxID=rxID)
mkdir_tele(teleID=teleID)
mkdir_gene(geneID=geneID)


# History
# -------
# 2017/11/30 written by T.Inaba
