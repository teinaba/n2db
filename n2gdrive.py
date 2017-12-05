#! /usr/bin/env python

# import modules
# --------------
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive


class n2gdrive(object):
    drive = None

    def __init__(self):
        self.authorize()
        pass

    def authorize(self):
        gauth = GoogleAuth()
        gauth.LocalWebserverAuth()
        self.drive = GoogleDrive(gauth)
        pass

    def file(self, id):
        f = self.drive.CreateFile({'id': str(id)})
        return f

    def search(self, msg):
        flist = self.drive.ListFile({'q': msg}).GetList()
        return flist

    def trash(self, file=None, id=None):
        if file is None: file = self.file(id)
        file.Trash()
        return

    def untrash(self, file=None, id=None):
        if file is None: file = self.file(id)
        file.UnTrash()
        return

    def delete(self, file=None, id=None):
        if file is None: file = self.file(id)
        file.Delete()
        return

    def create_folder(self, title, parents=None):
        mimeType = MimeTypeIdentifier('folder').mimeType
        metadata = {'mimeType': mimeType,
                    'title': title
                    }
        if parents is not None: metadata['parents'] = [{"kind": "drive#fileLink", "id": str(parents)}]
        folder = self.drive.CreateFile(metadata)
        folder.Upload()
        return folder

    def create_file(self, title, mimeType=None, parents=None, content=None):
        metadata = {'title': str(title),
                    'mimeType': MimeTypeIdentifier(mimeType).mimeType
                    }
        if parents is not None: metadata['parents'] = [{"kind": "drive#fileLink", "id": str(parents)}]
        file = self.drive.CreateFile(metadata)
        if content is not None:
            file.SetContentFile(filename=content)
        file.Upload()
        return file

    def upload_local_file(self, filepath, id=None):
        f = self.file(id=id)
        f.SetContentFile(filename=filepath)
        f.Upload()
        return

    def exists(self, title=None, parents=None, trashed=False, q=None):
        # Check input Values --
        if title is None and q is None:
            print('===============================\n'
                  '      -- Invalid Input --\n'
                  ' Both "title" and "q" are None\n'
                  '===============================\n')
            return

        # search by q --
        if q is not None:
            msg = q
            flist = self.search(msg=msg)
            if not flist:exists = False
            else: exists = True
            return exists

        # search by title and parents --
        if parents is None:
            msg = "title = '{}' and trashed={}".format(title, trashed)
        else:
            msg = "title = '{}' and '{}' in parents and trashed={}".format(title, parents, trashed)
        flist = self.search(msg=msg)

        # check result --
        if flist: exists = True
        else: exists = False
        return exists


class MimeTypeIdentifier(object):
    mimeType = None
    mimeTypes = {
                # Google applications
                'sheet': 'application/vnd.google-apps.spreadsheet',
                'doc': 'application/vnd.google-apps.document',
                'slide': 'application/vnd.google-apps.presentation',
                "folder": 'application/vnd.google-apps.folder',
                # microsoft formats
                "xls": 'application/vnd.ms-excel',
                "xlsx": 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                "word": 'application/msword',
                # basic formats
                "txt": 'text/plain',
                "csv": 'text/csv',
                "mp3": 'audio/mpeg',
                "jpg": 'image/jpeg',
                "png": 'image/png',
                "gif": 'image/gif',
                "pdf": 'application/pdf',
                "zip": 'application/zip',
                # others
                "js": 'text/js',
                "xml": 'text/xml',
                "html": 'text/html',
                "htm": 'text/html',
                "tmpl": 'text/plain',
                "bmp": 'image/bmp',
                "rar": 'application/rar',
                "tar": 'application/tar',
                "arj": 'application/arj',
                "cab": 'application/cab',
                "default": 'application/octet-stream',
                "php": 'application/x-httpd-php',
                "swf": 'application/x-shockwave-flash',
                "ods": 'application/vnd.oasis.opendocument.spreadsheet',
                }

    def __init__(self, mimeType):
        self.identify(mimeType=mimeType)
        pass

    def identify(self, mimeType):
        key = self.interpreter(mimeType)
        self.mimeType = self.mimeTypes[key]
        return self.mimeType

    def interpreter(self, mimeType):
        if mimeType is None: key = 'txt'
        elif 'sheet' in mimeType or 'spread' in mimeType: key = 'sheet'
        elif 'doc' in mimeType: key = 'doc'
        elif 'txt' in mimeType or 'text' in mimeType: key = 'txt'
        else: key = mimeType
        return key

def Authorize():
    drive = n2gdrive()
    return drive


# History
# -------
# 2017/11/30 written by T.Inaba
# 2017/12/02 T.Inaba : Add "upload_local_file" method.
# 2017/12/04 T.Inaba: ver.0.1.1
