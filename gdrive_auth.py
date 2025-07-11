from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive

def get_drive():
    gauth = GoogleAuth()
    gauth.LocalWebserverAuth()
    return GoogleDrive(gauth)
