"""
An example of usage of FIAS processing tools.
"""

from fias_downloader import fias_downloader as fd


upd = fd.get_update_status()

for key in upd:
    print(f'{key} : {upd[key]}')


vdate = fd.get_version_date(upd)

print(f'Version date identified successfully: {vdate}')
