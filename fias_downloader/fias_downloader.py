"""This is a module that contains tools to prepare the FIAS data for
processing. This workflow includes:
1. Go to go to FIAS website
2. Check the latest available version
3. Test if the latest version applies
4. Check out the destination folders
5. Check if they have enough free space and otherwise are suitable for 
processing
6. Download the source files, extract the source files and remove 
unnecessary files.
Once the processing is complete, the data is ready to be fed into the
main processing module.
"""

import json
import requests


def get_update_status():
    """Makes a request to a pre-defined FIAS update service and returns
    the response."""
    upd_url = r'http://fias.nalog.ru/WebServices/Public/GetLastDownloadFileInfo'
    r = requests.get(upd_url)
    try:
        r_json = r.json()
    except json.decoder.JSONDecodeError:
        print('Response is not valid JSON')
        if r.text == '':
            print('Response is empty string')
        return {}
    return r_json