import pandas as pd
import shutil

from Process import Process
from ScanFolder import scan_a_folder

def save_header(dest:str) -> None:
    """Save a template header to the CSVs directory, where it can be
    later merged together with the main bodies of data.
    """
    official_columns = ['id', 'guid', 'current', 'postalcode', 'housenum', 
                        'buildnum', 'strucnum', 'housenum_en', 'buildnum_en', 
                        'strucnum_en', 'street_s', 'street_f', 'street_s_en', 
                        'street_f_en', 'terr_s', 'terr_f', 'terr_s_en', 
                        'terr_f_en', 'place_s', 'place_f', 'place_s_en', 
                        'place_f_en', 'quarter_s', 'quarter_f', 'quarter_s_en',
                        'quarter_f_en', 'city_s', 'city_f', 'city_s_en', 
                        'city_f_en', 'area_s', 'area_f', 'area_s_en', 
                        'area_f_en', 'region_s', 'region_f', 'region_s_en', 
                        'region_f_en', 'region_iso_code']
    header = pd.DataFrame(columns = official_columns)
    header.to_csv(dest, sep='\u00ac', header=True, index=False)
    return None


class MergeCSVs(Process):
        
    def __init__(self, src, dst):
        super().__init__(greeting='Merging CSVs:', src=src, dst=dst)
    
    def action(self):
        """Take a folder of ready FIAS-formatted CSVs (with an id), which also
        contains `header.csv` and merge it all properly together in one large
        csv file.
        """
        yield ('start', 'Preparing\u2026')
        save_header(self.src + 'header.csv')
        fias = open(self.dst, 'w')
        header = open(self.src+'header.csv','r')
        shutil.copyfileobj(header, fias)
        header.close()
        csv_list = scan_a_folder(self.src)
        yield 'end'
        csv_list.remove('header.csv')
        for csv in csv_list:
            yield ('start', f'Merging file {csv}')
            csv_file = open(self.src + csv,'r')
            shutil.copyfileobj(csv_file, fias)
            csv_file.close()
            yield 'end'
        fias.close()