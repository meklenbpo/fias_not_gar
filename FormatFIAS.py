import pandas as pd
import time

from Process import Process
from ScanFolder import scan_a_folder

class FormatForFIAS(Process):
    
    def __init__(self, src, dst):
        super().__init__(greeting='Formatting for FIAS', src=src, dst=dst)
    
    @staticmethod
    def _select_records(df):
        """Take augmented AO / augmented H dataset, determine which type it is,
        select and return records valid for FIAS, based on it's type.
        """
        if 'H' in df.columns:
            df['guid'] = df.H
            df['current'] = df.CURR
            df['postalcode'] = df.pcH
            df['iso'] = df.hiso
            df = df.rename(columns={'hR':'housenum', 'bR':'buildnum', \
                'sR':'strucnum', 'hE':'housenum_en', 'bE':'buildnum_en',\
                'sE':'strucnum_en'})
        else:
            df['guid'] = df.A
            df['current'] = 1
            df['postalcode'] = df.pc
            df['housenum'], df['buildnum'], df['strucnum'] = '','',''
            df['housenum_en'], df['buildnum_en'], df['strucnum_en'] = '','',''
        return df.fillna('')
    
    @staticmethod
    def format_one_file(src, dst):
        df = pd.read_feather(src)
        sel = FormatForFIAS._select_records(df)
        # Compile part 1
        f1 = sel[['guid', 'current', 'postalcode', 'housenum', 'buildnum', 
                'strucnum', 'housenum_en', 'buildnum_en', 'strucnum_en']]
        # Format parents by level (compile part 2)
        f2 = pd.DataFrame(index=sel.index, columns=['street_s', 'street_f', 
            'street_s_en', 'street_f_en', 'terr_s', 'terr_f', 'terr_s_en', 
            'terr_f_en', 'place_s', 'place_f', 'place_s_en', 'place_f_en', 
            'quarter_s', 'quarter_f', 'quarter_s_en', 'quarter_f_en', 'city_s', 
            'city_f', 'city_s_en', 'city_f_en', 'area_s', 'area_f', 'area_s_en', 
            'area_f_en', 'region_s', 'region_f', 'region_s_en', 'region_f_en'])
        levels = ['','1','2','3','4','5']
        addrel = {'7':'street','65':'terr','6':'place','5':'quarter','4':'city',
                '3':'area','1':'region'}
        langs = {'SR':'_s', 'FR':'_f', 'SE':'_s_en', 'FE':'_f_en'}
        for lv in levels:
            for el in addrel:
                for lg in langs:
                    f2.loc[sel['L'+lv]==el,addrel[el]+langs[lg]] = sel[lg+lv]
        # Add iso and merge
        f2['region_iso_code'] = sel.iso
        fias = pd.concat([f1, f2], axis=1).reset_index(drop=True)
        fias.to_feather(dst)
    
    def action(self):
        fl = scan_a_folder(self.src)
        for part in fl:
            yield ('start', f'Formatting file {part}')
            FormatForFIAS.format_one_file(self.src+part, self.dst+part)
            yield 'end'