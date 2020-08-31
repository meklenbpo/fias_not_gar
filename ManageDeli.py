import pandas as pd

from HexLetters import hex_letters
from Process import Process
from ScanFolder import scan_a_folder


class SplitDeliverable(Process):
    
    def __init__(self, src, dst):
        super().__init__(greeting=f'Splitting ready CSV ({src}):', src=src, dst=dst)
    
    def action(self):
        """Take a ready FIAS deliverable and split it into parts for change log
        analysis.
        """
        id_cols = ['guid', 'current', 'postalcode']
        addr_cols = ['region_f', 'region_s', 'area_f', 'area_s', 'city_f', 
                    'city_s', 'quarter_f', 'quarter_s', 'place_f', 'place_s', 
                    'terr_f', 'terr_s', 'street_f', 'street_s', 'housenum', 
                    'buildnum', 'strucnum']
        fias = pd.read_csv(self.src, sep='\u00ac', chunksize=1000000, engine='python', dtype=str)
        yield ('start', f'Processing chunk 1')
        for chid, chunk in enumerate(fias, 1):
            for letter in hex_letters:
                part = chunk.loc[chunk.guid.str.startswith(letter)][id_cols]
                addrc = chunk.loc[chunk.guid.str.startswith(letter)][addr_cols]
                part['addr'] = part.postalcode.str.cat(addrc, sep='-', na_rep='')
                if len(part) > 0:
                    fn = self.dst + letter + '_' + str(chid) + '.fea'
                    part.reset_index(drop=True).to_feather(fn)
            yield 'end'
            yield ('start', f'Processing chunk {chid+1}')


class ReMergeDeli(Process):
    
    def __init__(self, src, dst):
        super().__init__(greeting=f'Remerging CSV ({src}) parts:', src=src, dst=dst)
    
    def action(self):
        """Take a folder of deliverable parts (chunk/letter) and merge them
        by letter. Save remaining 16 parts into a new folder.
        """
        for letter in hex_letters:
            yield ('start', f'Merging {letter}')
            fl = scan_a_folder(self.src, prefix=letter, abspth=True)
            to_merge = []
            for filename in fl:
                df = pd.read_feather(filename)
                to_merge.append(df)
            merged_part = pd.concat(to_merge, ignore_index=True)
            savefn = self.dst + letter + '.fea'
            merged_part.reset_index(drop=True).to_feather(savefn)
            yield 'end'