import pandas as pd
import time

from Process import Process
from Translit import translit
from RC_to_ISO import rc_to_ISO
from ScanFolder import scan_a_folder


class AugmentH(Process):
    
    def __init__(self, src, src_ao, dst):
        super().__init__(greeting='Augmenting houses', src=src, dst=dst)
        self.src_ao = src_ao
        self.ao = None
    
    def augment_one_file(self, src, dst):
        # Loading
        h = pd.read_feather(src)
        # Transliterate
        h['hE'] = h.HOUSENUM.apply(translit)
        h['bE'] = h.BUILDNUM.apply(translit)
        h['sE'] = h.STRUCNUM.apply(translit)
        # Rename columns and format
        h = h.rename(columns={'HOUSEGUID':'H', 'AOGUID':'A', 'HOUSENUM':'hR', 
        'BUILDNUM':'bR', 'STRUCNUM':'sR', 'ESTSTATUS':'E', 'POSTALCODE':'pcH',
        'STARTDATE':'SD', 'UPDATEDATE':'UD', 'ENDDATE':'ED', 'REGIONCODE':'RC'})\
        .fillna('')
        # Convert region codes to ISO
        h['hiso'] = rc_to_ISO(h.RC)
        # Currency status
        h['CURR'] = 'X'
        g = h.groupby(['H','pcH']).tail(1)
        h.loc[g.index, 'CURR'] = '0'
        g = h.groupby('H').tail(1)
        h.loc[g.index, 'CURR'] = '1'
        # Outdated HOUSEGUID chain
        h['outd'] = False
        outdated = h.loc[(h.CURR=='1')&(h.ED<time.strftime('%Y-%m-%d', time.localtime()))].H
        h.loc[h.H.isin(outdated), 'outd'] = True
        # Garage
        h['gar'] = False
        h.loc[h.E.isin(['4','6','7','8','9','10']), 'gar'] = True
        # Add AO and update AO usage
        hm = pd.merge(h, self.ao, how='left', on='A')
        hm.reset_index(drop=True).to_feather(dst)
    
    def action(self):
        yield ('start', 'Loading AO\u2026')
        self.ao = pd.read_feather(self.src_ao)
        yield 'end'
        fl = scan_a_folder(self.src)
        for part in fl:
            yield ('start', f'Augmenting {part}\u2026')
            self.augment_one_file(self.src + part, self.dst + part)
            yield 'end'
        yield ('start', 'Saving updated AO\u2026')
        self.ao.to_feather(self.src_ao)
        yield 'end'

