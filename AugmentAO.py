import pandas as pd

from Process import Process
from Translit import translit
from RC_to_ISO import rc_to_ISO
from Path_tools import compute_path, validate_path


class AugmentAO(Process):

    def __init__(self, src, dst):
        super().__init__(greeting='Augmenting AO', src=src, dst=dst)
    
    def action(self):
        yield ('start', 'Reading AO\u2026')
        ao = pd.read_feather(self.src)
        yield 'end'
        # Keep only live records
        yield('start', 'Formatting\u2026')
        ao = ao.loc[ao.LIVESTATUS=='1'].drop('LIVESTATUS', axis=1).reset_index(drop=True)
        # Correct region names
        ao.loc[(ao.REGIONCODE=='21')&(ao.AOLEVEL=='1'), 'SHORTNAME'] = 'Респ'
        ao.loc[(ao.REGIONCODE=='21')&(ao.AOLEVEL=='1'), 'FORMALNAME'] = 'Чувашская'
        ao.loc[(ao.REGIONCODE=='14')&(ao.AOLEVEL=='1'), 'FORMALNAME'] = 'Саха (Якутия)'
        yield 'end'
        # Transliterate
        yield ('start', 'Transliterating')
        ao['SE'] = ao.SHORTNAME.apply(translit)
        ao['FE'] = ao.FORMALNAME.apply(translit)
        yield 'end'
        # Rename columns & format
        yield ('start', 'further formatting\u2026')
        pc = ao.POSTALCODE.copy()
        rc = ao.REGIONCODE.copy()
        ao = ao.rename(columns = {'AOGUID':'A', 'PARENTGUID':'P', 'FORMALNAME':'FR', 
        'SHORTNAME':'SR', 'AOLEVEL':'L'})[['A', 'L', 'SR', 'SE', 'FR', 'FE', 'P']]
        # ISO codes
        iso = rc_to_ISO(rc)
        yield 'end'
        # Expand parents
        yield ('start','Expanding parents\u2026')
        aom = ao.copy()
        for x in range(0,5):
            col = 'P' if x==0 else 'P'+str(x)
            suf = str(x+1)
            aom = pd.merge(aom, ao, how='left', left_on=col, right_on='A', suffixes=('',suf))
        aom = aom.drop(['P','P1','P2','P3','P4','P5'], axis=1).fillna('')
        yield 'end'
        # Compute & validate parent path
        yield ('start','Computing and validating paths\u2026')
        aom['path'] = compute_path(aom.L, aom.L1, aom.L2, aom.L3, aom.L4, aom.L5)
        aom['p_v'] = validate_path(aom.path, iso)
        yield 'end'
        # Format and return
        yield ('start', 'Saving\u2026')
        aom['pc'] = pc
        aom['iso'] = iso
        aom['used'] = False
        aom.reset_index(drop=True).to_feather(self.dst)
        yield 'end'
