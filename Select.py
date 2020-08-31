import pandas as pd
import time

from Process import Process
from ScanFolder import scan_a_folder

class SelectForFIAS(Process):
    
    def __init__(self, src, dst, src_ao):
        super().__init__(greeting='Selecting records for FIAS', src=src, dst=dst)
        self.src_ao = src_ao

    @staticmethod
    def select_one_h_file(src, dst, aofn):
        h = pd.read_feather(src)
        ao = pd.read_feather(aofn)
        sel = h.loc[(h.CURR.isin(['0','1']))&(~h.outd)&(~h.gar)&(h.p_v.isin(['valid', 'doubtful']))]
        ao.loc[ao.A.isin(sel.A), 'used'] = True
        sel.reset_index(drop=True).to_feather(dst)
        ao.reset_index(drop=True).to_feather(aofn)
    
    @staticmethod
    def select_unused_ao(src_aug_ao, dst_sel_ao):
        ao = pd.read_feather(src_aug_ao)
        sel = ao.loc[(ao.used==False)&(ao.L.isin(['7','65','6']))&(ao.p_v.isin(['valid','doubtful']))]
        sel.reset_index(drop=True).to_feather(dst_sel_ao)

    def action(self):
        fl = scan_a_folder(self.src)
        for part in fl:
            yield ('start', f'Selecting valid records from {part}')
            SelectForFIAS.select_one_h_file(self.src+part, self.dst+part, self.src_ao)
            yield 'end'
        yield('start', 'Selecting unused AO records\u2026')
        SelectForFIAS.select_unused_ao(self.src_ao, self.dst+'unusedao.fea')
        yield 'end'