import pandas as pd

from Process import Process
from ScanFolder import scan_a_folder


class MergeChangeLog(Process):
    
    def __init__(self, src, dst):
        super().__init__(greeting=f'Merging change log parts:', src=src, dst=dst)
    
    def action(self):
        part_list = scan_a_folder(self.src)
        to_merge = []
        for part in part_list:
            yield ('start', f'Reading {part}')
            df = pd.read_feather(self.src + part)
            to_merge.append(df)
            del df
            yield 'end'
        yield('start', 'Merging\u2026')
        merged = pd.concat(to_merge, ignore_index=True)
        merged.to_csv(self.dst, sep='\u00ac', index=False)
        yield 'end'