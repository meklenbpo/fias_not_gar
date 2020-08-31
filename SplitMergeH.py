import os
import pandas as pd

from HexLetters import hex_letters
from Process import Process
from ShortName import shortname
from ScanFolder import scan_a_folder


class SplitH(Process):
    
    def __init__(self, src='', dst=''):
        super().__init__(greeting='Splitting houses by H1', src=src, dst=dst)
    
    @staticmethod
    def split_one_file(src, dst):
        df = pd.read_feather(src)
        sn = shortname(src)
        for x in hex_letters:
            part = df.loc[df.HOUSEGUID.str.startswith(x)]
            fn = f'{dst}{x}_{sn}'
            part.reset_index(drop=True).to_feather(fn)
    
    def action(self):
        fl = scan_a_folder(self.src)
        for part in fl:
            yield ("start", f'Splitting {part}')
            SplitH.split_one_file(self.src + part, self.dst)
            yield "end"


class MergeH(Process):
    
    def __init__(self, src='', dst=''):
        super().__init__(greeting='Merging houses by H1', src=src, dst=dst)
    
    @staticmethod
    def merge_group(file_list, dst):
        to_merge = []
        for part in file_list:
            df = pd.read_feather(part)
            to_merge.append(df)
            del df
        mgd = pd.concat(to_merge, ignore_index=True)
        mgd = mgd.sort_values(by=['HOUSEGUID', 'ENDDATE', 'UPDATEDATE', 'STARTDATE'])
        mgd.reset_index(drop=True).to_feather(dst)
    
    def action(self):
        for letter in hex_letters:
            yield ("start", f'Merging {letter}.fea')
            short_list = scan_a_folder(self.src, letter, True)
            MergeH.merge_group(short_list, self.dst + letter + '.fea')
            yield "end"
        