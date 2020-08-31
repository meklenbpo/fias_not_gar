import pandas as pd

from Process import Process
from ScanFolder import scan_a_folder


class Compare(Process):
    
    def __init__(self, src_old, src_new, dst):
        super().__init__(greeting=f'Comparing change log parts:', src=src_new, dst=dst)
        self.src_old = src_old
    
    @staticmethod
    def compare_one_batch(src_old, src_new, dst):
        """Take two id files, compare them for same records, new records and
        deleted records and save results to results file.
        """
        # Load
        df1 = pd.read_feather(src_old)
        df2 = pd.read_feather(src_new)
        df1['mrko'] = True
        df2['mrkn'] = True
        # Merge
        dfm1 = pd.merge(df1, df2, how='left', on=['guid', 'postalcode'])
        dfm2 = pd.merge(df2, df1, how='left', on=['guid', 'postalcode'])
        # Results
        newr = dfm2.loc[dfm2.mrko.isna()]
        delr = dfm1.loc[dfm1.mrkn.isna()]
        same = dfm1.loc[dfm1.mrkn.notna()]
        changed = same.loc[same.addr_x!=same.addr_y]
        # Format
        newr_df = pd.DataFrame(index=newr.index)
        delr_df = pd.DataFrame(index=delr.index)
        chgd_df = pd.DataFrame(index=changed.index)
        newr_df['guid'] = newr['guid']
        delr_df['guid'] = delr['guid']
        chgd_df['guid'] = changed['guid']
        newr_df['status'] = 'new address'
        delr_df['status'] = 'deleted address'
        chgd_df['status'] = 'address change'
        newr_df['address_new'] = newr['addr_x']
        newr_df['address_old'] = ' - '
        delr_df['address_new'] = ' - '
        delr_df['address_old'] = delr['addr_x']
        chgd_df['address_new'] = changed['addr_x']
        chgd_df['address_old'] = changed['addr_y']
        # Save
        changelog = pd.concat([newr_df, delr_df, chgd_df], ignore_index=True)
        changelog.reset_index(drop=True).to_feather(dst)
    
    def action(self):
        fnew = scan_a_folder(self.src)
        fold = scan_a_folder(self.src_old)
        attrs = zip(fold, fnew)
        for pair in attrs:
            yield ('start', f'Comparing {pair[0]}')
            oldv = self.src_old + pair[0]
            newv = self.src + pair[1]
            dstn = self.dst + pair[0]
            Compare.compare_one_batch(oldv, newv, dstn)
            yield 'end'

