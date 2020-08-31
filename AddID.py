import pandas as pd

from Process import Process
from ScanFolder import scan_a_folder

class AddID(Process):
    
    def __init__(self, src, dst):
        super().__init__(greeting='Adding ID and saving as CSV:', src=src, dst=dst)
    
    def action(self):
        """Take a folder of formatted FIAS files and add an `id` column in each
        one of them. The id numbers will propagate from one file to another,
        based on filename of the file.
        Save to a different folder as CSV.
        """
        fl = scan_a_folder(self.src)
        rid = 0
        for part in fl:
            yield ('start', f'Processing file {part}')
            df = pd.read_feather(self.src + part)
            try:
                df.insert(0, 'id', 0)
            except:
                pass
            df['id'] = range(rid, rid + len(df))
            rid = rid + len(df)
            filename = self.dst + part.split('.')[0] + '.csv'
            df.to_csv(filename, sep='\u00ac', index=False, header=False)
            yield 'end'