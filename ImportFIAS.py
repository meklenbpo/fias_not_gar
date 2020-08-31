from lxml import etree
import pandas as pd
import time

from Process import Process


class ImportAO(Process):
    
    def __init__(self, src, dst):
        super().__init__(greeting='Importing AO from XML (1M)', src=src, dst=dst)
        
    def action(self):
        time.sleep(10)
        req_attr_ao = ['AOGUID', 'LIVESTATUS', 'PARENTGUID', 'FORMALNAME', 
            'SHORTNAME', 'AOLEVEL', 'POSTALCODE', 'REGIONCODE']
        xml = etree.iterparse(self.src, tag='Object', events=['end'])
        ao = []
        batch_num = 1
        yield ('start', f'Batch {batch_num}')
        for _, element in xml:
            row = {}
            for attr in req_attr_ao:
                row[attr] = element.get(attr)
            element.clear()
            ao.append(row)
            if len(ao) % 1000000 == 0:
                yield 'end'
                batch_num += 1
                yield ('start', f'Batch {batch_num}')
        aodf = pd.DataFrame(ao)
        aodf.to_feather(self.dst)
        yield 'end'
        return


class ImportH(Process):
    
    def __init__(self, src, dst):
        super().__init__(greeting='Importing H from XML (5M)', src=src, dst=dst)
        
    def action(self):
    
        def save_batch(batch, batch_num):
            df = pd.DataFrame(batch, columns=req_attr_h)
            fn = f'{self.dst}h{str(batch_num).zfill(2)}.fea'
            df.to_feather(fn)
        
        req_attr_h = ['HOUSEGUID', 'AOGUID', 'HOUSENUM', 'BUILDNUM',
        'STRUCNUM', 'ESTSTATUS', 'POSTALCODE', 'STARTDATE', 'UPDATEDATE',
        'ENDDATE', 'REGIONCODE']
        xml = etree.iterparse(self.src, tag='House', events=['end'])
        batch = []
        batch_num = 1
        yield ('start', f'Batch {batch_num}')
        for _, element in xml:
            row = {}
            for attr in req_attr_h:
                row[attr] = element.get(attr)
            element.clear()
            batch.append(row)
            if len(batch) == 5000000:
                save_batch(batch, batch_num)
                del batch
                yield 'end'
                batch_num += 1
                yield ('start', f'Batch {batch_num}')
                batch = []
        save_batch(batch, batch_num)
        yield 'end'