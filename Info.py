import os
import pandas as pd
import time

from Process import Process
from ScanFolder import scan_a_folder


class CollectInfo(Process):
    
    def __init__(self, src, dst):
        """src - location of version output/ folder, e.g. './output/'
           dst - location where to save .csv info file.
        """
        super().__init__(greeting=f'Extracting metadata:', src=src, dst=dst)


    def action(self):
        """Collect version metadata for quick analysis."""

        vr = self.src

        metadata = []

        # Raw AO records
        yield ('start', 'Counting raw AO')
        rawao = pd.read_feather(vr + 'ao.fea')
        metadata.append([1, 'Raw AO records', len(rawao)])
        del rawao
        yield 'end'

        # Raw H records
        yield ('start','Counting raw houses')
        totalh = 0
        rawh = scan_a_folder(vr + 'rawh/')
        for f in rawh:
            h = pd.read_feather(vr + 'rawh/' + f)
            totalh = totalh + len(h)
            del h
        metadata.append([2, 'Raw H records', totalh])
        yield 'end'

        # AO stats
        yield ('start','AO statistics')
        ao = pd.read_feather(vr + 'augao.fea')
        metadata.append([3, 'Live AO records', len(ao)])
        metadata.append([4, 'Valid paths AO', len(ao.loc[ao.p_v=='valid'])])
        metadata.append([5, 'Valid oblasts', len(ao.loc[ao.path=='1'])])
        metadata.append([6, 'Valid rayons', len(ao.loc[ao.path=='3-1'])])
        metadata.append([7, 'Valid top-level cities', len(ao.loc[ao.path=='4-1'])])
        metadata.append([8, 'Valid rayons-level cities', len(ao.loc[ao.path=='4-3-1'])])
        metadata.append([9, 'Orphaned AO', len(ao.loc[ao.p_v=='orphan'])])
        metadata.append([10, 'Double-level path', len(ao.loc[ao.p_v=='double level'])])
        metadata.append([11, 'Gap in the path', len(ao.loc[ao.p_v=='gap'])])
        metadata.append([12, 'Doubtful path', len(ao.loc[ao.p_v=='doubtful'])])
        del ao
        yield 'end'

        # H stats
        yield ('start','H statistics')
        augh = scan_a_folder(vr + 'augh/')
        uqh = 0
        outdh = 0
        garh = 0
        hwoao = 0
        hinvpath = 0
        for h in augh:
            h = pd.read_feather(vr+'augh/'+h)
            uqh = uqh + len(h.loc[h.CURR=='1'])
            outdh = outdh + len(h.loc[(h.CURR=='1')&(h.outd==True)])
            garh = garh + len(h.loc[(h.CURR=='1')&(h.gar==True)])
            hwoao = hwoao + len(h.loc[(h.CURR=='1')&(h.p_v.isna())])
            hinvpath = hinvpath + len(h.loc[(h.CURR=='1')&(h.p_v!='valid')])
            del h
        metadata.append([13, 'Unique HOUSEGUIDs', uqh])
        metadata.append([14, 'Outdated HOUSEGUIDs', outdh])
        metadata.append([15, 'Garage HOUSEGUIDs', garh])
        metadata.append([16, 'HOUSEGUIDs that have no AO', hwoao])
        metadata.append([17, 'HOUSEGUIDs with invalid path', hinvpath])
        yield 'end'

        # Selected for FIAS
        yield ('start','Counting selected houses')
        unusedao = pd.read_feather(vr+'slct/unusedao.fea')
        unused = len(unusedao)
        del unusedao
        sel = scan_a_folder(vr + 'slct/')
        sel.remove('unusedao.fea')
        current = 0
        historic = 0
        for h in sel:
            hdf = pd.read_feather(vr+'slct/'+h)
            current = current + len(hdf.loc[hdf.CURR=='1'])
            historic = historic + len(hdf.loc[hdf.CURR=='0'])
            del h
        metadata.append([18, 'Selected for FIAS: Unused AO', unused])
        metadata.append([19, 'Selected for FIAS: Current', current])
        metadata.append([20, 'Selected for FIAS: Historic', historic])
        yield 'end'

        # Deliverable stats
        yield ('start','Assesing deliverable')
        delifn = vr + 'ready/fias.csv'
        with open(delifn) as f:
            for i, _ in enumerate(f):
                pass
        metadata.append([21, 'Total records in FIAS', i])
        size = os.path.getsize(delifn)
        metadata.append([22, 'Ready FIAS size', size])
        yield 'end'

        # Save metadata
        yield ('start','Saving')
        metadf = pd.DataFrame(metadata, columns=['id','desc','value'])
        metadf.to_csv(self.dst, sep=';', index=False)
        yield 'end'
        return None














