#!python3

from CreateFolders import create_folder_structure
from Process import Process
from ImportFIAS import ImportAO, ImportH
from SplitMergeH import SplitH, MergeH
from AugmentAO import AugmentAO
from AugmentH import AugmentH
from Select import SelectForFIAS
from FormatFIAS import FormatForFIAS
from AddID import AddID
from MergeCSVs import MergeCSVs
from ManageDeli import SplitDeliverable, ReMergeDeli
from Compare import Compare
from MergeChLog import MergeChangeLog
from Info import CollectInfo

workspace = '/Volumes/SamsungT5/GIS/03_FIAS_analysis/'
cv        = workspace + '17_version0802/'
oldv_deli = workspace + '16_version0701/output/ready/fias.csv'

# Init master script
master = Process('Master FIAS conversion script')
create_folder_structure(cv)

# Init subprocesses
master.add_sub(ImportAO(cv+'data/ADDROBJ0802.XML', cv+'output/ao.fea'))
master.add_sub(ImportH(cv+'data/HOUSE0802.XML', cv+'output/rawh/'))

cv = cv + 'output/'
cvc = cv + 'chlog/'

master.add_sub(SplitH(cv+'rawh/', cv+'splith/'))
master.add_sub(MergeH(cv+'splith/', cv+'mergedh/'))
master.add_sub(AugmentAO(cv+'ao.fea', cv+'augao.fea'))
master.add_sub(AugmentH(cv+'mergedh/', cv+'augao.fea', cv+'augh/'))
master.add_sub(SelectForFIAS(cv+'augh/', cv+'slct/', cv+'augao.fea'))
master.add_sub(FormatForFIAS(cv+'slct/', cv+'fmtd/'))
master.add_sub(AddID(cv+'fmtd/', cv+'csvs/'))
master.add_sub(MergeCSVs(cv+'csvs/', cv+'ready/fias.csv'))
# Changelog
master.add_sub(SplitDeliverable(cv+'ready/fias.csv', cvc+'newv/parts/'))
master.add_sub(SplitDeliverable(oldv_deli, cvc+'oldv/parts/'))
master.add_sub(ReMergeDeli(cvc+'newv/parts', cvc+'newv/mrgd/'))
master.add_sub(ReMergeDeli(cvc+'oldv/parts', cvc+'oldv/mrgd/'))
master.add_sub(Compare(cvc+'oldv/mrgd/', cvc+'newv/mrgd/', cvc+'cmpd/'))
master.add_sub(MergeChangeLog(cvc+'cmpd/', cv+'ready/changelog.csv'))
# Metadata
master.add_sub(CollectInfo(cv, cv+'ready/metadata.csv'))

# Launch!
master.launch()