"""Compare versions 03/23 and 04/26."""

import pandas as pd

ws = '/Volumes/SamsungT5/GIS/03_FIAS_analysis/'
v3 = ws + '05_version0323/output/'
v5 = ws + '13_version0506/output/'
chl = v5 + 'ready/changelog.csv'

chlog = pd.read_csv(chl, sep='\u00ac', engine='python')

metadata3 = pd.read_csv(v3+'ready/metadata.csv', sep=';')
metadata5 = pd.read_csv(v5+'ready/metadata.csv', sep=';')
metadata = metadata3.copy()
metadata['v05'] = metadata5['value']
metadata.rename(columns = {'value':'v03'}, inplace=True)

# Pre-load AO
ao3 = pd.read_feather(v3+'augao.fea')
ao5 = pd.read_feather(v5+'augao.fea')


# -----------------
# Problem analysis:
# -----------------


# Problem 1:
# -2 rayons
ray3 = ao3.loc[ao3.path=='3-1'][['A','SR','FR','iso']]
ray5 = ao5.loc[ao5.path=='3-1'][['A','SR','FR','iso']]
raym = pd.merge(ray3, ray5, how='left', on='A', suffixes=('3','5'))
raym.loc[raym.iso5.isna()]
# It's okay, the rayons are really obsolete

# Problem 2:
# +12 top-level cities
tlc3 = ao3.loc[ao3.path=='4-1'][['A','SR','FR','iso']]
tlc5 = ao5.loc[ao5.path=='4-1'][['A','SR','FR','iso']]
tlcm = pd.merge(tlc5, tlc3, how='left', on='A', suffixes=('5','3'))
new_tlc = tlcm.loc[tlcm.iso3.isna()]
ao3.loc[ao3.A.isin(new_tlc.A)]
# Results: seems to be normal, 11 cities were upgraded from 6/4-3-1 to 4-1
# +1 city was added (ZATO Solnechnyy)

# Problem 3:
# -101 rayon-level cities
rlc3 = ao3.loc[ao3.path=='4-3-1'][['A','SR','FR','iso']]
rlc5 = ao5.loc[ao5.path=='4-3-1'][['A','SR','FR','iso']]
rlcm = pd.merge(rlc3, rlc5, how='left', on='A', suffixes=('3','5'))
delr = rlcm.loc[rlcm.iso5.isna()]
promoted_to_4_1 = delr.loc[delr.A.isin(tlc5.A)]
# Results:
# 10 of the removed were promoted to 4-1  - ok
# 5 of the remaining were ZATO-like and removed - doubtful but confirmed on FIAS site (addr. moved elsewhere?)
# 20 are 'massiv' in Volkhov city - ok
# the rest are s/p g/p s/mo, terr (also repr. municipalities) - ok

# Problem 4:
# 54 AO with double-level paths
# some obscure terr. in Kemerovskaya obl. - ok

# Problem 5:
# 59 AO with gaps in path
# obscure looking territories in various oblasts (65-1)

# Doubtful paths included in FIAS, so no problem here.

# Deleted address analysis from change log:
# All deleted records are historic on FIAS website


