import pandas as pd

def compute_path(L1, L2, L3, L4, L5, L6) -> pd.Series:
    """Take six AOLEVEL Series and compute a symbolic representation of an
    address path.
    """
    path = L1.str.cat([L2, L3, L4, L5, L6], sep='-', na_rep='')
    for _ in range(3):
        path = path.str.replace('--','-')
    path.loc[path.str.endswith('-')] = path.str.slice(0,-1)
    return path


def validate_path(path, region):
    """Take a Series of paths, a Series of ISO region codes
    and return a Series of validity statuses
    """
    v = pd.Series(index=path.index, dtype='str')
    v.loc[path.str.contains('35')] = 'outdated'
    v.loc[path.str.contains('90')] = 'outdated'
    v.loc[path.str.contains('91')] = 'outdated'
    v.loc[path.str.contains('7-7')] = 'double level'
    v.loc[path.str.contains('65-65')] = 'double level'
    v.loc[path.str.contains('6-6')] = 'double level'
    v.loc[(path.str.contains('5-5'))&(~path.str.contains('65-5'))] = 'double level'
    v.loc[path.str.contains('4-4')] = 'double level'
    v.loc[path.str.contains('3-3')] = 'double level'
    v.loc[~path.str.endswith('1')] = 'orphan'
    
    # Combinations allowed only in capital cities
    capitals = ['MOW', 'SPE', 'SEV', 'KZ-BAY']
    v.loc[(path.str.contains('7-1'))&(~region.isin(capitals))] = 'gap'
    v.loc[(path.str.contains('65-1'))&(~region.isin(capitals))] = 'gap'
    v.loc[(path.str.contains('6-1'))&(~region.isin(capitals))] = 'doubtful'
    v.loc[(path.str.contains('7-3-1'))&(region!='MOW')] = 'doubtful'
    v.loc[v.isna()] = 'valid'
    return v