import pandas as pd

def rc_to_ISO(region_code: pd.Series):
    """Take a pandas Series of REGIONCODEs in FIAS format and return a pandas
    Series in ISO format.
    """
    rciso = {'01':'AD', '02':'BA', '03':'BU', '04':'AL', '05':'DA', '06':'IN',
             '07':'KB', '08':'KL', '09':'KC', '10':'KR', '11':'KO', '12':'ME',
             '13':'MO', '14':'SA', '15':'SE', '16':'TA', '17':'TY', '18':'UD',
             '19':'KK', '20':'CE', '21':'CU', 
             '22':'ALT', '23':'KDA', '24':'KYA', '25':'PRI', '26':'STA', 
             '27':'KHA', '28':'AMU', '29':'ARK', '30':'AST', '31':'BEL', 
             '32':'BRY', '33':'VLA', '34':'VGG', '35':'VLG', '36':'VOR', 
             '37':'IVA', '38':'IRK', '39':'KGD', '40':'KLU', '41':'KAM', 
             '42':'KEM', '43':'KIR', '44':'KOS', '45':'KGN', '46':'KRS', 
             '47':'LEN', '48':'LIP', '49':'MAG', '50':'MOS', '51':'MUR', 
             '52':'NIZ', '53':'NGR', '54':'NVS', '55':'OMS', '56':'ORE', 
             '57':'ORL', '58':'PNZ', '59':'PER', '60':'PSK', '61':'ROS', 
             '62':'RYA', '63':'SAM', '64':'SAR', '65':'SAK', '66':'SVE', 
             '67':'SMO', '68':'TAM', '69':'TVE', '70':'TOM', '71':'TUL', 
             '72':'TYU', '73':'ULY', '74':'CHE', '75':'ZAB', '76':'YAR', 
             '77':'MOW', '78':'SPE', '79':'YEV', '83':'NEN', '86':'KHM', 
             '87':'CHU', '89':'YAN', '91':'CR',  '92':'SEV', '99':'KZ-BAY'}
    iso = pd.Series(index=region_code.index, dtype=str)
    iso.loc[~region_code.isin(rciso)] = 'XXX'
    iso.loc[region_code.isin(rciso)] = region_code.replace(rciso)
    return iso