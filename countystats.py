import pandas as pd
import math
import numpy as np
import re

fips_url = 'https://en.wikipedia.org/wiki/List_of_United_States_FIPS_codes_by_county'

fips_df = pd.read_html(fips_url)[1]
fips_df['state_FIPS_prefix']=fips_df['FIPS'].apply(lambda x: math.floor(x/1000)*1000)
fips_df['FIPS'] = fips_df['FIPS']-fips_df['state_FIPS_prefix']
states = pd.unique(fips_df['State or equivalent'])

DATACOLUMNS = ['FIPS', 'County or equivalent', 'State or equivalent','state_FIPS_prefix','Population', 'Area','Source','Timestamp']   
    

def cleanup_state_data(state_df):
    state_df = state_df.rename(columns=lambda x: re.sub('\[\d+\]','',x))
    state_df = state_df.rename(columns=lambda x: re.sub('FIPS.*','FIPS',x))
    state_df = state_df.rename(columns=lambda x: re.sub('Pop.*','Population',x))
    state_df = state_df.rename(columns=lambda x: re.sub('.*[Ss]eat','Seat',x))
    state_df = state_df.rename(columns=lambda x: re.sub('(County|Parish|Borough).*','County or equivalent',x))
    state_df = state_df.rename(columns=lambda x: re.sub('.*Area.*','Area',x))
    if not np.issubdtype(state_df['FIPS'].dtypes,np.number):
        state_df['FIPS']=state_df['FIPS'].apply(lambda x:re.sub('[^\d]+','-1',x)).astype('int64')
    return state_df

def merge_state_data(state_df,fips_df):
    merged_state_df = fips_df.merge(state_df,on=['State or equivalent','FIPS'],suffixes=('','_y'))
    return merged_state_df[DATACOLUMNS]
    
def get_wikipedia_data(state):
    state = state.replace(' ','_')
    state = re.sub('[^A-Za-z_]','',state)
    url = 'https://en.wikipedia.org/wiki/List_of_counties_in_{state}'.format(state=state)
    webpage_data = pd.read_html(url)
    
    for k in range(0,len(webpage_data)):
        if (webpage_data[k].columns.dtype.kind=='O') & (not isinstance(webpage_data[k].columns,pd.core.indexes.multi.MultiIndex)):
            if (any([re.match('.*FIPS.*',x) for x in webpage_data[k].columns])):
                idx = k
                break
    state_data = webpage_data[idx]
    state_data['State or equivalent'] = state
    state_data['Source']=url
    state_data['Timestamp']=pd.Timestamp.now()
    return state_data
    
    

allcountiesdf = pd.DataFrame()
failed_states = []
for st in states:
    print('***Getting data for state: {state}***'.format(state=st))
    try:
        stdf = get_wikipedia_data(st)
    except:
        print('Error getting county data for {state}'.format(state=st))
        failed_states = failed_states + [st]
    else:
        stdf = cleanup_state_data(stdf)
        stdf = merge_state_data(stdf,fips_df)
        allcountiesdf = allcountiesdf.append(stdf)
 
# Manually fix everything else
    
district_of_columbia_df = pd.DataFrame({'FIPS':1,'County or equivalent':'District of Columbia','State or equivalent':'District of Columbia','state_FIPS_prefix':11000,'Population':705749, 'Area':'68.34 sq mi','Source':'https://en.wikipedia.org/wiki/Washington,_D.C.','Timestamp':pd.Timestamp(('2020-03-24 14:00:04.828918'))},index=[0])
    
allcountiesdf = allcountiesdf.append(district_of_columbia_df)

allcountiesdf['FIPS'] = allcountiesdf['FIPS'] + allcountiesdf['state_FIPS_prefix']


# Standardize Area on sq mi
allcountiesdf['Area (sq mi)']=allcountiesdf['Area'].apply(lambda x: float(re.match('[\d\.]+',x)[0]))

allcountiesdf.to_csv('all_counties_stats.csv')