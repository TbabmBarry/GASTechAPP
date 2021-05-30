import json 
import pandas as pd

def freqByHourParser(cc):
    cc.timestamp = pd.to_datetime(cc.timestamp)
    cc.timestamp = cc.timestamp.dt.strftime('%H').astype(int)
    freq = cc.groupby(['timestamp', 'location']).size().reset_index(name='counts')
    loca_list = list(cc.groupby('location').groups.keys())
    loca_dict = dict()
    for i in range(len(loca_list)):
        loca_dict[loca_list[i]] = i
    res = list()
    for index, row in freq.iterrows():
        res.append([row.timestamp, loca_dict[row.location], row.counts])
    return json.dumps(res)