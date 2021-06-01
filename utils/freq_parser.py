import json 
import pandas as pd

def freqByHourParser(df):
    df.timestamp = pd.to_datetime(df.timestamp)
    df.timestamp = df.timestamp.dt.strftime('%H').astype(int)
    freq = df.groupby(['timestamp', 'location']).size().reset_index(name='counts')
    max_val = int(freq["counts"].max())
    loca_list = list(df.groupby('location').groups.keys())
    loca_dict = dict()
    for i in range(len(loca_list)):
        loca_dict[loca_list[i]] = i
    res = list()
    for index, row in freq.iterrows():
        res.append([row.timestamp, loca_dict[row.location], row.counts])
    return [res, loca_list, max_val]

def freqByDayParser(df):
    df.timestamp = pd.to_datetime(df.timestamp)
    df.timestamp = df.timestamp.dt.dayofweek
    freq = df.groupby(['timestamp', 'location']).size().reset_index(name='counts')
    max_val = int(freq["counts"].max())
    loca_list = list(df.groupby('location').groups.keys())
    loca_dict = dict()
    for i in range(len(loca_list)):
        loca_dict[loca_list[i]] = i
    res = list()
    for index, row in freq.iterrows():
        res.append([row.timestamp, loca_dict[row.location], row.counts])
    return [res, loca_list, max_val]