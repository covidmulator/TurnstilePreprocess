#-*- coding:utf-8 -*-

import os
import pandas as pd
import json

time_keys = []

def initialize_time_keys():
  for hour in range(5, 24):
    _hour = str(hour).zfill(2)
    __hour = str(hour + 1).zfill(2)
    time_key = f'{_hour}~{__hour}'
    time_keys.append(time_key)
  time_keys.append('24~')

def get_data_file_name_list():
  return os.listdir('data/')

def get_station_list():
  with open("./stations.json") as file:
    return json.load(file)

def get_file_extension(file_name):
  return os.path.splitext(file_name)[1]

def parse_int(string):
  return int(string)

def restructure_row(row):
  indexes = row.index
  result = {
    'date': row['날짜'],
    'station': row['역명'].split('(')[0],
    'type': row['구분'],
  }

  time_indexes = indexes[5:]
  for time_index in time_indexes:
    result[time_index] = parse_int(row[time_index])
  
  return result

def get_dataframe_from_file(file_path):
  extension =  get_file_extension(file_path)
  if extension == '.xlsx':
    return pd.read_excel(file_path)
  elif extension == '.csv':
    return pd.read_csv(file_path)

def ensure_keys_in_tensor(tensor, date):
  if date not in tensor:
    tensor[date] = {}
    for time_key in time_keys:
      tensor[date][time_key] = {}

  return tensor

def get_row_info(row):
  return (
    row['date'],
    row['station'],
    row['type']
  )

def get_turnstile_weight(_type):
  return 1 if _type == '하차' else 0

def get_tensor_by_data(data, stations):
  tensor = {}
  data_size = data.shape[0]
  station_keys = list(stations.keys())
  for row_index in range(data_size):
    row = data.loc[row_index]
    row = restructure_row(row)

    (date, station, _type) = get_row_info(row)
    if station not in station_keys:
      continue

    tensor = ensure_keys_in_tensor(tensor, date)
    
    weight = get_turnstile_weight(_type)

    for time_key in time_keys:
      if station not in tensor[date][time_key]:
        tensor[date][time_key][station] = 0
      tensor[date][time_key][station] += row[time_key] * weight
  
  return tensor

if __name__ == '__main__':
  initialize_time_keys()
  data_file_names = get_data_file_name_list()
  stations = get_station_list()
  for file_name in data_file_names:
    data = get_dataframe_from_file(f'data/{file_name}')
    tensor = get_tensor_by_data(data, stations)
    print(json.dumps(tensor, ensure_ascii=False))
