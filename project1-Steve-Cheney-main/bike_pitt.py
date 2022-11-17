# Stephen Cheney
# Assignment 1
import argparse
import collections
import csv
import json
import glob
import math
import os
import pandas as pd
import re
from requests import get
import string
import sys
import time
import xml

class Bike():
    def __init__(self, baseURL, station_info, station_status):
        # initialize the instance

        # Normalize the json files to remove metadata
        stationinfoget = get(baseURL+station_info, verify = False)
        s_info = json.loads(stationinfoget.content)
        self.infoData = pd.json_normalize(s_info['data'], record_path=['stations'])

        stationstatusget = get(baseURL+station_status, verify = False)
        s_status = json.loads(stationstatusget.content)
        self.statusData = pd.json_normalize(s_status['data'], record_path=['stations'])

        pass

    def total_bikes(self):
        # return the total number of bikes available
        result = self.statusData['num_bikes_available'].sum()
        return result

    def total_docks(self):
        # return the total number of docks available
        result = self.statusData['num_docks_available'].sum()
        return result

    def percent_avail(self, station_id):
        # return the percentage of available docks
        result = 0
        df = self.statusData.loc[str(station_id) == self.statusData['station_id']] # new dataframe where station id matches
        bikesavail = float(df['num_bikes_available'].sum())
        docksavail = float(df['num_docks_available'].sum())
        total = docksavail + bikesavail
        # edge case div / 0
        if total == 0:
            return ''
        result = docksavail / total # percentage available
        result = '{:.0%}'.format(result) # format %
        return result

    def closest_stations(self, latitude, longitude):
        # return the stations closest to the given coordinates
        # create new empy column
        self.infoData['distance'] = ""
        # populate column with distance values
        self.infoData['distance'] = self.infoData.apply(lambda row: self.distance(latitude, longitude, row['lat'], row['lon']), axis=1)
        # sort distances and keep top 3
        dfsorted = self.infoData.sort_values('distance', ascending = True).head(3)
        # dataframe to dictionary
        dfdict = dfsorted.set_index('station_id')['name'].to_dict()
        return dfdict


    def closest_bike(self, latitude, longitude):
        # return the station with available bikes closest to the given coordinates
        # create new empy column
        self.infoData['distance'] = ""
        # populate column with distance values
        self.infoData['distance'] = self.infoData.apply(lambda row: self.distance(latitude, longitude, row['lat'], row['lon']), axis=1)
        dfinfo = self.infoData
        # get stations where there are available bikes
        dfstatus = self.statusData.loc[self.statusData['num_bikes_available'] > 0]
        # join the 2 dataframes based on staton id
        dfmerged = pd.merge(left = dfinfo, right = dfstatus, left_on = 'station_id', right_on = 'station_id')
        #sort by distance, keep top 1
        dfsorted = dfmerged.sort_values('distance', ascending = True).head(1)
        # dataframe to dictionary
        dfdict = dfsorted.set_index('station_id')['name'].to_dict()

        return dfdict

    def station_bike_avail(self, latitude, longitude):
        # return the station id and available bikes that correspond to the station with the given coordinates
        # dataframe of stations that == lat and lon
        dfinfo = self.infoData.loc[(self.infoData['lat'] == latitude) & (self.infoData['lon'] == longitude)]
        # if dataframe is empty, no matches
        if dfinfo.empty:
            result = {}
            return result
        dfstatus = self.statusData
        # merge the dataframes
        dfmerged = pd.merge(left = dfinfo, right = dfstatus, left_on = 'station_id', right_on = 'station_id')
        # dataframe to dictionary
        result = dfmerged.set_index('station_id')['num_bikes_available'].to_dict()
        return result


    def distance(self, lat1, lon1, lat2, lon2):
        p = 0.017453292519943295
        a = 0.5 - math.cos((lat2-lat1)*p)/2 + math.cos(lat1*p)*math.cos(lat2*p) * (1-math.cos((lon2-lon1)*p)) / 2
        return 12742 * math.asin(math.sqrt(a))


# testing and debugging the Bike class

if __name__ == '__main__':
    instance = Bike('https://api.nextbike.net/maps/gbfs/v1/nextbike_pp/en', '/station_information.json', '/station_status.json')
    print('------------------total_bikes()-------------------')
    t_bikes = instance.total_bikes()
    print(type(t_bikes))
    print(t_bikes)
    print()

    print('------------------total_docks()-------------------')
    t_docks = instance.total_docks()
    print(type(t_docks))
    print(t_docks)
    print()

    print('-----------------percent_avail()------------------')
    p_avail = instance.percent_avail(342885) # replace with station ID
    print(type(p_avail))
    print(p_avail)
    print()

    print('----------------closest_stations()----------------')
    c_stations = instance.closest_stations(40.444618, -79.954707) # replace with latitude and longitude
    print(type(c_stations))
    print(c_stations)
    print()

    print('-----------------closest_bike()-------------------')
    c_bike = instance.closest_bike(40.444618, -79.954707) # replace with latitude and longitude
    print(type(c_bike))
    print(c_bike)
    print()

    print('---------------station_bike_avail()---------------')
    s_bike_avail = instance.station_bike_avail(40.438761, -79.997436) # replace with exact latitude and longitude of station
    print(type(s_bike_avail))
    print(s_bike_avail)
