# -*- coding: utf-8 -*-
import sys
import json
from elasticsearch import Elasticsearch
from datetime import datetime
import requests
from threading import Timer

datestring = "2016/06/01 07:37:19.621" #start from this datetime

class GetEarthQuake():
	def __init__(self):
		self.__es = Elasticsearch()
		
	def run(self):
		Timer(60, self.run).start() #interval time: 1 minute
		
		r = requests.get('http://api.p2pquake.com/v1/human-readable') #get json data
		jsonData = r.json()
		
		global datestring		
		new_date=datestring #save the latest time
		print(datestring)
		
		for value in jsonData:
			if value["code"]==551: 
				if value["time"]>datestring:
					if value["time"]>new_date:
						new_date=value["time"]
					
					##process data previously
					#location:
					lat=value["earthquake"]["hypocenter"]["latitude"]
					lon=value["earthquake"]["hypocenter"]["longitude"]
					if lat.startswith("N"):
						lats=float(lat.lstrip("N"))
					elif lat.startswith("S"):
						lats=float(lat.lstrip("S"))
					if lon.startswith("E"):
						lons=float(lon.lstrip("E"))
					elif lon.startswith("W"):
						lons=float(lon.lstrip("W"))
					
					#depth
					dep=value["earthquake"]["hypocenter"]["depth"]
					if dep.endswith("km"):
						depth=float(dep.rstrip("km"))
					else:
						depth=0
						
					#datetime
					dtime=value["earthquake"]["time"]
					dtime=datetime.strptime(dtime,"%d日%H時%M分")
					dtime=dtime.replace(year=datetime.today().year)
					dtime=dtime.replace(month=datetime.today().month)
					dtime=datetime.strftime(dtime,"%Y/%m/%d %H:%M:%S")
					dtime=dtime+"+09:00" #add timezone
					
					doc = {
						"time":dtime,
						"place":value["earthquake"]["hypocenter"]["name"],
						"location":{
							"lat":lats,
							"lon":lons
						},
						"magnitude":value["earthquake"]["hypocenter"]["magnitude"],
						"depth":depth
					}
					self.__es.index(index="earthquakes", doc_type='earthquake', body=doc)
					
		datestring=new_date #next time from the latest time
		

if __name__ == '__main__':
	GetEarthQuake().run()
