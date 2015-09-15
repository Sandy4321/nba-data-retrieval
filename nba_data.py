# Construct a SQLite database with NBA player data

import urllib2
import json
import pandas as pd
import threading
import time
from sqlalchemy import create_engine
import sqlite3

PLAYER_TRACKING_TYPES = ["catchShootData", "defenseData", "drivesData", "passingData", "touchesData", "pullUpShootData", "reboundingData", "shootingData", "speedData"]
N_THREADS = 9
TYPES_LOCK = threading.Lock()
DB_LOCK = threading.Lock()
PLAYERS_LOCK = threading.Lock()
DB_NAME = "nba_stats.db"
sqlite3.connect(DB_NAME)
DB_ENGINE = create_engine('sqlite:///' + DB_NAME)

def getType():
	while True:
		TYPES_LOCK.acquire()
		if len(PLAYER_TRACKING_TYPES) == 0:
			TYPES_LOCK.release()
			break
		t = PLAYER_TRACKING_TYPES.pop()
		TYPES_LOCK.release()
		url = "http://stats.nba.com/js/data/sportvu/2014/" + t + ".json"
		response = urllib2.urlopen(url)
		result = json.loads(response.read())
		result_pd = pd.DataFrame(result['resultSets'][0]['rowSet'], columns = result['resultSets'][0]['headers'])
		with DB_LOCK:
			result_pd.to_sql(t, DB_ENGINE)


def getPlayerTrackingData():
	thread_list = []
	for i in range(N_THREADS):
		thread_list.append(threading.Thread(target = getType))
	for i in range(N_THREADS):
		thread_list[i].start()
	for i in range(N_THREADS):
		thread_list[i].join()


getPlayerTrackingData()
