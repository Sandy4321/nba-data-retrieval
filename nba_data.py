# Construct a SQLite database with NBA player data

import urllib2
import json
import pandas as pd
import threading
import time
from sqlalchemy import create_engine
import sqlite3
import os

N_THREADS = 9

TYPES_LOCK = threading.Lock()

PLAYER_IDS = []
PLAYERS_LOCK = threading.Lock()

DB_NAME = "nba_stats.db"

# if DB doesn't exist, create it
if os.path.isfile(DB_NAME):
	os.remove(DB_NAME)
sqlite3.connect(DB_NAME)

DB_ENGINE = create_engine('sqlite:///' + DB_NAME)
DB_LOCK = threading.Lock()

def getNextType(types, player_ids):
	while True:
		TYPES_LOCK.acquire()
		if len(types) == 0:
			TYPES_LOCK.release()
			break
		t = types.pop()
		TYPES_LOCK.release()
		url = "http://stats.nba.com/js/data/sportvu/2014/" + t + ".json"
		response = urllib2.urlopen(url)
		result = json.loads(response.read())
		result_pd = pd.DataFrame(result['resultSets'][0]['rowSet'], columns = result['resultSets'][0]['headers'])
		with PLAYERS_LOCK:
			player_ids = player_ids.update(list(result_pd.PLAYER_ID))
		with DB_LOCK:
			result_pd.to_sql(t, DB_ENGINE)

def getNextPlayer(player_ids):
	while True:
		PLAYERS_LOCK.acquire()
		if len(player_ids) == 0:
			PLAYERS_LOCK.release()
			break
		p = player_ids.pop()
		PLAYERS_LOCK.release()
		url = "http://stats.nba.com/stats/commonplayerinfo?LeagueID=00&PlayerID=" + str(p) + "&SeasonType=Regular+Season"
		response = urllib2.urlopen(url)
		result = json.loads(response.read())
		header = result['resultSets'][0]['headers']
		values = result['resultSets'][0]['rowSet']
		# TODO: insert values into DB

def getPlayerTrackingData():
	player_tracking_types = ["catchShootData", "defenseData", "drivesData", "passingData", "touchesData", "pullUpShootData", "reboundingData", "shootingData", "speedData"]
	player_ids = set()
	thread_list = []
	for i in range(N_THREADS):
		thread_list.append(threading.Thread(target = getNextType, args = (player_tracking_types, player_ids)))
	for i in range(N_THREADS):
		thread_list[i].start()
	for i in range(N_THREADS):
		thread_list[i].join()
	player_ids = list(player_ids)
	thread_list = []
	for i in range(N_THREADS):
		thread_list.append(threading.Thread(target = getNextPlayer, args = (player_ids,)))
	for i in range(N_THREADS):
		thread_list[i].start()
	for i in range(N_THREADS):
		thread_list[i].join()

if __name__ == "__main__":
	getPlayerTrackingData()