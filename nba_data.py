# Construct a SQLite database with NBA player data

import urllib2
import json
import pandas as pd
import threading
import time
from sqlalchemy import create_engine
import sqlite3
import os

N_THREADS = 20

TYPES_LOCK = threading.Lock()
PLAYER_IDS_LOCK = threading.Lock()
PLAYERS_LOCK = threading.Lock()

# if DB doesn't exist, create it
DB_NAME = "nba_stats.db"
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
		with PLAYER_IDS_LOCK:
			player_ids.update(list(result_pd.PLAYER_ID))
		with DB_LOCK:
			result_pd.to_sql(t, DB_ENGINE)

def getNextPlayer(player_ids, players):
	while True:
		PLAYER_IDS_LOCK.acquire()
		if len(player_ids) == 0:
			PLAYER_IDS_LOCK.release()
			break
		p = player_ids.pop()
		PLAYER_IDS_LOCK.release()
		url = "http://stats.nba.com/stats/commonplayerinfo?LeagueID=00&PlayerID=" + str(p) + "&SeasonType=Regular+Season"
		response = urllib2.urlopen(url)
		result = json.loads(response.read())
		header = result['resultSets'][0]['headers']
		values = result['resultSets'][0]['rowSet'][0]
		with PLAYERS_LOCK:
			first = len(players) == 0
			for i in range(len(header)):
				if not first:
					players[header[i]].append(values[i])
				else:
					players[header[i]] = [values[i]]

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
	players = dict()
	thread_list = []
	for i in range(N_THREADS):
		thread_list.append(threading.Thread(target = getNextPlayer, args = (player_ids, players)))
	for i in range(N_THREADS):
		thread_list[i].start()
	for i in range(N_THREADS):
		thread_list[i].join()
	players = pd.DataFrame.from_dict(players)
	players.to_sql('players', DB_ENGINE)

if __name__ == "__main__":
	start = time.time()
	getPlayerTrackingData()
	print "Finished in " + str(time.time() - start) + " seconds"