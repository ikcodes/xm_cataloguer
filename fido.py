import requests

from datetime import datetime
from pprint import pprint as pp
from config import config, connection, cursor, channels, channels_test

def decodeResponse(json):
	spins = []	# Fill in with dictionary db rows
	cuts = json['ModuleListResponse']['moduleList']['modules'][0]['moduleResponse']['moduleDetails']['liveChannelResponse']['liveChannelResponses']
	channelNumber = int(cuts[0]['channel']['channelNumber'])
	for marker in cuts[0]['markerLists']:
		if(marker['layer'] == 'cut'):
			for marker in marker['markers']:
				if(marker['layer'] == 'cut'): # Yes, again...
					
					ts_seconds = int(marker['time'] / 1000)	 # ms => s (app runs on s)
					
					db_row = {	# Don't need cols: [id, inserted, prs_xtra]
						'title': marker['cut']['title'],
						'artist': marker['cut']['artists'][0]['name'],
						'channel': channelNumber,
						'timestamp': datetime.utcfromtimestamp(ts_seconds).strftime('%Y-%m-%d %H:%M:%S'),
						'timestamp_utc': ts_seconds,
					}
					
					spins.append(db_row)
	return spins

def insertPerformance(row):
	
	cursor.execute("SELECT COUNT(*) as IsCommercial FROM sxm_commercials WHERE title = %s AND artist = %s;", [ row['title'],  row['artist']])
	res = cursor.fetchone()
	if(int(res[0]) == 0):	# Is this a commercial?
		
		cursor.execute('SELECT COUNT(*) as SpinExists FROM sxm_perfs WHERE title = %s AND timestamp_utc = %s;', [ row['title'], row['timestamp_utc']])
		res = cursor.fetchone()
		if(int(res[0]) == 0):
			
			sql = """INSERT INTO sxm_perfs (title, artist, channel, timestamp, timestamp_utc) VALUES (%s, %s, %s, %s, %s);"""
			params = tuple(row.values())
			cursor.execute(sql, params)
			connection.commit()
			return True
			
		else: # This was a duplicate spin
			return False
	else: # This was a commercial
		return False


#=========================================================
# THE MAIN SHOW
#----------------
# Loop channels and make a requests for each.
#	Decode the response body and check if it's a new spin.
#	Insert new plays, skip duplicates, log output.
#=========================================================
def runFido(logOutput = True):
	
	perfsInserted = [] # Just for loggins
	
	if(logOutput):
		print("RUN START ====> " + datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
	
	for channel in channels:
		url = config['base_api_url'].replace('__KEY__', channel[1])
		res = requests.get(url)
		spins = decodeResponse(res.json())
		for spin in spins:
			if(insertPerformance(spin)):
				newPerf = { 'title': spin['title'], 'artist': spin['artist'], 'channel': spin['channel'] }
				perfsInserted.append(newPerf)
			else:
				if(logOutput):
					print("SKIPPING SPIN ----> " + spin['title'] + ' by ' + spin['artist'] + ' on ' + str(spin['channel']))
	
	if(logOutput):
		print("::: LOGGING SPINS :::")
		pp(perfsInserted)
		print("RUN FINISHED ===========> " + datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
		print()
	
	return len(perfsInserted)