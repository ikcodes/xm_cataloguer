from datetime import datetime, time, timedelta
from dateutil.relativedelta import relativedelta
from config import config, connection, cursor

#============================================================
# MANAGE PERFS
#----------------
# For web app performance, the main table is limited to 
# 6 months' worth of spins. This script runs daily to keep
# that timespan tidy, ensuring all historical data
# is saved in the master table for select long-term queries.
#============================================================

def managePerfs():
	
	# 1. GET TIMESTAMPS
	#===================
	yesterday = datetime.today() - timedelta( days=1 )
	ts_yesterday = int(datetime.combine( yesterday, time.min ).timestamp())					# Beginning of yesterday
	ts_yesterday_upper = int(datetime.combine( yesterday, time.max ).timestamp())		# End of yesterday
	
	six_mo = datetime.today() - relativedelta( months=+6 )
	ts_6mo = int(datetime.combine(six_mo, time.min ).timestamp())					# 6 months and 1 day ago (beginning)
	ts_6mo_upper = int(datetime.combine( six_mo, time.max ).timestamp())	# 6 months and 1 day ago (end)
	
	# 2. COPY YESTERDAY'S PERFS TO SXM_PERFS_MASTER
	# ==============================================
	# copy_sql = "INSERT IGNORE INTO sxm_perfs_master (SELECT * FROM sxm_perfs WHERE timestamp_utc BETWEEN "+ str(ts_yesterday) +" AND "+ str(ts_yesterday_upper) +");"
	copy_sql = "INSERT IGNORE INTO `sxm_perfs_master` (`title`, `artist`, `channel`, `timestamp`, `timestamp_utc`, `inserted`) SELECT `title`, `artist`, `channel`, `timestamp`, `timestamp_utc`, `inserted` from `sxm_perfs` WHERE timestamp_utc BETWEEN %s AND %s;"
	
	cursor.execute(copy_sql, [ts_yesterday, ts_yesterday_upper])
	connection.commit()
	
	# 3. CHECK YOU ARE NOT ABOUT TO DELETE THE ONLY COPY OF THESE PLAYS
	# ==================================================================
	check_sql1 = "SELECT COUNT(*) as ct FROM sxm_perfs WHERE timestamp_utc BETWEEN %s AND %s;"
	cursor.execute(check_sql1, [ts_6mo, ts_6mo_upper])
	res1 = cursor.fetchone()
	sxm_perfs_ct = int(res1[0])
	# print(sxm_perfs_ct)
	
	check_sql2 = "SELECT COUNT(*) as ct FROM sxm_perfs_master WHERE timestamp_utc BETWEEN %s AND %s;"
	cursor.execute(check_sql2, [ts_6mo, ts_6mo_upper])
	res2 = cursor.fetchone()
	sxm_perfs_master_ct = int(res2[0])
	# print(sxm_perfs_master_ct)
	
	print("SXM_PERFS CT: " + str(sxm_perfs_ct) + "    vs.    " + str(sxm_perfs_master_ct))
	
	# 4. IF YOU CAN => DROP OLD PLAYS FROM SXM_PERFS
	# ===============================================
	if(sxm_perfs_ct > 0 and sxm_perfs_ct == sxm_perfs_master_ct): # To remove rows from SXM_PERFS, please make SURE 
		drop_sql = "DELETE FROM sxm_perfs WHERE timestamp_utc < %s;"
		cursor.execute(drop_sql, [ts_6mo])
		connection.commit()
		print("-----> DROPPED ALL ROWS OLDER THAN: " + str(ts_6mo))
	else:
		print("-----> SKIPPED DROPPING EXTRA ROWS.")


# ================================================================================================
# =========================={ 		^ LOGIC 		|||			EXECUTION  v		 }==========================
# ================================================================================================


managePerfs()
exit()