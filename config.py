import os 
import mysql
import mysql.connector
from dotenv import load_dotenv

# Setup MySQL connection from env
#=================================
load_dotenv()
PROD = os.getenv('ENV')
if(PROD == True):
	db_creds = {
		'host': os.getenv('PROD_DB_HOST'),
		'user': os.getenv('PROD_DB_USER'),
		'database': os.getenv('PROD_DATABASE'),
		'password': os.getenv('PROD_PASSWORD'),
	}
else:
	db_creds = {
		'host': os.getenv('LOCAL_DB_HOST'),
		'user': os.getenv('LOCAL_DB_USER'),
		'database': os.getenv('LOCAL_DATABASE'),
		'password': os.getenv('LOCAL_PASSWORD'),
	}

connection = mysql.connector.connect(
	host=db_creds['host'], 
	user=db_creds['user'], 
	password=db_creds['password'], 
	database=db_creds['database']
)
cursor = connection.cursor(buffered=True)

# Get channels, set other config
cursor.execute("SELECT * FROM sxm_channels WHERE active=1 ORDER BY channel_number desc")
channels = cursor.fetchall()

# Use this when debugging to avoid re-running every channel
channels_test = [ channels[0] ]
config = {
	'base_api_url': os.getenv('BASE_API_URL')
}
