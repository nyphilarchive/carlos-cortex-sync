# Cortex API calls for Carlos metadata updates
# This script will read various CSVs and execute the appropriate API calls to create or update records
# Make sure you have an .env file in this directory that looks like this:
# login = 'yourlogin'
# password = 'yourpassword'
# by Bill Levay

import requests, csv, sys, os, urllib.parse, time, datetime, logging
from os.path import join, dirname
from requests.exceptions import HTTPError
import xml.etree.ElementTree as ET
from dotenv import load_dotenv

# CONSTANTS
directory = '/mnt/x/CARLOS/CSV/cortex/'
baseurl = 'https://cortex.nyphil.org'
datatable = '/API/DataTable/v2.2/'

# get a new token from the Login API
def auth():
	# first grab the credentials from the .env file in the same folder as this script
	dotenv_path = join(dirname(__file__), '.env')
	load_dotenv(dotenv_path)
	login = os.environ.get('login', 'default')
	password = os.environ.get('password', 'default')
	password = urllib.parse.quote(password)

	auth_string = f"/API/Authentication/v1.0/Login?Login={login}&Password={password}"

	# send the POST request
	try:
		response = requests.request('POST', baseurl+auth_string)

		response.raise_for_status()
	except HTTPError as http_err:
		logger.info(f'HTTP error occurred: {http_err}')
	except Exception as err:
		logger.info(f'Other error occurred: {err}')
	else:
		logger.info('Success!')

	if response:
		logger.info(response.text)

		# parse the XML response
		response_string = response.content
		response_xml = ET.fromstring(response_string)
		code = response_xml.find('APIResponse').find('Code')
		if code.text == 'SUCCESS':
			token = response_xml.find('APIResponse').find('Token').text
			# logger.info(f'Your token is: {token}')
		else:
			logger.info('Authentication failed :( Bye!')
			sys.exit()
		return token

# create or update the program virtual folders
def make_folders(token):
	logger.info('Creating/updating program folders...')
	
	with open(directory+'cortex_folder_names.csv', 'r') as file:
		csvfile = csv.reader(file)

		for row in csvfile[1:]:
			season_folder_id = row[0]
			program_id = row[1]
			folder_name = row[2]
			ordinal = row[3]

			if ordinal == 'primary':
				parameters = f"Documents.Virtual-folder.Program:CreateOrUpdate?CoreField.Legacy-Identifier={program_id}&CoreField.Title:={folder_name}&NYP.Program-ID:={program_id}&CoreField.Parent-folder:=[Documents.Virtual-folder.Program:CoreField.Unique-identifier={season_folder_id}]"
				parameters = urllib.parse.quote(parameters)
				call = baseurl + datatable + parameters + '&token=' + token
				api_call(call,'Virtual Folder',program_id)

		for row in csvfile:
			program_id = row[1]
			folder_name = row[2]
			ordinal = row[3]
			parent_program = row[4]

			if ordinal == 'secondary':
				parameters = f"Documents.Virtual-folder.Program:CreateOrUpdate?CoreField.Legacy-Identifier={program_id}&CoreField.Title:={folder_name}&NYP.Program-ID:={program_id}&CoreField.Parent-folder:=[Documents.Virtual-folder.Program:CoreField.Legacy-Identifier={parent_program}]"
				parameters = urllib.parse.quote(parameters)
				call = baseurl + datatable + parameters + '&token=' + token

				api_call(call,'Virtual Folder',program_id)
	file.close()
	logger.info('Done')

# update metadata for the program virtual folders, but first clear out multi-value fields so we're not just appending stuff
def update_folders(token):
	logger.info('Updating program folder metadata...')
	
	with open(directory+'program_data_for_cortex.csv', 'r', encoding='ISO-8859-1') as file:
		csvfile = csv.reader(file)

		for row in csvfile[1:]:
			ID = row[0]
			SEASON = row[1]
			WEEK = row[2]
			ORCHESTRA_NAME = row[3]
			DATE = row[4]
			DATE_RANGE = row[5]
			PERFORMANCE_TIME = row[6]
			LOCATION_NAME = row[7]
			VENUE_NAME = row[8]
			SUB_EVENT_NAMES = row[9]
			SOLOIST_SLASH_INSTRUMENT = row[10]
			COMPOSER_TITLE = row[11]
			COMPOSER_TITLE_SHORT = row[12]
			NOTES_XML = row[13]

			# clear values from program folders
			parameters = f"Documents.Virtual-folder.Program:CreateOrUpdate?CoreField.Legacy-Identifier={ID}&NYP.Season--=&NYP.Orchestra--=&NYP.Program-Date(s)--=&NYP.Program-Times--=&NYP.Location--=&NYP.Venue--=&NYP.Event-Type--=&NYP.Soloist-/-Instrument--=&NYP.Composer/Work--=&NYP.Soloist--=&NYP.Conductor--=&NYP.Composer--="
			parameters = urllib.parse.quote(parameters)
			call = baseurl + datatable + parameters + '&token=' + token
			api_call(call,'Virtual Folder',ID)	
			
			# update program metadata
			parameters = f"Documents.Virtual-folder.Program:CreateOrUpdate?CoreField.Legacy-Identifier={ID}&NYP.Season+={SEASON}&NYP.Week:={WEEK}&NYP.Orchestra+:={ORCHESTRA_NAME}&NYP.Program-Date(s)++={DATE}&NYP.Program-Date-Range:={DATE_RANGE}&NYP.Program-Times++={PERFORMANCE_TIME}&NYP.Location++={LOCATION_NAME}&NYP.Venue++={VENUE_NAME}&NYP.Event-Type++={SUB_EVENT_NAMES}&NYP.Soloist-/-Instrument++={SOLOIST_SLASH_INSTRUMENT}&NYP.Composer/Work++={COMPOSER_TITLE_SHORT}&NYP.Notes-on-program:={NOTES_XML}&NYP.Composer/Work-Full-Title:={COMPOSER_TITLE}"
			parameters = urllib.parse.quote(parameters)
			call = baseurl + datatable + parameters + '&token=' + token
			api_call(call,'Virtual Folder',ID)	

	file.close()
	logger.info('Done')

# create or update the Source accounts that represent people
def create_sources(token):
	logger.info('Creating/updating Source records...')

	# We don't want to always overwrite the Role field
	# So we'll do a query for each Source, grab that field, parse it, then add any new values to it

	with open(directory+'source_accounts_composers.csv', 'r') as file:
		csvfile = csv.reader(file)

		for row in csvfile[1:]:
			COMPOSER_ID = row[0]
			DISPLAY = row[1]
			FIRST = row[2]
			MIDDLE = row[3]
			LAST = row[4]
			BIRTH = row[5]
			DEATH = row[6]

			query = '' # get roles
			# if 'Composer' not in roles:
				# add it

			parameters = f"Contacts.Source.Default:CreateOrUpdate?CoreField.Composer-ID={COMPOSER_ID}&CoreField.First-name:={FIRST}&CoreField.Middle-initial:={MIDDLE}&CoreField.Last-name:={LAST}&CoreField.Display-name:={DISPLAY}&CoreField.Birth-Year:={BIRTH}&CoreField.Death-Year:={DEATH}"
			parameters = urllib.parse.quote(parameters)
			call = baseurl + datatable + parameters + '&token=' + token
			api_call(call,'Source: Composer',COMPOSER_ID)
	file.close()

	with open(directory+'source_accounts_artists.csv', 'r') as file:
		csvfile = csv.reader(file)

		for row in csvfile[1:]:
			ARTIST_ID = row[0]
			DISPLAY = row[1]
			FIRST = row[2]
			MIDDLE = row[3]
			LAST = row[4]
			BIRTH = row[5]
			DEATH = row[6]
			ROLE = row[7]
			ORCHESTRA = row[8]
			ORCHESTRA_YEARS = row[9]

			query = '' # get roles
			# for role in ROLE.split('|'):
				# if role not in roles
					# add it
	
			parameters = f"Contacts.Source.Default:CreateOrUpdate?CoreField.Artist-ID={ARTIST_ID}&CoreField.First-name:={FIRST}&CoreField.Middle-initial:={MIDDLE}&CoreField.Last-name:={LAST}&CoreField.Display-name:={DISPLAY}&CoreField.Birth-Year:={BIRTH}&CoreField.Death-Year:={DEATH}&CoreField.Role:={ROLE}&CoreField.Orchestra-Membership:={ORCHESTRA}&CoreField.Orchestra-Membership-Year:={ORCHESTRA_YEARS}"
			parameters = urllib.parse.quote(parameters)
			call = baseurl + datatable + parameters + '&token=' + token
			api_call(call,'Source: Artist',ARTIST_ID)

	file.close()
	logger.info('Done')

# now that the Sources have been created/updated, we can add them to the program virtual folders
def add_sources_to_program(token):
	logger.info('Adding Sources to program folders...')

	with open(directory+'soloists.csv', 'r') as file:
		csvfile = csv.reader(file)

		for row in csvfile[1:]:
			Program_ID = row[0]
			Artist_ID = row[1]

			parameters = f"Documents.Virtual-folder.Program:CreateOrUpdate?CoreField.Legacy-Identifier={Program_ID}&NYP.Soloist+=[Contacts.Source.Default:CoreField.Artist-ID={Artist_ID}]"
			parameters = urllib.parse.quote(parameters)
			call = baseurl + datatable + parameters + '&token=' + token
			api_call(call,'Virtual Folder',Program_ID)
	file.close()

	with open(directory+'conductors.csv', 'r') as file:
		csvfile = csv.reader(file)

		for row in csvfile[1:]:
			Program_ID = row[0]
			Artist_ID = row[1]
			
			parameters = f"Documents.Virtual-folder.Program:CreateOrUpdate?CoreField.Legacy-Identifier={Program_ID}&NYP.Conductor+=[Contacts.Source.Default:CoreField.Artist-ID={Artist_ID}]"
			parameters = urllib.parse.quote(parameters)
			call = baseurl + datatable + parameters + '&token=' + token
			api_call(call,'Virtual Folder',Program_ID)
	file.close()

	with open(directory+'composers.csv', 'r') as file:
		csvfile = csv.reader(file)

		for row in csvfile[1:]:
			Program_ID = row[0]
			Composer_ID = row[1]
	
			parameters = f"Documents.Virtual-folder.Program:CreateOrUpdate?CoreField.Legacy-Identifier={Program_ID}&NYP.Composer+=[Contacts.Source.Default:CoreField.Composer-ID={Composer_ID}"
			parameters = urllib.parse.quote(parameters)
			call = baseurl + datatable + parameters + '&token=' + token
			api_call(call,'Virtual Folder',Program_ID)
	file.close()

def api_call(call,asset_type,ID):
	try: 
		response = requests.get(call)

		# If the response was successful, no Exception will be raised
		response.raise_for_status()
	except HTTPError as http_err:
		logger.info(f'HTTP error occurred with {asset_type} {ID}: {http_err}')
	except Exception as err:
		logger.info(f'Other error occurred with {asset_type} {ID}: {err}')
	else:
		logger.info(f'Success updating {asset_type} {ID}')



############################
## update Cortex metadata ##
############################

# Set up logging (found here: https://fangpenlin.com/posts/2012/08/26/good-logging-practice-in-python/)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# create a file handler
now = datetime.datetime.now()
logfile = now.strftime("%Y-%m-%d-%H-%M") + '.log'
handler = logging.FileHandler(directory + 'logs/' + now.strftime("%Y-%m-%d-%H-%M") + '.log')
handler.setLevel(logging.INFO)

# create a logging format
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)

# add the handlers to the logger
logger.addHandler(handler)

# Starting the run
logger.info('=======================')
logger.info('Script started...')

token = ''
token = auth()

if token != '':
	logger.info('We have a token! Proceeding...')

	# make_folders(token)
	# update_folders(token)
	# create_sources(token)
	# add_sources_to_program(token)
	
	logger.info('*********\nALL DONE!\n*********')