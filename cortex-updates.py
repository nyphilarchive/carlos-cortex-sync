# Cortex API calls for Carlos metadata updates
# This script will read various CSVs and execute the appropriate API calls to create or update records
#
# Make sure you have an .env file in this directory that looks like this:
# login=yourlogin
# password=yourpassword
# directory=/location/of/csvs/
# logs=/location/for/logs/
# baseurl=https://mydomain.org
# datatable=/API/DataTable/v2.2/
#
# by Bill Levay

import requests, csv, sys, os, time, datetime, logging
from urllib.parse import quote
from os.path import join, dirname
from requests.exceptions import HTTPError
import xml.etree.ElementTree as ET
from dotenv import load_dotenv

# first grab credentials and other values from the .env file in the same folder as this script
dotenv_path = join(dirname(__file__), '.env')
load_dotenv(dotenv_path)

login = os.environ.get('login', 'default')
password = os.environ.get('password', 'default')
password = quote(password)
directory = os.environ.get('directory', 'default')
baseurl = os.environ.get('baseurl', 'default')
datatable = os.environ.get('datatable', 'default')

# get a new token from the Login API
def auth():

	auth_string = f"/API/Authentication/v1.0/Login?Login={login}&Password={password}"

	# send the POST request
	try:
		response = requests.request('POST', baseurl+auth_string)

		response.raise_for_status()
	except HTTPError as http_err:
		logger.error(f'HTTP error occurred: {http_err}')
	except Exception as err:
		logger.error(f'Other error occurred: {err}')
	else:
		logger.info('✔️ Authentication successful')

	if response:
		# logger.info(response.text)

		# parse the XML response
		response_string = response.content
		response_xml = ET.fromstring(response_string)
		code = response_xml.find('APIResponse').find('Code')
		if code.text == 'SUCCESS':
			token = response_xml.find('APIResponse').find('Token').text
		else:
			token = ''
			logger.error('❌ Authentication failed')
		return token

# create or update the program virtual folders
def make_folders(token):
	
	with open(directory+'cortex_folder_names.csv', 'r') as file:
		csvfile = csv.reader(file)

		# Convert csv.reader object to list so we can loop through it twice
		rows = list(csvfile)

		# Get a count of Programs to be updated and log it
		row_count = len(rows[1:])
		logger.info(f"Creating/updating {row_count} program folders...")

		# Loop through rows for primary folders first and make sure they're created
		for row in rows[1:]:
			season_folder_id = row[0]
			program_id = row[1]
			folder_name = row[2]
			ordinal = row[3]

			if ordinal == 'primary':
				parameters = f"Documents.Virtual-folder.Program:CreateOrUpdate?CoreField.Legacy-Identifier={program_id}&CoreField.Title:={folder_name}&NYP.Program-ID:={program_id}&CoreField.visibility-class:=Internal use only&CoreField.Parent-folder:=[Documents.Virtual-folder.Program:CoreField.Unique-identifier={season_folder_id}]"
				parameters = quote(parameters)
				call = baseurl + datatable + parameters + '&token=' + token
				api_call(call,'Virtual Folder',program_id)

		# Now loop through again for secondary programs and assign them to the primary folders
		for row in rows[1:]:
			program_id = row[1]
			folder_name = row[2]
			ordinal = row[3]
			parent_program = row[4]

			if ordinal == 'secondary':
				parameters = f"Documents.Virtual-folder.Program:CreateOrUpdate?CoreField.Legacy-Identifier={program_id}&CoreField.Title:={folder_name}&NYP.Program-ID:={program_id}&CoreField.visibility-class:=Internal use only&CoreField.Parent-folder:=[Documents.Virtual-folder.Program:CoreField.Legacy-Identifier={parent_program}]"
				parameters = quote(parameters)
				call = baseurl + datatable + parameters + '&token=' + token

				api_call(call,'Program Folder',program_id)
	file.close()
	logger.info('Done')

# update metadata for the program virtual folders, but first clear out multi-value fields so we're not just appending stuff
def update_folders(token):
	logger.info('Updating program folder metadata...')
	
	with open(directory+'program_data_for_cortex.csv', 'r', encoding='ISO-8859-1') as file:
		csvfile = csv.reader(file)
		next(csvfile)

		for row in csvfile:
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
			parameters = quote(parameters)
			call = baseurl + datatable + parameters + '&token=' + token
			api_call(call,'Program Metadata',ID)	
			
			# update program metadata
			parameters = f"Documents.Virtual-folder.Program:CreateOrUpdate?CoreField.Legacy-Identifier={ID}&NYP.Season+={SEASON}&NYP.Week:={WEEK}&NYP.Orchestra+:={ORCHESTRA_NAME}&NYP.Program-Date(s)++={DATE}&NYP.Program-Date-Range:={DATE_RANGE}&NYP.Program-Times++={PERFORMANCE_TIME}&NYP.Location++={LOCATION_NAME}&NYP.Venue++={VENUE_NAME}&NYP.Event-Type++={SUB_EVENT_NAMES}&NYP.Soloist-/-Instrument++={SOLOIST_SLASH_INSTRUMENT}&NYP.Composer/Work++={COMPOSER_TITLE_SHORT}&NYP.Notes-on-program:={NOTES_XML}&NYP.Composer/Work-Full-Title:={COMPOSER_TITLE}"
			parameters = quote(parameters)
			call = baseurl + datatable + parameters + '&token=' + token
			api_call(call,'Virtual Folder',ID)	

	file.close()
	logger.info('Done')

# create or update the Source accounts that represent people
def create_sources(token):
	logger.info('Creating/updating Source records...')

	with open(directory+'source_accounts_composers.csv', 'r') as file:
		csvfile = csv.reader(file)
		next(csvfile)

		for row in csvfile:
			COMPOSER_ID = row[0]
			DISPLAY = row[1]
			DISPLAY_url = quote(DISPLAY.encode("utf-8"))
			FIRST = row[2]
			FIRST_url = quote(FIRST.encode("utf-8"))
			MIDDLE = row[3]
			MIDDLE_url = quote(MIDDLE.encode("utf-8"))
			LAST = row[4]
			LAST_url = quote(LAST.encode("utf-8"))
			BIRTH = row[5]
			DEATH = row[6]
			ROLES = []

			# We want to preserve the Role field for our Source, which may have other data
			# So we'll do a Read for each Source, grab the Role field, then add any new values to it
			parameters = f'Contacts.Source.Default:Read?CoreField.Composer-ID={COMPOSER_ID}'
			query = baseurl + datatable + parameters + '&token=' + token
			try:
				r = requests.get(query)
			except:
				logger.warning(f'Unable to get Composer ID {COMPOSER_ID}')
				pass
			if r:
				r_string = r.content
				r_xml = ET.fromstring(r_string)
				if r_xml is not None:
					ROLES_xml = r_xml.find('Response').find('Record').find('CoreField.Role')
					if ROLES_xml is not None:
						ROLES = ROLES_xml.text.split('|')

			if 'Composer' not in ROLES:
				ROLES.append('Composer')
				logger.info(f'Adding Composer role to {DISPLAY}')
			
			ROLES = ('|').join(ROLES)

			parameters = f"Contacts.Source.Default:CreateOrUpdate?CoreField.Composer-ID={COMPOSER_ID}&CoreField.First-name:={FIRST_url}&CoreField.Middle-initial:={MIDDLE_url}&CoreField.Last-name:={LAST_url}&CoreField.Display-name:={DISPLAY_url}&CoreField.Birth-Year:={BIRTH}&CoreField.Death-Year:={DEATH}&CoreField.Role:={ROLES}"
			# parameters = quote(parameters)
			call = baseurl + datatable + parameters + '&token=' + token
			api_call(call,'Source: Composer',COMPOSER_ID)
	file.close()
	logger.info('Finished updating Composers')

	logger.info('Starting on Artists...')
	with open(directory+'source_accounts_artists.csv', 'r') as file:
		csvfile = csv.reader(file)
		next(csvfile)

		for row in csvfile:
			ARTIST_ID = row[0]
			DISPLAY = row[1]
			DISPLAY_url = quote(DISPLAY.encode("utf-8"))
			FIRST = row[2]
			FIRST_url = quote(FIRST.encode("utf-8"))
			MIDDLE = row[3]
			MIDDLE_url = quote(MIDDLE.encode("utf-8"))
			LAST = row[4]
			LAST_url = quote(LAST.encode("utf-8"))
			BIRTH = row[5]
			DEATH = row[6]
			ROLES = row[7]
			ORCHESTRA = row[8]
			ORCHESTRA_YEARS = row[9]

			# We want to preserve the Role field for our Source, which may have other data
			# So we'll do a Read for each Source, grab the Role field, then add any new values to it
			parameters = f'Contacts.Source.Default:Read?CoreField.Artist-ID={ARTIST_ID}'
			query = baseurl + datatable + parameters + '&token=' + token
			try:
				r = requests.get(query)
			except:
				logger.warning(f'Unable to get Artist ID {ARTIST_ID}')
				pass
			if r:
				r_string = r.content
				r_xml = ET.fromstring(r_string)
				existing_roles_xml = r_xml.find('Response').find('Record').find('CoreField.Role')
				existing_roles = existing_roles_xml.text.split('|')

				# if we have existing roles for this person, see if the new roles are already there; if not, add them to existing
				new_roles = ROLES.split('|')
				for role in new_roles:
					if role not in existing_roles:
						logger.info(f'Adding new role {role} to {DISPLAY}')
						existing_roles.append(role)
				ROLES = ('|').join(existing_roles)
	
			parameters = f"Contacts.Source.Default:CreateOrUpdate?CoreField.Artist-ID={ARTIST_ID}&CoreField.First-name:={FIRST_url}&CoreField.Middle-initial:={MIDDLE_url}&CoreField.Last-name:={LAST_url}&CoreField.Display-name:={DISPLAY_url}&CoreField.Birth-Year:={BIRTH}&CoreField.Death-Year:={DEATH}&CoreField.Role:={ROLES}&CoreField.Orchestra-Membership:={ORCHESTRA}&CoreField.Orchestra-Membership-Year:={ORCHESTRA_YEARS}"
			# parameters = quote(parameters)
			call = baseurl + datatable + parameters + '&token=' + token
			api_call(call,'Source: Artist',ARTIST_ID)

	file.close()
	logger.info('Done')

# now that the Sources have been created/updated, we can add them to the program virtual folders
def add_sources_to_program(token):
	logger.info('Adding Sources to program folders...')

	with open(directory+'soloists.csv', 'r') as file:
		csvfile = csv.reader(file)
		next(csvfile)

		for row in csvfile:
			Program_ID = row[0]
			Artist_ID = row[1]

			parameters = f"Documents.Virtual-folder.Program:CreateOrUpdate?CoreField.Legacy-Identifier={Program_ID}&NYP.Soloist+=[Contacts.Source.Default:CoreField.Artist-ID={Artist_ID}]"
			parameters = quote(parameters)
			call = baseurl + datatable + parameters + '&token=' + token
			api_call(call,'Virtual Folder',Program_ID)
	file.close()

	with open(directory+'conductors.csv', 'r') as file:
		csvfile = csv.reader(file)
		next(csvfile)

		for row in csvfile:
			Program_ID = row[0]
			Artist_ID = row[1]
			
			parameters = f"Documents.Virtual-folder.Program:CreateOrUpdate?CoreField.Legacy-Identifier={Program_ID}&NYP.Conductor+=[Contacts.Source.Default:CoreField.Artist-ID={Artist_ID}]"
			parameters = quote(parameters)
			call = baseurl + datatable + parameters + '&token=' + token
			api_call(call,'Virtual Folder',Program_ID)
	file.close()

	with open(directory+'composers.csv', 'r') as file:
		csvfile = csv.reader(file)
		next(csvfile)

		for row in csvfile:
			Program_ID = row[0]
			Composer_ID = row[1]
	
			parameters = f"Documents.Virtual-folder.Program:CreateOrUpdate?CoreField.Legacy-Identifier={Program_ID}&NYP.Composer+=[Contacts.Source.Default:CoreField.Composer-ID={Composer_ID}"
			parameters = quote(parameters)
			call = baseurl + datatable + parameters + '&token=' + token
			api_call(call,'Virtual Folder',Program_ID)
	file.close()

def api_call(call,asset_type,ID):
	try: 
		response = requests.get(call)

		# If the response was successful, no Exception will be raised
		response.raise_for_status()
	except HTTPError as http_err:
		logger.error(f'Failed to update {asset_type} {ID} with API call {call}')
		logger.error(f'HTTP error occurred with {asset_type} {ID}: {http_err}')
	except Exception as err:
		logger.error(f'Failed to update {asset_type} {ID} with API call {call}')
		logger.error(f'Other error occurred with {asset_type} {ID}: {err}')
	else:
		logger.info(f'Success updating {asset_type} {ID}')
		return response



############################
## update Cortex metadata ##
############################

# Set up logging (found here: https://fangpenlin.com/posts/2012/08/26/good-logging-practice-in-python/)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# create a file handler
now = datetime.datetime.now()
logfile = directory + 'logs/' + now.strftime("%Y-%m-%d-%H-%M") + '_cortex-updates.log'
handler = logging.FileHandler(logfile)
handler.setLevel(logging.INFO)

# create a logging format
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)

# add the handlers to the logger
logger.addHandler(handler)

# Starting the run
logger.info('=======================')
logger.info('Script started...')

# Run the auth function to get a token
token = auth()

if token != '':
	logger.info('🔑 We have a token! Proceeding...')
	print(f'Your token is: {token}')

	# make_folders(token)
	# update_folders(token)
	# create_sources(token)
	# add_sources_to_program(token)
	
	logger.info('ALL DONE! Bye 👋')

else:
	logger.info('Goodbye')