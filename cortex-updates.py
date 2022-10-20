# Keep Cortex/OrangeDAM synced with Carlos metadata updates
#
# This script will read various CSVs and execute the appropriate API calls to create or update records
#
# by Bill Levay
#
# Make sure you have an .env file in this directory that looks like this:
# login=yourlogin
# password=yourpassword
# directory=/location/of/csvs/
# logs=/location/for/logs/
# baseurl=https://mydomain.org
# datatable=/API/DataTable/v2.2/
################################

import requests, csv, sys, os, time, datetime, logging, json
from urllib.parse import quote
from os.path import join, dirname
from requests.exceptions import HTTPError
import xml.etree.ElementTree as ET
from dotenv import load_dotenv


# First, grab credentials and other values from the .env file in the same folder as this script
dotenv_path = join(dirname(__file__), '.env')
load_dotenv(dotenv_path)

login = os.environ.get('login', 'default')
password = os.environ.get('password', 'default')
password = quote(password) # now URL encoded
directory = os.environ.get('directory', 'default')
logs = os.environ.get('logs', 'default')
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
		logger.info('Authentication successful')

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
			logger.error('Authentication failed')
	else:
		token = ''
		logger.error('Authentication failed')	
	return token


# create or update the program virtual folders
def make_folders(token):
	
	with open(directory+'cortex_folder_names.csv', 'r', encoding='UTF-8') as file:
		csvfile = csv.reader(file)

		# Convert csv.reader object to list so we can loop through it twice
		rows = list(csvfile)

		# Get a count of Programs to be updated and log it
		row_count = len(rows[1:])
		logger.info(f"Creating/updating {row_count} program folders...")

		# Loop through rows for primary folders first and make sure they're created
		count = 1
		total = len(rows[1:])
		percent = round(count/total, 4)*100

		for row in rows[1:]:
			season_folder_id = row[0]
			program_id = row[1]
			folder_name = row[2]
			ordinal = row[3]
			
			if ordinal == 'primary':
				parameters = f"Documents.Virtual-folder.Program:CreateOrUpdate?CoreField.Legacy-Identifier={program_id}&CoreField.Title:={folder_name}&NYP.Program-ID:={program_id}&CoreField.visibility-class:=Internal use only&CoreField.Parent-folder:=[Documents.Virtual-folder.Program:CoreField.Unique-identifier={season_folder_id}]"
				# parameters = quote(parameters)
				call = baseurl + datatable + parameters + '&token=' + token
				logger.info(f'Updating Program {count} of {total} -- {percent}% complete')
				api_call(call,'Program Folder',program_id)
				count += 1
				percent = round(count/total, 4)*100

		# Now loop through again for secondary programs and assign them to the primary folders
		for row in rows[1:]:
			season_folder_id = row[0]
			program_id = row[1]
			folder_name = row[2]
			ordinal = row[3]
			parent_program = row[4]

			if ordinal == 'secondary':

				# check if there's a value for parent_program... if not, kick it back up to the season level
				if parent_program != '':
					parameters = f"Documents.Virtual-folder.Program:CreateOrUpdate?CoreField.Legacy-Identifier={program_id}&CoreField.Title:={folder_name}&NYP.Program-ID:={program_id}&CoreField.visibility-class:=Internal use only&CoreField.Parent-folder:=[Documents.Virtual-folder.Program:CoreField.Legacy-Identifier={parent_program}]"
				else:
					parameters = f"Documents.Virtual-folder.Program:CreateOrUpdate?CoreField.Legacy-Identifier={program_id}&CoreField.Title:={folder_name}&NYP.Program-ID:={program_id}&CoreField.visibility-class:=Internal use only&CoreField.Parent-folder:=[Documents.Virtual-folder.Program:CoreField.Unique-identifier={season_folder_id}]"

				call = baseurl + datatable + parameters + '&token=' + token
				logger.info(f'Updating Program {count} of {total} -- {percent}% complete')
				api_call(call,'Program Folder',program_id)
				count +=1
				percent = round(count/total, 4)*100

	file.close()
	logger.info('Done')


# update metadata for the program virtual folders, but first clear out multi-value fields so we're not just appending stuff
def update_folders(token):
	logger.info('Updating program folder metadata...')
	
	with open(directory+'program_data_for_cortex.csv', 'r', encoding='UTF-8') as file:
		csvfile = csv.reader(file)
		next(csvfile)

		count = 1
		records = list(csvfile)
		total = len(records[1:])
		percent = round(count/total, 4)*100

		for row in records[1:]:
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
			COMPOSER_TITLE = row[11].replace('|','\n\n').replace('Intermission, / .','Intermission').replace('<','').replace('>','')
			COMPOSER_TITLE_SHORT = row[12].replace('<','').replace('>','')
			NOTES_XML = row[13].replace('<br>','\n')

			# get the Digital Archives (Hadoop) ID from public Solr
			lookup = f"http://proslrapp01.nyphil.live:9993/solr/assets/select?q=npp%5C%3AProgramID%3A{ID}&fl=id&wt=json"
			try:
				response = requests.get(lookup)
				# If the response was successful, no Exception will be raised
				response.raise_for_status()
			except HTTPError as http_err:
				logger.error(f'Failed to get Solr data for program {ID} - HTTP error occurred: {http_err}')
			except Exception as err:
				logger.error(f'Failed to get Solr data for program {ID} - Other error occurred: {err}')
			else:
				logger.info(f'Success retrieving Solr data for program {ID}')

			if response:
				# parse JSON results
				response = response.json()
				if response["response"]["numFound"] == 1:
					digarch_id = response["response"]["docs"][0]["id"]
				else:
					digarch_id = ''
			else:
				digarch_id = ''

			# if ID in update_list:
			if ID !='':

				# Create the dict
				data = {
					'CoreField.Legacy-Identifier': ID,
					'NYP.Season+': SEASON,
					'NYP.Week+:': WEEK,
					'NYP.Orchestra:': ORCHESTRA_NAME,
					'NYP.Program-Date(s)++': DATE,
					'NYP.Program-Date-Range:': DATE_RANGE,
					'NYP.Program-Times++': PERFORMANCE_TIME,
					'NYP.Location++': LOCATION_NAME,
					'NYP.Venue++': VENUE_NAME,
					'NYP.Event-Type++': SUB_EVENT_NAMES,
					'NYP.Soloist-/-Instrument++': SOLOIST_SLASH_INSTRUMENT,
					'NYP.Composer/Work++': COMPOSER_TITLE_SHORT,
					'NYP.Composer/Work-Full-Title:': COMPOSER_TITLE,
					'NYP.Notes-on-program:': NOTES_XML,
					'NYP.Digital-Archives-ID:': digarch_id,
				}
				# fix for linebreaks and such - dump to string and load back to JSON
				data = json.dumps(data)
				data = json.loads(data)

				# log some info
				logger.info(f'Updating Program {count} of {total} = {percent}% complete')

				# clear values from program folders
				parameters = f"Documents.Virtual-folder.Program:Update?CoreField.Legacy-Identifier={ID}&NYP.Season--=&NYP.Program-Date(s)--=&NYP.Program-Times--=&NYP.Location--=&NYP.Venue--=&NYP.Event-Type--=&NYP.Soloist-/-Instrument--=&NYP.Composer/Work--=&NYP.Soloist--=&NYP.Conductor--=&NYP.Composer--="
				call = baseurl + datatable + parameters + '&token=' + token
				api_call(call,'Program - clear old metadata',ID)
				
				# update program metadata with token as a parameter and dict as body
				action = 'Documents.Virtual-folder.Program:Update'
				params = {'token': token}
				url = baseurl + datatable + action
				api_call_ext(url,params,data,'Program - add new metadata',ID)

				count += 1
				percent = round(count/total, 4)*100

	file.close()
	logger.info('Done')


# create or update the Source accounts that represent people
def create_sources(token):
	logger.info('Creating/updating Source records...')

	with open(directory+'source_accounts_composers.csv', 'r', encoding="UTF-8") as file:
		csvfile = csv.reader(file)
		next(csvfile)

		for row in csvfile:
			COMPOSER_ID = row[0]
			DISPLAY = row[1]
			DISPLAY_url = quote(DISPLAY, encoding='UTF-8')
			FIRST = row[2].replace('"',"'").replace('&','and')
			FIRST_url = quote(FIRST, encoding='UTF-8')
			MIDDLE = row[3]
			MIDDLE_url = quote(MIDDLE, encoding='UTF-8')
			LAST = row[4].replace('"',"'").replace('&','and')
			LAST_url = quote(LAST, encoding='UTF-8')
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
				if r_xml is not None and r_xml.find('Response').text is not None:
					ROLES_xml = r_xml.find('Response').find('Record').find('CoreField.Role')
					if ROLES_xml is not None:
						ROLES = ROLES_xml.text.split('|')

			if 'Composer' not in ROLES:
				ROLES.append('Composer')
				logger.info(f'Adding Composer role to {DISPLAY}')
			
			ROLES = ('|').join(ROLES)

			parameters = f"Contacts.Source.Default:CreateOrUpdate?CoreField.Composer-ID={COMPOSER_ID}&CoreField.First-name:={FIRST_url}&CoreField.Middle-initial:={MIDDLE_url}&CoreField.Last-name:={LAST_url}&CoreField.Display-name:={DISPLAY_url}&CoreField.Birth-Year:={BIRTH}&CoreField.Death-Year:={DEATH}&CoreField.Role:={ROLES}"
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
			DISPLAY_url = quote(DISPLAY, encoding='UTF-8')
			FIRST = row[2].replace('"',"'").replace('&','and')
			FIRST_url = quote(FIRST, encoding='UTF-8')
			MIDDLE = row[3]
			MIDDLE_url = quote(MIDDLE, encoding='UTF-8')
			LAST = row[4].replace('"',"'").replace('&','and')
			LAST_url = quote(LAST, encoding='UTF-8')
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
				if r_xml is not None and r_xml.find('Response').text is not None:
					existing_roles_xml = r_xml.find('Response').find('Record').find('CoreField.Role')
					if existing_roles_xml is not None:
						existing_roles = existing_roles_xml.text.split('|')
					else:
						existing_roles = []

					# if we have existing roles for this person, see if the new roles are already there; if not, add them to existing
					new_roles = ROLES.split('|')
					for role in new_roles:
						if role not in existing_roles and role != '':
							logger.info(f'Adding new role {role} to {DISPLAY}')
							existing_roles.append(role)
					ROLES = ('|').join(existing_roles)
	
			parameters = f"Contacts.Source.Default:CreateOrUpdate?CoreField.Artist-ID={ARTIST_ID}&CoreField.First-name:={FIRST_url}&CoreField.Middle-initial:={MIDDLE_url}&CoreField.Last-name:={LAST_url}&CoreField.Display-name:={DISPLAY_url}&CoreField.Birth-Year:={BIRTH}&CoreField.Death-Year:={DEATH}&CoreField.Role:={ROLES}&CoreField.Orchestra-Membership:={ORCHESTRA}&CoreField.Orchestra-Membership-Year:={ORCHESTRA_YEARS}"
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

			# add new values
			parameters = f"Documents.Virtual-folder.Program:Update?CoreField.Legacy-Identifier={Program_ID}&NYP.Soloist+=[Contacts.Source.Default:CoreField.Artist-ID={Artist_ID}]"
			call = baseurl + datatable + parameters + '&token=' + token
			api_call(call,'Program - add soloists',Program_ID)
	file.close()

	with open(directory+'conductors.csv', 'r') as file:
		csvfile = csv.reader(file)
		next(csvfile)

		for row in csvfile:
			Program_ID = row[0]
			Artist_ID = row[1]
			
			parameters = f"Documents.Virtual-folder.Program:Update?CoreField.Legacy-Identifier={Program_ID}&NYP.Conductor+=[Contacts.Source.Default:CoreField.Artist-ID={Artist_ID}]"
			call = baseurl + datatable + parameters + '&token=' + token
			api_call(call,'Program - add conductor',Program_ID)
	file.close()

	with open(directory+'composers.csv', 'r') as file:
		csvfile = csv.reader(file)
		next(csvfile)

		for row in csvfile:
			Program_ID = row[0]
			Composer_ID = row[1]
	
			parameters = f"Documents.Virtual-folder.Program:Update?CoreField.Legacy-Identifier={Program_ID}&NYP.Composer+=[Contacts.Source.Default:CoreField.Composer-ID={Composer_ID}]"
			call = baseurl + datatable + parameters + '&token=' + token
			api_call(call,'Program - add composers',Program_ID)
	file.close()

# do the API call
def api_call(call,asset_type,ID):
	try:
		response = requests.post(call)

		# If the response was successful, no Exception will be raised
		response.raise_for_status()
	except HTTPError as http_err:
		logger.error(f'Failed to update {asset_type} {ID} - HTTP error occurred: {http_err}')
	except Exception as err:
		logger.error(f'Failed to update {asset_type} {ID} - Other error occurred: {err}')
	else:
		logger.info(f'Success updating {asset_type} {ID}')
		return response

# API call with params and body
def api_call_ext(url,params,data,asset_type,ID):
	try:
		response = requests.post(url, params=params, data=data)

		# If the response was successful, no Exception will be raised
		response.raise_for_status()
	except HTTPError as http_err:
		logger.error(f'Failed to update {asset_type} {ID} - HTTP error occurred: {http_err}')
	except Exception as err:
		logger.error(f'Failed to update {asset_type} {ID} - Other error occurred: {err}')
	else:
		logger.info(f'Success updating {asset_type} {ID}')
		return response


############################
## update Cortex metadata ##
############################

# Find the previous log file and rename it
filepath = logs + 'cortex-updates.log'

if os.path.exists(filepath):
	# Get the date modified value of the previous log file and format as string
	# Borrowed from here: https://www.geeksforgeeks.org/how-to-get-file-creation-and-modification-date-or-time-in-python/
	mod_time = os.path.getmtime(filepath) 
	mod_timestamp = time.ctime(mod_time)
	time_obj = time.strptime(mod_timestamp)
	time_string = time.strftime("%Y-%m-%d-%H-%M", time_obj)
	new_filepath = logs + time_string + '_cortex-updates.log'
	# Rename the last log file
	os.rename(filepath, new_filepath)

# Set up logging (found here: https://fangpenlin.com/posts/2012/08/26/good-logging-practice-in-python/)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# create a file handler
logfile = logs + 'cortex-updates.log'
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

# update_list = ['7878','7877']

# Run the auth function to get a token
token = auth()

if token and token != '':
	logger.info(f'We have a token: {token} Proceeding...')
	print(f'Your token is: {token}')

	make_folders(token)
	update_folders(token)
	create_sources(token)
	add_sources_to_program(token)
	
	logger.info('ALL DONE! Bye bye :)')

else:
	logger.info('No API token :( Goodbye')