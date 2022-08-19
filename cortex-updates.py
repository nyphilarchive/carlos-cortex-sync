# Cortex API calls for Carlos metadata updates
# This script will read various CSVs and execute the appropriate API calls to create or update records
# Make sure you have an .env file in this directory that looks like this:
# login = 'yourlogin'
# password = 'yourpassword'
# by Bill Levay

import requests, csv, sys, os
from os.path import join, dirname
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

	auth_string = "/API/Authentication/v1.0/Login?Login={}&Password={}".format(login, password)

	# send the POST request
	response = requests.request('POST', baseurl+auth_string)
	print(response.text)

	# parse the XML response
	response_string = response.content
	response_xml = ET.fromstring(response_string)
	code = response_xml.find('APIResponse').find('Code')
	if code.text == 'SUCCESS':
		token = response_xml.find('APIResponse').find('Token').text
	else:
		print("Authentication failed :( Bye!")
		sys.exit()
	return token

# create or update the program virtual folders
def make_folders(token):
	with open(directory+'cortex_folder_names.csv', 'r') as file:
		csvfile = csv.reader(file)
		for row in csvfile:
			season_folder_id = row[0]
			program_id = row[1]
			folder_name = row[2]
			ordinal = row[3]
			parent_program = row[4]

			if ordinal == 'primary':
				parameters = 'Documents.Virtual-folder.Program:CreateOrUpdate?CoreField.Legacy-Identifier={}&CoreField.Title:={}&NYP.Program-ID:={}&CoreField.Parent-folder:=[Documents.Virtual-folder.Program:CoreField.Unique-identifier={}]'.format(program_id,folder_name,program_id,season_folder_id)
				print(baseurl + datatable + parameters + '&token=' + token)
			elif ordinal == 'secondary':
				parameters = 'Documents.Virtual-folder.Program:CreateOrUpdate?CoreField.Legacy-Identifier={}&CoreField.Title:={}&NYP.Program-ID:={}&CoreField.Parent-folder:=[Documents.Virtual-folder.Program:CoreField.Legacy-Identifier={}]'.format(program_id,folder_name,program_id,parent_program)
				print(baseurl + datatable + parameters + '&token=' + token)
	file.close()

# update metadata for the program virtual folders, but first clear out multi-value fields so we're not just appending stuff
def update_folders(token):
	with open(directory+'program_data_for_cortex.csv', 'r', encoding='ISO-8859-1') as file:
		csvfile = csv.reader(file)
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
			parameters = 'Documents.Virtual-folder.Program:CreateOrUpdate?CoreField.Legacy-Identifier={}&NYP.Season--=&NYP.Orchestra--=&NYP.Program-Date(s)--=&NYP.Program-Times--=&NYP.Location--=&NYP.Venue--=&NYP.Event-Type--=&NYP.Soloist-/-Instrument--=&NYP.Composer/Work--=&NYP.Soloist--=&NYP.Conductor--=&NYP.Composer--='.format(ID)
			print(baseurl + datatable + parameters + '&token=' + token)
	
			# update program metadata
			parameters = 'Documents.Virtual-folder.Program:CreateOrUpdate?CoreField.Legacy-Identifier={}&NYP.Season+={}&NYP.Week:={}&NYP.Orchestra+:={}&NYP.Program-Date(s)++={}&NYP.Program-Date-Range:={}&NYP.Program-Times++={}&NYP.Location++={}&NYP.Venue++={}&NYP.Event-Type++={}&NYP.Soloist-/-Instrument++={}&NYP.Composer/Work++={}&NYP.Notes-on-program:={}&NYP.Composer/Work-Full-Title:={}'.format(ID,SEASON,WEEK,ORCHESTRA_NAME,DATE,DATE_RANGE,PERFORMANCE_TIME,LOCATION_NAME,VENUE_NAME,SUB_EVENT_NAMES,SOLOIST_SLASH_INSTRUMENT,COMPOSER_TITLE_SHORT,NOTES_XML,COMPOSER_TITLE)
			print(baseurl + datatable + parameters + '&token=' + token)
	file.close()

# create or update the Source accounts that represent people
def create_sources(token):

	# We don't want to always overwrite the Role field
	# So we'll do a query for each Source, grab that field, parse it, then add any new values to it

	with open(directory+'source_accounts_composers.csv', 'r') as file:
		csvfile = csv.reader(file)
		for row in csvfile:
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

			parameters = 'Contacts.Source.Default:CreateOrUpdate?CoreField.Composer-ID={}&CoreField.First-name:={}&CoreField.Middle-initial:={}&CoreField.Last-name:={}&CoreField.Display-name:={}&CoreField.Birth-Year:={}&CoreField.Death-Year:={}'.format(COMPOSER_ID,FIRST,MIDDLE,LAST,DISPLAY,BIRTH,DEATH)
			print(baseurl + datatable + parameters + '&token=' + token)
	file.close()

	with open(directory+'source_accounts_artists.csv', 'r') as file:
		csvfile = csv.reader(file)
		for row in csvfile:
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
	
			parameters = 'Contacts.Source.Default:CreateOrUpdate?CoreField.Artist-ID={}&CoreField.First-name:={}&CoreField.Middle-initial:={}&CoreField.Last-name:={}&CoreField.Display-name:={}&CoreField.Birth-Year:={}&CoreField.Death-Year:={}&CoreField.Role:={}&CoreField.Orchestra-Membership:={}&CoreField.Orchestra-Membership-Year:={}'.format(ARTIST_ID,FIRST,MIDDLE,LAST,DISPLAY,BIRTH,DEATH,ROLE,ORCHESTRA,ORCHESTRA_YEARS)
			print(baseurl + datatable + parameters + '&token=' + token)
	file.close()

# now that the Sources have been created/updated, we can add them to the program virtual folders
def add_sources_to_program(token):
	with open(directory+'soloists.csv', 'r') as file:
		csvfile = csv.reader(file)
		for row in csvfile:
			Program_ID = row[0]
			Artist_ID = row[1]

			parameters = 'Documents.Virtual-folder.Program:CreateOrUpdate?CoreField.Legacy-Identifier={}&NYP.Soloist+=[Contacts.Source.Default:CoreField.Artist-ID={}]'.format(Program_ID,Artist_ID)
			print(baseurl + datatable + parameters + '&token=' + token)
	file.close()

	with open(directory+'conductors.csv', 'r') as file:
		csvfile = csv.reader(file)
		for row in csvfile:
			Program_ID = row[0]
			Artist_ID = row[1]
			
			parameters = 'Documents.Virtual-folder.Program:CreateOrUpdate?CoreField.Legacy-Identifier={}&NYP.Conductor+=[Contacts.Source.Default:CoreField.Artist-ID={}]'.format(Program_ID,Artist_ID)
			print(baseurl + datatable + parameters + '&token=' + token)
	file.close()

	with open(directory+'composers.csv', 'r') as file:
		csvfile = csv.reader(file)
		for row in csvfile:
			Program_ID = row[0]
			Composer_ID = row[1]
	
			parameters = 'Documents.Virtual-folder.Program:CreateOrUpdate?CoreField.Legacy-Identifier={}&NYP.Composer+=[Contacts.Source.Default:CoreField.Composer-ID={}'.format(Program_ID,Composer_ID)
			print(baseurl + datatable + parameters + '&token=' + token)
	file.close()

############################
## update Cortex metadata ##
############################

token = ''
token = auth()
make_folders(token)
update_folders(token)
create_sources(token)
add_sources_to_program(token)