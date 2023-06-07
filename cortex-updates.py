"""
Keep Cortex/OrangeDAM synced with Carlos metadata updates

This script will read various CSVs and execute the appropriate API calls to create or update records

by Bill Levay

Make sure you have an .env file in this directory that looks like this:
 login=yourCortexLogin
 password=yourCortexPassword
 directory=/location/of/csvs/
 library=/location/of/printed/music/xml/
 logs=/location/for/logs/
 baseurl=https://mydomain.org
 datatable=/API/DataTable/v2.2/
"""

import requests, csv, sys, os, time, datetime, logging, json, re
from urllib.parse import quote
from os.path import join, dirname
from requests.exceptions import HTTPError
from dotenv import load_dotenv
from lxml import etree


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
library = os.environ.get('library', 'default')

# Some helper functions for cleanup
def replace_angle_brackets(text):
    # Define a regular expression pattern to match angle brackets and the enclosed text
    pattern = r'<(.*?)>'
    
    # Define a replacement function that adds the appropriate HTML tags
    def replace(match):
        return '<em>{}</em>'.format(match.group(1))
    
    # Use re.sub() to replace the matched patterns with the appropriate HTML tags
    replaced_text = re.sub(pattern, replace, text)
    
    return replaced_text

# get a new token from the Login API
def auth():

	auth_string = f"/API/Authentication/v1.0/Login?Login={login}&Password={password}&format=json"

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
		# parse the JSON response
		json_data = response.json()
		if json_data['APIResponse']['Code'] == 'SUCCESS':
			token = json_data['APIResponse']['Token']
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

			"""
			See if this folder already exists in Cortex and what the parent folder is
			If the folder exists and the existing parent is the same as the parent in the CSV, we can skip that parameter in the API call
			This will prevent Cortex from changing the manual order of the program within the Season folder
			"""
			parameters = f'CoreField.Legacy-Identifier:{program_id} DocSubType:Program&fields=Document.LineageParentName&format=json'
			query = f'{baseurl}/API/search/v3.0/search?query={parameters}&token={token}'
			try:
				r = requests.get(query)
			except:
				logger.warning(f'Unable to find Program ID {program_id}')
				pass

			if r:
				r_data = r.json()
				if r_data['APIResponse']['GlobalInfo']['TotalCount'] == 1:
					# We got one result, which is good
					existing_parent_id = r_data['APIResponse']['Items'][0]["Document.LineageParentName"]
				else:
					# We have no result, or more than one parent, so we'll assign the parent from the CSV as usual
					existing_parent_id = ''
			else:
				existing_parent_id = ''
				logger.warning(f'Unable to find Program ID {program_id}')

			"""
			Now we can compare the parent_id from Cortex to the CSV
			If the values match, do not update this field
			"""
			if existing_parent_id == season_folder_id:
				update_parent = ''
			else:
				update_parent = f'&CoreField.Parent-folder:=[Documents.Virtual-folder.Season:CoreField.Legacy-identifier={season_folder_id}]'
			
			# loop through the programs
			parameters = f"Documents.Virtual-folder.Program:CreateOrUpdate?CoreField.Legacy-Identifier={program_id}&CoreField.Title:={folder_name}&NYP.Program-ID:={program_id}&CoreField.visibility-class:=Public{update_parent}"
			call = f'{baseurl}{datatable}{parameters}&token={token}'
			logger.info(f'Updating Program {count} of {total} -- {percent}% complete')
			api_call(call,'Program Folder',program_id)
			count += 1
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
			COMPOSER_TITLE = row[11].replace('|','\n\n').replace('Intermission, / .','Intermission').replace('<','').replace('>','')
			COMPOSER_TITLE_SHORT = row[12].replace('<','').replace('>','')
			NOTES_XML = row[13].replace('<br>','\n')

			if int(SEASON[0:4]) < 1940:

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
					response = ''

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
					parameters = f"Documents.Virtual-folder.Program:Update?CoreField.Legacy-Identifier={ID}&NYP.Season--=&NYP.Program-Date(s)--=&NYP.Program-Times--=&NYP.Location--=&NYP.Venue--=&NYP.Event-Type--=&NYP.Composer/Work--=&NYP.Soloist--=&NYP.Conductor--=&NYP.Composer--="
					call = baseurl + datatable + parameters + '&token=' + token
					api_call(call,'Program - clear old metadata',ID)
					
					# update program metadata with token as a parameter and dict as body
					action = 'Documents.Virtual-folder.Program:Update'
					params = {'token': token}
					url = baseurl + datatable + action
					api_call(url,'Program - add new metadata',ID,params,data)

					count += 1
					percent = round(count/total, 4)*100

	file.close()
	logger.info('Done')


# create or update the Source accounts that represent people
def create_sources(token):
	logger.info('Creating/updating Source records...')

	with open(f'{directory}source_accounts_composers.csv', 'r', encoding="UTF-8") as file:
		csvfile = csv.reader(file)
		next(csvfile)

		for row in csvfile:
			COMPOSER_ID = row[0]
			FIRST = row[1].replace('"',"'").replace('&','and')
			FIRST_url = quote(FIRST, encoding='UTF-8')
			MIDDLE = row[2]
			MIDDLE_url = quote(MIDDLE, encoding='UTF-8')
			LAST = row[3].replace('"',"'").replace('&','and')
			LAST_url = quote(LAST, encoding='UTF-8')
			BIRTH = row[4]
			DEATH = row[5]
			ROLES = []

			# We want to preserve the Role field for our Source, which may have other data
			# So we'll do a Read for each Source, grab the Role field, then add any new values to it
			parameters = f'Contacts.Source.Default:Read?CoreField.Composer-ID={COMPOSER_ID}&format=json'
			query = f"{baseurl}{datatable}{parameters}&token={token}"
			response = api_call(query,'Getting Roles for Composer',COMPOSER_ID)

			if response:
				response_data = response.json()
				if response_data is not None and response_data['ResponseSummary']['TotalItemCount'] > 0:
					existing_roles = response_data['Response'][0].get('CoreField.Role')
					if existing_roles:
						ROLES = existing_roles.split('|')

			if 'Composer' not in ROLES:
				ROLES.append('Composer')
				logger.info(f'Adding Composer role to {FIRST} {LAST}')
			
			ROLES = ('|').join(ROLES)

			parameters = f"Contacts.Source.Default:CreateOrUpdate?CoreField.Composer-ID={COMPOSER_ID}&CoreField.First-name:={FIRST_url}&CoreField.Middle-initial:={MIDDLE_url}&CoreField.Last-name:={LAST_url}&CoreField.Birth-Year:={BIRTH}&CoreField.Death-Year:={DEATH}&CoreField.Role:={ROLES}"
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
			FIRST = row[1].replace('"',"'").replace('&','and')
			FIRST_url = quote(FIRST, encoding='UTF-8')
			MIDDLE = row[2]
			MIDDLE_url = quote(MIDDLE, encoding='UTF-8')
			LAST = row[3].replace('"',"'").replace('&','and')
			LAST_url = quote(LAST, encoding='UTF-8')
			BIRTH = row[4]
			DEATH = row[5]
			ROLES = row[6]
			ORCHESTRA = row[7]
			ORCHESTRA_YEARS = row[8]

			# We want to preserve the Role field for our Source, which may have other data
			# So we'll do a Read for each Source, grab the Role field, then add any new values to it
			parameters = f'Contacts.Source.Default:Read?CoreField.Artist-ID={ARTIST_ID}&format=json'
			query = baseurl + datatable + parameters + '&token=' + token
			response = api_call(query,'Getting Roles for Artist',ARTIST_ID)

			if response:
				response_data = response.json()
				if response_data is not None and response_data['ResponseSummary']['TotalItemCount'] > 0:
					existing_roles = response_data['Response'][0].get('CoreField.Role')
					if existing_roles:
						existing_roles = existing_roles.split('|')
					else:
						existing_roles = []

					# if we have existing roles for this person, see if the new roles are already there; if not, add them to existing
					new_roles = ROLES.split('|')
					for role in new_roles:
						if role not in existing_roles and role != '':
							logger.info(f'Adding new role {role} to {FIRST} {LAST}')
							existing_roles.append(role)
					ROLES = ('|').join(existing_roles)
	
			parameters = f"Contacts.Source.Default:CreateOrUpdate?CoreField.Artist-ID={ARTIST_ID}&CoreField.First-name:={FIRST_url}&CoreField.Middle-initial:={MIDDLE_url}&CoreField.Last-name:={LAST_url}&CoreField.Birth-Year:={BIRTH}&CoreField.Death-Year:={DEATH}&CoreField.Role:={ROLES}&CoreField.Orchestra-Membership:={ORCHESTRA}&CoreField.Orchestra-Membership-Year:={ORCHESTRA_YEARS}"
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
			api_call(call,f'Add soloist {Artist_ID} to Program',Program_ID)
	file.close()

	with open(directory+'conductors.csv', 'r') as file:
		csvfile = csv.reader(file)
		next(csvfile)

		for row in csvfile:
			Program_ID = row[0]
			Artist_ID = row[1]
			
			parameters = f"Documents.Virtual-folder.Program:Update?CoreField.Legacy-Identifier={Program_ID}&NYP.Conductor+=[Contacts.Source.Default:CoreField.Artist-ID={Artist_ID}]"
			call = baseurl + datatable + parameters + '&token=' + token
			api_call(call,f'Add conductor {Artist_ID} to Program',Program_ID)
	file.close()

	with open(directory+'composers.csv', 'r') as file:
		csvfile = csv.reader(file)
		next(csvfile)

		for row in csvfile:
			Program_ID = row[0]
			Composer_ID = row[1]
	
			parameters = f"Documents.Virtual-folder.Program:Update?CoreField.Legacy-Identifier={Program_ID}&NYP.Composer+=[Contacts.Source.Default:CoreField.Composer-ID={Composer_ID}]"
			call = baseurl + datatable + parameters + '&token=' + token
			api_call(call,f'Add composer {Composer_ID} to Program',Program_ID)
	file.close()

# Let's update Scores and Parts
def library_updates(token):

	def xpath_text(element, path):
		value = element.xpath(path)
		return value[0] if value else ''

	# parse the XML file
	tree = etree.parse(f'{library}library_updates.xml')
	root = tree.getroot()

	# parse each row in the XML and assign values to variables
	for row in root.xpath(".//row"):
		legacy_id = xpath_text(row, "id/text()")

		# temporary addition to fix just a few Library records in Cortex
		# once updated, remove this row and un-tab rest of this function
		composer_id = xpath_text(row, "composer_id/text()")
		notes_xml = xpath_text(row, "notes_xml/text()").replace('<br>','\n')
		notes_xml = replace_angle_brackets(notes_xml)
		publisher_name = xpath_text(row, "publisher_name/text()")
		composer_name = xpath_text(row, "composer_name/text()").replace('  ', ' ')
		composer_first = xpath_text(row, "composer_first_name/text()")
		composer_middle = xpath_text(row, "composer_middle_name/text()")
		composer_last = xpath_text(row, "composer_last_name/text()")
		ar_works_title = xpath_text(row, "ar_works_title/text()")
		composer_name_title = xpath_text(row, "composer_name_title/text()").replace('  ', ' ')
		composer_name_title = replace_angle_brackets(composer_name_title)
		usedby_ids = [xpath_text(tag, "text()") for tag in row.xpath("usedby_id")]
		score_id_display = xpath_text(row, "score_id_display/text()")
		score_location = xpath_text(row, "score_location/text()")
		score_marking_ids = xpath_text(row, "score_marking_ids/text()").split(';')
		score_edition_type = xpath_text(row, "score_edition_type_desc/text()")

		# these part variables will be present for each part
		part_id_display = [xpath_text(tag, "text()") for tag in row.xpath("part_id_display")]
		part_location = [xpath_text(tag, "text()") for tag in row.xpath("part_location")]
		part_type_desc = [xpath_text(tag, "text()") for tag in row.xpath("part_type_desc")]
		part_edition_type = [xpath_text(tag,"text()") for tag in row.xpath("part_edition_type_desc")]
		
		# these part variables may not be present for each part
		part_stand_notes = [xpath_text(tag, "text()") for tag in row.xpath("part_stand_notes")]
		
		# these part variables may not be present for each part, and some may have multiple values separated by a semi-colon
		part_marking_ids = row.xpath("part_marking_ids")
		part_marking_ids_list = []
		for tag in part_marking_ids:
		    text = tag.text.strip() if tag.text else ""
		    if text:
		        values = [val.strip() for val in text.split(";")]
		        part_marking_ids_list.append(values if len(values) > 1 else values[0])
		    else:
		        part_marking_ids_list.append("")
		
		# Determine the display "suffix" for the asset title
		if score_id_display and part_id_display[0]:
			display = "Score and Parts"
		elif score_id_display and not part_id_display[0]:
			display = "Score"
		elif not score_id_display and part_id_display[0]:
			display = "Parts"
		else:
			display = ""

		# Format the asset title
		title = f"{composer_name} / {ar_works_title} - {display}"

		# Create/Update the Printed Music record

		# clear old values
		parameters = (
			f"Documents.Folder.Printed-Music:CreateOrUpdate"
			f"?CoreField.Legacy-Identifier={legacy_id}"
			f"&NYP.Composer/Work--=&NYP.Marking-Artist--=&NYP.Conductor--=&NYP.Composer--="
		)
		url = f"{baseurl}{datatable}{parameters}&token={token}"
		api_call(url,'Clear old metadata on Printed Music folder',legacy_id)

		# Send this data as a JSON payload because it might exceed the character limit for a regular API call
		data = {
			'CoreField.Legacy-Identifier': legacy_id,
			'CoreField.parent-folder:': '[Documents.All:CoreField.Identifier=PH1N31F]',
			'CoreField.Title:': title,
			'NYP.Publisher+:': publisher_name,
			'CoreField.Notes:': notes_xml,
			'NYP.Composer/Work++': f'{composer_name} / {ar_works_title}',
			'NYP.Composer/Work-Full-Title:': composer_name_title
		}
		# fix for linebreaks and such - dump to string and load back to JSON
		data = json.dumps(data)
		data = json.loads(data)

		action = 'Documents.Folder.Printed-Music:CreateOrUpdate'
		params = {'token': token}
		url = f"{baseurl}{datatable}{action}"
		api_call(url,'Create/Update Printed Music folder',legacy_id,params,data)

		# link composer to record
		parameters = (
			f"Documents.Folder.Printed-Music:Update"
			f"?CoreField.Legacy-Identifier={legacy_id}"
			f"&NYP.Composer+=[Contacts.Source.Default:CoreField.Composer-ID={composer_id}]"
		)
		url = f"{baseurl}{datatable}{parameters}&token={token}"
		api_call(url, f'Link composer {composer_id} to the Printed Music folder',legacy_id)

		# link the score marking artist(s) to the library record
		if score_marking_ids[0]:
			for marking_id in score_marking_ids:

				# first see if the marking artist exists in Cortex
				parameters = f'Contacts.Source.Default:Read?CoreField.Artist-ID={marking_id}&format=json'
				query = baseurl + datatable + parameters + '&token=' + token
				response = api_call(query,'Checking if Marking Artist exists',marking_id)

				if response:
					response_data = response.json()
					if response_data is not None and response_data['ResponseSummary']['TotalItemCount'] > 0:
						logger.info(f'Artist {marking_id} already exists in Cortex')
					else:
						#do a Create/Update on the artist
						parameters = (
							f"Contacts.Source.Default:CreateOrUpdate?CoreField.Artist-ID={marking_id}"
						)
						url = f"{baseurl}{datatable}{parameters}&token={token}"
						api_call(url, 'Create/update Source record for artist', marking_id)

				# now link them to the record
				parameters = (
					f"Documents.Folder.Printed-Music:Update"
					f"?CoreField.Legacy-Identifier={legacy_id}"
					f"&NYP.Marking-Artist+=[Contacts.Source.Default:CoreField.Artist-ID={marking_id}]"
				)
				url = f"{baseurl}{datatable}{parameters}&token={token}"
				api_call(url, f'Link marking artist {marking_id} to Printed Music folder', legacy_id)
		else:
			logger.info(f"No score marking artists for Library record {legacy_id}")

		# link the "parts used by" artists to the record
		if usedby_ids[0]:
			for user in usedby_ids:
				parameters = (
					f"Documents.Folder.Printed-Music:Update"
					f"?CoreField.Legacy-Identifier={legacy_id}"
					f"&NYP.Conductor+=[Contacts.Source.Default:CoreField.Artist-ID={user}]"
				)
				url = f"{baseurl}{datatable}{parameters}&token={token}"
				api_call(url, f'Link Conductor {user} to the Printed Music folder',legacy_id)
		else:
			logger.info(f"No Parts Used By conductor for record {legacy_id}")

		# Do the Score call, if there is a score
		if score_id_display:

			# clear old values
			parameters = (
				f"Documents.Folder.Score:CreateOrUpdate"
				f"?CoreField.Legacy-Identifier=MS_{legacy_id}"
				f"&NYP.Composer/Work--=&NYP.Marking-Artist--=&NYP.Composer--="
			)
			url = f"{baseurl}{datatable}{parameters}&token={token}"
			api_call(url,'Clear old metadata on Score',legacy_id)

			# Send this data as a JSON payload because it might exceed the character limit for a regular API call
			data = {
				'CoreField.Legacy-Identifier': f'MS_{legacy_id}',
				'CoreField.parent-folder:': f'[Documents.Folder.Printed-Music:CoreField.Legacy-Identifier={legacy_id}]',
				'CoreField.Title:': f'MS_{legacy_id}',
				'NYP.Publisher+:': publisher_name,
				'NYP.Edition-Type+:': score_edition_type,
				'CoreField.Notes:': notes_xml,
				'NYP.Composer/Work++': f'{composer_name} / {ar_works_title}',
				'NYP.Composer/Work-Full-Title:': composer_name_title,
				'NYP.Archives-location:': score_location
			}
			# fix for linebreaks and such - dump to string and load back to JSON
			data = json.dumps(data)
			data = json.loads(data)

			action = 'Documents.Folder.Score:CreateOrUpdate'
			params = {'token': token}
			url = f"{baseurl}{datatable}{action}"
			api_call(url, f'Create/Update Score MS_{legacy_id} in Printed Music folder', legacy_id, params, data)

			# link the composer to the score
			parameters = (
				f"Documents.Folder.Score:Update"
				f"?CoreField.Legacy-Identifier=MS_{legacy_id}"
				f"&NYP.Composer+=[Contacts.Source.Default:CoreField.Composer-ID={composer_id}]"
			)
			url = f"{baseurl}{datatable}{parameters}&token={token}"
			api_call(url, f'Link composer {composer_id} to Score MS_{legacy_id} in Printed Music folder', legacy_id)

			# link the marking artist(s) to the score if there are any
			if score_marking_ids[0]:
				for marking_id in score_marking_ids:

					# now link them to the record
					parameters = (
						f"Documents.Folder.Score:Update"
						f"?CoreField.Legacy-Identifier=MS_{legacy_id}"
						f"&NYP.Marking-Artist+=[Contacts.Source.Default:CoreField.Artist-ID={marking_id}]"
					)
					url = f"{baseurl}{datatable}{parameters}&token={token}"
					api_call(url, f'Link marking artist {marking_id} to Score MS_{legacy_id} in Printed Music folder', legacy_id)
			else:
				logger.info(f"No marking artists for Score MS_{legacy_id}")

		# Create/Update the Parts
		if part_id_display:
			for index, part_id in enumerate(part_id_display):
				if part_id:

					# clear old values
					parameters = (
						f"Documents.Folder.Part:CreateOrUpdate"
						f"?CoreField.Legacy-Identifier=MP_{part_id}"
						f"&NYP.Composer/Work--=&NYP.Marking-Artist--=&NYP.Composer--=&NYP.Conductor--=&NYP.Instrument--="
					)
					url = f"{baseurl}{datatable}{parameters}&token={token}"
					api_call(url,f'Clear old metadata on Part MP_{part_id}',legacy_id)

					# Send this data as a JSON payload because it might exceed the character limit for a regular API call
					data = {
						'CoreField.Legacy-Identifier': f'MP_{part_id}',
						'CoreField.parent-folder:': f'[Documents.Folder.Printed-Music:CoreField.Legacy-Identifier={legacy_id}]',
						'CoreField.Title:': f'MP_{part_id} - {part_type_desc[index]}',
						'NYP.Publisher+:': publisher_name,
						'NYP.Edition-Type+:': part_edition_type[index],
						'CoreField.Notes:': part_stand_notes[index],
						'NYP.Composer/Work++': f'{composer_name} / {ar_works_title}',
						'NYP.Composer/Work-Full-Title:': composer_name_title,
						'NYP.Instrument++': part_type_desc[index],
						'NYP.Archives-location:': part_location[index]
					}
					# fix for linebreaks and such - dump to string and load back to JSON
					data = json.dumps(data)
					data = json.loads(data)

					action = 'Documents.Folder.Part:CreateOrUpdate'
					params = {'token': token}
					url = f"{baseurl}{datatable}{action}"
					api_call(url, f'Create/Update Part MP_{part_id} in Printed Music folder', legacy_id, params, data)

					# link the composer to the parts
					parameters = (
						f"Documents.Folder.Part:Update"
						f"?CoreField.Legacy-Identifier=MP_{part_id}"
						f"&NYP.Composer+=[Contacts.Source.Default:CoreField.Composer-ID={composer_id}]"
					)
					url = f"{baseurl}{datatable}{parameters}&token={token}"
					api_call(url, f'Link composer {composer_id} to Part MP_{part_id} in Printed Music folder', legacy_id)

					# link the marking artist(s) to the parts
					if part_marking_ids_list[index]:
						# check if we have a list
						if type(part_marking_ids_list[index]) is list:
							for artist_id in set(part_marking_ids_list[index]):
								if artist_id:

									# first see if the marking artist exists in Cortex
									parameters = f'Contacts.Source.Default:Read?CoreField.Artist-ID={artist_id}&format=json'
									query = baseurl + datatable + parameters + '&token=' + token
									response = api_call(query,'Checking if Marking Artist exists',artist_id)

									if response:
										response_data = response.json()
										if response_data is not None and response_data['ResponseSummary']['TotalItemCount'] > 0:
											logger.info(f'Artist {artist_id} already exists in Cortex')
										else:
											#do a Create/Update on the artist
											parameters = (
												f"Contacts.Source.Default:CreateOrUpdate?CoreField.Artist-ID={artist_id}"
											)
											url = f"{baseurl}{datatable}{parameters}&token={token}"
											api_call(url, 'Create/update Source record for artist', artist_id)

									# now link the artist to the Part
									parameters = (
										f"Documents.Folder.Part:Update"
										f"?CoreField.Legacy-Identifier=MP_{part_id}"
										f"&NYP.Marking-Artist+=[Contacts.Source.Default:CoreField.Artist-ID={artist_id}]"
									)
									url = f"{baseurl}{datatable}{parameters}&token={token}"
									api_call(url, f'Link marking artist {artist_id} to Part MP_{part_id} in Printed Music folder', legacy_id)

						# then we must have a single value
						else:
							# see if the marking artist exists in Cortex
							parameters = f'Contacts.Source.Default:Read?CoreField.Artist-ID={part_marking_ids_list[index]}&format=json'
							query = baseurl + datatable + parameters + '&token=' + token
							response = api_call(query,'Checking if Marking Artist exists',part_marking_ids_list[index])

							if response:
								response_data = response.json()
								if response_data is not None and response_data['ResponseSummary']['TotalItemCount'] > 0:
									logger.info(f'Artist {part_marking_ids_list[index]} already exists in Cortex')
								else:
									# do a Create/Update on the artist
									parameters = (
										f"Contacts.Source.Default:CreateOrUpdate?CoreField.Artist-ID={part_marking_ids_list[index]}"
									)
									url = f"{baseurl}{datatable}{parameters}&token={token}"
									api_call(url, 'Create/update Source record for artist', part_marking_ids_list[index])

							parameters = (
								f"Documents.Folder.Part:Update"
								f"?CoreField.Legacy-Identifier=MP_{part_id}"
								f"&NYP.Marking-Artist+=[Contacts.Source.Default:CoreField.Artist-ID={part_marking_ids_list[index]}]"
							)
							url = f"{baseurl}{datatable}{parameters}&token={token}"
							api_call(url, f'Link marking artist {part_marking_ids_list[index]} to Part MP_{part_id} in Printed Music folder', legacy_id)
					else:
						logger.info(f"No marking artist for Part MP_{part_id}")

					# link the Parts Used By conductor to the Part
					if usedby_ids[0]:
						for user in usedby_ids:
							parameters = (
								f"Documents.Folder.Part:Update"
								f"?CoreField.Legacy-Identifier=MP_{part_id}"
								f"&NYP.Conductor+=[Contacts.Source.Default:CoreField.Artist-ID={user}]"
							)
							url = f"{baseurl}{datatable}{parameters}&token={token}"
							api_call(url, f'Link Conductor {user} to Part MP_{part_id} in Printed Music folder',legacy_id)

	logger.info('All done with Printed Music updates!')

def api_call(url, asset_type, ID, params=None, data=None):
    # Set the maximum number of attempts to make the call
    max_attempts = 2

    # Set the initial number of attempts to 0
    attempts = 0

    # Set a flag to indicate whether the call was successful
    success = False

    # Continue making the call until it is successful, or until the maximum number of attempts has been reached
    while not success and attempts < max_attempts:
        try:
            # Import the requests module
            import requests

            # Make the API call with the provided params and data
            response = requests.post(url, params=params, data=data)

            # If the response was successful, no Exception will be raised
            response.raise_for_status()

            # If no exceptions were raised, the call was successful
            success = True
        except ImportError as import_err:
            # Handle errors that occur when importing the requests module
            logger.error(f'Failed to import the requests module: {import_err}')
        except HTTPError as http_err:
            # Handle HTTP errors
            logger.error(f'Failed: {asset_type} {ID} - HTTP error occurred: {http_err}')

            # Increment the number of attempts
            attempts += 1

            # If the maximum number of attempts has been reached, raise an exception to stop the loop
            if attempts >= max_attempts:
            	# raise
                logger.error('Moving on...')
                pass
        except Exception as err:
            # Handle all other errors
            logger.error(f'Failed: {asset_type} {ID} - Other error occurred: {err}')

    # If the loop exited successfully, the call was successful
    logger.info(f'Success: {asset_type} {ID}')
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

	# make_folders(token)
	update_folders(token)
	# create_sources(token)
	# add_sources_to_program(token)
	# library_updates(token)

	logger.info('ALL DONE! Bye bye :)')

else:
	logger.info('No API token :( Goodbye')