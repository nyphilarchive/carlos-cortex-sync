"""
Keep Cortex/OrangeDAM synced with Carlos metadata updates

This script will read various CSVs and execute the appropriate API calls to create or update records

by Bill Levay

Make sure you have an .env file in this directory that looks like this:
 login=yourCortexLogin
 password=yourCortexPassword
 directory=/location/of/csvs/
 carlos_xml_path=/location/of/carlos/xml/
 dbtext_xml_path=/location/of/dbtext/xml
 logs=/location/for/logs/
 baseurl=https://mydomain.org
 datatable=/API/DataTable/v2.2/
"""

import requests, csv, sys, os, time, datetime, logging, json, re, codecs, tempfile
from urllib.parse import quote
from os.path import join, dirname
from requests.exceptions import HTTPError
from dotenv import load_dotenv
import xml.etree.ElementTree as ET
from lxml import etree
from fuzzywuzzy import process
from my_logger import logger


# First, grab credentials and other values from the .env file in the same folder as this script
dotenv_path = join(dirname(__file__), '.env')
load_dotenv(dotenv_path)

login = os.environ.get('login', 'default')
password = os.environ.get('password', 'default')
password = quote(password) # now URL encoded
directory = os.environ.get('directory', 'default')
baseurl = os.environ.get('baseurl', 'default')
datatable = os.environ.get('datatable', 'default')
carlos_xml_path = os.environ.get('carlos_xml_path', 'default')
dbtext_xml_path = os.environ.get('dbtext_xml_path', 'default')

# File paths for our source data
program_xml = f"{carlos_xml_path}/program_updates.xml" #Program Deltas
# program_xml = f"{carlos_xml_path}/program.xml" #All programs
business_records_xml = f"{dbtext_xml_path}/CTLG1024-1.xml" #BR Deltas
# business_records_xml = f"{dbtext_xml_path}/CTLG1024-1_full.xml" #ALL BRs
name_id_mapping_file = f"{dbtext_xml_path}/names-1.csv" #All DBText names

# Constants
WORK_PARENT_FOLDER_IDENTIFIER = "PH1QHU6"
PM_PARENT_FOLDER_IDENTIFIER = "PH1N31F"
BR_PARENT_FOLDER_IDENTIFIER = "PH1N31H"
PAGE_IMPORT_MAPPING_TEMPLATE = "2TTUSVANGT"
PROGRAM_SEARCH_API_URL = "https://cortex.nyphil.org/API/search/v3.0/search?query=DocSubType:Program%20Purpose:Pending%20&format=json&fields=Identifier,NYP.Program-ID,CoreField.Asset-date"

# Some helper functions for data cleanup
def xpath_text(element, path):
	value = element.xpath(path)
	return value[0] if value else ''

def remove_angle_brackets(text):
	replaced_text = text.replace('<','').replace('>','')
	return replaced_text

def replace_angle_brackets(text):
	# Define a regular expression pattern to match angle brackets and the enclosed text
	pattern = r'<(.*?)>'
	
	# Define a replacement function that adds the appropriate HTML tags
	def replace(match):
		return '<em>{}</em>'.format(match.group(1))
	
	# Use re.sub() to replace the matched patterns with the appropriate HTML tags
	replaced_text = re.sub(pattern, replace, text)
	return replaced_text

def replace_spaces(text):
	replaced_text = text.replace('  ', ' ')
	return replaced_text

def replace_chars(text):
	cleaned_text = text.replace('&','%26amp;').replace('#','%23').replace('+','%2B')
	return cleaned_text

def reformat_date(date_str, input_format, output_format):
	try:
		# Convert the input date string to a datetime object
		date_obj = datetime.datetime.strptime(date_str, input_format)
	except ValueError:
		logger.warning(f"Date string '{date_str}' does not match the expected format '{input_format}'.")
		return ''
	# Format the datetime object as specified in the output format
	formatted_date = date_obj.strftime(output_format)
	return formatted_date

def process_date(date, output_format):
	if not date:
		return ''
	try:
		if len(date) >= 10:
			return reformat_date(date, '%d %b %Y', output_format)
		elif len(date) == 4:
			return reformat_date(f"01/01/{date}", '%m/%d/%Y', output_format)
		else:
			return ''
	except ValueError as e:
		logger.error(f"Could not process the date. Error: {e}")
		return ''

def get_date_range(dates, input_format, output_format):
	if not dates:
		return ""
	try:
		first_date = dates[0]
		last_date = dates[-1]
		if not first_date or not last_date:
			return ""

		reformatted_first_date = reformat_date(first_date, input_format, output_format)
		reformatted_last_date = reformat_date(last_date, input_format, output_format)

		# Ensure the order is correct
		if reformatted_first_date > reformatted_last_date:
			reformatted_first_date, reformatted_last_date = reformatted_last_date, reformatted_first_date

		date_range = f"{reformatted_first_date}/{reformatted_last_date}"
		return date_range
	except ValueError as e:
		logger.error(f"Could not get the date range. Error: {e}")
		return ""

def sanitize_data(data):
	if data is None:
		return []  # Return an empty list for None to safely join later
	elif isinstance(data, list):
		return [str(item) for item in data if item is not None]  # Convert items to strings, ignore None
	else:
		return [str(data)]  # Wrap non-list data in a list as string
	

# Set up our Program classes
class ProgramWork:
	def __init__(self, program_works_id, works_id, composer_number, composer_title_short,
				 composer_name, title_short, title_full, movement, works_conductor_ids,
				 works_encore, works_soloists_functions, works_soloists_ids,
				 works_soloists_names, works_soloists_inst_names):
		self.program_works_id = program_works_id
		self.works_id = works_id
		self.composer_number = composer_number
		self.composer_title_short = composer_title_short
		self.composer_name = composer_name
		self.title_short = title_short
		self.title_full = title_full
		self.movement = movement
		self.works_conductor_ids = works_conductor_ids
		self.works_encore = works_encore
		self.works_soloists_functions = works_soloists_functions
		self.works_soloists_ids = works_soloists_ids
		self.works_soloists_names = works_soloists_names
		self.works_soloists_inst_names = works_soloists_inst_names

class Program:
	def __init__(self, row_element):
		self.id = row_element.find('id').text
		self.season = self.season_fix(row_element.find('season').text)
		self.orchestra_name = row_element.find('orchestra_name').text
		self.dates = [date.text for date in row_element.findall('date')]
		self.date_range = get_date_range(self.dates, '%m/%d/%Y', '%Y-%m-%d')
		self.performance_times = [time.text for time in row_element.findall('performance_time')]
		self.combined_datetimes = self.combine_dates_and_times()
		self.location_names = [loc_name.text for loc_name in row_element.findall('location_name')]
		self.venue_names = [venue_name.text for venue_name in row_element.findall('venue_name')]
		self.event_type_names = [event_type.text for event_type in row_element.findall('event_type_names')]
		self.sub_event_names = [sub_event_name.text for sub_event_name in row_element.findall('sub_event_names')]
		self.conductor_id = row_element.find('conductor').text
		self.soloist_id = row_element.find('soloist').text
		self.soloist_function = row_element.find('soloist_function').text
		self.soloist_instrument = row_element.find('soloist_instrument').text
		self.program_works = []

		program_works_ids = row_element.findall('program_works_ids')
		works_ids = row_element.findall('works_ids')
		composer_numbers = row_element.findall('composer_number')
		composer_title_shorts = row_element.findall('composer_title_short')
		title_shorts = row_element.findall('title_short')
		composer_titles = row_element.findall('composer_title')
		title_pipes = row_element.findall('title_pipes')
		works_conductors_ids = row_element.findall('works_conductors_ids')
		works_encore = row_element.findall('works_encore')
		works_soloists_functions = row_element.findall('works_soloists_functions')
		works_soloists_ids = row_element.findall('works_soloists_ids')
		works_soloists_names = row_element.findall('works_soloists_names')
		works_soloists_inst_names = row_element.findall('works_soloists_inst_names')
		
		for idx, program_works_id in enumerate(program_works_ids):
			if program_works_id.text is not None:  # Check if program_works_id has text
				self.program_works.append(ProgramWork(
					program_works_id.text.replace('*', '-'),  # Replace '*' with '-'
					works_ids[idx].text,
					composer_numbers[idx].text,
					replace_spaces(remove_angle_brackets(composer_title_shorts[idx].text)),
					replace_spaces(composer_title_shorts[idx].text.split(' / ')[0]),
					remove_angle_brackets(title_shorts[idx].text),
					replace_angle_brackets(title_pipes[idx].text.split(' | ')[0]),
					title_pipes[idx].text.split(' | ')[1] if ' | ' in title_pipes[idx].text else '',
					works_conductors_ids[idx].text,
					works_encore[idx].text,
					works_soloists_functions[idx].text,
					works_soloists_ids[idx].text,
					works_soloists_names[idx].text,
					works_soloists_inst_names[idx].text
				))

	def season_fix(self, season):
		# fix for 1899-00 and 1999-00
		if len(season) == 7 and season.endswith('00'):
			year = season.split('-')[0]
			next_year = str(int(year) + 1)
			season = year + '-' + next_year.zfill(2)
		return season

	def combine_dates_and_times(self):
		# Initialize an empty list to store the combined datetime strings
		combined_datetimes = []

		# Find the longer of the two lists
		longest_list = max(len(self.dates), len(self.performance_times))

		# Iterate over the index of the longer list
		for i in range(longest_list):
			date = self.dates[i] if i < len(self.dates) else ""
			time = self.performance_times[i] if i < len(self.performance_times) else ""

			# Reformat the date to "YYYY-MM-dd" if date exists
			try:
				if date:
					date_formatted = datetime.datetime.strptime(date, "%m/%d/%Y").strftime("%Y-%m-%d")
				else:
					date_formatted = ""
			except ValueError as e:
				logger.error(f"An error occurred while formatting the date for Program {self.id}: {e}")
				date_formatted = ""
			
			# Convert the time from 12-hour to 24-hour format if time exists
			try:
				if time:
					time_24hr = datetime.datetime.strptime(time, "%I:%M%p").strftime("%H:%M:%S")
					combined_datetime = f"{date_formatted}T{time_24hr}"
				else:
					logger.info(f"Time data is empty for Program {self.id}")
					combined_datetime = f"{date_formatted}"
			except ValueError as e:
				logger.error(f"An error occurred while formatting the time for Program {self.id}: {e}")
				combined_datetime = f"{date_formatted}"

			combined_datetimes.append(combined_datetime)

		return combined_datetimes


def load_program_data(file_path):
	try:
		# Open the XML file and read its content
		with open(file_path, 'r', encoding='utf-8', errors='replace') as file:
			xml_data = file.read()

		# Create a temporary file to save corrected XML content
		with tempfile.NamedTemporaryFile(mode='w', delete=False, encoding='utf-8') as temp_file:
			temp_file.write(xml_data)
			temp_file_path = temp_file.name

		# Parse the XML using the corrected temporary file
		root = ET.parse(temp_file_path).getroot()
		programs = []

		for row_element in root.findall('row'):
			program = Program(row_element)
			programs.append(program)

		return programs

	finally:
		# Clean up: remove the temporary file
		if temp_file_path:
			os.remove(temp_file_path)


# Set up our Business Records class
class BusinessRecord:
	def __init__(self, record_element, namespace, name_id_mapping):
		self.folder_number = record_element.find('inm:BOX-NUMBER', namespaces=namespace).text
		self.folder_name = record_element.find('inm:FOLDER-TITLE', namespaces=namespace).text
		self.record_group = record_element.find('inm:RECORD-GROUP', namespaces=namespace).text
		self.series = record_element.find('inm:SERIES', namespaces=namespace).text
		self.subseries = record_element.find('inm:SUB-SERIES', namespaces=namespace).text

		date_from_element = record_element.find('inm:FROM', namespaces=namespace)
		date_to_element = record_element.find('inm:TO', namespaces=namespace)

		if date_from_element is not None and date_from_element.text is not None:
			self.date_from = process_date(date_from_element.text, '%d %b %Y')
		else:
			self.date_from = ''
			logger.warning(f"Missing or invalid 'FROM' date for folder_number: {self.folder_number}")

		if date_to_element is not None and date_to_element.text is not None:
			self.date_to = process_date(date_to_element.text, '%d %b %Y')
		else:
			self.date_to = ''
			logger.warning(f"Missing or invalid 'TO' date for folder_number: {self.folder_number}")

		self.date_range = get_date_range([self.date_from, self.date_to], '%d %b %Y', '%Y-%m-%d')
		self.abstract = record_element.find('inm:ABSTRACT', namespaces=namespace).text
		self.notes = record_element.find('inm:NOTES', namespaces=namespace).text
		self.subjects = record_element.find('inm:SUBJECTS', namespaces=namespace).text
		self.names = record_element.find('inm:NAMES', namespaces=namespace).text
		self.contents = record_element.find('inm:CONTENTS', namespaces=namespace).text
		self.content_type = record_element.find('inm:CONTENT-TYPE', namespaces=namespace).text
		self.language = record_element.find('inm:LANGUAGE', namespaces=namespace).text
		self.archives_location = record_element.find('inm:LOCATION', namespaces=namespace).text
		self.accession_date = record_element.find('inm:ACCESSION-DATE-FORMATTED', namespaces=namespace).text
		self.size = record_element.find('inm:SIZE', namespaces=namespace).text
		self.condition = record_element.find('inm:CONDITION', namespaces=namespace).text
		self.make_public = record_element.find('inm:MAKE-PUBLIC', namespaces=namespace).text
		self.is_public = record_element.find('inm:Is-Item-Public', namespaces=namespace).text
		self.digitization_notes = record_element.find('inm:Digitize-Notes', namespaces=namespace).text

		# Store the name ID mapping passed during object creation
		self.name_id_mapping = name_id_mapping

	def get_id_for_name(self, name):
		if name in self.name_id_mapping:
			logger.info(f"Exact match found: {name} -> {self.name_id_mapping[name]}")
			return self.name_id_mapping[name]

		# Perform fuzzy matching if exact match not found
		best_match, score = process.extractOne(name, self.name_id_mapping.keys())
		
		# Set a threshold for how "fuzzy" we want our matches to be
		if score >= 90:
			logger.warning(f"Fuzzy match found: {name} -> {best_match} with score {score}")
			return self.name_id_mapping[best_match]

		logger.error(f"No satisfactory match found for {name}. Best match was {best_match} with score {score}")
		return None


# Load Business Record data from the source XML
def load_business_records_data(file_path, name_id_mapping_file):
	# Load the name ID mapping from the CSV file
	# CSV is in the format DateChanged|ID|Term
	name_id_mapping = {}
	with open(name_id_mapping_file, 'r') as csvfile:
		csvreader = csv.reader(csvfile, delimiter='|')
		for row in csvreader:
			name_id_mapping[row[2]] = row[1]

	# Load Business Records data from a file
	with open(file_path, 'r') as file:
		xml_data = file.read()

	root = ET.fromstring(xml_data)

	# Define the namespace
	namespace = {"inm": "http://www.inmagic.com/webpublisher/query"}

	# Get all inm:Record elements
	record_elements = root.findall(".//inm:Record", namespaces=namespace)

	records = []
	for record_element in record_elements:
		record = BusinessRecord(record_element, namespace, name_id_mapping)
		records.append(record)

	return records


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
		if total != 0:
			percent = round(count/total, 4)*100
		else:
			percent = 0

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
			parameters = f"Documents.Virtual-folder.Program:CreateOrUpdate?CoreField.Legacy-Identifier={program_id}&CoreField.Title:={folder_name}&NYP.Program-ID:={program_id}{update_parent}"
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

		count = 1
		records = list(csvfile)
		total = len(records[1:])
		if total != 0:
			percent = round(count/total, 4)*100
		else:
			percent = 0

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
			COMPOSER_TITLE_SHORT = remove_angle_brackets(row[12])
			NOTES_XML = row[13].replace('<br>','\n')
			RELATED_PROGRAMS = row[14].split('|')

			# get the Digital Archives (Hadoop) ID from public Solr
			lookup = f"http://proslrapp01.nyphil.live:9993/solr/assets/select?q=npp%5C%3AProgramID%3A{ID}&fl=id&wt=json"
			response = ""

			try:
				response = requests.get(lookup)
				# If the response was successful, no Exception will be raised
				response.raise_for_status()
			except HTTPError as http_err:
				logger.error(f'Failed to get Solr data for program {ID} - HTTP error occurred: {http_err}')
			except Exception as err:
				logger.error(f'Failed to get Solr data for program {ID} - Other error occurred: {err}')

			if response:
				logger.info(f'Success retrieving Solr data for program {ID}')
				# parse JSON results
				response = response.json()
				if response["response"]["numFound"] == 1:
					digarch_id = response["response"]["docs"][0]["id"]
					logger.info(f'Digital archives ID is {digarch_id}')
				else:
					digarch_id = ''
			else:
				digarch_id = ''
				logger.error(f'Failed to get Solr data for program {ID}')

			# Format the combined_datetime string for the Program Dates field
			# Convert the pipe-separated strings to lists
			date_list = DATE.split("|")
			time_list = PERFORMANCE_TIME.split("|")

			# Initialize an empty list to store the combined datetimes
			combined_datetimes = []

			# If the lengths of the date and time lists don't match, set a flag
			lengths_match = len(date_list) == len(time_list)

			for i, date in enumerate(date_list):
				try:
					# Reformat the date to "YYYY-MM-dd"
					date_formatted = datetime.datetime.strptime(date, "%m/%d/%Y").strftime("%Y-%m-%d")
				except ValueError as e:
					logger.error(f"An error occurred while formatting the date for Program {ID}: {e}")
					continue  # Skip to the next iteration

				# If the lengths match and there's a corresponding time, convert it to 24-hour format
				if lengths_match:
					time = time_list[i]
					try:
						if time:  # Check if time is not empty or None
							time_24hr = datetime.datetime.strptime(time, "%I:%M%p").strftime("%H:%M:%S")
							combined_datetime = f"{date_formatted}T{time_24hr}"
						else:
							logger.info("Time data is empty.")
							combined_datetime = f"{date_formatted}"
					except ValueError as e:
						logger.error(f"An error occurred while formatting the time for Program {ID}: {e}")
						combined_datetime = f"{date_formatted}"
				else:
					combined_datetime = f"{date_formatted}"

				# Append the combined datetime to the list
				combined_datetimes.append(combined_datetime)

			# Convert the list back to a pipe-separated string
			combined_datetimes_str = "|".join(combined_datetimes)

			# set the visibility based on whether the event is past or future
			if datetime.datetime.strptime(DATE.split('|')[0], '%m/%d/%Y').date() < datetime.datetime.now().date():
				visibility = "Public"
			else:
				visibility = "Pending"

			# Create the dict
			data = {
				'CoreField.Legacy-Identifier': ID,
				'NYP.Season+': SEASON,
				'NYP.Week+:': WEEK,
				'NYP.Orchestra+:': ORCHESTRA_NAME,
				'CoreField.Asset-date:': combined_datetimes[0],
				'NYP.Program-Dates+': combined_datetimes_str,
				'NYP.Program-Date(s)++': DATE,
				'NYP.Program-Times++': PERFORMANCE_TIME,
				'NYP.Program-Date-Range:': DATE_RANGE,
				'NYP.Location++': LOCATION_NAME,
				'NYP.Venue++': VENUE_NAME,
				'NYP.Event-Type++': SUB_EVENT_NAMES,
				# 'NYP.Composer/Work++': COMPOSER_TITLE_SHORT,
				'NYP.Composer/Work-Full-Title:': COMPOSER_TITLE,
				'NYP.Notes-on-program:': NOTES_XML,
				'NYP.Digital-Archives-ID:': digarch_id,
				'CoreField.Visibility-class:' : visibility
			}
			# fix for linebreaks and such - dump to string and load back to JSON
			data = json.dumps(data)
			logger.info(data)
			data = json.loads(data)

			# log some info
			logger.info(f'Updating Program {count} of {total} = {percent}% complete')

			# clear values from program folders
			parameters = f"Documents.Virtual-folder.Program:Update?CoreField.Legacy-Identifier={ID}&NYP.Season--=&NYP.Program-Dates--=&NYP.Program-Date(s)--=&NYP.Program-Times--=&NYP.Location--=&NYP.Venue--=&NYP.Event-Type--=&NYP.Composer/Work--=&NYP.Composer-/-Work--=&NYP.Soloist--=&NYP.Conductor--=&NYP.Composer--=&NYP.Related-Programs--="
			call = baseurl + datatable + parameters + '&token=' + token
			api_call(call,'Program - clear old metadata',ID)
			
			# update program metadata with token as a parameter and dict as body
			action = 'Documents.Virtual-folder.Program:Update'
			params = {'token': token}
			url = baseurl + datatable + action
			api_call(url,'Program - add new metadata',ID,params,data)

			# Link Related Programs to the Program
			if RELATED_PROGRAMS:
				for related_program in RELATED_PROGRAMS:
					# Skip if related_program is None or an empty string
					if not related_program:
						logger.info(f"No related programs for Program ID {ID}")
						continue
					
					parameters = (
						f"Documents.Virtual-folder.Program:Update"
						f"?CoreField.Legacy-Identifier={ID}"
						f"&NYP.Related-Programs+=[Documents.Virtual-Folder.Program:CoreField.Legacy-Identifier={related_program}]"
					)
					url = f"{baseurl}{datatable}{parameters}&token={token}"
					logger.info(url)
					api_call(url, f'Link Related Program {related_program} to Program', ID)
			else:
				logger.info(f"No related programs for Program ID {ID}")

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
			call = f"{baseurl}{datatable}{parameters}&token={token}"
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
			query = f"{baseurl}{datatable}{parameters}&token={token}"
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
			call = f"{baseurl}{datatable}{parameters}&token={token}"
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
			call = f"{baseurl}{datatable}{parameters}&token={token}"
			api_call(call,f'Add soloist {Artist_ID} to Program',Program_ID)
	file.close()

	with open(directory+'conductors.csv', 'r') as file:
		csvfile = csv.reader(file)
		next(csvfile)

		for row in csvfile:
			Program_ID = row[0]
			Artist_ID = row[1]
			
			parameters = f"Documents.Virtual-folder.Program:Update?CoreField.Legacy-Identifier={Program_ID}&NYP.Conductor+=[Contacts.Source.Default:CoreField.Artist-ID={Artist_ID}]"
			call = f"{baseurl}{datatable}{parameters}&token={token}"
			api_call(call,f'Add conductor {Artist_ID} to Program',Program_ID)
	file.close()

	with open(directory+'composers.csv', 'r') as file:
		csvfile = csv.reader(file)
		next(csvfile)

		for row in csvfile:
			Program_ID = row[0]
			Composer_ID = row[1]
	
			parameters = f"Documents.Virtual-folder.Program:Update?CoreField.Legacy-Identifier={Program_ID}&NYP.Composer+=[Contacts.Source.Default:CoreField.Composer-ID={Composer_ID}]"
			call = f"{baseurl}{datatable}{parameters}&token={token}"
			api_call(call,f'Add composer {Composer_ID} to Program',Program_ID)
	file.close()


def create_or_update_works(programs, token):
    logger.info("Starting creation/update of Works...")

    # Enhanced to store both existence and update status
    work_status = {}  # Example format: {works_id: {'exists': True, 'updated': False}}

    for program in programs:
        for work in program.program_works:
            # Initialize update_needed as False
            update_needed = False

            # Check if the work already exists and has been updated
            if work.works_id in work_status:
                exists = work_status[work.works_id]['exists']
                updated = work_status[work.works_id]['updated']
                logger.info(f'Work {work.works_id} status found in local cache: {"exists" if exists else "does not exist"}, {"updated" if updated else "not updated"}')
                if not updated:  # Only proceed if the work has not been updated already
                    update_needed = True
            else:
                # Assume work needs to be checked (and potentially updated)
                update_needed = True

            if update_needed:
                parameters = f'CoreField.Legacy-Identifier:WORK_{work.works_id} DocSubType:Work&format=json'
                query = f'{baseurl}/API/search/v3.0/search?query={parameters}&token={token}'
                try:
                    r = requests.get(query)
                    r_data = r.json()

                    if r_data['APIResponse']['GlobalInfo']['TotalCount'] == 1:
                        exists = True
                        logger.info('Work exists in Cortex')
                    else:
                        exists = False
                        logger.info('This looks like a new work so let\'s add it to Cortex')
                        
                    # Update the work_status dictionary with existence and updated status
                    work_status[work.works_id] = {'exists': exists, 'updated': False}

                except Exception as e:
                    logger.warning(f'Unable to find Program ID {program.program_id}. Exception: {e}')
                    # Assume work does not exist and hasn't been updated
                    work_status[work.works_id] = {'exists': False, 'updated': False}
                    exists = False

            # Check if we should assign a parent folder
            assign_parent = ''
            if not exists or not work_status[work.works_id]['updated']:
                assign_parent = f'&CoreField.Parent-folder:=[Documents.All:CoreField.Identifier={WORK_PARENT_FOLDER_IDENTIFIER}]'

                # Create or update the work in Cortex
                parameters = (
                    f"Documents.Virtual-folder.Work:CreateOrUpdate"
                    f"?CoreField.Legacy-Identifier=WORK_{work.works_id}"
                    f"&NYP.Works-ID:={work.works_id}"
                    f"&CoreField.Title:={work.composer_name} / {replace_chars(work.title_short)}"
                    f"{assign_parent}"
                    f"&NYP.Work-Title-Full:={replace_chars(work.title_full)}"
                    f"&NYP.Work-Title-Short:={replace_chars(work.title_short)}"
                )
                url = f"{baseurl}{datatable}{parameters}&token={token}"
                api_call(url,'Update the Work',work.works_id)

                # Mark the work as updated to avoid redundant updates
                work_status[work.works_id]['updated'] = True

    logger.info("Finished creation/update of Works.")


# Go through each Program object and create/update the Works within each program virtual folder
def program_works(programs, token):

	logger.info("Starting Program Works updates...")

	# Initialize the dictionary to store existing works and their status
	work_status = {}

	# update_list = [program for program in programs if int(program.season[0:4]) >= 1971]
	# for program in update_list:
	for program in programs:
		
		# iterate through the Program Works
		for work in program.program_works:

			for attr, value in vars(program).items():
				print(f"{attr}: {value}")

			for attr, value in vars(work).items():
				print(f"{attr}: {value}")

			# clear old values, give the program work a title, and situate it within a Program

			# add movement to the title if there is one
			if work.movement:
				movement = f' / {work.movement}'
			else:
				movement = ''

			# set the visibility based on whether the event is past or future
			if datetime.datetime.strptime(program.dates[0], '%m/%d/%Y').date() < datetime.datetime.now().date():
				visibility = "Public"
			else:
				visibility = "Pending"

			# set the title field
			title = f"{work.composer_name} / {replace_chars(work.title_short)}{movement}"
			# Truncate to 200 characters if necessary
			title = title[:200]
			if title.endswith(', / .'):
				title = title[:-5]

			parameters = (
				f"Documents.Virtual-Folder.Program-Work:CreateOrUpdate"
				f"?CoreField.Legacy-Identifier={work.program_works_id}"
				f"&CoreField.Title:={title}"
				f"&CoreField.Parent-folder:=[Documents.Virtual-folder.Program:CoreField.Legacy-identifier={program.id}]"
				f"&NYP.Program-ID:={program.id}"
				f"&NYP.Composer/Work--=&NYP.Conductor--=&NYP.Composer--=&NYP.Soloist--="
				f"&NYP.Season--=&NYP.Program-Dates--=&NYP.Program-Date(s)--=&NYP.Program-Times--=&NYP.Location--=&NYP.Venue--=&NYP.Event-Type--="
				f"&CoreField.visibility-class:={visibility}"
			)
			url = f"{baseurl}{datatable}{parameters}&token={token}"
			api_call(url,'Establish Program Work',work.program_works_id)

			# link Work to Program Work
			parameters = (
				f"Documents.Virtual-Folder.Program-work:Update"
				f"?CoreField.Legacy-Identifier={work.program_works_id}"
				f"&NYP.Composer-/-Work+=[Documents.Virtual-folder.Work:CoreField.Legacy-identifier=WORK_{work.works_id}]"
			)
			url = f"{baseurl}{datatable}{parameters}&token={token}"
			api_call(url,f'Link Work {work.works_id} to Program Work',work.program_works_id)

			# link Work to parent Program
			parameters = (
				f"Documents.Virtual-Folder.Program:Update"
				f"?CoreField.Legacy-Identifier={program.id}"
				f"&NYP.Composer-/-Work+=[Documents.Virtual-folder.Work:CoreField.Legacy-identifier=WORK_{work.works_id}]"
			)
			url = f"{baseurl}{datatable}{parameters}&token={token}"
			api_call(url,f'Link Work {work.works_id} to Program',program.id)

			# add metadata to each program work

			# Encores
			if work.works_encore == 'Y':
				encore = 'Yes'
			else:
				encore = ''

			# Fix for potentially missing venues and locations
			sanitized_venue_names = sanitize_data(program.venue_names)
			sanitized_locations = sanitize_data(program.location_names)
			sanitized_sub_events = sanitize_data(program.sub_event_names)

			parameters = (
				f"Documents.Virtual-Folder.Program-work:Update"
				f"?CoreField.Legacy-Identifier={work.program_works_id}"
				f"&NYP.Composer/Work++={replace_chars(work.composer_title_short)}"
				f"&NYP.Composer/Work-Full-Title:={work.composer_name} / {replace_chars(work.title_full)}"
				f"&NYP.Movement:={work.movement}"
				f"&NYP.Encore:={encore}"
				f"&NYP.Season+={program.season}"
				f"&NYP.Orchestra:={program.orchestra_name}"
				f"&NYP.Program-Dates+={'|'.join(program.combined_datetimes)}"
				f"&NYP.Program-Date(s)++={'|'.join(program.dates)}"
				f"&NYP.Program-Date-Range:={program.date_range}"
				f"&NYP.Program-Times++={'|'.join(time for time in program.performance_times if time is not None)}"
				f"&NYP.Location++={'|'.join(sanitized_locations)}"
				f"&NYP.Venue++={'|'.join(sanitized_venue_names)}"
				f"&NYP.Event-Type++={'|'.join(sanitized_sub_events)}"
			)
			url = f"{baseurl}{datatable}{parameters}&token={token}"
			api_call(url,'Add metadata to Program Work',work.program_works_id)

			# link the composer to the program work
			parameters = (
				f"Documents.Virtual-Folder.Program-work:Update"
				f"?CoreField.Legacy-Identifier={work.program_works_id}"
				f"&NYP.Composer+=[Contacts.Source.Default:CoreField.Composer-ID={work.composer_number}]"
			)
			url = f"{baseurl}{datatable}{parameters}&token={token}"
			api_call(url,f'Link Composer {work.composer_number} to Program Work',work.program_works_id)

			# link the soloists (semi-colon separated values)
			if work.works_soloists_ids is not None:
				for soloist in work.works_soloists_ids.split(';'):
					parameters = (
						f"Documents.Virtual-Folder.Program-work:Update"
						f"?CoreField.Legacy-Identifier={work.program_works_id}"
						f"&NYP.Soloist+=[Contacts.Source.Default:CoreField.Artist-ID={soloist.strip()}]"
					)
					url = f"{baseurl}{datatable}{parameters}&token={token}"
					api_call(url, f'Link Soloist {soloist} to Program Work', work.program_works_id)

			# add paired-value field for soloist/instrument: https://link.orangelogic.com/CMS4/LMS/Home/Published-Documentation/API/Update-a-Paired-Value-Field-Using-the-CreateOrUpdate-API/
			for name, inst, role in zip(
				work.works_soloists_names.split(';') if work.works_soloists_names else [''], 
				work.works_soloists_inst_names.split(';') if work.works_soloists_inst_names else [''], 
				work.works_soloists_functions.split(';') if work.works_soloists_functions else ['']
			):

				# Strip the values to remove any extra spaces
				name, inst, role = name.strip(), inst.strip(), role.strip()

				# Only log a warning if at least one value is non-empty
				if any([name, inst, role]):
					if not name:
						logger.warning(f"Null or blank value for name in Program Work ID {work.program_works_id}")
					if not inst:
						logger.warning(f"Null or blank value for instrument in Program Work ID {work.program_works_id}")
					if not role:
						logger.warning(f"Null or blank value for role in Program Work ID {work.program_works_id}")

				# Prepare parameters for the API call
				soloist_inst = f"{name.strip()} / {inst.strip()}"
				soloist_role = f"{role.strip().replace('S','Soloist').replace('A','Assisting Artist')}"

				parameters = (
					f"Documents.Virtual-Folder.Program-work:Update"
					f"?CoreField.Legacy-Identifier={work.program_works_id}"
					f"&NYP.Soloist-/-Instrument-/-Role++={soloist_inst}{{'LinkedKeyword':'{soloist_role}'}}"
				)
				url = f"{baseurl}{datatable}{parameters}&token={token}"
				api_call(url, 'Add soloist and role to Program Work', work.program_works_id)

			# link the conductors
			if work.works_conductor_ids is not None:
				for conductor in work.works_conductor_ids.split(';'):
					parameters = (
						f"Documents.Virtual-Folder.Program-work:Update"
						f"?CoreField.Legacy-Identifier={work.program_works_id}"
						f"&NYP.Conductor+=[Contacts.Source.Default:CoreField.Artist-ID={conductor.strip()}]"
					)
					url = f"{baseurl}{datatable}{parameters}&token={token}"
					api_call(url,f'Link Conductor {conductor} to Program Work',work.program_works_id)

	logger.info("All done with Program Works")


# Update Scores and Parts
def library_updates(token):

	logger.info("Starting Printed Music updates...")

	# parse the XML file
	tree = etree.parse(f'{carlos_xml_path}/library_updates.xml')
	root = tree.getroot()

	# parse each row in the XML and assign values to variables
	for row in root.xpath(".//row"):
		legacy_id = xpath_text(row, "id/text()")

		# temporary addition to fix just a few Library records in Cortex
		# once updated, remove this row and un-tab rest of this function
		composer_id = xpath_text(row, "composer_id/text()")
		works_id = xpath_text(row, "works_id/text()")
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
		title = f"{composer_name} - {ar_works_title} - {display}"

		# Create/Update the Printed Music record

		# clear old values
		parameters = (
			f"Documents.Folder.Printed-Music:CreateOrUpdate"
			f"?CoreField.Legacy-Identifier=PM_{legacy_id}"
			f"&NYP.Composer/Work--=&NYP.Marking-Artist--=&NYP.Conductor--=&NYP.Composer--="
		)
		url = f"{baseurl}{datatable}{parameters}&token={token}"
		api_call(url,'Clear old metadata on Printed Music folder',legacy_id)

		# Send this data as a JSON payload because it might exceed the character limit for a regular API call
		data = {
			'CoreField.Legacy-Identifier': f'PM_{legacy_id}',
			'CoreField.parent-folder:': f'[Documents.All:CoreField.Identifier={PM_PARENT_FOLDER_IDENTIFIER}]',
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

		# link work to record
		parameters = (
			f"Documents.Folder.Printed-Music:Update"
			f"?CoreField.Legacy-Identifier=PM_{legacy_id}"
			f"&NYP.Composer-/-Work+=[Documents.Virtual-folder.Work:CoreField.Legacy-identifier=WORK_{works_id}]"
		)
		url = f"{baseurl}{datatable}{parameters}&token={token}"
		api_call(url, f'Link work {works_id} to the Printed Music folder',legacy_id)

		# link composer to record
		parameters = (
			f"Documents.Folder.Printed-Music:Update"
			f"?CoreField.Legacy-Identifier=PM_{legacy_id}"
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
					f"?CoreField.Legacy-Identifier=PM_{legacy_id}"
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
					f"?CoreField.Legacy-Identifier=PM_{legacy_id}"
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
				'CoreField.parent-folder:': f'[Documents.Folder.Printed-Music:CoreField.Legacy-Identifier=PM_{legacy_id}]',
				'CoreField.Import-Field-Mapping-Template:': PAGE_IMPORT_MAPPING_TEMPLATE,
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

			# link work to the score
			parameters = (
				f"Documents.Folder.Score:Update"
				f"?CoreField.Legacy-Identifier=MS_{legacy_id}"
				f"&NYP.Composer-/-Work+=[Documents.Virtual-folder.Work:CoreField.Legacy-identifier=WORK_{works_id}]"
			)
			url = f"{baseurl}{datatable}{parameters}&token={token}"
			api_call(url, f'Link work {works_id} to Score MS_{legacy_id} in Printed Music folder',legacy_id)

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
						'CoreField.parent-folder:': f'[Documents.Folder.Printed-Music:CoreField.Legacy-Identifier=PM_{legacy_id}]',
						'CoreField.Import-Field-Mapping-Template:': PAGE_IMPORT_MAPPING_TEMPLATE,
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

					# link work to the parts
					parameters = (
						f"Documents.Folder.Part:Update"
						f"?CoreField.Legacy-Identifier=MP_{part_id}"
						f"&NYP.Composer-/-Work+=[Documents.Virtual-folder.Work:CoreField.Legacy-identifier=WORK_{works_id}]"
					)
					url = f"{baseurl}{datatable}{parameters}&token={token}"
					api_call(url, f'Link work {works_id} to Part MP_{part_id} in Printed Music folder',legacy_id)

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


# update Concert Program folders
def concert_programs(programs, token):
	logger.info("Starting to update Concert Program folders...")

	count = 1
	total = len(programs)
	if total != 0:
		percent = round(count/total, 4)*100
	else:
		percent = 0

	logger.info(f"Creating/updating {total} Concert Program folders...")

	for program in programs:

		# Start making API calls
		logger.info("===========")
		logger.info(f'Updating Folder {count} of {total} -- {percent}% complete')
		logger.info("===========")

		# Create a folder within Archives/Concert Programs in the correct season and in the format PR_1234
		parameters = (
			f"Documents.Folder.Concert-program:CreateOrUpdate"
			f"?CoreField.Legacy-Identifier=PR_{program.id}"
			f"&CoreField.Title:=PR_{program.id}"
			f"&CoreField.visibility-class:=Public"
			f"&NYP.Program-ID:={program.id}"
			f"&NYP.Season+={program.season}"
			f"&CoreField.Parent-folder:=[Documents.Folder.Default:CoreField.Legacy-Identifier=PR_{program.season}]"
		)
		url = f"{baseurl}{datatable}{parameters}&token={token}"
		logger.info(url)
		api_call(url,'Create/Update Concert Program folder for Program',program.id)

		# Create an asset link with the corresponding Program virtual folder
		parameters = (
			f"Documents-links:ParentChildLink"
			f"?ParentRecord=[DataTable/V2.2/Documents.Virtual-Folder.Program:Read?CoreField.Legacy-Identifier={program.id}]"
			f"&ChildRecords=[DataTable/v2.2/Documents.Folder.Concert-program:Read?CoreField.Legacy-Identifier=PR_{program.id}]"
		)
		url = f"{baseurl}{datatable}{parameters}&token={token}"
		logger.info(url)
		api_call(url,'Link Concert Program folder to Program record',program.id)

		count += 1
		percent = round(count/total, 4)*100

	logger.info("Finished updating Concert Program folders")


# update Business Records
def update_business_records(token, filepath, name_id_mapping_file):

	# Initialize a set to store checked DBText IDs
	dbtext_id_checked = set()

	# Initialize a set to store checked Boxes
	box_checked = set()

	def check_and_create_source(record, name, attr, link_parameter):
		name_id = record.get_id_for_name(name)

		# Skip the function if name_id is None
		if name_id is None:
			logger.error(f"Could not find {name} in the DBText Name Thesaurus. Skipping this name.")
			return

		# Initialize a flag to control whether we should check in Cortex
		should_check_cortex = True

		if name_id in dbtext_id_checked:
			logger.info(f"We already checked {name_id} in Cortex. Moving on...")
			should_check_cortex = False
			
		if should_check_cortex:
			# Check if this DBText ID exists in Cortex
			parameters = f'Contacts.Source.Default:Read?CoreField.DBText-ID={name_id}&format=json'
			query = f"{baseurl}{datatable}{parameters}&token={token}"
			response = api_call(query, 'Checking if exists in Cortex: DBText ID', name_id)
			
			if response:
				response_data = response.json()
				dbtext_id_checked.add(name_id)

				if response_data and response_data['ResponseSummary']['TotalItemCount'] == 1:
					logger.info(f"We got a result in Cortex for that DBText ID. Moving on...")
				elif response_data and response_data['ResponseSummary']['TotalItemCount'] == 0:
					logger.warning(f"DBText Name ID {name_id} for {name} not found in Cortex")
					
					# Create a Source record
					parameters = f"Contacts.Source.Default:Create?CoreField.DBText-ID:={name_id}&CoreField.Last-name:={name}"
					url = f"{baseurl}{datatable}{parameters}&token={token}"
					api_call(url, f'Create Source record for', name)
				else:
					logger.error(f"We got more than one result for that DBText ID... Something is wrong.")
			
		# Link the source to the Business Record regardless of whether we checked in Cortex
		parameters = f"Documents.Folder.Business-document:Update?CoreField.Legacy-Identifier=BR_{record.folder_number}&{link_parameter}=[Contacts.Source.Default:CoreField.DBText-ID={name_id}]"
		url = f"{baseurl}{datatable}{parameters}&token={token}"
		api_call(url, f'Link {name} in the {attr} field for', record.folder_number)

	# Log the start of the process
	logger.info("Starting to update Business Records")

	# Load business records from XML
	records = load_business_records_data(filepath, name_id_mapping_file)

	count = 1
	total = len(records)
	if total != 0:
		percent = round(count/total, 4)*100
	else:
		percent = 0
		
	logger.info(f"Creating/updating {total} Business Records...")

	for record in records:

		# Start making API calls
		logger.info("===========")
		logger.info(f'Updating BR {count} of {total} -- {percent}% complete')
		logger.info("===========")

		# Make sure the Box folder exists; if not, create it and set as parent folder
		box_number = record.folder_number.rsplit('-', 1)[0]

		# Initialize a flag to control whether we should check the box in Cortex
		should_check_cortex = True

		if box_number in box_checked:
			logger.info(f"We already checked {box_number} in Cortex. Moving on...")
			should_check_cortex = False
			
		if should_check_cortex:
			# Check if this Box exists in Cortex
			parameters = f'Documents.Folder.Archives-Box:Read?CoreField.Legacy-Identifier=BOX_{box_number}&format=json'
			query = f"{baseurl}{datatable}{parameters}&token={token}"
			response = api_call(query, 'Checking if exists in Cortex: Box Number', box_number)
			
			if response:
				response_data = response.json()
				box_checked.add(box_number)

				if response_data and response_data['ResponseSummary']['TotalItemCount'] == 1:
					logger.info(f"We got a result in Cortex for that Box. Moving on...")
				elif response_data and response_data['ResponseSummary']['TotalItemCount'] == 0:
					logger.warning(f"Box {box_number} not found in Cortex")
					
					# Create a Box folder
					parameters = f"Documents.Folder.Archives-Box:Create?CoreField.Legacy-Identifier:=BOX_{box_number}&CoreField.Title:={box_number}&CoreField.Parent-folder:=[Documents.Folder.Default:CoreField.Identifier={BR_PARENT_FOLDER_IDENTIFIER}]"
					url = f"{baseurl}{datatable}{parameters}&token={token}"
					api_call(url, f'Create Box folder for', box_number)
				else:
					logger.error(f"We got more than one result for that Box number... Something is wrong.")

		"""
		See if this folder already exists in Cortex and what the parent folder is
		If the folder exists and the existing parent is correct, we can skip that parameter in the API call
		This will prevent Cortex from changing the manual order of the BR within the Box folder
		"""
		parameters = f'CoreField.Legacy-Identifier:BR_{record.folder_number} DocSubType:Business-document&fields=Document.LineageParentName&format=json'
		query = f'{baseurl}/API/search/v3.0/search?query={parameters}&token={token}'
		try:
			r = requests.get(query)
		except:
			logger.warning(f'Unable to find Business Record {record.folder_number}')
			pass

		if r:
			r_data = r.json()
			if r_data["APIResponse"]["GlobalInfo"]["TotalCount"] == 1:
				# We got one result, which is good
				existing_parent_id = r_data["APIResponse"]["Items"][0]["Document.LineageParentName"]
			else:
				# We have no result, or more than one parent, so we'll assign the parent as usual
				existing_parent_id = ""
		else:
			existing_parent_id = ""
			logger.warning(f"Unable to find Business Record {record.folder_number}")

		"""
		Now we can compare the parent_id from Cortex to the Box Number
		If the values match, do not update this field
		"""
		if existing_parent_id == box_number:
			update_parent = ""
		else:
			update_parent = f"&CoreField.Parent-folder:=[Documents.Folder.Archives-Box:CoreField.Legacy-identifier=BOX_{box_number}]"

		# Establish the record in Cortex
		parameters = (
			f"Documents.Folder.Business-document:CreateOrUpdate"
			f"?CoreField.Legacy-Identifier=BR_{record.folder_number}"
			f"&NYP.Folder-Number:={record.folder_number}"
			f"&NYP.People--=&NYP.Subjects--=&NYP.Language--=&NYP.Content-Type--="
			f"&CoreField.Import-Field-Mapping-Template:={PAGE_IMPORT_MAPPING_TEMPLATE}"
			f"{update_parent}"
		)
		url = f"{baseurl}{datatable}{parameters}&token={token}"
		api_call(url,"Establish Business Record",record.folder_number)

		# Should this be Public?
		if (record.make_public and record.make_public.startswith("Y")) or (record.is_public and record.is_public.startswith("Y")):
			visibility = "Public"
		else:
			visibility = "Hidden"

		# Reformat the accession date, which may or may not be in a standard format
		if record.accession_date:
			accession_date = process_date(record.accession_date.strip(), '%m/%d/%Y')
		else:
			accession_date = ''

		# Add more metadata
		data = {
			"CoreField.Legacy-Identifier": f"BR_{record.folder_number}",
			"CoreField.Title:": f"BR_{record.folder_number} - {record.folder_name}",
			"CoreField.Description:": record.abstract.strip() if record.abstract else '',
			"NYP.Archives-Folder-Title:": record.folder_name,
			"NYP.Date-Range:": record.date_range,
			"NYP.Subjects++": record.subjects,
			"NYP.Language++": record.language,
			"NYP.Archives-Location:": record.archives_location,
			"NYP.Record-Group+:": record.record_group,
			"NYP.Series+:": record.series,
			"NYP.Extent:": record.size,
			"NYP.Condition:": record.condition,
			"NYP.Content-Type++": record.content_type,
			"CoreField.notes:": record.notes,
			"NYP.Digitization-Notes:": record.digitization_notes,
			"CoreField.visibility-class:": visibility,
			"NYP.Accession-Date:": accession_date,
		}
		# fix for linebreaks and such - dump to string and load back to JSON
		data = json.dumps(data)
		logger.info(data)
		data = json.loads(data)

		action = 'Documents.Folder.Business-document:Update'
		params = {'token': token}
		url = f"{baseurl}{datatable}{action}"
		api_call(url, f'Add metadata to Business Record', record.folder_number, params, data)

		# Link Names to Record
		if record.names:
			for name in record.names.split('|'):
				check_and_create_source(record, name, "People", "NYP.People++")

		# Link Subseries to Record
		if record.subseries:
			check_and_create_source(record, record.subseries, "Subseries", "NYP.Sub-Series:")

		count += 1
		percent = round(count/total, 4)*100

	logger.info("Finished updating Business Records")


# update the visibility of Programs if they are in the past
def update_program_visibility(token):
	# Append the token to the search API URL
	search_api_url_with_token = f"{PROGRAM_SEARCH_API_URL}&token={token}"

	# Make the API request to get the Programs
	response = requests.get(search_api_url_with_token)
	
	if response.status_code == 200:
		json_data = response.json()
		today_date = datetime.datetime.now().date()

		programs_to_update = 0

		for item in json_data['APIResponse']['Items']:
			# Ignore items without a Program-ID
			if not item.get('NYP.Program-ID'):
				continue

			asset_date_str = item.get('CoreField.Asset-date', '')

			if not asset_date_str:
				continue

			asset_date_obj = datetime.strptime(asset_date_str, "%m/%d/%Y")

			if asset_date_obj.date() <= today_date:
				# Count the number of programs to update
				programs_to_update += 1

				# Log updating visibility
				logger.info(f"Updating visibility for Program-ID {item['NYP.Program-ID']} to Public")
				
				# Construct the API URL for updating the visibility
				update_url = f"https://cortex.nyphil.org/API/DataTable/v2.2/Documents.Virtual-folder.Program:Update?CoreField.Identifier={item['Identifier']}&CoreField.Visibility-class:=Public&token={token}"
				
				# Make the API request to update the visibility
				update_response = requests.post(update_url)

				if update_response.status_code == 200:
					logger.info(f"Successfully updated visibility for Program-ID {item['NYP.Program-ID']}")
				else:
					logger.error(f"Failed to update visibility for Program-ID {item['NYP.Program-ID']}. HTTP Status Code: {update_response.status_code}")

		# Log if there are no programs to update
		if programs_to_update == 0:
			logger.info("No programs to update.")

	else:
		logger.error(f"Failed to fetch data. HTTP Status Code: {response.status_code}")


# set up our api call structure
def api_call(url, asset_type, ID, params=None, data=None):
	# Initialize response to None
	response = None

	# Set the maximum number of attempts to make the call
	max_attempts = 2

	# Set the initial number of attempts to 0
	attempts = 0

	# Set a flag to indicate whether the call was successful
	success = False

	# Continue making the call until it is successful, or until the maximum number of attempts has been reached
	while not success and attempts < max_attempts:
		try:
			# Make the API call with the provided params and data
			response = requests.post(url, params=params, data=data)

			# If the response was successful, no Exception will be raised
			response.raise_for_status()

			# If no exceptions were raised, the call was successful
			success = True
		except HTTPError as http_err:
			# Handle HTTP errors
			logger.error(f'Failed: {asset_type} {ID} - HTTP error occurred: {http_err}')

			# Increment the number of attempts
			attempts += 1

			# Delay before the next attempt (if applicable)
			time.sleep(2)  # Sleep for 2 seconds

			# If the maximum number of attempts has been reached, raise an exception to stop the loop
			if attempts >= max_attempts:
				logger.error('Moving on...')
		except Exception as err:
			# Handle all other errors
			logger.error(f'Failed: {asset_type} {ID} - Other error occurred: {err}')

	# If the loop exited successfully, the call was successful
	if success:
		logger.info(f'Success: {asset_type} {ID}')
	return response


def main():
	# Starting the run
	logger.info('=======================')
	logger.info('Script started...')

	# Run the auth function to get a token
	token = auth()

	if token and token != '':
		logger.info(f'We have a token: {token} Proceeding...')
		print(f'Your token is: {token}')

		programs = load_program_data(program_xml) # right now we only need to load this data for the program_works function, but we'll eventually update the other functions to use Program objects, so we'll keep this function separate

		make_folders(token)
		update_folders(token)
		create_sources(token)
		add_sources_to_program(token)
		update_program_visibility(token)
		library_updates(token)
		create_or_update_works(programs, token)
		program_works(programs, token)
		concert_programs(programs, token)
		update_business_records(token, business_records_xml, name_id_mapping_file)

		logger.info('ALL DONE! Bye bye :)')

	else:
		logger.info('No API token :( Goodbye')


if __name__ == "__main__":
	main()