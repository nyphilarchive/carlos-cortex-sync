# take a CSV export from Carlos and produce several CSVs for Cortex
# 1. list of virtual folders; Cortex should create any that do not exist
# 2. list of people to update our Source Accounts in Cortex
# 3. composers.csv, conductors.csv, soloists.csv
# 4. metadata for the virtual folders, excluding composers, conductors, and soloists
# by Bill Levay

import sys, csv, json, re, os
from os.path import join, dirname
from dotenv import load_dotenv

# First, grab the value for the Carlos export directory from the .env file in the same folder as this script
dotenv_path = join(dirname(__file__), '.env')
load_dotenv(dotenv_path)

directory = os.environ.get('export', 'default')

def make_folders(data,all_data):
# create csv of virtual folder names

	print('Creating Virtual Folder output file...')

	# set up the folder list
	folders = []

	# loop through the Carlos data
	for program in data:

		folder = {}
		season = data[program]['SEASON']

		# fix for 1899-00 and 1999-00
		if season.endswith('00'):
			year = season.split('-')[0]
			next_year = str(int(year) + 1)
			season = year + '-' + next_year.zfill(2)

		folder['season'] = season
		folder['program_id'] = program

		# do we have a week number?
		if data[program]['WEEK'] != '':
			week = f'Wk {data[program]["WEEK"]} / '
		else:
			week = ''

		# check for multiple dates; if yes add a star
		if '|' in data[program]['DATE']:
			star = '*'
		else:
			star = ''

		# get the date
		date = data[program]['DATE_RANGE'][0:10]

		# clean up conductor name
		conductor = data[program]['CONDUCTOR_LAST_NAME'].split('|')[0]
		conductor = bytes(conductor,'iso-8859-1').decode('utf-8')
		conductor = re.sub("\\[.*?\\]","",conductor)
		conductor = re.sub("\\(.*?\\)","",conductor)
		if conductor != '':
			conductor = ' / ' + conductor.strip()

		# clean up sub event
		sub_event = data[program]['SUB_EVENT_NAMES'].split('|')[0].replace('Subscription Season', 'Sub').replace('Non-Subscription', 'Non-Sub').strip()
		if sub_event != '':
			sub_event = ' / ' + sub_event

		# build the folder name
		folder['folder_name'] =	f'{week}{date}{star}{sub_event}{conductor}'
		
		# finally, add it to the list
		folders.append(folder)

	# with open(directory+'cortex_folder_names.json', 'w', encoding='UTF-8') as jsonfile:
	# 	jsonfile.write(json.dumps(folders, indent=4))

	fieldnames = folders[0].keys()
	with open(directory+'cortex/cortex_folder_names.csv', 'w', newline='', encoding='UTF-8') as csvfile:
		writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
		writer.writeheader()
		for folder in folders:
			writer.writerow(folder)

	print('Done')



def sources(data):
	### Export all people (conductors, soloists, composers) in case we need to update or create new Source Account in Cortex
	### We'll have to do this in two files since soloist/conductors use Artist ID and composers use Composer ID
	# include: 
	# Artist or Composer ID
	# First, Middle, Last names
	# Roles -- conductor, composer, instrument
	# Orchestra members

	print('Creating Sources output files')

	artists = {}
	composers = {}

	for program in data:

		# loop through conductors, soloists, and composers
		# append dictionary items for each person if they don't yet exist in their respective dicts

		# Start with conductors
		i = 0
		for conductor in data[program]['CONDUCTOR'].split('|'):
			if conductor != '' and int(conductor) not in artists:
				artists[int(conductor)] = {
					'Artist ID': conductor,
					'First Name': bytes(data[program]['CONDUCTOR_FIRST_NAME'].split('|')[i], 'iso-8859-1').decode('utf-8'),
					'Middle Name': bytes(data[program]['CONDUCTOR_MIDDLE_NAME'].split('|')[i], 'iso-8859-1').decode('utf-8'),
					'Last Name': bytes(data[program]['CONDUCTOR_LAST_NAME'].split('|')[i], 'iso-8859-1').decode('utf-8'),
					'Birth Year': data[program]['CONDUCTOR_YEAR_OF_BIRTH'].split('|')[i],
					'Death Year': data[program]['CONDUCTOR_YEAR_OF_DEATH'].split('|')[i],
					'Role': 'Conductor',
					'Orchestra': '',
					'Orchestra Years': ''
					}
			i += 1

		# Now loop through soloists
		# reset counter
		i = 0
		for soloist in data[program]['SOLOIST'].split('|'):

			if soloist != '':
				soloistID = int(soloist)
			else:
				soloistID = None

			# fix for soloists that don't have an instrument on a program
			if i < len(data[program]['SOLOIST_INSTRUMENT'].split('|')):
				instrument = data[program]['SOLOIST_INSTRUMENT'].split('|')[i]
			else:
				instrument = ''

			if i < len(data[program]['SOLOIST_MEMBER_ORCH_NAME'].split('|')):
				orchestra = data[program]['SOLOIST_MEMBER_ORCH_NAME'].split('|')[i].lstrip()
			else:
				orchestra = ''

			if i < len(data[program]['SOLOIST_MEMBER_ORCH_YEARS'].split('|')):
				orch_years = data[program]['SOLOIST_MEMBER_ORCH_YEARS'].split('|')[i].replace(' ','')
			else:
				orch_years = ''

			if soloistID is not None and soloistID not in artists:
				artists[soloistID] = {
					'Artist ID': soloist,
					'First Name': bytes(data[program]['SOLOIST_FIRST_NAME'].split('|')[i], 'iso-8859-1').decode('utf-8'),
					'Middle Name': bytes(data[program]['SOLOIST_MIDDLE_NAME'].split('|')[i], 'iso-8859-1').decode('utf-8'),
					'Last Name': bytes(data[program]['SOLOIST_LAST_NAME'].split('|')[i], 'iso-8859-1').decode('utf-8'),
					'Birth Year': data[program]['SOLOIST_YEAR_OF_BIRTH'].split('|')[i],
					'Death Year': data[program]['SOLOIST_YEAR_OF_DEATH'].split('|')[i],
					'Role': instrument,
					'Orchestra': orchestra,
					'Orchestra Years': orch_years,
				}

			# if the person is already in the artist list, combine all roles
			elif soloistID in artists:

				# grab existing roles from the dict and turn into list
				existing_roles = artists[soloistID]['Role'].split('|')

				if instrument not in existing_roles:
					existing_roles.append(instrument)

				artists[soloistID]['Role'] = ('|').join(existing_roles)

			i += 1

		# Finally, loop through composers
		# reset counter
		i = 0
		for composer in data[program]['COMPOSER_NUMBER'].split('|'):
			if composer != '' and int(composer) not in composers:
				composers[int(composer)] = {
					'Composer ID': composer,
					'First Name': bytes(data[program]['COMPOSER_FIRST_NAME'].split('|')[i], 'iso-8859-1').decode('utf-8'),
					'Middle Name': bytes(data[program]['COMPOSER_MIDDLE_NAME'].split('|')[i], 'iso-8859-1').decode('utf-8'),
					'Last Name': bytes(data[program]['COMPOSER_LAST_NAME'].split('|')[i], 'iso-8859-1').decode('utf-8'),
					'Birth Year': data[program]['COMPOSER_YEAR_OF_BIRTH'].split('|')[i],
					'Death Year': data[program]['COMPOSER_YEAR_OF_DEATH'].split('|')[i],
					'Role': 'Composer',
				}
			i += 1

	# save as json
	# print('Writing Artist JSON file...')
	# with open(directory+'cortex/source_accounts_artists.json', 'w', encoding='UTF-8') as jsonfile:
	# 	jsonfile.write(json.dumps(artists, indent=4, sort_keys=True))
	# print('Done')

	# save as json
	# print('Writing Composer JSON file...')
	# with open(directory+'cortex/source_accounts_composers.json', 'w', encoding='UTF-8') as jsonfile:
	# 	jsonfile.write(json.dumps(composers, indent=4, sort_keys=True))
	# print('Done')

	# save as csv
	print('Writing Artist CSV file...')
	fieldnames = next(iter(artists.values())).keys()
	with open(directory+'cortex/source_accounts_artists.csv', 'w', newline='', encoding='UTF-8') as csvfile:
		writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
		writer.writeheader()
		for artist in artists:
			writer.writerow(artists[artist])
	print('Done')

	# save as csv
	print('Writing Composer CSV file...')
	fieldnames = next(iter(composers.values())).keys()
	with open(directory+'cortex/source_accounts_composers.csv', 'w', newline='', encoding='UTF-8') as csvfile:
		writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
		writer.writeheader()
		for composer in composers:
			writer.writerow(composers[composer])
	print('Done')



def people(data, person_type):
	# match people and programs by their Carlos ID

	print('Creating People output files...')

	# set up list
	people = []

	# variables based on person_type
	if person_type == 'composers':
		ID = 'COMPOSER_NUMBER'
		ID_header = 'Composer ID'

	elif person_type == 'conductors':
		ID = 'CONDUCTOR'
		ID_header = 'Artist ID'

	elif person_type == 'soloists':
		ID = 'SOLOIST'
		ID_header = 'Artist ID'

	for program in data:
		# create list of tuples containing program ID and person ID
		program_people = data[program][ID].split('|')
		for person in program_people:
			if person != '':
				row = (program, person)
				people.append(row)

	# dump this list to CSV
	print('Writing', person_type, 'to CSV...')
	with open(directory+'cortex/'+person_type+'.csv', 'w', encoding='UTF-8') as f:
		csv_writer = csv.writer(f)
		headers = ['Program ID', ID_header]
		csv_writer.writerow(headers)
		for row in people:
			csv_writer.writerow(row)
	print('Done')



def program_data(data):
	print('Creating Program Data output file...')

	for program in data:

		# fix for 1899-00 and 1999-00
		season = data[program]['SEASON']
		if season.endswith('00'):
			year = season.split('-')[0]
			next_year = str(int(year) + 1)
			season = year + '-' + next_year.zfill(2)
			data[program]['SEASON'] = season
		
		# clean up whitespace around "New York Philharmonic", etc
		data[program]['ORCHESTRA_NAME'] = data[program]['ORCHESTRA_NAME'].strip()
		data[program]['COMPOSER_NAME'] = bytes(data[program]['COMPOSER_NAME'], 'iso-8859-1').decode('utf-8').replace('  ',' ')
		data[program]['COMPOSER_TITLE'] = bytes(data[program]['COMPOSER_TITLE'], 'iso-8859-1').decode('utf-8').replace('  ',' ')
		data[program]['COMPOSER_TITLE_SHORT'] = bytes(data[program]['COMPOSER_TITLE_SHORT'], 'iso-8859-1').decode('utf-8').replace('  ',' ')
		data[program]['SOLOIST_SLASH_INSTRUMENT'] = bytes(data[program]['SOLOIST_SLASH_INSTRUMENT'], 'iso-8859-1').decode('utf-8').replace('/',' / ')

		# remove some columns we don't need for this output file
		cols_to_remove = ['PRIMARY_PROGRAM_FLAG','CONDUCTOR','CONDUCTOR_NAMES','CONDUCTOR_FIRST_NAME','CONDUCTOR_MIDDLE_NAME','CONDUCTOR_LAST_NAME','CONDUCTOR_YEAR_OF_BIRTH','CONDUCTOR_YEAR_OF_DEATH','SOLOIST','SOLOIST_INSTRUMENT','SOLOIST_YEAR_OF_BIRTH','SOLOIST_YEAR_OF_DEATH','SOLOIST_NAME','SOLOIST_FIRST_NAME','SOLOIST_MIDDLE_NAME','SOLOIST_LAST_NAME','SOLOIST_MEMBER_ORCH_NAME','SOLOIST_MEMBER_ORCH_YEARS','COMPOSER_NUMBER','COMPOSER_NAME','COMPOSER_FIRST_NAME','COMPOSER_MIDDLE_NAME','COMPOSER_LAST_NAME','COMPOSER_YEAR_OF_BIRTH','COMPOSER_YEAR_OF_DEATH','RELATED_PROG_INFO']
		for col in cols_to_remove:
			data[program].pop(col, None)

	# save as json
	# print('Writing JSON file...')
	# with open(directory+'cortex/program_data_for_cortex.json', 'w', encoding='UTF-8') as jsonfile:
	# 	jsonfile.write(json.dumps(data, indent=4))
	# print('Done')

	# save as csv
	print('Writing CSV file...')
	fieldnames = next(iter(data.values())).keys()
	with open(directory+'cortex/program_data_for_cortex.csv', 'w', newline='', encoding='UTF-8') as csvfile:
		writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
		writer.writeheader()
		for program in data:
			writer.writerow(data[program])
	print('Done')


#####################
## KICK THINGS OFF ##
#####################

# pass the file name as an argument
path = sys.argv[1]

# set up dictionaries and open csv
carlos_data = {}
carlos_input = csv.DictReader(open(path, encoding='ISO-8859-1'))
all_carlos_data = {}
all_carlos_path = os.path.realpath(path).replace('_updates','')
all_carlos_input = csv.DictReader(open(all_carlos_path, encoding='ISO-8859-1'))

# populate dictionaries
for row in carlos_input:
	programId = row['ID']
	carlos_data[programId] = row

for row in all_carlos_input:
	programId = row['ID']
	all_carlos_data[programId] = row

#########################
## create output files ##
#########################

make_folders(carlos_data,all_carlos_data)

sources(carlos_data)

people(carlos_data,'composers')
people(carlos_data,'conductors')
people(carlos_data,'soloists')

program_data(carlos_data)