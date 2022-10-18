# take a CSV export from Carlos and produce several CSVs for Cortex
# 1. list of virtual folders; Cortex should create any that do not exist
# 2. list of people to update our Source Accounts in Cortex
# 3. composers.csv, conductors.csv, soloists.csv
# 4. metadata for the virtual folders, excluding composers, conductors, and soloists
# by Bill Levay

import sys, csv, json, re, os
from dotenv import load_dotenv

# First, grab the value for the Carlos export directory from the .env file in the same folder as this script
dotenv_path = join(dirname(__file__), '.env')
load_dotenv(dotenv_path)

directory = os.environ.get('export', 'default')

def make_folders(data,all_data):
# create csv of virtual folder names
# for programs with related programs only the one with the primary flag should get a top-level folder
# the other related programs should be child folders

	print('Creating Virtual Folder output file...')

	# get the list of Season folders and their Cortex IDs
	season_file = csv.DictReader(open(directory+'cortex-seasons.csv'))
	seasons = {}
	for row in season_file:
		season = row['season']
		season_id = row['id']
		seasons[season] = season_id

	# set up the folder list
	folders = []

	# loop through the Carlos data
	for program in data:

		# only create folders for seasons that are already in Cortex
		# you may need to run a new Cortex report for season folders
		# go to Programs top-level folder, turn off "see thru", filter for Containers only, run report
		season = data[program]['SEASON']
		if season in seasons:
			folder = {}
			folder['season_folder_id'] = seasons[season]
			folder['program_id'] = program

			# do we have a week number?
			if data[program]['WEEK'] != '':
				week = 'Wk ' + data[program]['WEEK'] + ' / '
			else:
				week = ''

			# check for multiple dates or if a primary folder; if yes add a star
			if '|' in data[program]['DATE']:
				star = '*'
			elif data[program]['PRIMARY_PROGRAM_FLAG'] == 'Primary':
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
			sub_event = data[program]['SUB_EVENT_NAMES'].split('|')[0].replace('Subscription Season', 'Sub').replace('Non-Subscription', 'Non-Sub')
			if sub_event != '':
				sub_event = ' / ' + sub_event

			# build the folder name
			folder['folder_name'] =	week + date + star + sub_event + conductor

			# should this be a primary or secondary folder?
			# if it has no related programs it's a primary
			if data[program]['RELATED_PROG_IDS'] == '':
				folder['level'] = 'primary'
				folder['parent_program_id'] = ''
			# if it has the Primary flag it's a primary
			elif data[program]['PRIMARY_PROGRAM_FLAG'] == 'Primary':
				folder['level'] = 'primary'
				folder['parent_program_id'] = ''
			# otherwise it's a secondary
			else:
				folder['level'] = 'secondary'


			# if it's a secondary, get the primary ID
			# look in all_data because the primary might not be included in the update csv
			if folder['level'] == 'secondary':
				
				# get the related program IDs
				# loop through them and determine which one is the primary
				related = []
				also_related = []
				if '|' in all_data[program]['RELATED_PROG_IDS']:
					related = all_data[program]['RELATED_PROG_IDS'].split('|')
				else:
					related = [all_data[program]['RELATED_PROG_IDS']]
				for x in related:
					if x in all_data and all_data[x]['PRIMARY_PROGRAM_FLAG'] == 'Primary':
						folder['parent_program_id'] = x
					elif x in all_data:
						also_related += all_data[x]['RELATED_PROG_IDS'].split('|')
				
				# if we didn't get a parent_program_id, check the also related list
				if 'parent_program_id' not in folder and len(also_related) > 0:
					for y in also_related:
						if y != '' and all_data[y]['PRIMARY_PROGRAM_FLAG'] == 'Primary':
							folder['parent_program_id'] = y
			
			# finally, add it to the list
			folders.append(folder)

	# with open(directory+'cortex_folder_names.json', 'w', encoding='UTF-8') as jsonfile:
	# 	jsonfile.write(json.dumps(folders, indent=4))

	fieldnames = folders[1].keys()
	with open(directory+'cortex/cortex_folder_names.csv', 'w', newline='', encoding='UTF-8') as csvfile:
		writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
		writer.writeheader()
		for folder in folders:
			if folder['level'] == 'primary':
				writer.writerow(folder)
		for folder in folders:
			if folder['level'] == 'secondary':
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
					'Display Name': bytes(data[program]['CONDUCTOR_NAMES'].split('|')[i], 'iso-8859-1').decode('utf-8'),
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
					'Display Name': bytes(data[program]['SOLOIST_NAME'].split('|')[i], 'iso-8859-1').decode('utf-8'),
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
					'Display Name': bytes(data[program]['COMPOSER_NAME'].split('|')[i], 'iso-8859-1').decode('utf-8').replace('  ',' '),
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
		
		# clean up whitespace around "New York Philharmonic", etc
		data[program]['ORCHESTRA_NAME'] = data[program]['ORCHESTRA_NAME'].strip()
		data[program]['COMPOSER_NAME'] = bytes(data[program]['COMPOSER_NAME'], 'iso-8859-1').decode('utf-8').replace('  ',' ')
		data[program]['COMPOSER_TITLE'] = bytes(data[program]['COMPOSER_TITLE'], 'iso-8859-1').decode('utf-8').replace('  ',' ')
		data[program]['COMPOSER_TITLE_SHORT'] = bytes(data[program]['COMPOSER_TITLE_SHORT'], 'iso-8859-1').decode('utf-8').replace('  ',' ')
		data[program]['SOLOIST_SLASH_INSTRUMENT'] = bytes(data[program]['SOLOIST_SLASH_INSTRUMENT'], 'iso-8859-1').decode('utf-8').replace('/',' / ')

		# remove some columns we don't need for this output file
		cols_to_remove = ['PRIMARY_PROGRAM_FLAG','CONDUCTOR','CONDUCTOR_NAMES','CONDUCTOR_FIRST_NAME','CONDUCTOR_MIDDLE_NAME','CONDUCTOR_LAST_NAME','CONDUCTOR_YEAR_OF_BIRTH','CONDUCTOR_YEAR_OF_DEATH','SOLOIST','SOLOIST_INSTRUMENT','SOLOIST_YEAR_OF_BIRTH','SOLOIST_YEAR_OF_DEATH','SOLOIST_NAME','SOLOIST_FIRST_NAME','SOLOIST_MIDDLE_NAME','SOLOIST_LAST_NAME','SOLOIST_MEMBER_ORCH_NAME','SOLOIST_MEMBER_ORCH_YEARS','COMPOSER_NUMBER','COMPOSER_NAME','COMPOSER_FIRST_NAME','COMPOSER_MIDDLE_NAME','COMPOSER_LAST_NAME','COMPOSER_YEAR_OF_BIRTH','COMPOSER_YEAR_OF_DEATH','RELATED_PROG_IDS','RELATED_PROG_INFO']
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

# get the list of Season folders
season_file = csv.DictReader(open(directory+'cortex-seasons.csv'))
seasons = []
for row in season_file:
	seasons.append(row['season'])

# populate dictionaries
for row in carlos_input:
	programId = row['ID']
	if row['SEASON'] in seasons:
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