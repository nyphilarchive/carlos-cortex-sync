# carlos-cortex-sync
Scripts to mirror changes to our performance history database in our DAMS.

## parse-carlos-data.py
Takes a CSV export from Carlos and produces several CSVs for Cortex:
1. list of virtual folders; Cortex should create any that do not exist
2. list of people to update our Source Accounts in Cortex
3. composers.csv, conductors.csv, soloists.csv
4. metadata for the virtual folders, excluding composers, conductors, and soloists

## cortex-updates.py
Reads the CSVs created above and executes the appropriate API calls to create or update records in Cortex/OrangeDAM