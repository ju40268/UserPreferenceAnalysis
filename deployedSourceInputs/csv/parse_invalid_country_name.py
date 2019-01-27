#!/usr/local/bin/python
# coding: latin-1
import csv
import sys
month = str(sys.argv[1])
filename = 'geo_only_list_' + month 
csvfile = open(filename + '.csv', 'rb') # 1
def parse(country_name):
	return{
		'Czechia': 'Czech republic',
		'Curaçao': 'Curacao',
		'Bonaire, Sint Eustatius, and Saba': 'Netherlands', 
		'Sint Maarten':'Netherlands', 
		# 'Réunion': 'Reunion',
		'Réunion': 'France',
		'Guadeloupe':'France',
		'Åland' : 'Finland',
		'Mayotte' : 'France',
		'Martinique': 'France',
		'Gibraltar' : 'United Kingdom',
		'U.S. Virgin Islands': 'United States of America',
		'Saint-Barthélemy': 'France',
		'Myanmar [Burma]': 'Myanmar'
	}.get(country_name, country_name) 

fix_list = []
for row in csv.reader(csvfile, delimiter=','):
	row[0] = parse(row[0])
	fix_list.append([parse(row[0]), row[1]])
f = open('fixed_' + filename + '.csv','w')
w = csv.writer(f)
w.writerows(fix_list)