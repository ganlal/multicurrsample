# -*- coding: utf-8 -*-
"""
Created on Wed Aug 14 08:44:09 2024

@author: ganla
"""

import csv, sqlite3

with open("c:/data/mc/config.csv",newline='') as config_data:
    reader = csv.DictReader(config_data)
    config_dict = {}
    for row in reader:
        #print(row['config_tag'],row['config_value'])
        config_dict[row['config_tag']] = row['config_value']

#print(config_dict)
print (config_dict["curr_info_file"] )       

con = sqlite3.connect("c:/data/mc/test.db") # change to 'sqlite:///your_filename.db'
cur = con.cursor()
cur.execute("""CREATE TABLE IF NOT EXISTS curr_exp_ref (
        curr_num text null,
        curr_code text null,
        exponent float null);""" ) # use your column names here

with open(config_dict["curr_info_file"],'r') as fin: # `with` statement available in 2.5+
    # csv.DictReader uses first line in file for column headings by default
    dr = csv.DictReader(fin) # comma is default delimiter
    to_db = [(i['curr_num'], i['curr_code'], i['exponent']) for i in dr]

cur.executemany("INSERT INTO curr_exp_ref (curr_num, curr_code,exponent) VALUES (?, ?,?);", to_db)
con.commit()
con.close()