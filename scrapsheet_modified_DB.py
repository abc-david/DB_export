'''
Created on 23 dec. 2014

@author: david
'''
#!/usr/bin/env python
# -*- coding: utf-8 -*-

from import_functions_modified_DB import * 
from pandas.io import sql
from os import walk, rename


db_name = "test_base" #"postgres"
db_user = "postgres"
db_host = "192.168.0.49"
db_pass = "postgres"
db_package = [db_name, db_user, db_host, db_pass]
db_connect = DB_direct_connection(db_package)
if db_connect[0]:
    conn = db_connect[1]
    cur = db_connect[2]
    conn.close()
#db_schema = "export_regies"
write_table = "base"
csv_file = "/media/freebox/Fichiers/Pandas/Test/base/PCP-1_[mail,domain]_new-records_base_1k.csv"
csv_columns = ['mail', 'domain']

def show_df(df):
    print df.head(5)
    print str(len(df.index)) + " lines."
    
import_files = []
file_path = "/media/freebox/Fichiers/ImportDB/Test modified DB"
#file_path = "/media/freebox/Fichiers/ImportDB/Done"
#file_path = "/media/freebox/Fichiers/Export Regies/PCP/regie_pcp_sm"
for (dirpath, dirnames, filenames) in walk(file_path):
    import_files.extend(filenames)
    break


files_dict = {}
import collections
import os
import gc
for file_name in sorted(import_files):

    #if file_name == "6_IDM_Nov2009_[mail,civilite,prenom,nom,birth,sms,ad1,ad2,cp,ville]_556k.csv":
#for file_name in file_name_list:
    #args = extract_arguments(file_name)
    if "OK" != file_name[:2] and "PB" != file_name[:2]:
        file_num = float(extract_front_arguments(file_name)[0].replace('-', '.').replace('X', '.9'))
        files_dict[file_num] = file_name
    
    #import_file_to_DB(db_package, file_name, file_path, md5_mapping = True, md5_query_limit = "", mail_cleanup = True)

ordered_file_dict = collections.OrderedDict(sorted(files_dict.items()))
for file_num, file_name in ordered_file_dict.iteritems():
    args = extract_arguments(file_name)
    try:
        import_file_to_DB(db_package, file_name, file_path, md5_mapping = True, md5_query_limit = "", \
                          mail_cleanup = True)
        gc.collect()
        os.rename(file_path + "/" + file_name, file_path + "/" + "OK_" + file_name)
    except:
        os.rename(file_path + "/" + file_name, file_path + "/" + "PB_" + file_name)