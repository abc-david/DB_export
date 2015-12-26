'''
Created on 23 july. 2014

@author: david
'''
#!/usr/bin/env python
# -*- coding: utf-8 -*-

from os import walk
from functions_panda import *

def explore_path(path):
    import_files = []
    for (dirpath, dirnames, filenames) in walk(path):
        import_files.extend(filenames)
        break
    return import_files

path = "/media/freebox/Fichiers/Export Regies/Raffles/Random extracts"
file_list = explore_path(path)
df_list = []
for xfile in file_list:
    df = pd.read_csv(path + "/" + xfile)
    #show_df(df)
    df_list.append(df)

mpath = "/media/freebox/Fichiers/Export Regies/Raffles"
mfile = "md5-unknown-raffles_15-juil-14.csv"
mdf = pd.read_csv(mpath + "/" + mfile)

for cpt in range(len(file_list)):
    print file_list[cpt]
    res = map_existing_rows(mdf, df_list[cpt])
    if res[0]:
        #show_df(res[1])
        print res[2]
        
        

