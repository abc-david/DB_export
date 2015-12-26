'''
Created on 23 july. 2014

@author: david
'''
#!/usr/bin/env python
# -*- coding: utf-8 -*-

#from functions_panda import *
#from export_regie_functions import *
import random
import time
import pandas as pd
import dateutil.parser as dparser
from import_functions_OVH_DB import write_to_csv


def show_df(df):
    print df.head(5)
    print str(len(df.index)) + " lines."

def format_civilite_for_PCP(value):
    select_case = {'NaN' : 'NaN', '1.0' : 'M', '2.0' : 'MLLE', '3.0' : 'MME'}
    str_value = str(value)
    if str_value in select_case.keys():
        if select_case[str_value]:
            return select_case[str_value]
    return str_value

def remove_floating_part(value):
    num_sep = [",", "."]
    for item in num_sep:
        if item in value:
            pos = value.find(item)
            return int(value[:pos])
    if isinstance(value, basestring):
        if value.lower() == "nan":
            return ""
    return value

def isoformat_birth(value, dayfirst = True):
    if str(value).lower() == "nan": return ""
    if isinstance(value, basestring):
        try:
            parsed_date = dparser.parse(value, fuzzy = True, dayfirst = dayfirst)
        except:
            return ""
        try:
            formatted_date = parsed_date.strftime('%Y%m%d000000')
            #formatted_date = parsed_date.isoformat()
        except:
            try:
                print "date problem :" + str(parsed_date)
            except:
                pass
            return ""
        #print "clean_birth : " + value + " --> " + str(parsed_date) + " --> " + str(formatted_date)
        return formatted_date
    else:
        try:
            return value.isoformat()
        except:
            print "date problem :" + str(value)

def load_available_IP():
    db_name = "prod" #"postgres"
    db_user = "postgres"
    db_host = "localhost"
    db_pass = "penny9690"
    db_package = [db_name, db_user, db_host, db_pass]
    connect_token = "dbname='" + db_package[0] + "' user='" + db_package[1] + \
                    "' host='" + db_package[2] + "' password='" + db_package[3] + "'"
    import psycopg2
    conn = psycopg2.connect(connect_token)
    from pandas.io import sql
    df_ip = sql.read_sql("SELECT DISTINCT ip FROM %s" % ('lead'), conn)
    conn.close()
    show_df(df_ip)
    return df_ip

def new_time(start, end, prop, day_light = True):
    """ creates a time somewhere in btw. start & end time
    day_light option makes sure the hour is btw. 6 and 23 """
    ptime = start + prop * (end - start)
    if day_light:
        time_as_tuple = time.localtime(ptime)
        h = time_as_tuple[3]
        if h < 6:
            time_as_list = list(time_as_tuple)
            time_as_list[3] = random.randint(6,23)
            ptime = time.mktime(tuple(time_as_list))
    return ptime

def list_of_random_dates(start, end, format, number, day_light = True):
    """ generates a list of n random dates btw. start & end dates at a given str format
    ex. list_of_random_dates("1/1/2008 1:30 PM", "1/1/2009 4:50 AM", '%m/%d/%Y %I:%M %p', 1000) """
    stime = time.mktime(time.strptime(start, format))
    etime = time.mktime(time.strptime(end, format))
    random_dates = []
    for cpt_dates in range(number):
        prop = random.random()
        random_time = new_time(stime, etime, prop, day_light)
        string_date = time.strftime(format, time.localtime(random_time))
        random_dates.append(string_date)
    return random_dates

def format_csv_for_PCP(folder_path, folder, file_name, csv_col_list):
    path_to_file = folder_path + "/" + folder + "/" + file_name
    df = pd.read_csv(path_to_file, header = 0, names = csv_col_list, sep = ";", dtype = object)
    show_df(df)
    df['prenom'] = df['prenom'].str.lower()
    df['nom'] = df['nom'].str.lower()
    df['ville'] = df['ville'].str.lower()
    df['civilite'] = df['civilite'].apply(format_civilite_for_PCP)
    df['civilite'] = df['civilite'].apply(remove_floating_part)
    df['birth'] = df['birth'].apply(isoformat_birth)
    ip_list = list(load_available_IP()['ip'])
    ip_list = random.sample(ip_list, len(df.index))
    df['ip'] = ip_list
    df['date'] = list_of_random_dates('20131010133000', '20140221045000', \
                                    '%Y%m%d%H%M%S', len(df.index))
    df.sort('mail', inplace = True)
    #for cpt in len(df.index):
    #    cpt_df = df.index[cpt]
    #    this_mail = df.at[cpt_df, 'mail']
    #    partial_df = df[df['mail'] == this_mail]
    #    len_partial_df = len(partial_df.index)

    df['pays'] = "france"
    df['adresse'] = ""
    pcp_col_order_list = ['mail', 'date', 'ip', 'civilite', 'nom', 'prenom', 'adresse', 'cp', 'ville', 'birth', 'pays']
    show_df(df[pcp_col_order_list])
    path_to_file = folder_path + "/" + folder + "/PCPFormat_" + file_name
    df[pcp_col_order_list].to_csv(path_to_file, index = False, header = False, sep = ";", na_rep = "")
    #write_to_csv(df[pcp_col_order_list], folder_path, folder, "/PCPFormat_" + file_name, header = False, na_rep = "")

def rewrite_csv_with_mail_unik(folder_path, folder, file_name, csv_col_list):
    path_to_file = folder_path + "/" + folder + "/" + file_name
    df = pd.read_csv(path_to_file, header = 0, names = csv_col_list, sep = ";", dtype = object)
    show_df(df)
    df = df.drop_duplicates('mail').sort('mail')
    write_to_csv(df, folder_path, folder, "/MailUnik_" + file_name, header = False, na_rep = "")

folder_path = "/home/david/fichiers/export"
folder = "regie_mail/pcp"
file_name = "ExportMail_PCP_IGLau-MailUnik_IDUnik_2015-09-17_[mail,civilite,prenom,nom,cp,ville,birth].csv"
#file_name = "MailUnik_ExportPCPWeLove_IGLau-Clean_2015-09-17_[mail,civilite,prenom,nom,cp,ville,birth].csv"
file_date = "2015-09-17"
col_string = "[mail,civilite,prenom,nom,cp,ville,birth]"

#rewrite_csv_with_mail_unik(folder_path, folder, \
#                           "ExportPCPWeLove_f200-Clean_2015-04-20_[mail,civilite,prenom,nom,cp,ville,birth].csv", \
#                           list(col_string[1:-1].split(',')))
format_csv_for_PCP(folder_path, folder, file_name, list(col_string[1:-1].split(',')))
#load_available_IP()

# Script to get rid of duplicates between two files already in 'export regie' format
"""
adlead_path = "/media/freebox/Fichiers/Export Regies/AdLead"
adlead_name = "AdLead_Export_23-juin-2014_[mail,prenom,nom,civilite,birth,cp,ville,ip,provenance,date]_[all-emarsys]_non-dedup.csv"
adlead_header = ["mail","prenom","nom","civilite","birth","cp","ville","ip","provenance","date"]
load_res = load_dataframe_from_export_file(adlead_path, adlead_name, adlead_header, status = "adlead")
if load_res[0]:
    ad_df = load_res[1]
    show_df(ad_df)
    
rad_path = "/media/freebox/Fichiers/Export Regies/R Advertising"
rad_name = "R-Advertising_Export_23-juin-2014_[mail,prenom,nom,civilite,birth,cp,ville,ip,provenance,date]_[all-id]_non-dedup.csv"
rad_header = ["mail","prenom","nom","civilite","birth","cp","ville","ip","provenance","date"]
load_res = load_dataframe_from_export_file(rad_path, rad_name, rad_header, status = "r-advertising")
if load_res[0]:
    ra_df = load_res[1]
    show_df(ra_df)

flint_path = "/media/freebox/Fichiers/Export Regies/FL Interactive"
flint_name = "FL-Interactive_Export_18-juin-2014_[mail,prenom,nom,civilite,birth,cp,ville,ip,provenance,date]_[sm-wl]_non-dedup.csv"
flint_header = ["mail","prenom","nom","civilite","birth","cp","ville","ip","provenance","date"]
load_res = load_dataframe_from_export_file(flint_path, flint_name, flint_header, status = "fl_interactive")
if load_res[0]:
    fl_df = load_res[1]
    show_df(fl_df)
    
merged_df = pd.merge(ra_df, fl_df, how = 'left', sort = False)
merged_df = pd.merge(merged_df, ad_df, how = 'left', sort = False)
#show_df(merged_df)

newfl_df = merged_df[(merged_df["fl_interactive"] != True) & (merged_df["adlead"] != True)]
print "Only new mails"
show_df(newfl_df)

plainte_path = "/media/freebox/Fichiers/Export Regies/Plaintes Mimi"
plainte_df = load_dataframe_blacklist(plainte_path, 'plainte_mimi')
print "All Plaintes"
show_df(plainte_df)

merged_df = pd.merge(newfl_df, plainte_df, how = 'left', sort = False)
#show_df(merged_df)

newfl_df = merged_df[(merged_df["plainte"] != True)]
print "Without Plaintes"
show_df(newfl_df)

mailonly_df = pd.DataFrame(columns = ["mail"])
mailonly_df["mail"] = newfl_df["mail"].drop_duplicates()
print "Only Mails -- unique & sorted"
show_df(mailonly_df)

mailonly_df = check_mot_cle_columnwide(mailonly_df)
newfl_df = pd.merge(newfl_df, mailonly_df, how = 'left', sort = False)
newfl_df = newfl_df[newfl_df['exclusion_mot_cle'] == False]
newfl_df = newfl_df[flint_header]
print "Without Mots cles repoussoir"
show_df(newfl_df)

newfl_name = "FL-Interactive_Export-3_4-sept-2014_[mail,prenom,nom,civilite,birth,cp,ville,ip,provenance,date]_[intersect-emars-rad]_non-dedup.csv"
write_to_csv(newfl_df, flint_path, "Intersection R-Advertising", newfl_name, header = True)

clean_csv(flint_path + "/Intersection R-Advertising/" + newfl_name)
"""

"""
res_path = "/media/freebox/Fichiers/Export Regies/FL Interactive/Intersection R-Advertising"
res_name = "FL-Interactive_Export-3_4-sept-2014_[mail,prenom,nom,civilite,birth,cp,ville,ip,provenance,date]_[intersect-emars-rad]_non-dedup.csv"
res_name_dedup = "FL-Interactive_Export-3_4-sept-2014_[mail,prenom,nom,civilite,birth,cp,ville,ip,provenance,date]_[intersect-emars-rad]_dedup.csv"
res_name_mail_only = "FL-Interactive_Export-3_4-sept-2014_[mail]_[intersect-emars-rad]_dedup.csv"
res_header = ["mail","prenom","nom","civilite","birth","cp","ville","ip","provenance","date"]
load_res = load_dataframe_from_export_file(res_path, res_name, res_header)
if load_res[0]:
    res_df = load_res[1]
    show_df(res_df)
res_df = res_df.drop_duplicates(["mail"])
show_df(res_df)
write_to_csv(res_df, res_path, folder = '', file_name = res_name_dedup, header = True)
clean_csv(res_path + "/" + res_name_dedup)
res_df = res_df[["mail"]]
show_df(res_df)
write_to_csv(res_df, res_path, folder = '', file_name = res_name_mail_only, header = True)
"""

# Script to exclude files from other files
"""
res_path = "/media/freebox/Fichiers/Export Regies/Weedoit"
file_list = ["WeedoIT_ActifPCP-et-Ouvreurs_[mail,date,civilite,prenom,nom,cp, ville, birth]_2015-03-12.csv"]
col_list = ['mail', 'date', 'civilite', 'prenom', 'nom', 'cp', 'ville', 'birth']
optin_df = pd.DataFrame(columns = col_list)
if len(file_list) > 1:
    for file_name in file_list:
        res_df = load_dataframe_from_export_file(res_path, file_name, header = col_list, status = "optin")
        if res_df[0]:
            temp_df = pd.DataFrame(columns = ['mail'])
            temp_df = res_df[1]
            #show_df(temp_df)
            optin_df = pd.concat([optin_df, temp_df])
else:
    file_name = file_list[0]
    optin_df = pd.read_csv(res_path + "/" + file_name, sep = ";", header=None, names = col_list)
    optin_df['optin'] = True
    show_df(optin_df)
        
#optin_df = clean_mail_syntax(optin_df)
#optin_df = append_md5_field(optin_df)
optin_df = optin_df.dropna(subset = ['mail'])
optin_df = optin_df.drop_duplicates(['mail'])
show_df(optin_df)

res_df = pd.read_csv(res_path + "/Nettoyage optout/desinscrits i love dessert.csv", sep = ";")
show_df(res_df)
desabo_df = res_df[['mail']]
desabo_df['desabo'] = 'True'
show_df(desabo_df)
    
res_df = load_dataframe_from_export_file(res_path, "/Nettoyage optout/npai i love dessert.csv", header = ['mail'], status = "npai")
if res_df[0]:
    npai_df = res_df[1]
    show_df(npai_df)

plainte_path = "/media/freebox/Fichiers/Export Regies/Plaintes Mimi"
plainte_df = load_dataframe_blacklist(plainte_path, 'plainte_mimi')
show_df(plainte_df)

exclude_df_list = [desabo_df, npai_df, plainte_df]
for exclude_df in exclude_df_list:
    optin_df = pd.merge(optin_df, exclude_df, on = 'mail', how = 'left')

show_df(optin_df)
res_df = optin_df[(optin_df['desabo'] != 'True') & (optin_df['npai'] != 'True') & (optin_df['plainte'] != 'True')]
#res_df = optin_df[(optin_df['desabo'] != 'True') & (optin_df['npai'] != 'True')]
show_df(res_df)

floating_col = ['civilite', 'cp']
for col in floating_col:
    if col in col_list:
        res_df[col] = res_df[col].apply(lambda x: remove_floating_part(x))
res_df[col_list].to_csv(res_path + "/WedooIT_Export-Optin-Ilovedessert_2015-04-07_[mail,date,civilite,prenom,nom,cp,ville,birth]_737k.csv", \
                        index = False, header = False, )
"""

# Script to append id fields to mail-only file
"""
res_path = "/media/freebox/Fichiers/Export Regies/AdLead/Exploitation en dedie"
mail_file = "AdLead_Export-Optin-FeelGood_24Fev2015_[mail]_1972k.csv"
id_file = "AdLead_Export_23-juin-2014_[mail,prenom,nom,civilite,birth,cp,ville,ip,provenance,date]_[all-emarsys]_non-dedup.csv"

mail_df = load_dataframe_from_export_file(res_path, mail_file, header = ['mail'])[1]
show_df(mail_df)

names = ['mail','prenom','nom','civilite','birth','cp','ville','ip','provenance','date','x']
dtype_dict = {'mail':object,'prenom':object,'nom':object,'civilite':int,'birth':object,'cp':object, \
                             'ville':object,'ip':object,'provenance':object,'date':object,'x':object}
id_df = pd.read_csv(res_path + "/" + id_file, sep = ",", \
                    header = 0, \
                    dtype = object)
id_df['civilite'] = id_df['civilite'].apply(str)
id_df['civilite'] = id_df['civilite'].apply(lambda x : remove_floating_part(x))
id_df['cp'] = id_df['cp'].apply(str)
id_df['cp'] = id_df['cp'].apply(lambda x : remove_floating_part(x))
show_df(id_df)

res_df = pd.merge(mail_df, id_df, on = 'mail', how = 'left')
show_df(res_df)

res_df[names[:-1]].to_csv(res_path + "/AdLead_Export-Optin-FeelGood_24Fev2015_[mail,prenom,nom,civilite,birth,cp,ville,ip,provenance,date]_non-dedup.csv", \
                          index = False)

"""

"""
res_path = "/media/freebox/Fichiers/Export Regies/Cloud"
res_file = "Cloud_f200_[mail,date,civilite,prenom,nom,cp, ville, birth]_2015-04-03.csv"
res_col = ['mail', 'date', 'civilite', 'prenom', 'nom', 'cp', 'ville', 'birth']
res_df = pd.read_csv(res_path + "/" + res_file, names = res_col, header = None, sep = ";")
show_df(res_df)
res_df[['mail']].to_csv(res_path + "/Cloud_f200_[mail]_2015-04-03.csv", index = False, header = False)
"""