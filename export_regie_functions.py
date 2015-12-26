'''
Created on 27 apr. 2014

@author: david
'''
#!/usr/bin/env python
# -*- coding: utf-8 -*-

from os import walk
import collections
import random
import time
from datetime import datetime, timedelta
import pandas as pd
from pandas.io import sql
from SQL_query_builder import query_builder
from import_functions import DB_direct_connection, extract_arguments, drop_null_or_missing_values, \
                                sort_unique, add_header, check_mot_cle_columnwide, \
                                old_populate_dataframe, remove_floating_part, \
                                append_md5_field, create_file_name, write_to_csv, write_csv_to_DB
#from import_functions import * 

def show_df(df, n_line = 5):
    #pd.set_option('display.max_columns', 20)
    print df.head(n_line)
    print str(len(df.index)) + " lines."
    
def sample_df(df, n):
    return df.ix[random.sample(df.index, n)]

def clean_up(df, uniqueness = ['mail']):
    df = drop_null_or_missing_values(df, ['mail'])
    res_sort = sort_unique(df, uniqueness, ['mail'])
    if res_sort[0]:
        df = res_sort[1]
    df = df.reset_index(drop=True)
    show_df(df)
    return df

def fix_year_problem_df(df, col, out_format = '%Y-%m-%d', strip_time = True):
    if not col in list(df.columns):
        return df
    #show_df(df[col])
    for cpt_index in range(len(df.index)):
        cpt = df.index[cpt_index]
        data = df.at[cpt, col]
        #print cpt, col, data
        if str(data) == "None" or str(data).lower() == "nan":
            df.at[cpt, col] = ""
    df[col] = pd.to_datetime(df[col])
    col_mod = 'date_modif'
    df[col_mod] = ""
    for cpt_index in range(len(df.index)):
        cpt = df.index[cpt_index]
        data = df.at[cpt, col]
        if data:
            try:
                year = data.year
                if year <= 1:
                    df.at[cpt, col_mod] = "NaT"
                elif year <= 1900:
                    diff = 19 - (year // 100)
                    df.at[cpt, col_mod] = data + timedelta(days = (diff * 100 * 365))
                elif year > 2014:
                    diff = (year // 100) - 19
                    df.at[cpt, col_mod] = data - timedelta(days = (diff * 100 * 365))
                elif year > 3000:
                    df.at[cpt, col_mod] = "NaT"
                else:
                    df.at[cpt, col_mod] = data
            except:
                df.at[cpt, col_mod] = "NaT"
    if out_format or strip_time:
        col_mod_format = 'date_modif_format'
        df[col_mod_format] = ""
        col_mod_format_strip = 'date_modif_format_stripped'
        df[col_mod_format_strip] = ""
        if out_format:
            for cpt_index in range(len(df.index)):
                cpt = df.index[cpt_index]
                data = df.at[cpt, col_mod]
                try:
                    data_formatted = datetime.strftime(data, format)
                    df.at[cpt, col_mod_format] = data_formatted
                except:
                    df.at[cpt, col_mod_format] = str(data)
        else:
            df[col_mod_format] = df[col_mod]
        if strip_time:
            for cpt_index in range(len(df.index)):
                cpt = df.index[cpt_index]
                data = str(df.at[cpt, col_mod_format])
                pos = data.find(" ")
                if pos > 0:
                    try:
                        data_strip = data[:pos]
                        df.at[cpt, col_mod_format_strip] = data_strip
                    except:
                        df.at[cpt, col_mod_format_strip] = data
                else:
                    df.at[cpt, col_mod_format_strip] = data
            show_df(df[[col, col_mod, col_mod_format, col_mod_format_strip]], 30)
            df[col] = df[col_mod_format_strip]
            df = df.drop(col_mod_format_strip,1)
            df = df.drop(col_mod_format,1)
        else:
            show_df(df[[col, col_mod, col_mod_format]], 30)
            df[col] = df[col_mod_format]
            df = df.drop(col_mod_format,1)        
    else:
        show_df(df[[col, col_mod]], 30)
        df[col] = df[col_mod]
    df = df.drop(col_mod,1)
    return df

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

#print list_of_random_dates("1/1/2008 1:30 PM", "1/1/2009 4:50 AM", '%m/%d/%Y %I:%M %p', 1000)

def load_available_IP():
    db_name = "test_base"
    db_user = "postgres"
    db_host = "192.168.0.52"
    db_pass = "postgres"
    db_package = [db_name, db_user, db_host, db_pass]
    db_connect = DB_direct_connection(db_package)
    if db_connect[0]:
        conn = db_connect[1]
        #cur = db_connect[2]
    else:
        return
    df_ip = sql.read_sql("SELECT ip FROM %s" % ('lead'), conn)
    df_ip = drop_null_or_missing_values(df_ip, ['ip'])
    df_ip = df_ip.drop_duplicates(['ip'])
    show_df(df_ip) 
    conn.close()
    return df_ip

def from_list(input, position = 0):
    if type(input) is list:
        res = input[position]
        return from_list(res)
    else:
        return input

def explore_path_for_export_regie(path):
    import_files = []
    for (dirpath, dirnames, filenames) in walk(path):
        import_files.extend(filenames)
        break
    file_dict = {}
    for file_name in sorted(import_files):
        file_dict[file_name] = extract_arguments(file_name, silent = True)
    for file_name, args in file_dict.iteritems():
        file_status = []
        for item in args[2]:
            file_status.append(item[0])
        args.append(file_status)
    return file_dict

def load_dataframe(path, name, header, status, regie = ""):
    #res_populate = old_populate_dataframe(path, name, names = "", header = "", \
    #                                  skiprows = "", index_col = "", silent = True)
    res_populate = old_populate_dataframe(path, name, sep = "", attempt_number = "", \
                       header = False, skiprows = "", index_col = "", silent = False)
    if res_populate[0]:
    #if True:
        partial = res_populate[1]
        #partial = pd.read_csv(path + "/" + name)
        res_header = add_header(partial, header)
        if res_header[0]:
            partial = res_header[1]
            partial = drop_null_or_missing_values(partial, ['mail'])
            res_sort = sort_unique(partial, ['mail'], ['mail'])
            if res_sort[0]:
                partial = res_sort[1]
            if regie:
                partial[regie] = True
            else:
                if status:
                    if type(status) is list:
                        for item in status:
                            partial[item] = True
                    else:
                        partial[status] = True
            return [True, partial]
    return [False]
                    
def filter_boolean_conditions(dataframe, include, exclude_fields):
    if len(exclude_fields) == 1:
        whats_left = dataframe[(dataframe[include] == True) & (dataframe[exclude_fields[0]] != True)]
    elif len(exclude_fields) == 2:
        whats_left = dataframe[(dataframe[include] == True) & (dataframe[exclude_fields[0]] != True) & \
                            (dataframe[exclude_fields[1]] != True)]
    elif len(exclude_fields) == 3:
        whats_left = dataframe[(dataframe[include] == True) & (dataframe[exclude_fields[0]] != True) & \
                            (dataframe[exclude_fields[1]] != True) & (dataframe[exclude_fields[2]] != True)]
    else:
        pass
    return whats_left

def load_data_for_export_regie(path, regie, exclude_fields = ['desabo', 'plainte']):
    file_dict = explore_path_for_export_regie(path)
    include = pd.DataFrame(columns = ['mail'])
    exclude = pd.DataFrame(columns = ['mail'])
    for name, args in file_dict.iteritems():
        if regie in args[-1] or not args[-1]:
            res_load = load_dataframe(path, name, args[1], args[-1], regie)
            if res_load[0]:
                partial = res_load[1]
                include = pd.concat([partial, include]).drop_duplicates(['mail']).sort(['mail'])
                print name, args[1], args[-1]
                print "    ", len(include.index), include.columns
        else:
            res_load = load_dataframe(path, name, args[1], args[-1])
            if res_load[0]:
                partial = res_load[1]
                exclude = pd.concat([partial, exclude])
                print name, args[1], args[-1]
                print "    ", len(exclude.index), exclude.columns
    merged = pd.merge(include, exclude, on = 'mail', how = 'left')
    print "    " * 2, len(merged.index), merged.columns
    if exclude_fields:
        merged = filter_boolean_conditions(merged, regie, exclude_fields)
    return merged

def generate_export_file(regie, path, plainte_path, res_file_path, res_file_name):
    df_regie = load_data_for_export_regie(path, regie, exclude_fields = ['desabo', 'plainte'])
    show_df(df_regie)

    # filter out plaintes
    df_plainte = load_data_for_export_regie(plainte_path, 'plainte_mimi', exclude_fields = "")
    show_df(df_plainte)
    whats_left = pd.merge(df_regie, df_plainte, on = 'mail', how = 'left')
    show_df(whats_left)
    whats_left = whats_left[whats_left['plainte_mimi'] != True]
    show_df(whats_left)
    
    # filter out mot_cle
    whats_left = check_mot_cle_columnwide(whats_left)
    whats_left = whats_left[whats_left['exclusion_mot_cle'] == False]
    show_df(whats_left)
    
    # add random dates
    #whats_left['date'] = list_of_random_dates("10/10/2013 1:30 PM", "2/21/2014 4:50 AM", \
    #                                          '%m/%d/%Y %I:%M %p', len(whats_left.index))
    #whats_left['date'] = list_of_random_dates("04/08/2014", "04/30/2014", \
    #                                           '%m/%d/%Y', len(whats_left.index))
    #show_df(whats_left)
    
    # add random IP
    #ip_list = list(load_available_IP()['ip'])
    #ip_list = random.sample(ip_list, len(whats_left.index))
    #whats_left['ip'] = ip_list
    #show_df(whats_left)
    
    #whats_left = whats_left[['mail', 'date', 'ip']]
    whats_left = whats_left[['mail', 'date']]
    show_df(whats_left)
    
    whats_left.to_csv(res_file_path + "/" + res_file_name, index = False)
    return whats_left

db_name = "test_base"
db_user = "postgres"
db_host = "192.168.0.52"
db_pass = "postgres"
db_package = [db_name, db_user, db_host, db_pass]
db_connect = DB_direct_connection(db_package)
if db_connect[0]:
    conn = db_connect[1]
    cur = db_connect[2]



""" Configuration pour Export PCP
regie = "pcp"
path = "/media/freebox/Fichiers/Export Regies/PCP"
plainte_path = "/media/freebox/Fichiers/Export Regies/Plaintes Mimi"
res_file_path = "/media/freebox/Fichiers/Export Regies/PCP/Resultat"
res_file_name = "PCP_Export_7-mai-2014.csv"
generate_export_file(regie, path, plainte_path, res_file_path, res_file_name)
"""


db_name = "test_base"
db_user = "postgres"
db_host = "192.168.0.52"
db_pass = "postgres"
db_package = [db_name, db_user, db_host, db_pass]
db_connect = DB_direct_connection(db_package)
if db_connect[0]:
    conn = db_connect[1]
    cur = db_connect[2]
    
#Configuration pour Export Predictys (mails avec IP) / YouPick / FLInteractive
plainte_path = "/media/freebox/Fichiers/Export Regies/Plaintes Mimi"
res_file_path = "/media/freebox/Fichiers/Export Regies/R Advertising/"
res_file_name = "R-Advertising_Export_23-juin-2014_[mail,prenom,nom,civilite,birth,cp,ville,ip,provenance,date]_[all-id]_non-dedup.csv"
res_file_name_mail_dedup = "R-Advertising_Export_23-juin-2014_[mail,prenom,nom,civilite,birth,cp,ville,ip,provenance,date]_[all-id]_dedup.csv"
res_file_name_mail_only = "R-Advertising_Export_23-juin-2014_[mail]_[all-id]_dedup.csv"
res_file_name_appetence = "R-Advertising_Export_23-juin-2014_[mail,interet,score]_[all-id]_non-dedup.csv"

#list_fichier_mimi = ['127'] # 'OK' --> export // [127] --> fichier TOPER
#list_fichier_mimi = [82,110,121,124] #fichiers de leads
list_fichier_mimi = ['EXP-PCP'] # correspond a tous les optins welove et specialmode chez PCP en date de mai 2014
sub_where_dict = {'fichier_num' : list_fichier_mimi}
sub_query_dict = {'select' : {'fichier_list' : 'fichier_id'}, \
                  'where' : {'fichier_num' : list_fichier_mimi}}

select_dict = collections.OrderedDict()
select_dict['base'] = ['mail', 'id']
#select_dict['fichier_match'] = ['left', 'fichier_id']
select_dict['id'] = ['left', 'prenom', 'nom', 'civilite', 'birth', 'cp', 'ville']
select_dict['lead'] = ['left', 'ip', 'provenance', 'date']
#select_dict['base_emarsys'] = 'mail_id'

where_dict = collections.OrderedDict()
#where_dict['lead.ip'] = 'not null'
#where_dict['lead.provenance'] = 'not null'
#where_dict['lead.date'] = 'not null'
#where_dict['base_emailverifier.id'] = 'not null'
#where_dict['base_emarsys.mail_id'] = 'not null'
#where_dict['id.birth'] = 'not null'
where_dict['fichier_match.fichier_id'] = sub_query_dict

query_dict = {'select' : select_dict, \
              'where' : where_dict, \
              'limit' : 1000}

#query = query_builder(query_dict)
# Query for Predictys (date de collecte not null)
query_predictys = "SELECT base.mail, id.prenom, id.nom, id.civilite, id.birth, id.cp, id.ville, lead.ip, lead.provenance, lead.date FROM lead RIGHT OUTER JOIN id ON lead.mail_id = id.mail_id RIGHT OUTER JOIN base ON lead.mail_id = base.id WHERE lead.date is NOT NULL;"
# Query for AdLead (all emarsys)
#query_adlead = "SELECT base.mail, id.prenom, id.nom, id.civilite, id.birth, id.cp, id.ville, lead.ip, lead.provenance, lead.date FROM base LEFT OUTER JOIN id ON base.id = id.mail_id LEFT OUTER JOIN lead ON lead.mail_id = base.id RIGHT JOIN fichier_match on fichier_match.id = base.id WHERE fichier_match.fichier_id = '7';"
#query_adlead_appetence = "SELECT base.mail, appetence_list.interet, appetence_match.score from appetence_match LEFT JOIN base ON base.id = appetence_match.mail_id INNER JOIN appetence_list ON appetence_match.appetence_id = appetence_list.id LEFT JOIN fichier_match ON fichier_match.id = appetence_match.mail_id WHERE fichier_match.fichier_id = '7';"
# Query for R-Advertising (all mails with prenom OR nom OR civilite non null)
query_R = "SELECT base.mail, id.prenom, id.nom, id.civilite, id.birth, id.cp, id.ville, lead.ip, lead.provenance, lead.date FROM base " + \
"INNER JOIN id ON base.id = id.mail_id " + \
"INNER JOIN lead ON lead.mail_id = id.mail_id " + \
"WHERE id.prenom is NOT NULL OR id.nom is NOT NULL OR id.civilite is NOT NULL LIMIT 10000;"

query_Adscoria = "SELECT base.mail, id.prenom, id.nom, id.civilite, id.birth, id.cp, id.ville, lead.ip, lead.provenance, lead.date FROM base " + \
"INNER JOIN id ON base.id = id.mail_id " + \
"INNER JOIN lead ON lead.mail_id = id.mail_id " + \
"WHERE lead.ip is NOT NULL LIMIT 1000;"

query_all_mail = "SELECT base.mail, md5.md5, id.prenom, id.nom, id.civilite, id.birth, id.cp, id.ville, lead.ip, lead.provenance, lead.date FROM base " + \
"LEFT JOIN md5 ON base.id = md5.mail_id " + \
"LEFT JOIN id ON base.id = id.mail_id " + \
"LEFT JOIN lead ON lead.mail_id = id.mail_id " + \
"LIMIT 10000;"

query_all_mail_md5 = "SELECT base.mail, md5.md5 FROM base " + \
"INNER JOIN md5 ON base.id = md5.mail_id;"

query_all_mail_md5_civilite = "SELECT DISTINCT base.mail, md5.md5, id.civilite FROM base " + \
"INNER JOIN md5 ON base.id = md5.mail_id " + \
"LEFT JOIN id ON base.id = id.mail_id " + \
"LEFT JOIN lead ON base.id = lead.mail_id " + \
"WHERE id.civilite is NOT NULL and lead.date is NOT NULL;"


df = sql.read_sql(query_all_mail_md5_civilite, conn)
#df = clean_up(df, uniqueness = 'mail')
show_df(df)

df_ref = pd.read_csv("/media/freebox/Fichiers/Export Regies/Adscoria/Adscoria_Export_2-dec-2014_[mail]_[all-id]_dedup.csv", \
                     header = 0)
df_ref = append_md5_field(df_ref)
show_df(df_ref)

whats_left = pd.merge(df_ref, df, on = 'md5', how = 'left')
show_df(whats_left)
df_res = whats_left[['mail_x', 'civilite']]
show_df(df_res)

res_file_path = "/media/freebox/Fichiers/Export Regies/Adscoria/Test"
res_file_name = "Adscoria_19-dec-14_[mail, md5, civilite].csv"
#res_file_name = "All-mail_16-juil-14_[mail, md5]_.csv"
df_res.to_csv(res_file_path + "/" + res_file_name, sep = ";", index = False)


""" Script to manipulate dataframes 
df = clean_up(df, uniqueness = None) #uniqueness = None for all the columns
#df = sample_df(df, 350000)


df['provenance'] = df['provenance'].str.strip()
try:
    col = "civilite"
    df[col] = df[col].apply(str)
    df[col] = df[col].apply(lambda x: remove_floating_part(x))
except:
    pass
df = fix_year_problem_df(df, 'birth')
show_df(df)
#df['birth'] = df['birth'].apply(lambda x: x.year() + 1000)
#clean_birth(value, dayfirst = True)
#show_df(df)



df_plainte = load_data_for_export_regie(plainte_path, 'plainte_mimi', exclude_fields = "")
show_df(df_plainte)

whats_left = pd.merge(df, df_plainte, on = 'mail', how = 'left')
show_df(whats_left)
    
whats_left = whats_left[whats_left['plainte_mimi'] != True]
show_df(whats_left)
whats_left.to_csv(res_file_path + "/" + res_file_name, index = False)
# dedup sur les mails
whats_left = clean_up(whats_left, uniqueness = 'mail')
whats_left.to_csv(res_file_path + "/" + res_file_name_mail_dedup, index = False)
# que les mails
#whats_left = whats_left[['mail']]
#whats_left.to_csv(res_file_path + "/" + res_file_name_mail_only, index = False)
"""
    
""" Extra cleaning / presentation script (not always necessary)
whats_left = check_mot_cle_columnwide(whats_left)
res_file_name = "EDIWARE_Export-Emarsys_21-mai-2014_[all-with-motcle]_.csv"
whats_left.to_csv(res_file_path + "/" + res_file_name, index = False)

whats_left = whats_left[whats_left['exclusion_mot_cle'] == False]
whats_left = whats_left[['mail', 'prenom', 'nom', 'civilite', 'birth', 'cp', 'ville', 'ip', 'provenance']]
whats_left = whats_left[['mail', 'mail_id', 'civilite', 'prenom', 'nom', 'cp', 'ville', 'interet', 'birth', 'fichier_id']]
show_df(whats_left)
res_file_name = "EDIWARE_Export-Emarsys_21-mai-2014_[mail,prenom,nom,civilite,birth,cp,ville,provenance]_.csv"
whats_left.to_csv(res_file_path + "/" + res_file_name, index = False)

whats_left = whats_left['mail']
res_file_name = "EDIWARE_Export-Emarsys_21-mai-2014_[mail]_.csv"
whats_left.to_csv(res_file_path + "/" + res_file_name, index = False)
"""


""" Script to get md5 file for Raffles
fichier_wl = "('45', '47', '51', '48', '50', '52', '53')" # fichiers d'origine welove chez pcp
# fichier_toper_emarsys = "('40', '42')"
fichier_ouvreur = "('8', '10', '13', '14', '15', '18', '19', '23', '25', '27', '28', '36', '55', '56')"
fichier_avec_ip = "('3', '9', '21', '32', '37', '38')"
query_str = "SELECT md5.md5 FROM md5 INNER JOIN fichier_match ON md5.mail_id = fichier_match.id WHERE fichier_match.fichier_id IN ('7', '8', '10', '13', '14', '15', '18', '19', '23', '25', '27', '28', '36', '41', '42', '46', '55', '56');"
#query_str = "SELECT md5.md5 FROM md5 INNER JOIN fichier_match ON md5.mail_id = fichier_match.id WHERE fichier_match.fichier_id IN ('7', '8') LIMIT 1000;"
query_str = "SELECT md5.md5 FROM md5 INNER JOIN fichier_match ON md5.mail_id = fichier_match.id WHERE fichier_match.fichier_id IN" + \
            " %s;" % str(fichier_ouvreur)
#query_str = "SELECT md5.md5 FROM md5 RIGHT OUTER JOIN regie_pcp_sm ON md5.mail_id = regie_pcp_sm.id;"
print query_str
df = sql.read_sql(query_str, conn)
show_df(df)
df = df.drop_duplicates('md5')
print len(df.index)
#df = df.ix[random.sample(df.index, 300000)]
#print len(df.index)
df.to_csv('/media/freebox/Fichiers/Export Regies/Raffles/md5_export_ALL_ouv.csv', index = None)

query_str = "SELECT md5.md5 FROM md5 INNER JOIN fichier_match ON md5.mail_id = fichier_match.id WHERE fichier_match.fichier_id IN" + \
            " %s;" % str(fichier_wl)
#query_str = "SELECT md5.md5 FROM md5 RIGHT OUTER JOIN regie_pcp_sm ON md5.mail_id = regie_pcp_sm.id;"
print query_str
df = sql.read_sql(query_str, conn)
show_df(df)
df = df.drop_duplicates('md5')
print len(df.index)
#df = df.ix[random.sample(df.index, 300000)]
#print len(df.index)
df.to_csv('/media/freebox/Fichiers/Export Regies/Raffles/md5_export_ALL_wl.csv', index = None)
"""


""" Fixing of the dates as of today's date... 
res_file_path = "/media/freebox/Fichiers/Export Regies/YouPick/"
res_file_name = "YOUPICK_Export-Emarsys_20-mai-2014_[mail,prenom,nom,civilite,birth,cp,ville,provenance]_.csv"
df = pd.read_csv(res_file_path + "/" + res_file_name)
df['birth'] = df['birth'].apply(lambda x: "" if str(x) == "2014-05-20" else x)
df.to_csv(res_file_path + "/" + res_file_name, index = False)
"""

