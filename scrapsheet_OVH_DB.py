'''
Created on 23 dec. 2014

@author: david
'''
#!/usr/bin/env python
# -*- coding: utf-8 -*-

from import_functions_OVH_DB import *
from pandas.io import sql
from os import walk, rename
import collections
import os
import gc


db_name = "prod" #"postgres"
db_user = "postgres"
db_host = "localhost"
db_pass = "penny9690"
global db_package
db_package = [db_name, db_user, db_host, db_pass]
"""db_connect = DB_direct_connection(db_package)
if db_connect[0]:
    conn = db_connect[1]
    cur = db_connect[2]
    conn.close()
#db_schema = "export_regies"""""

extra_fields_list_dict = {'id' : ['nom', 'prenom', 'civilite', 'cp', 'birth', 'ville'], \
                          'lead' : ['ip', 'provenance', 'date'], \
                          'md5' : ['md5'], \
                          'sms' : ['port'], \
                          'appetence_match' : ['score']}

def show_df(df):
    print df.head(5)
    print str(len(df.index)) + " lines."

def truncate_table(table, connection):
    if table in ['id_unik']:
        truncate = "TRUNCATE TABLE %s RESTART IDENTITY;" % table
        cursor = connection.cursor()
        cursor.execute(truncate)
        connection.commit()

def refresh_id_unik(db_package):
    query = "SELECT " + \
            "i.mail_id," + \
            "array_to_string(array_agg(distinct i.prenom),',') AS prenom," + \
            "array_to_string(array_agg(distinct i.nom),',') AS nom," + \
            "array_to_string(array_agg(distinct i.civilite),',') AS civilite," + \
            "array_to_string(array_agg(distinct i.birth),',') AS birth," + \
            "array_to_string(array_agg(distinct i.cp),',') AS cp," + \
            "array_to_string(array_agg(distinct i.ville),',') AS ville " + \
            "FROM id as i GROUP BY i.mail_id;"
    table = "id_unik"
    initiate_threaded_connection_pool(db_package)
    with getconnection() as db_connect:
        truncate_table(table, db_connect)
        df = pd.read_sql(query, db_connect, coerce_float=False)
    show_df(df)
    field_list = list(df.columns)
    file_name = create_file_name("PYT", table, len(df.index), header = field_list, comment = "")
    csv_file = write_to_csv(df, "/home/david/csv_files", table, file_name, header = False)
    write_file_in_DB(db_package, csv_file, table, field_list)
    return df

def get_file_names(file_path):
    file_name_list = []
    for (dirpath, dirnames, filenames) in walk(file_path):
        file_name_list.extend(filenames)
        break
    return file_name_list

def rename_files(file_path):
    replace_dict = {"civilite" : "civi", "prenom" : "firstname", "nom" : "name", "birth" : "naissance", \
                    "cp" : "codepostal", "ville" : "city", "sms" : "port", "telport" : "port", "teldom" : "tel"}
    for file_name in get_file_names(file_path):
        old_name = str(file_name)
        for old, new in replace_dict.iteritems():
            file_name = file_name.replace(old, new)
        os.rename(file_path + "/" + old_name, file_path + "/" + file_name)

def sort_file_names(file_path):
    import_files = get_file_names(file_path)
    files_dict = {}
    for file_name in sorted(import_files):
        if "OK" != file_name[:2] and "PB" != file_name[:2]:
            file_num = float(extract_front_arguments(file_name)[0].replace('-', '.').replace('X', '.9'))
            if file_num not in files_dict:
                files_dict[file_num] = file_name
            else:
                while file_num in files_dict:
                    file_num += 0.1
                files_dict[file_num] = file_name
    return collections.OrderedDict(sorted(files_dict.items()))

def process_import(file_path):
    for file_num, file_name in sort_file_names(file_path).iteritems():
        print file_num, file_name
    for file_num, file_name in sort_file_names(file_path).iteritems():
        args = extract_arguments(file_name)
        try:
            import_file_to_DB(db_package, file_name, file_path, md5_mapping = True, md5_query_limit = "", \
                              mail_cleanup = True)
            gc.collect()
            os.rename(file_path + "/" + file_name, file_path + "/" + "OK_" + file_name)
        except:
            os.rename(file_path + "/" + file_name, file_path + "/" + "PB_" + file_name)

def simplify_files_for_score_import(file_path):
    for file_num, file_name in sort_file_names(file_path).iteritems():
        df = pd.read_csv(file_path + "/" + file_name, header = 0, dtype = object, \
                         names = ['mail', 'id'], sep = ";")
        show_df(df)
        df[['mail']].to_csv(file_path + "/NEW_" + file_name, header = False, index = False)

def process_import_specific_only(file_path, specific):
    specific_field_list = extra_fields_list_dict[specific]
    specific_file_dict = {}
    for file_num, file_name in sort_file_names(file_path).iteritems():
        for field in specific_field_list:
            if field in file_name:
                specific_file_dict[file_num] = file_name
                break
    sorted_specific_file_dict = collections.OrderedDict(sorted(specific_file_dict.items()))
    for file_num, file_name in sorted_specific_file_dict.iteritems():
        print file_num, file_name
    for file_num, file_name in sorted_specific_file_dict.iteritems():
        args = extract_arguments(file_name)
        try:
            import_file_to_DB(db_package, file_name, file_path, md5_mapping = True, md5_query_limit = "", \
                              mail_cleanup = True)
            gc.collect()
            os.rename(file_path + "/" + file_name, file_path + "/" + "OK_" + file_name)
        except:
            os.rename(file_path + "/" + file_name, file_path + "/" + "PB_" + file_name)

def process_import_status_only(file_path, status):
    status_file_dict = {}
    for file_num, file_name in sort_file_names(file_path).iteritems():
        if status in file_name:
            status_file_dict[file_num] = file_name
    sorted_status_file_dict = collections.OrderedDict(sorted(status_file_dict.items()))
    for file_num, file_name in sorted_status_file_dict.iteritems():
        print file_num, file_name
    for file_num, file_name in sorted_status_file_dict.iteritems():
        args = extract_arguments(file_name)
        try:
            import_file_to_DB(db_package, file_name, file_path, md5_mapping = True, md5_query_limit = "", \
                              mail_cleanup = True)
            gc.collect()
            os.rename(file_path + "/" + file_name, file_path + "/" + "OK_" + file_name)
        except:
            os.rename(file_path + "/" + file_name, file_path + "/" + "PB_" + file_name)

def revert_name_modifications(file_path):
    for file_name in get_file_names(file_path):
        if "OK" == file_name[:2] or "PB" == file_name[:2]:
            os.rename(file_path + "/" + file_name, file_path + "/" + file_name[3:])

def process_tag_regie(file_path):
    for file_name in get_file_names(file_path):
        args = extract_arguments(file_name)
        col_list = args[1]
        to_be_replaced = str(col_list).replace(", ", ",").replace("'","")
        df = pd.read_csv(file_path + "/" + file_name, sep = ";", header = 0, names = col_list)
        df[['mail']].to_csv(file_path + "/" + file_name.replace(to_be_replaced, "[mail]"), index = False, header = False)
        os.remove(file_path + "/" + file_name)
    process_import(file_path)

# Functions needed to output csv files in PCP format
"""
import random
import time
import pandas as pd
import dateutil.parser as dparser

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

def format_civilite_for_PCP(value):
    select_case = {'NaN' : 'NaN', '1.0' : 'M', '2.0' : 'MLLE', '3.0' : 'MME'}
    str_value = str(value)
    if str_value in select_case.keys():
        if select_case[str_value]:
            return select_case[str_value]
    return str_value

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
    # creates a time somewhere in btw. start & end time
    # day_light option makes sure the hour is btw. 6 and 23
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
    # generates a list of n random dates btw. start & end dates at a given str format
    # ex. list_of_random_dates("1/1/2008 1:30 PM", "1/1/2009 4:50 AM", '%m/%d/%Y %I:%M %p', 1000)
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

pcp_file = "ExportMailPCP_communCAP_horsPlainteNPAI_2015-08-03_[mail,civi,prenom,nom,cp,ville,birth].csv"
format_csv_for_PCP("/home/david/fichiers/export/regie_mail", "pcp", pcp_file, \
                   ['mail', 'civilite', 'prenom', 'nom', 'cp', 'ville', 'birth'])
"""
file_path = "/home/david/fichiers/import_DB/to_be_imported"
#file_path = "/home/david/fichiers/import_DB/to_be_renamed"
process_import(file_path)
#process_tag_regie(file_path) #attention!! this function deletes files!!
#refresh_id_unik(db_package)

#rename_files(file_path)
#revert_name_modifications(file_path)
#print sorted(get_file_names(file_path))
#for k,v in sort_file_names(file_path).iteritems():
#    print k,v

#simplify_files_for_score_import(file_path_simplify)
#process_import_specific_only(file_path, specific = "id")

#process_import_status_only(file_path, status = "[ouvreur]")
#process_import_specific_only(file_path, specific = "id")
#for cpt_million in range(34,36):
#    check_mot_cle_DB(cpt_million)
#write_files_in_DB("/home/david/csv_files/mot_match", "mot_match", ['mail_id', 'mot_list_id'])

#clean_id_table(db_pack = db_package, sort = 'mail_id')
#unify_id_table(db_package)

""" Script to sync MD5 table
initiate_threaded_connection_pool(db_package)
with getconnection() as db_connect:
    #sync_md5_table(db_connect, csv_path = "/home/david/csv_files")
    csv_file = "/home/david/csv_files/md5/MISSING-md5_[mail_id,md5]_md5_34205k.csv"
    csv_reader = codecs.open(csv_file, 'r', encoding='utf-8')
    db_connect.cursor().copy_from(file = csv_reader, table = 'md5',
                                       sep = ";", null = "", columns = ['mail_id', 'md5'])
    db_connect.commit()
"""

# Script to extract clean mails from fichier XXX
""" script to extract clean mails from fichier XXX
# Mails ouvreurs, actifs de PCP
query = "SELECT b.mail, actif.date, id.civilite, id.prenom, id.nom, id.cp, id.ville, id.birth FROM base b " + \
        "LEFT JOIN id ON b.id = id.mail_id " + \
        "INNER JOIN base_mimi_ouvreur ouv ON b.id = ouv.mail_id " + \
        "INNER JOIN regie_actif actif ON b.id = actif.mail_id " + \
        "WHERE b.id IN (SELECT actif.mail_id FROM regie_actif actif " + \
                        "INNER JOIN regie_list regie ON actif.regie_id = regie.id " + \
                        "WHERE regie.abreviation SIMILAR TO '%(pcpwl|pcpsm)%') " + \
        "AND NOT EXISTS (SELECT 1 FROM base_mimi_plainte pla WHERE pla.mail_id = b.id) " + \
        "AND NOT EXISTS (SELECT 1 FROM mot_match mot WHERE mot.mail_id = b.id)"
        #"AND base.id IN (SELECT base_mimi_ouvreur.mail_id FROM base_mimi_ouvreur)"

# Mails en provenance du fichier XXX
query2 = "SELECT b.mail, id.civilite, id.prenom, id.nom, id.cp, id.ville, id.birth FROM base b " + \
         "LEFT JOIN id ON b.id = id.mail_id " + \
         "WHERE b.id IN (SELECT fima.mail_id FROM fichier_match fima " + \
                        "INNER JOIN fichier_list fili ON fima.fichier_id = fili.id " + \
                        "WHERE fili.fichier_num = '127')" + \
         "AND NOT EXISTS (SELECT 1 FROM base_mimi_plainte pla WHERE pla.mail_id = b.id) " + \
         "AND NOT EXISTS (SELECT 1 FROM mot_match mot WHERE mot.mail_id = b.id)" + \
         "AND NOT EXISTS (SELECT 1 FROM base_mimi_npai npai WHERE npai.mail_id = b.id)" + \
         "AND NOT EXISTS (SELECT 1 FROM base_mimi_ouvreur ouvr WHERE ouvr.mail_id = b.id);"
         #"AND b.id NOT IN (SELECT base_mimi_ouvreur.mail_id FROM base_mimi_ouvreur);"

# mails optin, minus desabos
query3 = "SELECT b.mail, id.civilite, id.prenom, id.nom FROM base b " + \
         "LEFT JOIN id ON b.id = id.mail_id " + \
         "WHERE b.id IN (SELECT optin.mail_id FROM optin_match optin " + \
                        "INNER JOIN optin_list opt_li ON optin.optin_id = opt_li.id " + \
                        "WHERE opt_li.abreviation = 'sho') " + \
         "AND b.id NOT IN (SELECT desabo.mail_id FROM optin_desabo desabo " + \
                        "INNER JOIN optin_list opt_li ON desabo.optin_id = opt_li.id " + \
                        "WHERE opt_li.abreviation = 'sho');"
         #"AND NOT EXISTS (SELECT 1 FROM base_mimi_plainte pla WHERE pla.mail_id = b.id) " + \
         #"AND NOT EXISTS (SELECT 1 FROM mot_match mot WHERE mot.mail_id = b.id)" + \
         #"AND NOT EXISTS (SELECT 1 FROM base_mimi_npai npai WHERE npai.mail_id = b.id);"

# mails en regie chez XXX, optin YYY, minus desabos YYY
query4 = "SELECT b.mail, id.civilite, id.prenom, id.nom, id.cp, id.ville, id.birth FROM base b " + \
         "LEFT JOIN id ON b.id = id.mail_id " + \
         "WHERE b.id IN (SELECT regie.mail_id FROM regie_match regie " + \
                        "INNER JOIN regie_list reg_li ON regie.regie_id = reg_li.id " + \
                        "WHERE reg_li.abreviation = 'ads') " + \
         "AND b.id IN (SELECT optin.mail_id FROM optin_match optin " + \
                        "INNER JOIN optin_list opt_li ON optin.optin_id = opt_li.id " + \
                        "WHERE opt_li.abreviation = 'clu') " + \
         "AND b.id NOT IN (SELECT desabo.mail_id FROM optin_desabo desabo " + \
                        "INNER JOIN optin_list opt_li ON desabo.optin_id = opt_li.id " + \
                        "WHERE opt_li.abreviation = 'clu') " + \
         "AND NOT EXISTS (SELECT 1 FROM base_mimi_plainte pla WHERE pla.mail_id = b.id) " + \
         "AND NOT EXISTS (SELECT 1 FROM mot_match mot WHERE mot.mail_id = b.id) " + \
         "AND NOT EXISTS (SELECT 1 FROM base_mimi_npai npai WHERE npai.mail_id = b.id);"

# mails optin in XXX, minus mails optin in YYY
query5 = "SELECT b.mail FROM base b " + \
         "WHERE b.id IN (SELECT optin.mail_id FROM optin_match optin " + \
                        "INNER JOIN optin_list opt_li ON optin.optin_id = opt_li.id " + \
                        "WHERE opt_li.abreviation = 'wel') " + \
         "AND b.id NOT IN (SELECT optin.mail_id FROM optin_match optin " + \
                        "INNER JOIN optin_list opt_li ON optin.optin_id = opt_li.id " + \
                        "WHERE opt_li.abreviation = 'new');"

query_md5_f200 = "SELECT md5.md5 FROM md5 " + \
         "WHERE md5.mail_id IN (SELECT fima.mail_id FROM fichier_match fima " + \
                        "INNER JOIN fichier_list fili ON fima.fichier_id = fili.id " + \
                        "WHERE fili.fichier_num = '200')" + \
         "AND NOT EXISTS (SELECT 1 FROM base_mimi_plainte pla WHERE pla.mail_id = md5.mail_id) " + \
         "AND NOT EXISTS (SELECT 1 FROM mot_match mot WHERE mot.mail_id = md5.mail_id)" + \
         "AND NOT EXISTS (SELECT 1 FROM base_mimi_npai npai WHERE npai.mail_id = md5.mail_id)" + \
         "AND NOT EXISTS (SELECT 1 FROM base_mimi_ouvreur ouvr WHERE ouvr.mail_id = md5.mail_id) LIMIT 50000;"
         #"AND md5.mail_id NOT IN (SELECT base_mimi_ouvreur.mail_id FROM base_mimi_ouvreur);"

# mails qui ont une civilite, cp & birth et qui ne sont pas en regie chez Adscoria
query_adscoria_civi_cp_birth = \
         "SELECT b.mail, id.civilite, id.prenom, id.nom, id.cp, id.ville, id.birth, lead.ip, lead.date, lead.provenance FROM base b " + \
         "LEFT JOIN id ON b.id = id.mail_id " + \
         "LEFT JOIN lead ON b.id = lead.mail_id " + \
         "WHERE id.civilite IS NOT NULL AND id.cp IS NOT NULL AND id.birth IS NOT NULL " + \
         "AND EXISTS (SELECT 1 FROM regie_actif actif WHERE actif.mail_id = b.id) " + \
         "AND NOT EXISTS (SELECT 1 FROM regie_match regie WHERE regie.mail_id = b.id AND regie.regie_id = '4') " + \
         "AND NOT EXISTS (SELECT 1 FROM base_mimi_plainte pla WHERE pla.mail_id = b.id) " + \
         "AND NOT EXISTS (SELECT 1 FROM mot_match mot WHERE mot.mail_id = b.id) " + \
         "AND NOT EXISTS (SELECT 1 FROM base_mimi_npai npai WHERE npai.mail_id = b.id);"

#mails & md5 qui sont en regie chez Adscoria
query_adscoria_mail_md5 = \
        "SELECT b.mail, md5.md5 FROM base b " + \
        "LEFT JOIN md5 ON b.id = md5.mail_id " + \
        "WHERE EXISTS (SELECT 1 FROM regie_match regie WHERE regie.mail_id = b.id AND regie.regie_id = '4');"

#mails & md5 qui sont en regie chez Adscoria
query_showroom_mail = \
        "SELECT b.mail FROM base b " + \
        "WHERE EXISTS (SELECT 1 FROM optin_match optin WHERE optin.mail_id = b.id AND optin.optin_id = '2');"

# SMS & Mails en provenance du fichier XXX
query_sms = \
         "SELECT b.mail, sms.port, id.civilite, id.prenom, id.nom, id.cp, id.ville, id.birth FROM base b " + \
         "LEFT JOIN id ON b.id = id.mail_id " + \
         "RIGHT JOIN sms ON b.id = sms.mail_id " + \
         "WHERE b.id IN (SELECT fima.mail_id FROM fichier_match fima " + \
                        "INNER JOIN fichier_list fili ON fima.fichier_id = fili.id " + \
                        "WHERE fili.fichier_num = '305')" + \
         "AND NOT EXISTS (SELECT 1 FROM base_mimi_plainte pla WHERE pla.mail_id = b.id) " + \
         "AND NOT EXISTS (SELECT 1 FROM mot_match mot WHERE mot.mail_id = b.id)" + \
         "AND NOT EXISTS (SELECT 1 FROM base_mimi_npai npai WHERE npai.mail_id = b.id);"
         #"AND NOT EXISTS (SELECT 1 FROM base_mimi_ouvreur ouvr WHERE ouvr.mail_id = b.id);"
         #"AND b.id NOT IN (SELECT base_mimi_ouvreur.mail_id FROM base_mimi_ouvreur);"

# SMS & Mails en provenance du fichier XXX
query_pcp = \
         "SELECT DISTINCT ON (b.mail) b.mail, id.civilite, id.prenom, id.nom, id.cp, id.ville, id.birth FROM base b " + \
         "LEFT JOIN id ON b.id = id.mail_id " + \
         "WHERE b.id IN (SELECT fima.mail_id FROM fichier_match fima " + \
                        "INNER JOIN fichier_list fili ON fima.fichier_id = fili.id " + \
                        "WHERE fili.fichier_num = '305')" + \
         "AND NOT EXISTS (SELECT 1 FROM base_mimi_plainte pla WHERE pla.mail_id = b.id) " + \
         "AND NOT EXISTS (SELECT 1 FROM mot_match mot WHERE mot.mail_id = b.id)" + \
         "AND NOT EXISTS (SELECT 1 FROM base_mimi_npai npai WHERE npai.mail_id = b.id);"
         #"AND NOT EXISTS (SELECT 1 FROM base_mimi_ouvreur ouvr WHERE ouvr.mail_id = b.id);"
         #"AND b.id NOT IN (SELECT base_mimi_ouvreur.mail_id FROM base_mimi_ouvreur);"

query_all_sms = "SELECT b.mail, sms.port, id.civilite, id.prenom, id.nom, id.cp, id.ville, id.birth FROM base b " + \
         "LEFT JOIN id ON b.id = id.mail_id " + \
         "RIGHT JOIN sms ON b.id = sms.mail_id " + \
         "AND NOT EXISTS (SELECT 1 FROM base_mimi_plainte pla WHERE pla.mail_id = b.id) " + \
         "AND NOT EXISTS (SELECT 1 FROM mot_match mot WHERE mot.mail_id = b.id)" + \
         "AND NOT EXISTS (SELECT 1 FROM base_mimi_npai npai WHERE npai.mail_id = b.id);"

query_md5_radvert = \
    "SELECT DISTINCT md5.md5 AS md5 FROM md5 " + \
    "WHERE EXISTS (SELECT 1 FROM regie_match reg WHERE reg.mail_id = md5.mail_id)" \
    "OR EXISTS (SELECT 1 FROM partner_match par WHERE par.mail_id = md5.mail_id)"

#mails & sms qui sont en commun avec CAP (pour sms rajouter un right join sur la table des sms)
query_sms_cap = \
        "SELECT DISTINCT ON (md5.md5) md5.md5, b.mail, id.civilite, id.prenom, id.nom, id.cp, id.ville, id.birth, " + \
        "lead.provenance, lead.date, lead.ip as ip, mbaz.ip as ip2 FROM md5 " + \
        "LEFT JOIN id ON md5.mail_id = id.mail_id " + \
        "LEFT JOIN base b ON md5.mail_id = b.id " + \
        "LEFT JOIN lead ON md5.mail_id = lead.mail_id " + \
        "LEFT JOIN campagne_ouvreur mbaz ON md5.mail_id = mbaz.mail_id " + \
        "WHERE EXISTS (SELECT 1 FROM partner_match ptnr WHERE ptnr.mail_id = b.id AND ptnr.partner_id = '1') " + \
        "AND NOT EXISTS (SELECT 1 FROM regie_match reg WHERE reg.mail_id = b.id AND reg.regie_id IN ('8')) " + \
        "AND NOT EXISTS (SELECT 1 FROM base_mimi_plainte pla WHERE pla.mail_id = b.id) " + \
        "AND NOT EXISTS (SELECT 1 FROM mot_match mot WHERE mot.mail_id = b.id)" + \
        "AND NOT EXISTS (SELECT 1 FROM base_mimi_plainte pla WHERE pla.mail_id = b.id) " + \
        "AND NOT EXISTS (SELECT 1 FROM optin_desabo dabo WHERE dabo.mail_id = b.id AND dabo.comment IN ('spam', 'npai')) " + \
        ";"

#mails qui sont en commun avec CAP
query_mail_cap = \
        "SELECT DISTINCT b.mail, id.civilite, id.prenom, id.nom, id.cp, id.ville, id.birth, mbaz.ip FROM base b " + \
        "LEFT JOIN id_unik id ON b.id = id.mail_id " + \
        "LEFT JOIN md5 ON b.id = md5.mail_id " + \
        "LEFT JOIN mindbaz_ouvreurs mbaz ON b.id = mbaz.mail_id " + \
        "WHERE EXISTS (SELECT 1 FROM partner_match ptnr WHERE ptnr.mail_id = b.id AND ptnr.partner_id = '1') " + \
        "AND NOT EXISTS (SELECT 1 FROM base_mimi_plainte pla WHERE pla.mail_id = b.id) " + \
        "AND NOT EXISTS (SELECT 1 FROM mot_match mot WHERE mot.mail_id = b.id)" + \
        "AND NOT EXISTS (SELECT 1 FROM regie_match reg WHERE reg.mail_id = b.id AND reg.regie_id IN ('14')) " + \
        "AND NOT EXISTS (SELECT 1 FROM optin_desabo dabo WHERE dabo.mail_id = b.id AND dabo.comment IN ('spam', 'npai'));"

query_92 = \
    "SELECT DISTINCT b.mail AS mail FROM base b JOIN id ON id.mail_id = b.id WHERE id.ville = 'Levallois' or left(id.cp,2) = '92';"

initiate_threaded_connection_pool(db_package)
folder_path = "/home/david/fichiers/export"
folder = "regie_mail/r-advertising"
#folder = "comptage"
file_name_id = "ExportMD5_RAdvert_Actifs"
file_date = "2015-09-28"
col_string = "[mail,civi,prenom,nom,cp,ville,birth,ip]"
#col_string = "[mail]"
with getconnection() as conn:
    df = pd.read_sql(query_md5_radvert, conn, coerce_float=False)

#df['provenance'] = df['provenance'].str.strip()
show_df(df)

#df.rename(columns = {'civilite' : 'civilite_criteo'}, inplace = True)
#df = df.sort('mail')
#show_df(df)

#df = df[(df['ip'].str.contains(".") == True) | (df['ip2'].str.contains(".") == True)]
#df = df[(df['ip'] != "None") | (df['ip2'] != "None")]
#df = sort_unique(df, 'md5', 'md5')[1]
#show_df(df)

col_string = "[" + ",".join(df.columns) + "]"
#write_to_csv(df, folder_path, folder, file_name_id + "_" + file_date + "_" + col_string + ".csv", \
#            na_rep = "")

#df = sort_unique(df, 'mail', 'mail')[1]
#show_df(df)
write_to_csv(df, folder_path, folder, file_name_id + "-MailUnik_IDUnik_" + file_date + "_" + col_string + ".csv", \
            na_rep = "")
#df = df.ix[random.sample(df.index, 30000)]
#write_to_csv(df, folder_path, folder, file_name_id + "-MailUnik-30ksample_" + file_date + "_" + col_string + ".csv", \
#            na_rep = "")
#write_to_csv(df[['mail']], folder_path, folder, file_name_id + "-MailUnik_" + file_date + "_" + "[mail]" + ".csv", \
#            na_rep = "")
"""

#Script to analyse mails given to regie (pour ADL Partner)
""" #Script to analyse mails given to regie (pour ADL Partner)
regie_id = 9
sql = "SELECT COUNT(DISTINCT b.id) n_mail, COUNT(DISTINCT sms.port) n_sms, COUNT(DISTINCT id.cp) n_cp FROM base b " + \
      "RIGHT JOIN sms ON sms.mail_id = b.id RIGHT JOIN id ON id.mail_id = b.id " + \
      "WHERE EXISTS (SELECT DISTINCT mat.mail_id FROM regie_match mat WHERE mat.mail_id = b.id AND mat.regie_id = '%s');" % str(regie_id)

def analyse_regie_sms_cp(db_package, regie_id):
    sql = "SELECT b.mail mail, sms.port sms, id.cp FROM base b " + \
          "LEFT JOIN sms ON sms.mail_id = b.id LEFT JOIN id_unik id ON id.mail_id = b.id " + \
          "WHERE EXISTS (SELECT DISTINCT mat.mail_id FROM regie_match mat WHERE mat.mail_id = b.id AND mat.regie_id IN (%s));" % str(regie_id)

    initiate_threaded_connection_pool(db_package)
    with getconnection() as conn:
        df = pd.read_sql(sql, conn, coerce_float=False)
    show_df(df)
    res = {}
    res['mail'] = len(df['mail'].drop_duplicates(['mail']).index)
    sms = df['sms'].dropna()
    res['sms'] = len(sms.index)
    cp = df['cp'].dropna()
    res['cp'] = len(cp.index)
    return res

#res = {}
#for regie_id in [1,2,4,6,9]:
#    res['regie_' + str(regie_id)] = analyse_regie_sms_cp(db_package, regie_id)

#for k,v in res.iteritems():
#    print k, v

print analyse_regie_sms_cp(db_package, "'1','2','4','6','9'")
"""

# Script to compare outside base with DB
""" Script to compare outside base with DB
path = "/home/david/fichiers/audit/cap"
file = "FichierCAP_2015-10-08_[mail,civilite,prenom,nom,cp,ville,birth]_1200k_Tatatata.txt"
file = "FichierCAP_2015-11-19_[mail,civilite,nom,prenom,cp,ville,birth]_700k_donne-en-regie-a-FL-et-adscoria.txt"
col_list = list("[mail,civilite,prenom,nom,cp,ville,birth]"[1:-1].split(","))
df_audit = pd.read_csv(path + "/" + file, header = None, names = col_list, sep = ";")
show_df(df_audit)

query_mail_md5 = "SELECT b.mail, md5.md5 FROM base b LEFT JOIN md5 ON b.id = md5.mail_id;"
query_mail = "SELECT b.mail FROM base b;"
initiate_threaded_connection_pool(db_package)
with getconnection() as conn:
    df = pd.read_sql(query_mail, conn, coerce_float=False)
show_df(df)

res_df = pd.merge(df_audit, df, how='inner')
show_df(res_df)

folder_path = "/home/david/fichiers/"
folder = "audit/cap"
file_name_id = "Match-With-CAP"
file_date = "2015-10-11"
col_string = "[mail,civi,prenom,nom,cp,ville,birth]"
#col_string = "[mail]"
file_name = file_name_id + file_date + "_" + "col_string" + ".csv"
res_df.to_csv(folder_path + "/" + folder + "/" + file_name, sep = ";", na_rep = "", header = False, index = False)

#write_to_csv(res_df, folder_path, folder, file_name_id + file_date + "_" + "col_string" + ".csv", \
#            na_rep = "")
"""

# Script to manipulate f200 files
""" Script to manipulate f200 files
res_path = "/home/david/fichiers/export/fichier_200_sup_netvision"
ouvr_file = "f200_2015-04-03_[mail,civilite,prenom,nom,cp,ville,birth]_[ouvreur].csv"
no_ouvr_file = "f200_2015-04-03_[mail,civilite,prenom,nom,cp,ville,birth].csv"
col_list = ['mail','civilite','prenom','nom','cp','ville','birth']
ouvr_df = pd.read_csv(res_path + "/" + ouvr_file, header = None, names = col_list, sep = ";")
ouvr_df['ouvreur'] = True
show_df(ouvr_df)
no_ouvr_df = pd.read_csv(res_path + "/" + no_ouvr_file, header = None, names = col_list, sep = ";")
show_df(no_ouvr_df)
df = pd.concat([ouvr_df, no_ouvr_df])[['mail']]
show_df(df)
write_to_csv(df, "/home/david/fichiers/export", "fichier_200_sup_netvision", \
                 "f200-ALL_2015-04-03_[mail].csv", \
                 na_rep = "")
"""

# Script to create a file from parts of a bigger file
""" Script to create a file from parts of a bigger file
list_rows = no_ouvr_df.index
list_list = [[],[],[],[],[]]
list_df = []
from random import randint
for item in list_rows:
    list_list[randint(0,4)].append(item)
col_list.append('ouvreur')
for no in range(5):
    random_df = no_ouvr_df[no_ouvr_df.index.isin(list_list[no])]
    if no == 0:
        res_df = pd.concat([ouvr_df,random_df]).sort(['mail'])
        res_df['telephone'] = ""
        res_df['pays'] = "France"
        res_df['date_collecte'] = ""
        res_df = res_df[['mail', 'civilite', 'nom', 'prenom', 'telephone', 'birth', 'cp', 'ville', 'pays', 'date_collecte']]
    else:
        res_df = pd.concat([ouvr_df,random_df]).sort(['mail'])[col_list]
    show_df(res_df)
    write_to_csv(res_df, "/home/david/fichiers/export", "fichier_200_sup_netvision", \
                 str(no) + "_f200_2015-04-03_[mail,civilite,prenom,nom,cp,ville,birth,ouvreur].csv", \
                 na_rep = "")
    write_to_csv(res_df[['mail']], "/home/david/fichiers/export", "fichier_200_sup_netvision", \
                 str(no) + "_f200_2015-04-03_[mail].csv", \
                 na_rep = "")
"""

# Script for filling up redis DB
"""
gc.collect()
#df = unify_id_table(db_package)
df = dataframe_from_md5_and_id_unik(db_package, only_id=False)
insert_dataframe_into_redis(df, 200000, 50000)
"""

# Script to test unpack_id_attribute function
"""
initiate_threaded_connection_pool(db_package)
id_query = "SELECT * FROM id_unik WHERE mail_id = '210';"
with getconnection() as db_connect:
    id_df = pd.read_sql(id_query, db_connect, coerce_float=False)
show_df(id_df)
print id_df.at[id_df.index[0], 'prenom']
attribute_list = ['civilite', 'prenom', 'nom', 'cp', 'ville', 'birth']
id = {}
for attribute in attribute_list:
    id = unpack_id_attribute(id, attribute, id_df.at[id_df.index[0], attribute])
print id
print id['prenom_3']
"""

# Script to test query in redis
""" Script to test query in redis
import redis
r = redis.StrictRedis(host='localhost', port=6379, db=0)
md5 = 'a17468a9416bdad60d9d16ea84980c29'
res = r.hgetall(md5)
id = json.loads(res['id'])
for key, value in id.iteritems():
    print key, value
"""