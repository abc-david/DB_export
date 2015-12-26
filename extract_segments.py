'''
Created on 23 dec. 2014

@author: david
'''
#!/usr/bin/env python
# -*- coding: utf-8 -*-

from import_functions_OVH_DB import *
import random
import time
import pandas as pd
import dateutil.parser as dparser
import collections
import os
import gc

db_name = "prod" #"postgres"
db_user = "postgres"
db_host = "localhost"
db_pass = "penny9690"
global db_package
db_package = [db_name, db_user, db_host, db_pass]

extra_fields_list_dict = {'id' : ['nom', 'prenom', 'civilite', 'cp', 'birth', 'ville'], \
                          'lead' : ['ip', 'provenance', 'date'], \
                          'md5' : ['md5'], \
                          'sms' : ['port'], \
                          'appetence_match' : ['score']}

def show_df(df):
    print df.head(5)
    print str(len(df.index)) + " lines."

# Functions needed to output csv files in PCP format
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
    query = "SELECT DISTINCT ip FROM lead UNION SELECT DISTINCT ip FROM campagne_ouvreur;"
    df_ip = sql.read_sql(query, conn)
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

def format_df_for_PCP(df, show = False, col_only = ['mail','prenom','nom','cp','ville','civilite','birth']):
    df = df[col_only]
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
    df['pays'] = "france"
    df['adresse'] = ""
    pcp_col_order_list = ['mail', 'date', 'ip', 'civilite', 'nom', 'prenom', 'adresse', 'cp', 'ville', 'birth', 'pays']
    if show:
        show_df(df[pcp_col_order_list])
    return df[pcp_col_order_list]

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

#pcp_file = "ExportMailPCP_communCAP_horsPlainteNPAI_2015-08-03_[mail,civi,prenom,nom,cp,ville,birth].csv"
#format_csv_for_PCP("/home/david/fichiers/export/regie_mail", "pcp", pcp_file, \
#                   ['mail', 'civilite', 'prenom', 'nom', 'cp', 'ville', 'birth'])

def get_df_from_DB(query, show = True):
    initiate_threaded_connection_pool(db_package)
    with getconnection() as conn:
        df = pd.read_sql(query, conn, coerce_float=False)
    if show:
        show_df(df)
    return df

def write_file(df, folder_path, folder, file_name_id, file_date, pcp = False, unik = True, unik_on = "mail"):
    col_list = list(df)
    col_string = "[" + ",".join(col_list) + "]"
    if unik:
        df = sort_unique(df, unik_on, unik_on)[1]
        file_name_id = file_name_id + "-MailUnik"
    file_name = file_name_id + "_" + file_date + "_" + col_string + "_" + str(int(len(df.index)/1000)) + "k.csv"
    if pcp:
        df = format_df_for_PCP(df)
        path_to_file = folder_path + "/" + folder + "/PCPFormat_" + file_name
        df.to_csv(path_to_file, index = False, header = False, sep = ";", na_rep = "")
    else:
        write_to_csv(df, folder_path, folder, file_name, na_rep = "")

def write_file_for_import_in_regie_match(df, regie_abv, file_description, file_date,
                                         folder = "/home/david/fichiers/import_DB/to_be_imported"):
    df_mail = df[['mail']].drop_duplicates('mail')
    file_name = "999_ExportRegie_%s_[mail]_[regie:%s]_false_%s_%sk.csv" \
                % (str(file_date), str(regie_abv), str(file_description), str(int(len(df_mail.index)/1000)))
    df_mail.to_csv(folder + "/" + file_name, index = False, header = False)

def export_segments_common_with_partner(partner_id, regie_dict, folder_path, file_date):
    for folder, abreviation in regie_dict.iteritems():
        query_partner_regie = \
        "SELECT b.mail, id.civilite, id.prenom, id.nom, id.cp, id.ville, id.birth FROM base b " + \
        "LEFT JOIN id ON b.id = id.mail_id " + \
        "WHERE EXISTS (SELECT 1 FROM partner_match ptnr WHERE ptnr.mail_id = b.id AND ptnr.partner_id = '%s') " % str(partner_id) + \
        "AND NOT EXISTS (SELECT 1 FROM regie_match regie " + \
            "INNER JOIN regie_list reg_li ON regie.regie_id = reg_li.id " + \
            "WHERE regie.mail_id = b.id AND reg_li.abreviation LIKE '%s') " % str(abreviation) + \
        "AND NOT EXISTS (SELECT 1 FROM base_mimi_plainte pla WHERE pla.mail_id = b.id) " + \
        "AND NOT EXISTS (SELECT 1 FROM mot_match mot WHERE mot.mail_id = b.id) " + \
        "AND NOT EXISTS (SELECT 1 FROM optin_desabo dabo WHERE dabo.mail_id = b.id AND dabo.comment IN ('spam', 'npai'));"
        print "---------------- " + str(folder) + " -------------------"
        df = get_df_from_DB(query_partner_regie, show = True)
        file_description = "Commun-CAP3"
        file_name_id = "ExportMail-%s_%s_Black+Plainte+NPAI+Mot" % (str(folder), str(file_description))
        if folder == 'pcp':
            write_file(df, folder_path, folder, file_name_id, file_date, pcp = True)
        else:
            write_file(df, folder_path, folder, file_name_id, file_date, pcp = False)
        write_file_for_import_in_regie_match(df, abreviation, file_description, file_date)

def export_segments_common_with_file(file_dict, regie_dict, folder_path, file_date):
    for file_name, file_num in file_dict.iteritems():
        for folder, abreviation in regie_dict.iteritems():
            query_file_regie = \
            "SELECT b.mail, id.civilite, id.prenom, id.nom, id.cp, id.ville, id.birth FROM base b " + \
            "LEFT JOIN id ON b.id = id.mail_id " + \
            "WHERE EXISTS (SELECT 1 FROM fichier_match fima " + \
                            "INNER JOIN fichier_list fili ON fima.fichier_id = fili.id " + \
                            "WHERE b.id = fima.mail_id AND fili.fichier_num = '%s')" % str(file_num) + \
            "AND NOT EXISTS (SELECT 1 FROM regie_match regie " + \
                            "INNER JOIN regie_list reg_li ON regie.regie_id = reg_li.id " + \
                            "WHERE regie.mail_id = b.id AND reg_li.abreviation LIKE '%s') " % str(abreviation) + \
            "AND NOT EXISTS (SELECT 1 FROM base_mimi_plainte pla WHERE pla.mail_id = b.id) " + \
            "AND NOT EXISTS (SELECT 1 FROM mot_match mot WHERE mot.mail_id = b.id) " + \
            "AND NOT EXISTS (SELECT 1 FROM optin_desabo dabo WHERE dabo.mail_id = b.id AND dabo.comment IN ('spam', 'npai'));"
            print "---------------- " + str(folder) + " -------------------"
            df = get_df_from_DB(query_file_regie, show = True)
            file_description = "Commun-File-%s" % str(file_name)
            file_name_id = "ExportMail-%s_%s_Black+Plainte+NPAI+Mot" % (str(folder), str(file_description))
            if folder == 'pcp':
                write_file(df, folder_path, folder, file_name_id, file_date, pcp = True)
            else:
                write_file(df, folder_path, folder, file_name_id, file_date, pcp = False)
            write_file_for_import_in_regie_match(df, abreviation, file_description, file_date)

regie_dict_commun_cap = {'pcp':'pcpwl', 'mailomedia':'mai', 'r-advertising':'rad'}
regie_dict = {'mailomedia-elea':'mail-ele'}
regie_dict = {'adscoria':'ads', 'adlead':'adlmu'}
file_dict = {'IgConseil-1' : 305}
folder_path = "/home/david/fichiers/export/regie_mail"
file_date = "2015-11-19"
#export_segments_common_with_partner(3, regie_dict_commun_cap, folder_path, file_date)


def query_builder(select = 'classic', where = ['plainte', 'mot', 'spam', 'npai'], ouvreur = "",
                  id_table = "id", partner = ["in", [1,2,3]], fichier = ["in", [307]], regie = ["out", ["rad"]]):
    what = {
        'classic' :
    "SELECT DISTINCT b.mail, id.civilite, id.prenom, id.nom, id.cp, id.ville, id.birth FROM base b " + \
        "LEFT JOIN %s id ON b.id = id.mail_id " % (id_table),
        'ip' :
    "SELECT DISTINCT b.mail, id.civilite, id.prenom, id.nom, id.cp, id.ville, id.birth, mbaz.ip FROM base b " + \
        "LEFT JOIN %s id ON b.id = id.mail_id " % (id_table) + \
        "LEFT JOIN campagne_ouvreur mbaz ON b.id = mbaz.mail_id ",
        'provenance' :
    "SELECT DISTINCT b.mail, id.civilite, id.prenom, id.nom, id.cp, id.ville, id.birth, l.ip, l.provenace, l.date FROM base b " + \
        "LEFT JOIN %s id ON b.id = id.mail_id " % (id_table) + \
        "LEFT JOIN lead l ON b.id = l.mail_id ",
        'id_lau' :
    "SELECT DISTINCT ON (id.id) b.mail, id.civilite, id.prenom, id.nom, id.cp, id.ville, id.birth FROM base b " + \
         "LEFT JOIN id ON b.id = id.mail_id "
    }
    clause = {
        'plainte' : "NOT EXISTS (SELECT 1 FROM base_mimi_plainte pla WHERE pla.mail_id = b.id) ",
        'mot' : "NOT EXISTS (SELECT 1 FROM mot_match mot WHERE mot.mail_id = b.id) ",
        'spam' : "NOT EXISTS (SELECT 1 FROM optin_desabo dabo WHERE dabo.mail_id = b.id AND dabo.comment IN ('spam', 'npai')) ",
        'npai' : "NOT EXISTS (SELECT 1 FROM base_mimi_npai npai WHERE npai.mail_id = b.id) ",
        'ouvreur_mimi' : "EXISTS (SELECT 1 FROM base_mimi_ouvreur ouvr WHERE ouvr.mail_id = b.id) ",
        'ouvreur_mbaz' : "EXISTS (SELECT 1 FROM campagne_ouvreur ouvr WHERE ouvr.mail_id = b.id) ",
        'criteo' : "EXISTS (SELECT 1 FROM log_criteo criteo WHERE criteo.mail_id = b.id) "
        }
    if partner:
        try:
            in_out = "" if partner[0] == "in" else "NOT "
            in_list = "(" + ",".join(["'"+str(item)+"'" for item in partner[1]]) + ")"
            clause['partner'] = \
    "%sEXISTS (SELECT 1 FROM partner_match ptnr WHERE ptnr.mail_id = b.id AND ptnr.partner_id IN %s) " % (str(in_out), str(in_list))
        except:
            pass
    if fichier:
        try:
            in_out = "" if fichier[0] == "in" else "NOT "
            in_list = "(" + ",".join(["'"+str(item)+"'" for item in fichier[1]]) + ")"
            clause['fichier'] = \
    "%sEXISTS (SELECT 1 FROM fichier_match fichier " % str(in_out) + \
        "INNER JOIN fichier_list fich_li ON fichier.fichier_id = fich_li.id " + \
        "WHERE fichier.mail_id = b.id AND fich_li.fichier_num IN %s) " % str(in_list)
#    "b.id %sIN (SELECT fima.mail_id FROM fichier_match fima " % str(in_out)+ \
#        "INNER JOIN fichier_list fili ON fima.fichier_id = fili.id " + \
#        "WHERE fili.fichier_num IN %s) " % str(in_list)
        except:
            pass
    if regie:
        try:
            in_out = "" if regie[0] == "in" else "NOT "
            in_list = "(" + ",".join(["'"+str(item)+"'" for item in regie[1]]) + ")"
            clause['regie'] = \
    "%sEXISTS (SELECT 1 FROM regie_match regie " % str(in_out) + \
        "INNER JOIN regie_list reg_li ON regie.regie_id = reg_li.id " + \
        "WHERE regie.mail_id = b.id AND reg_li.abreviation IN %s) " % str(in_list)
        except:
            pass

    restrict = ""
    for key, value in clause.iteritems():
        if key in where:
            restrict = restrict + "AND " + value

    query = what[select] + "WHERE " + restrict
    query = query.replace("WHERE AND", "WHERE")
    if partner: query = query + "AND " + clause['partner']
    if fichier: query = query + "AND " + clause['fichier']
    if regie: query = query + "AND " + clause['regie']
    if select == 'id_lau': query = query + "ORDER BY id.id DESC, b.mail, id.civilite, id.prenom, id.nom, id.cp, id.ville, id.birth "
    query = query[:-1] + ";"
    return query

def extract_based_on_query(query, folder = "cap", path = "/mnt/storage/fichiers/export", comment = "", mail_only = False,
                           mail_unik = True, sort = False, sort_by = 'mail', show = True, sample = False, sample_size = 30000):
    initiate_threaded_connection_pool(db_package)
    with getconnection() as conn:
        df = pd.read_sql(query, conn, coerce_float=False)
    #df.rename(columns = {'civilite' : 'civilite_criteo'}, inplace = True)

    if show: show_df(df)
    if mail_unik: df = df.drop_duplicates('mail')
    if sort: df = df.sort(sort_by)
    if sample: df = df.ix[random.sample(df.index, sample_size)]
    if mail_only: df = df[['mail']]
    if show: show_df(df)

    col_list = list(df)
    col_string = "[" + ",".join(col_list) + "]"
    col_string = col_string.replace("civilite", "civi")
    file_date = datetime.date.today().isoformat()
    file_name_id = "ExportMail_" + folder.replace("regie_mail/", "")
    if mail_unik: file_name_id = file_name_id + "_Mail-unik"
    if sample: file_name_id = file_name_id + "_Sample" + str(int(sample_size / 1000)) + "k"
    file_name = file_name_id + "_" + file_date + "_" + col_string + "_" + comment + "_" + str(int(len(df.index)/1000)) + "k.csv"

    df.to_csv(path + "/" + folder + "/" + file_name, index = False, header = False)
    #write_to_csv(df, path, folder, file_name, na_rep = "")


q = query_builder(partner = ["in", [2]], fichier =False, regie = ['out', ['rad']],
                  where = ['plainte', 'mot', 'spam', 'npai'], id_table = "id")
#partner = ["out", [1,2,3]]
print q
extract_based_on_query(q, 'regie_mail/r-advertising', sample = True, sample_size = 350000, comment = 'in-cap-2_sample')

#print "(" + ",".join(["'"+str(item)+"'" for item in [305,306,307]])

def clean_ig_lau_file(path, name, col):
    lau = pd.read_csv(path+name, names=col, sep = ";")
    show_df(lau)
    initiate_threaded_connection_pool(db_package)
    for table in ['base_mimi_plainte', 'base_mimi_npai', 'mot_match']:
        query = "SELECT DISTINCT b.mail FROM base b JOIN %s ex ON b.id = ex.mail_id;" % table
        with getconnection() as conn:
            df = pd.read_sql(query, conn, coerce_float=False)
        df['exclude'] = True
        lau = pd.merge(lau, df, how='left', on='mail')
        lau = lau[lau['exclude'] != True]
        lau = lau[col]
        show_df(lau)

    query = "SELECT DISTINCT b.mail FROM base b JOIN optin_desabo dabo ON dabo.mail_id = b.id WHERE dabo.comment IN ('spam', 'npai');"
    with getconnection() as conn:
        df = pd.read_sql(query, conn, coerce_float=False)
    df['exclude'] = True
    lau = pd.merge(lau, df, how='left', on='mail')
    lau = lau[lau['exclude'] != True]
    lau = lau[col]
    show_df(lau)

    col_list = list(lau)
    col_string = "[" + ",".join(col_list) + "]"
    col_string = col_string.replace("civilite", "civi")
    file_date = datetime.date.today().isoformat()
    file_name_id = "IGLau"
    file_name = file_name_id + "_" + file_date + "_" + col_string + "_" + str(int(len(lau.index)/1000)) + "k.csv"

    lau.to_csv(path + "/" + file_name, index = False, header = False)
    #write_to_csv(df, path, "", file_name, na_rep = "")

path = "/home/david/fichiers/ig_lau/"
name = "307_IGConseil-3_2015-12-07_[mail,civilite,nom,prenom,cp,ville,birth]_450k_45ans.txt"
#name = "307_IGConseil-3_2015-12-07_[mail,civilite,nom,prenom,cp,ville,birth]_450k_femmes-50+.txt"
col = ['mail', 'civilite', 'nom', 'prenom', 'cp', 'ville', 'birth']
#clean_ig_lau_file(path, name, col)

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
path = "/home/david/fichiers/audit"
file = "ExportPCP_22-july-2015_CompteTiers_[md5]_1400k.txt"
df_md5 = pd.read_csv(path + "/" + file, header = None, names = ["md5"])
show_df(df_md5)

query_mail_md5 = "SELECT b.mail, md5.md5 FROM base b LEFT JOIN md5 ON b.id = md5.mail_id;"
initiate_threaded_connection_pool(db_package)
with getconnection() as conn:
    df = pd.read_sql(query_mail_md5, conn, coerce_float=False)
show_df(df)

res_df = pd.merge(df_md5, df, how='inner')
show_df(res_df)

folder_path = "/home/david/fichiers/"
folder = "audit"
file_name_id = "Match-With-CAP"
file_date = "2015-07-22"
#col_string = "[mail,sms,civi,prenom,nom,cp,ville,birth]"
col_string = "[mail]"
write_to_csv(res_df[['mail']], folder_path, folder, file_name_id + file_date + "_" + "[mail]" + ".csv", \
            na_rep = "")
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
query2 = "SELECT DISTINCT ON (id.id) b.mail, id.civilite, id.prenom, id.nom, id.cp, id.ville, id.birth FROM base b " + \
         "LEFT JOIN id ON b.id = id.mail_id " + \
         "WHERE EXISTS (SELECT 1 FROM partner_match ptnr WHERE ptnr.mail_id = b.id AND ptnr.partner_id IN ('1','2','3')) " + \
         "AND NOT EXISTS (SELECT 1 FROM base_mimi_plainte pla WHERE pla.mail_id = b.id) " + \
         "AND NOT EXISTS (SELECT 1 FROM mot_match mot WHERE mot.mail_id = b.id)" + \
         "AND NOT EXISTS (SELECT 1 FROM base_mimi_npai npai WHERE npai.mail_id = b.id) " + \
         "AND NOT EXISTS (SELECT 1 FROM optin_desabo dabo WHERE dabo.mail_id = b.id AND dabo.comment IN ('spam', 'npai')) " + \
         "ORDER BY id.id DESC, b.mail, id.civilite, id.prenom, id.nom, id.cp, id.ville, id.birth;"
         #"AND NOT EXISTS (SELECT 1 FROM base_mimi_ouvreur ouvr WHERE ouvr.mail_id = b.id);"
         #"AND b.id NOT IN (SELECT base_mimi_ouvreur.mail_id FROM base_mimi_ouvreur);"
         #"AND NOT EXISTS (SELECT 1 FROM regie_match regie " + \
         #   "INNER JOIN regie_list reg_li ON regie.regie_id = reg_li.id " + \
         #   "WHERE regie.mail_id = b.id AND reg_li.abreviation LIKE '%s') " % str('mai-ele') + \
#"WHERE b.id IN (SELECT fima.mail_id FROM fichier_match fima " + \
#                "INNER JOIN fichier_list fili ON fima.fichier_id = fili.id " + \
#                "WHERE fili.fichier_num = '307')" + \

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
                        "WHERE fili.fichier_num = '127')" + \
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

#mails & sms qui sont en commun avec CAP
query_sms_cap = \
        "SELECT b.mail, sms.port, id.civilite, id.prenom, id.nom, id.cp, id.ville, id.birth FROM base b " + \
        "LEFT JOIN id ON b.id = id.mail_id " + \
        "RIGHT JOIN sms ON b.id = sms.mail_id " + \
        "WHERE EXISTS (SELECT 1 FROM partner_match ptnr WHERE ptnr.mail_id = b.id AND ptnr.partner_id = '1') " + \
        "AND NOT EXISTS (SELECT 1 FROM base_mimi_plainte pla WHERE pla.mail_id = b.id) " + \
        "AND NOT EXISTS (SELECT 1 FROM optin_desabo dabo WHERE dabo.mail_id = b.id AND dabo.comment = 'spam');"

#mails qui sont en commun avec CAP
query_mail_cap = \
        "SELECT DISTINCT b.mail, id.civilite, id.prenom, id.nom, id.cp, id.ville, id.birth, mbaz.ip FROM base b " + \
        "LEFT JOIN id_unik id ON b.id = id.mail_id " + \
        "LEFT JOIN campagne_ouvreur mbaz ON b.id = mbaz.mail_id " + \
        "WHERE EXISTS (SELECT 1 FROM partner_match ptnr WHERE ptnr.mail_id = b.id AND ptnr.partner_id IN ('1','2','3')) " + \
        "AND NOT EXISTS (SELECT 1 FROM regie_match regie " + \
            "INNER JOIN regie_list reg_li ON regie.regie_id = reg_li.id " + \
            "WHERE regie.mail_id = b.id AND reg_li.abreviation LIKE '%s') " % str('rad') + \
        "AND NOT EXISTS (SELECT 1 FROM base_mimi_plainte pla WHERE pla.mail_id = b.id) " + \
        "AND NOT EXISTS (SELECT 1 FROM mot_match mot WHERE mot.mail_id = b.id) " + \
        "AND NOT EXISTS (SELECT 1 FROM optin_desabo dabo WHERE dabo.mail_id = b.id AND dabo.comment IN ('spam', 'npai'));"
#        "ORDER BY id.birth DESC LIMIT 300000;"
#        "WHERE b.id IN (SELECT fima.mail_id FROM fichier_match fima " + \
#                        "INNER JOIN fichier_list fili ON fima.fichier_id = fili.id " + \
#                        "WHERE fili.fichier_num = '306')" + \
#

query_dpt = \
    "select distinct b.mail as mail from base b " + \
    "join id on id.mail_id = b.id " + \
    "where left(id.cp,2) in ('75','77','78','91','92','93','94','95') " + \
    "AND NOT EXISTS (SELECT 1 FROM base_mimi_plainte pla WHERE pla.mail_id = b.id) " + \
    "AND NOT EXISTS (SELECT 1 FROM mot_match mot WHERE mot.mail_id = b.id) " + \
    "AND NOT EXISTS (SELECT 1 FROM optin_desabo dabo WHERE dabo.mail_id = b.id AND dabo.comment IN ('spam', 'npai'));"

query_black = \
    "SELECT mail FROM base b " + \
    "WHERE b.id IN (SELECT fima.mail_id FROM fichier_match fima " + \
                        "INNER JOIN fichier_list fili ON fima.fichier_id = fili.id " + \
                        "WHERE fili.fichier_num = '306')" + \
    "AND (EXISTS (SELECT 1 FROM base_mimi_plainte pla WHERE pla.mail_id = b.id) " + \
    "OR EXISTS (SELECT 1 FROM mot_match mot WHERE mot.mail_id = b.id) " + \
    "OR EXISTS (SELECT 1 FROM optin_desabo dabo WHERE dabo.mail_id = b.id AND dabo.comment IN ('spam', 'npai')));"
"""