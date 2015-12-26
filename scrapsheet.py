'''
Created on 18 avr. 2014

@author: david
'''
#!/usr/bin/env python
# -*- coding: utf-8 -*-

from import_functions import * 
from pandas.io import sql


db_name = "test_base" #"postgres"
db_user = "postgres"
db_host = "192.168.0.52"
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

def md5_table(conn, limit = "", silent = False):
    query = "select %s, %s from %s;" % ('mail_id', 'md5', 'md5')
    if limit:
        if limit > 0:
            query = "select %s, %s from %s limit %s;" % ('mail_id', 'md5', 'md5', str(limit))   
    #query = "select %s, %s from %s limit 1000;" % ('mail_id', 'md5', 'md5')
    md5 = pd.read_sql(query, conn)
    show_df(md5)
    if not silent:
        message = "OK : md5 table extracted from DB: " + str(len(md5.index)) + " records found."
        print_to_log(log_file, 3, message)
    return md5

def clean_dataframe(file_path, file_name, \
                    filtre_in = "", filtre_out = "", \
                    silent = False):
    file_args = extract_arguments(file_name, silent = False)
    header = file_args[1]
    status_header = file_args[2]
    import_result = populate_dataframe(file_path, file_name)
    if import_result[0]:
        dataframe = import_result[1]
        add_header_result = add_header(dataframe, header)
        if add_header_result[0]:
            dataframe = add_header_result[1]
            header = add_header_result[2]
            dataframe = drop_null_or_missing_values(dataframe, 'mail')
            sort_result = sort_unique(dataframe, header, 'mail')
            if sort_result[0]:
                dataframe = sort_result[1]
            dataframe = add_status_columns(dataframe, status_header)
            #dataframe = cleanup_mail_syntax_columnwide(dataframe, filtre_in, filtre_out)
            sort_result_2 = sort_unique(dataframe, header, 'mail')
            if sort_result_2[0]:
                dataframe = sort_result_2[1]
                md5_result = append_md5_field_to_dataframe(dataframe, header)
                show_df(md5_result[0])
                return md5_result

def map_existing_rows(df, ref_df, join_key = 'md5', output = 'df', exist_key = 'exist', new_key = 'new'):
    header = list(df.columns)
    df[exist_key] = False
    df[new_key] = False
    nb_records_dict = {}
    nb_records_dict['initial'] = len(df.index)
    ref_header = list(ref_df.columns)
    common_fields = [field for field in header if field in ref_header]
    if len(common_fields) == 0:
        message = "Pb. with map_existing_rows(). There are no fields common to the databases. Will return all records as new records."
        print_to_log(log_file, 3, message)
        if output in ['row', 'rows', 'index']:
            return [True, df.index, ""]
        else:
            return [True, df, nb_records_dict]
    else:
        if not join_key in common_fields:
            message = "Info : map_existing_rows(). Specified join_key '%s' does not exist in dataframes. " % str(join_key) + \
                        "Will use '%s' instead as join_key (this field is common to both tables)." % str(common_fields[0])
            print_to_log(log_file, 3, message)
            join_key = common_fields[0]
        ref_df['flag'] = True
        try:
            new_df = pd.merge(df, ref_df, on=join_key, how='left')
            message = "OK : Mails associated with existing records in table 'base' (using matching with 'md5' table)."
            print_to_log(log_file, 2, message)
        except:
            message = "Pb. with pd.merge() : Unable to merge dataframes."
            print_to_log(log_file, 3, message)
            e = sys.exc_info()
            for item in e:
                message = str(item)
                print_to_log(log_file, 6, message)
            return [False]
        common_rows_with_ref_df = new_df[new_df['flag'] == True].index
        new_rows_unknown_to_ref_df = new_df[new_df['flag'] != True].index
        new_df = new_df.drop('flag',1)
        if output in ['row', 'rows', 'index']:
            return [True, common_rows_with_ref_df, new_rows_unknown_to_ref_df]
        else:
            new_df.loc[common_rows_with_ref_df, exist_key] = True
            new_df.loc[new_rows_unknown_to_ref_df, new_key] = True
            nb_records_dict['sorted'] = len(new_df.index)
            nb_records_dict[exist_key] = len(common_rows_with_ref_df)
            nb_records_dict[new_key] = len(new_rows_unknown_to_ref_df)
            return [True, new_df, nb_records_dict]

def lookup_md5(df, limit = ""):
    md5_df = md5_table(conn, limit = limit)
    md5_df.rename(columns={'mail_id' : 'pg_id'}, inplace=True)
    map_rows = map_existing_rows(df, md5_df, join_key = 'md5', output = "df", \
                                 exist_key = 'exist', new_key = 'insert')
    if map_rows[0]:
        df = map_rows[1]
        show_df(df)
        show_df(df[(df['exist'] == True)])
        show_df(df[(df['insert'] == True)])
        result = map_rows[2]
        for k,v in result.iteritems():
            print k,v
        message = "OK : 'exist' and 'insert' fields updated (using matching with 'md5' table)."
        print_to_log(log_file, 2, message)
        return [True, df]
    else:
        message = "Pb. with md5 matching. Will revert to looping through all records in 'base' table."
        print_to_log(log_file, 2, message)
        return [False]
    
def appetence_table(conn):
    app_list = pd.read_sql("SELECT * FROM appetence_list;", conn)
    app_list.rename(columns = {'id' : 'appetence_id'}, inplace=True)
    show_df(app_list)
    app_match = pd.read_sql("SELECT mail_id, appetence_id, score FROM appetence_match;", conn)
    show_df(app_match)
    app_join = pd.merge(app_match, app_list, on = 'appetence_id')
    show_df(app_join)
    message = "OK : appetence table extracted from DB: " + str(len(app_join.index)) + " records found."
    print_to_log(log_file, 3, message)
    return app_join

def export_regie_table(file_path, file_name):
    regie_df = pd.read_csv(file_path + "/" + file_name)
    #show_fields = ['id', 'mail', 'prenom', 'nom']
    show_fields = ['id', 'mail']
    regie_df = regie_df[show_fields].drop_duplicates()
    show_df(regie_df)
    return regie_df

def lookup_appetence(file_path, file_name):
    app_df = appetence_table(conn)
    regie_df = export_regie_table(file_path, file_name)
    regie_df.rename(columns={'id' : 'mail_id'}, inplace=True)
    
    regie_app_df = pd.merge(regie_df, app_df, on = 'mail_id')
    #show_fields = ['mail_id', 'mail', 'prenom', 'nom', 'interet', 'score']
    show_fields = ['mail_id', 'mail', 'interet', 'score']
    regie_app_df = regie_app_df[show_fields]
    show_df(regie_app_df)
    
    return regie_app_df

def test_text_file(file, n_rows = 5, keep_char = "@"):
    enc = guess_encoding(file)
    try:
        part = pd.read_csv(file, encoding = enc, header = None, nrows = n_rows)
    except:
        inspection = inspect_text_file(file, desired_encoding = 'utf-8')
        if inspection[0]:
            enc = inspection[2]
            part = pd.read_csv(file, encoding = enc, header = None, nrows = n_rows)
        else:
            return [False]
    n_col = len(part.columns)
    skip_rows = []
    for cpt_line in range(n_rows - 1):
        if keep_char not in str("".join(list(str(part.at[cpt_line, cpt_col]) for cpt_col in range(n_col)))):
            skip_rows.append(cpt_line)
    return [True, enc, skip_rows, n_col]

def load_text_file(file, header = ""):
    test = test_text_file(file)
    if test[0]:
        if header:
            header = list(header)
            if len(header) < test[3]:
                for cpt in range(test[3] - len(header)):
                    header.append("unknown_" + str(cpt + 1))
            if len(header) > test[3]:
                for cpt in range(len(header) - test[3]):
                    header.pop()
            try:
                df = pd.read_csv(file, names = header, encoding = test[1], skiprows = test[2])
            except:
                df = pd.read_csv(file, names = header, encoding = test[1], skiprows = test[2], \
                                 error_bad_lines = False)
        else:
            try:
                df = pd.read_csv(file, header = None, encoding = test[1], skiprows = test[2])
            except:
                df = pd.read_csv(file, header = None, encoding = test[1], skiprows = test[2], \
                                 error_bad_lines = False)
        return [True, df]
    else:
        return [False]

file_path = "/media/freebox/Fichiers/ImportDB/Done"
file_name = "119_Leads_Nov2012_[mail,nom,prenom,cp,ville,civilite,birth,provenance,date]_48k.csv"

file_name = "82_Ducray_nov2009_[mail,prenom,cp,ip]_65k.csv"
file_path = "/media/freebox/Fichiers/ImportDB/Done"

file_path = "/media/freebox/Fichiers/ImportDB"
file_name = "4_WELOVE_Apr-2010_[mail,nom,prenom]_210k.csv"


#file_name = "EXP-PCP_export-pcp-spmode-optin_2014-05-22_[civilite,nom,prenom,mail,cp,ville]_[ouvreur]_test1.csv"
#file_name = "EXP-PCP_export-pcp-welove-optin_2014-05-22_[civilite,nom,prenom,mail,cp,ville]_false.csv"
file_name_list = ["30_AS-DE-MARQUES-TEST_June2010_[mail]_724k.txt", "21_AS-DE-MARQUES_Dec2009_[mail]_300k.txt", \
                  "9_SP_Nov2009_[mail]_980k.txt"]
from os import walk, rename
mypath = "/media/freebox/Fichiers/ImportDB/PCP"

db_schema = ""
import_files = []
file_path = "/media/freebox/Fichiers/ImportDB/Vieux fichiers"
#file_path = "/media/freebox/Fichiers/Export Regies/PCP/regie_pcp_sm"
for (dirpath, dirnames, filenames) in walk(file_path):
    import_files.extend(filenames)
    break

for file_name in sorted(import_files):

    #if file_name == "6_IDM_Nov2009_[mail,civilite,prenom,nom,birth,sms,ad1,ad2,cp,ville]_556k.csv":
#for file_name in file_name_list:
    args = extract_arguments(file_name)
    import_file_to_DB(db_package, file_name, file_path, md5_mapping = True, md5_query_limit = "", mail_cleanup = True)


file = file_path + "/" + file_name


#print test_text_file(file)
#load = load_text_file(file, header = args[1])
#if load[0]:
#    show_df(load[1])
#df = pd.read_csv(file, skiprows = 1, names = header, parse_dates = [6, 8], encoding = encoding)
#show_df(df)

#res_lookup = lookup_appetence(file_path, file_name)
#file_name_w = "PREDICTYS_Export-Emarsys_20-mai-2014_[" + str(",".join(list(res_lookup.columns))) + \
#                "]_.csv"
#print file_name_w
#res_lookup.to_csv(file_path + "/" + file_name_w, index = None)



""" test with fake df
df1 = pd.DataFrame({'mail': ['david', 'ana', 'penny'], 'pcp': True})
print df1
df2 = pd.DataFrame({'mailox' : ['lau', 'david', 'penny'], 'pcpox' : False, 'plainte' : True})
print df2
df_concat = pd.concat([df1, df2])
print map_existing_rows(df1, df2, join_key = 'dede')
"""


""" A finir pour amelioration processus d'import
md5 = md5_table()
file_name = "82_Ducray_nov2009_[mail,prenom,cp,ip]_65k.csv"
file_path = "/media/freebox/Fichiers/ImportDB/Done"
df = clean_dataframe(file_path, file_name)[0]
new_df = map_existing_rows(df, md5)
show_df(new_df)
"""

def join_files_in_directory(file_path, header = ""):
    from os import walk
    import_files = []
    for (dirpath, dirnames, filenames) in walk(file_path):
        import_files.extend(filenames)
        break
    merged = pd.DataFrame()
    for file_name in sorted(import_files):
        import_result = populate_dataframe(file_path, file_name, sep = "", attempt_number = "", \
                                           skiprows = "", index_col = "", silent = False)
        if import_result[0]:
            partial = import_result[1]
            if header:
                partial = add_header(partial, header)[1]
            else:
                partial = add_header(partial, ['mail'])[1]
            show_df(partial)
            merged = pd.concat([merged, partial], axis = 0)
    merged = drop_null_or_missing_values(merged, 'mail')
    merged = merged.drop_duplicates(['mail']).sort(['mail'])
    show_df(merged)
    merged = merged['mail']
    return merged

file_path = "/media/freebox/Fichiers/Emarsys"
#df = join_files_in_directory(file_path, header = ['emarsys-id', 'mail'])
file_path = file_path + "/Merged/"
file_name = "EMA_Merged-Emarsys_Mai-2014_[mail]_[emarsys]_.csv"
#df.to_csv(file_path + file_name, index = False)
#import_file_to_DB(db_package, file_name, file_path)

csv_file = "/media/freebox/Fichiers/ImportDB/Pandas/Test/base_emarsys/EMA_[id,date]_all_base_emarsys_1977k.csv"
#write_csv_to_DB('base_emarsys', csv_file, ['mail_id', 'date'], db_schema = "")
"""
mypath = "/media/freebox/Fichiers/Fichiers PCP/Export Regies/PCP/Resultat"
file_name = "PCP_Export-via-Emarsys_dataframe.csv"
df = pd.read_csv(mypath + "/" + file_name)
df_select = df[(df['exclusion_mot_cle'] == False) & (df['plainte'] != True)]
df_select[['mail']].to_csv(mypath + "/" + "PCP_Export-via-Emarsys.csv")
"""

#check_mot_cle_sql(127)

file_path = "/media/freebox/Fichiers/TOPER/Done"
file_name = "127_TOPER_[mail]_[appetence].csv"
file_name = "127_TOPER_[mail]_[appetence_dict].csv"

milestones = {1 : 1000, 2 : 2000, 3 : 5000, 4 : 10000, 5 : 20000, 6 : 50000, \
                  7 : 100000, 8 : 200000, 9 : 500000, 10 : 1000000, 11 : 2000000, 12: 5000000}
cpt_milestones = 1

#write_csv_to_DB("mot_list", "/home/david/Bureau/table_mot_cle.csv", ['mot', 'secteur', 'exclusion'], db_schema = "")

#query = "SELECT * FROM mot_list;"
#mot_df = sql.read_sql(query, conn)
#mot_df.to_csv("/home/david/Bureau/table_mot_cle.csv", index = None)