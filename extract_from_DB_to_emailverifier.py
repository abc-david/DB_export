

from import_functions import DB_direct_connection, drop_null_or_missing_values, sort_unique
#import pandas as pd
from pandas.io import sql

def show_df(df):
    print df.head(5)
    print str(len(df.index)) + " lines."
    
def clean_up(df, uniqueness = ['mail']):
    df = drop_null_or_missing_values(df, ['mail'])
    res_sort = sort_unique(df, uniqueness, ['mail'])
    if res_sort[0]:
        df = res_sort[1]
    df = df.reset_index(drop=True)
    show_df(df)
    return df


    
file_path = "/media/freebox/Fichiers/Export Regies/YouPick/"
file_name = "extract_qualif.csv"
#extract.to_csv(file_path + file_name, index = False)

""" A mettre au propre : sert a decouper un fichier
res_file_path = "/media/freebox/Fichiers/Nettoyage"
res_file_name = "LEADS-82-110-121-124_120k_Part-"
step = 120000
last_step = nb_mail // step
for nb_step in range(1, last_step):
    partial = df_base[(nb_step - 1) * step : (nb_step * step)]
    print partial.head(5)
    print partial.tail(5)
    partial['mail'].to_csv(res_file_path + "/" + res_file_name + str(nb_step) + ".txt", index = False)

partial = df_base[(last_step - 1) * step:]
print partial.head(5)
print partial.tail(5)
partial['mail'].to_csv(res_file_path + "/" + res_file_name + str(last_step) + ".txt", index = False)
"""

""" Archives : used before query_builder()
def extract_if_provenance(list_fichier_mimi):
    # df_base = sql.read_sql("SELECT id, mail FROM %s.%s" % (db_schema, 'base'), conn, index_col='id')
    sub_query = "select fichier_id from fichier_list where fichier_num similar to '%s'" \
        % str("|".join([str(i) for i in list_fichier_mimi]))
    main_query = "select base.mail, fichier_match.fichier_id " + \
        "from base inner join fichier_match on base.id = fichier_match.id " + \
        "where fichier_match.fichier_id in (%s);" % sub_query
    df_extract = sql.read_sql(main_query, conn)
    return df_extract
    
#list_fichier_mimi = [82,110,121,124]
#base = extract_if_provenance(list_fichier_mimi)

def extract_if_qualif(qualif_table, list_qualif_fields, \
                      lead_table, list_lead_fields, \
                      list_fichier_mimi, limit = ""):
    select_part = "select base.mail, %s, %s" % (str(", ".join( \
                [qualif_table + "." + str(i) for i in list_qualif_fields])), \
                                               (str(", ".join( \
                [lead_table + "." + str(i) for i in list_lead_fields]))))
    frm_part = []
    frm_part.append(" from base inner join %s on base.id = %s.mail_id" % (qualif_table, qualif_table))
    frm_part.append(" inner join %s on base.id = %s.mail_id" % (lead_table, lead_table))
    frm_part.append(" inner join fichier_match on base.id = fichier_match.id")
    frm_part.append(" inner")
    frm_part = str("".join(frm_part))
    sub_query = "select fichier_id from fichier_list where fichier_num similar to '%s'" \
                % str("|".join([str(i) for i in list_fichier_mimi]))
    where_part = "where fichier_match.fichier_id in (%s)" % sub_query + \
                " and id.birth is not null" + \
                " and id.civilite is not null"
    limit_part = ""
    if limit:
        if limit > 0:
            limit_part = "limit %s" % str(limit)
    main_query = select_part + " " + frm_part + " " + where_part + " " + limit_part + ";"
    print main_query
    df_extract = sql.read_sql(main_query, conn)
    show_df(df_extract)
    return df_extract
    
qualif_table = "id"
list_qualif_fields = ['prenom', 'nom', 'civilite', 'birth', 'cp', 'ville']
qualif_lead = "lead"
list_lead_fields = ['ip', 'provenance', 'date']
list_fichier_mimi = [114,117,110]
list_fichier_mimi = [82,110,121,124]
"""