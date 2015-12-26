'''
Created on 18 nov. 2013

@author: administrateur
'''
from DB_mapping import *
import random
"""
import psycopg2, codecs, sys
import random, math, datetime
from sqlalchemy import *
from sqlalchemy.orm import *
from sqlalchemy.ext.declarative import declarative_base
dburl = 'postgresql+psycopg2://postgres:postgres@192.168.0.52/postgres'
"""
dossier_pgSQL = "/media/freebox/pgSQL/"

#mail_list = session.query(Mail).join(Fich_x_mail, Mail.id).\
#    join(Fichier, Fichier.id).filter(Fichier.num == 4).limit(100).all()

def check_fichier(num):
    #fichier_liste_exclude = [5, 6, 7, 10, 14, 23, 31, 32, 37, 40, 41, 42, 43, 45, 46, 47, 48, 53, 58, 59, 60, 61, 64, 65, 66, 67, 68, 69, 70, 71, 74, 75, 76, 78, 83, 84, 85, 87, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99, 100, 113]
    #fichier_liste_include = [2, 26, 34, 36, 51, 54, 55]
    #fichier_liste_include = [5, 6, 7, 10, 14, 23, 31]
    fichier_liste_include = [8, 12, 13, 16, 17, 18]
    if str(num).find("-") > -1:
        num = str(num)[:str(num).find("-")]
    for item in fichier_liste_include:
        if str(item) == str(num):
            return True
    return False 

query_limit = 1000
sample_size = 2000000
fichier_id_max = 96
#fichier_id_max = 1
for fichier_id in range (1, fichier_id_max + 1):

    fichier_num = session.query(Fichier.num).filter(Fichier.id == fichier_id).first()[0]
    print fichier_num, check_fichier(fichier_num)
    
    if check_fichier(fichier_num):
    
        mail_list = session.query(Mail).select_from(join(Mail, Fich_x_mail)).\
        filter(Fich_x_mail.fichier_id == fichier_id).\
        filter(~ Mail.domain.in_(["hotmail.com", "hotmail.fr", "live.com", "gmail.com", "wanadoo.fr", "orange.fr", "orange.com"])).\
        filter(~ Mail.mxm.any()).\
        filter(~ Mail.mnpai.any()).\
        filter(~ Mail.mplainte.any()).\
        filter(~ Mail.not_b2c.any()).all()
        #filter(~ Mail.not_b2c.any()).limit(query_limit).all()
        
        print "mail_list --> ", len(mail_list)
        
        #mail_list_2 = mail_list.join(Mot_x_mail.mail_id)
        
        if sample_size < len(mail_list):
            sample = random.sample(mail_list, sample_size)
            print "random done"
        else:
            sample = mail_list
            print "no random -- extract from db smaller than random size"
            
        select_mail = []
        print "preparing to treat mails in sample --> " + str(len(sample))
        for mail in sample:
            #if mail.mouvreur and (not (mail.not_b2c or mail.mplainte or mail.mnpai or mail.mxm)):
                #select_mail.append(mail.mail)
            info_mail = []
            info_mail.append(str(mail.mail))
            #info_mail.append(str(fichier_num))
            #info_mail.append(str(len(mail.fxm)))
            info_prov = ""
            """for item in mail.fxm:
                info_prov = info_prov + str(item.fichier.num) + "--"
            info_prov = info_prov[:len(info_prov) - 2]
            #print "info_prov --> ", info_prov
            info_mail.append(info_prov)"""
            #print "info_mail --> ", info_mail
            select_mail.append(info_mail) 
        
        print "mail treatment done -- preparing to save on file " + str(fichier_num)
        export_file_name = "/media/freebox/Fichiers/Export/Extract ALL pour Guillaume f_" + str(fichier_num) + ".csv"
        print export_file_name
        export_file = open(export_file_name, 'a+')
        #export_file.save()
        for mail in select_mail:
            line = ""
            for item in mail:
                line = line + str(item) + ','
            export_file.write(str(line) + '\n')
        
        export_file.close()
        print "saved to file"     