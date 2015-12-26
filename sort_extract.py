'''
Created on 17 nov. 2013

@author: administrateur
'''
#!/usr/bin/env python
# -*- coding: utf-8 -*-
import codecs
dossier_source = "/media/Freebox/Fichiers/Export/"
dossier_export = "/media/Freebox/Fichiers/Export/CSV Guillaume/"
fichier_path = dossier_source + "35000_unik_sort2.csv"
reader = codecs.open(fichier_path, 'r', encoding='latin-1')
num_ref = ""
meta = []
meta_nb = 0
total_nb = 0
file_nb = 1
file_name = dossier_export + "G_10k_#" + str(file_nb) + "_fich." + str(1) + ".csv"
export_file = open(file_name, 'a')
for line in reader:
    row = line.split(',')
    block =[]
    mail = str(row[0].strip())
    block.append(mail)
    num = str(row[1].strip())
    block.append(num)
    multi = str(row[2].strip())
    block.append(multi)
    if str(num) == num_ref:
        meta.append(block)
    else:
        num_ref = str(num)
        print num_ref, len(meta)
        if len(meta) > 85000:
            for mail in meta:
                line = ""
                for item in mail:
                    line = line + str(item) + ','
                export_file.write(str(line) + '\n')
            export_file.close()
            total_nb += len(meta)
            print "CLOSING " + str(file_nb) + " -- " + str(len(meta)) + " -- " + str(total_nb)
            file_nb += 1
            file_name = dossier_export + "G_10k_#" + str(file_nb) + "_fich." + str(num) + ".csv"
            export_file = open(file_name, 'a')
            insert_nb = 0
            meta = []
            meta.append(block)
            meta_nb = 1
for mail in meta:
    line = ""
    for item in mail:
        line = line + str(item) + ','
    export_file.write(str(line) + '\n')
export_file.close()
total_nb += len(meta)
print "CLOSING " + str(file_nb) + " -- " + str(len(meta)) + " -- " + str(total_nb)
        

        