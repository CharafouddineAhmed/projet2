#!/usr/bin/python
# coding=utf8

import sys, os, yaml, logging

from datetime import datetime
from elasticsearch import Elasticsearch
import logging

logging.basicConfig(format=' %(asctime)s %(levelname)s %(message)s ', filename='main.log', filemode='w',  level=logging.INFO)

try: 
    #ouverture du fichier config
    with open("config.yml", 'r') as fichier_yml:
        cfg = yaml.load(fichier_yml)

    #Connexion avec Elasticsearch
    es = Elasticsearch("%s:%s/"%(cfg['elastic']['host'],cfg['elastic']['port']), verify_certs=True)
    if not es.ping():
        raise ValueError("Conneion avec Elasticsearch (http://%s:%s)  refusée "%(cfg['elastic']['host'],cfg['elastic']['port'] ))
  
    print "Connexion à la base ES reuissie"
    logging.info(es.ping())

    #Affichage des indexes
    compteur = 1
    for indexe in es.indices.get_alias(cfg['index']['name']) :      
        if compteur > cfg['autre']['date_retention'] : 

            #supression d'index
            message = es.indices.delete(index=indexe, ignore=[400,404])
            print message
            logging.info(message)

        compteur = compteur + 1

except Exception, message:
    print "Erreur : " ,message
    sys.exit(1)


print "PROGRAMME TERMINE"
logging.info("Programme fini sans probleme")
