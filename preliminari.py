# apertuna connessione per eseguire il file da shell
from pyspark import SparkContext
sc=SparkContext(appName="", master="local[*]") 
sc.setLogLevel("ERROR")
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("").config("","").getOrCreate()

# librerie utili
import pyspark.sql.functions as F
from pyspark.sql import Row
from pyspark.sql.functions import col
from datetime import datetime
from math import sqrt


####################################################################
####################################################################
# ANALISI PRELIMINARI: CONTROLLARE CHE PLACE COINCIDE CON COORDINATES, lo facciamo solo per un dataset

cnt = sc.accumulator(0)
# funzione che aggiunge uno al contatore ogni volta che c'è corrispondenza tra place e coordinates
# da applicare ad ogni riga del dataset
def coincide(tweet):
    global cnt
    x_list = []
    y_list = []
    for coord in tweet.place:
        x_list.append(coord[0])
        y_list.append(coord[1])
    x_max = max(x_list)
    y_max = max(y_list)
    x_min = min(x_list)
    y_min = min(y_list)
    x = tweet.coordinates[0]
    y = tweet.coordinates[1]
    if x >= x_min and x <= x_max and y >= y_min and y <= y_max:
        cnt += 1
        
df_prova = spark.read.json("georef-tweets-20151121.json")
df_prova = df_prova.select([df_prova.place.bounding_box.coordinates[0].alias("place"), df_prova.coordinates.coordinates.alias("coordinates")]).dropna()
df_prova.foreach(coincide)
n_tot = df_prova.count()
#print('Il numero totale di coincidenze è: ', cnt, ' su ', n_tot)

# OUTPUT:
# Il numero totale di coincidenze è:  81572  su  81844