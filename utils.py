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

# funzioni utili
def media(lista):
   n = len(lista)
   somma=0
   for x in lista:
	   somma +=x
   return float(somma)/n
# funzione che calcola il 'baricentro' di una lista di punti (punti inteso come coordinate)
def centroide(coord_list):
	x_list=[]
	y_list=[]
	for coord in coord_list:
		x_list.append(coord[0])
		y_list.append(coord[1])
	x = media(x_list)
	y = media(y_list)
	return (x,y)
# distanza euclidea tra due punti del piano
def distanza(coord1, coord2):
	return sqrt((coord1[0]-coord2[0])**2 + (coord1[1]-coord2[1])**2)
# per ogni punto di una lista di punti, distanza tra il punto e il punto successivo nella lista
def dist_tot(lista):
	dist=0.0
	for i in range(len(lista)-1):
		dist += distanza(lista[i], lista[i+1])
	return dist
#numero di spostamenti 
def spost(lista):
	count=0
	for x in range(len(lista)-1):
		if lista[x]!=lista[x+1]:
			count +=1
	return count
# data la stringa come nella colonna 'created_at', restituisce il datetime 
def tempo(stringa):
	time_split = stringa.split()
	result = time_split[0]+" "+time_split[1]+" "+time_split[2]+" "+time_split[3]+" "+time_split[5]
	date_result= datetime.strptime(result, '%a %b %d %H:%M:%S %Y')
	return date_result
# in input un dataframe, output = rdd con chiave id_utente e valore lista dei 'baricentri' dei place visitati in ordine cronologico
def lista_spost(df):
    df = df.select(["user.id", "created_at", df.place.bounding_box.coordinates[0].alias("coordinates")]).dropna()
    df = df.rdd
    df = df.map(lambda x: (x.id, [(tempo(x.created_at), centroide(x.coordinates))]))
    df_place = df.reduceByKey(lambda x,y: x+y)
    df_place = df_place.map(lambda x: (x[0], sorted(x[1])))
    df_place = df_place.map(lambda x: (x[0], [coppia[1] for coppia in x[1]]))	
    return df_place

def init_df():
    df_list = [] 
    df_list.append(spark.read.json("georef-tweets-20151121.json"))
    df_list.append(spark.read.json("georef.json"))
    df_list.append(spark.read.json("georef-tweets-20151123.json"))
    df_list.append(spark.read.json("georef-tweets-20151124.json"))
    df_list.append(spark.read.json("georef-tweets-20151125.json"))
    df_list.append(spark.read.json("georef-tweets-20151126.json"))
    df_list.append(spark.read.json("georef-tweets-20151127.json"))
    df_list.append(spark.read.json("georef-tweets-20151128.json"))
    df_list.append(spark.read.json("georef-tweets-20151129.json"))

    return df_list