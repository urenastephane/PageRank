from __future__ import print_function
import time
import re
import sys
from operator import add, itemgetter
import numpy as np
from pyspark import SparkContext
import time

# fonction calculant la contribution d'une URL au rang des autres

def getScore(urls, rank):
    num_urls = len(urls)
    for url in urls:
        yield (url, rank / num_urls)

print("-------------- initialisation du SparkContext: methode classique --------------")
sc = SparkContext()

print("------------ lecture du fichier pageRank.txt : OK methode classique --------------------")
lignes = sc.textFile("pageRank3.txt")
print("------------ 1. affichage du contenu de lines: on a recupere les differentes lignes du fichier --------------------")
print(lignes.collect())

print("---------------- 2. avec split et un map creant un tuple (A,B) on va recuperer les couples durl: methode nouvelle --------------")
liens = lignes.map(lambda urls: urls.split(";")).map(lambda url:(url[0], url[1]))

print(liens.collect())
print("")
	
print("----------------3. Calcul de ranks en t0 avec initialization a 1/N: on applique a links une fonction qui recree des couples avec lurl en clef et un 1 en valeur --------------")
links=liens.groupByKey()
size_ranks=links.count()
rangs_t0 = links.map(lambda url_neighbors: (url_neighbors[0], 1.0/size_ranks))
print(rangs_t0.collect())
print("")
print("")
print(" --------- 4. calcul des nouveaux rangs via une methode parallelisee -------")
print(" --------- A. analyse de ce qui est contenu par la structure links.join(rang_t0) -------")
analyze = links.join(rangs_t0).collect()
for a in analyze:
	for b in a:
		if isinstance(b,tuple):
			for c in b:
				if isinstance(c,float):
					print(c)
				else:
					for k in c:
						print("result iterable contains :")
						print(k)
		else:
			print("letter pointing at :")
			print(b)

print(" --------- A. MAP: on applique getScore au tuple compose de la liste des urls vers lesquels pointe le lien et du score: on divise le score par le nombre de liens sortants -------")
scores = links.join(rangs_t0).flatMap(lambda x: getScore(x[1][0], x[1][1]))
print(scores.collect())
print(" --------- B. REDUCE -------")
print("----new_ranks ------")

new_ranks = scores.reduceByKey(add)

print(scores.collect())
print(new_ranks.collect())
s=0
for a in new_ranks.collect():
	s=s+a[1]
print(s)
print("---- termine ------")
