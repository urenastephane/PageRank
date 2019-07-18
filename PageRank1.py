from __future__ import print_function

#import re
import sys
from operator import add, itemgetter
import numpy as np
from pyspark import SparkContext
import time
from math import log10
#parse une ligne (contenant 2 URLS reliees) et les separe tout en les stockant dans un tableau pour effectuer le map
print("----- ETAPE 1: lecture fichier, des lignes regroupees en RDD, on cree ensuite des tuples avec A pointe vers B par ex ----")
sc = SparkContext()
print("----------------------------------------------------------------------------------------------------------")
print("-------------- initialisation du SparkContext(): et lecture du fichier: recuperation des lignes --------------")
print("----------------------------------------------------------------------------------------------------------")

lines = sc.textFile("pageRank3.txt")
print(lines.collect())
print("")

print("----------------------------------------------------------------------------------------------------------")
print("---------------- on a recupere les couples durl : links2.collect = URL en paires dans links2 --------------")
print("----------------------------------------------------------------------------------------------------------")
links2 = lines.map(lambda line: line.split(';')).map(lambda elt: tuple(elt))
print(links2.collect())
print(type(links2.collect()))
for item in links2.collect():
    print(type(item))
print("")
print("")
print("")
print("----------------------------------------------------------------------------------------------------------")
print("----------------On regroupe par cle: groupByKey OK , ie qu'on prend les premieres cles des couples recuperes dans le tableau precedent: necessaire pour creer les structures suivantes--------------")
print("---------------------------- links = links2.groupByKey() ------------------------------------------------------------")
links=links2.groupByKey()
print(links.collect())
print("")
print("----------------- DEBUT DU MAPPER --------------------")
print("----------------------------------------------------------------------------------------------------------")
print("----------------Calcul de ranks: on applique a links une fonction qui recree des couples avec lurl en clef et un 1/N en valeur initiale --------------")
print("----------------------------------------------------------------------------------------------------------")
count_links=links.count()
ranks1 = links.map(lambda key: [key[0], 1.0/count_links])
print("--------------- RANKS1: initialisation a 1/N du RANKING ---------------")
print(ranks1.collect())

print("")
print("")
print("------- CREATION DE LA LISTE contenant OUTGOING LINKS ie le nb de liens sortants par page ---------------")
print("")
print("")
def outGoingLinks(liens):
	couples=[]
	keys=[x[0] for x in liens]
	for elt in keys:
		couples.append([elt,keys.count(elt)])
	pairs=[]
	for val in couples:
		if val not in pairs:
			pairs.append(val)
	return pairs

print("impression des LIENS SORTANTS --------------->")
liens_sortants=outGoingLinks(links2.collect())
print(liens_sortants)
print("")
print("")
print("")
print("")
pointed_pages=[]
for elt in links2.collect():
	pointed_pages.append(elt[::-1])
print("---------- impression de la liste des couples POINTED PAGES ie B est pointee par A ie (B,A) dans ce cas --------")
print(pointed_pages)
print("")
print("")
print("")
print("")

print("----------- creation de la structure qui contient chaque page avec en tableau les liens qui pointent vers elle et leurs pageRank -----")
print("--------- remarque: retrospectivement, le fait de contenir les page ranks est inutile pour ces structures : inutilse--------")
liste2=[]
for elt in pointed_pages:
	liste2.append([elt[0],[]])
print(liste2)
print(type(liste2[0][1]))
print("")
print("")
print (" *************************** TESTING UNIQUE LIST OK ie liste precedente sans doublons pour arriver a lobjectif*********************")
print("")
print("")

liste_interm=[]
for e in liste2:
	if e not in liste_interm:
		liste_interm.append(e)
liste_unique=liste_interm
print(liste_unique)
print(type(liste_unique[0][1]))

print("")
print("")
print("----- pour obtenir la lite voulue, on continu et ici: test de la liste contenant les pages pointees couplees a la liste des pages qui pointent vers elles -------------")
print("")
print("")

for e1 in liste_unique:
	for e2 in pointed_pages:
		if e1[0] in e2[0]:
			e1[1].append([e2[1],0.0])

print("*********************************************************************************************************")
print("------------------------------------------ FIN ETAPE 1 : RAPPEL des parametres initiaux  -----------------------------------------------------")
print("")
print("---- liste des pages distinctes couplee a la liste des pages qui pointent vers elles ----")
pages_pointed_by=liste_unique
print(pages_pointed_by)
print(type(pages_pointed_by[0]))
print("---- RANKS at time = 0 ----")
ranks_t0=ranks1.collect()
print(type(ranks_t0))
print(ranks_t0)
len_ranks=len(ranks_t0)
print(len_ranks)
print("---- NB LIENS sortants de chaque page---")
print(liens_sortants)
print("")
print("*********************************************************************************************************")
time.sleep(5)

print("------------- CREATION dune structure avec des elts de type [A,[[B,0.16],[C,034]]] ------------------")
print(" ----------   signifie que A est pointe par B et C qui ont des rangs respectifs de 0.16 et 0.34 -------------")
print(pages_pointed_by)

#time.sleep(2)

print("")
print("")
print("")
print("")

ranks_list_temp=ranks_t0
for i in range(len(ranks_t0)):
	ranks_list_temp[i][1]=0.0
print(ranks_list_temp)
time.sleep(2)
print("")
print("")
print("")
print("")

print("------ creation des listes de verification ---------")
check1=[]
for a in ranks_list_temp:
	check1.append(a[1])
check2=[0.0]*len(ranks_list_temp)
check3=[0.0]*len(ranks_list_temp)
print(check1)
print(type(check1))
print(type(check1[0]))
print(check2)
print(type(check2))
print(type(check2[0]))
ranks_list_temp_0=ranks_list_temp
print(ranks_list_temp_0)
z=0
timer=[0.0]*len(ranks_list_temp)

ranks_init=ranks1.collect()
ranks_listing=ranks1.collect()
#while not any(i>0.5 for i in timer):
for k in range(0,2):
	print("ranks_init")
	print(ranks_init)
	print("ranks_list_temp")
	print(ranks_list_temp)
	print("------- value of K -----------")
	print(k)
	time.sleep(3)
	i=-1	
	for a in pages_pointed_by: #pour chaque elt de type [u'B', [[u'C', 0.1111],[u'D', 0.1111]], ....]
		print("value of a :")
		print(a)
		i=i+1
		for b in a: # dans la structure du dessus, on itere entre la lettre solo,ici le B, et la liste des liens avec leur rank
			if isinstance(b,unicode):#ici on selectionne la lettre de type unicode -le B- pour en faire lurl dinteret dont on calcule le rank
				url=b
				print("url considere en ce moment:")
				print(url)
			if isinstance(b,list):#ici on recupere la liste durls et de rangs associes a lurl dinteret, ie ((C,0.111),(D,0.111),...)
				print("value of b - liste des liens pointants vers lurl :")
				print(b)
				r=-1
				for j in range(0,len(b)): #(2) on parcourt la liste des couples (url,pagerank) pointant vers la page
					print(len(b))
					r=r+1
					print(" valeur de r : ")
					print(r)
					for l in liens_sortants:#on parcours la liste contenant les paires (sous forme de liste) les url et le nb de lien en sortant: ex ((A,1), (B,4), ...) ici l=(A,1) et l(0)=A	
						print(b[j][0])
						print(l[0])
						print(l[0] == b[j][0])
						if l[0] == b[j][0]:
							print(ranks_init)
							for c in ranks_init:
								print(c)
								if c[0] == b[j][0]:
									for d in ranks_list_temp:
										if url == d[0]:
											print("passaje j = ")
											print(j)
											print("-------")
											print("d[1]")
											print(d[1])
											print(type(d[1]))
											print("c[1]")
											print(c[1])
											print(type(c[1]))
											print("b[j][1]")
											print(b[j][1])
											print(type(b[j][1]))
											print("l[1]")
											print(l[1])
											print(type(c[1]))
											print("d[1]=c[1]/float(l[1])+d[1]")
											d[1]=c[1]/float(l[1])+d[1]
											print("c[1]")
											print(c[1])
											print("d[1]")
											print(d[1])
											print("url")
											print(url)
											print("ranks_list_temp")
											print(ranks_list_temp)
											time.sleep(2)
	print("ranks-list-temp")
	print(ranks_list_temp)
	print("--------- inversions for next iteration --------")
	print(ranks_list_temp)
	print(ranks_init)
	for i in range(0,len(ranks_list_temp)):
		for j in range(0,len(ranks_init)):
			if ranks_init[j][0]==ranks_list_temp[i][0]:
				print(ranks_init[j][1])
				ranks_init[j][1]=ranks_list_temp[i][1]
				print(ranks_list_temp[i][1])
				print(ranks_init[j][1])	
	print("ranks_init")
	print(ranks_init)
	ranks_list_null=ranks_t0
	for i in range(len(ranks_t0)):
		ranks_list_null[i][1]=0.0
	print("ranks_list_null")
	print(ranks_list_null)
	ranks_list_temp=ranks_list_null
	print("ranks_list_temp")
	print(ranks_list_temp)
	#time.sleep(30)
	time.sleep(3)
	print("")
	print("")


print(ranks_init)
d=0
for a in ranks_init:
	d=d+a[1]
print(d)

s=0

for a in ranks_init:
	a[1]=a[1]*0.85+0.15
print("ranks_init avec *0.85 + 0.15 :")
print(ranks_init)

for a in ranks_init:
	s=s+a[1]
print(s)
print("-------- termine --------------")
