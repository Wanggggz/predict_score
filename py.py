#-*- coding:utf-8 -*-
from pyspark import SparkContext
import sys



def parseLines(str):
    sc = SparkContext()
    x = sc.textFile(str)
    rdd = x.filter(lambda x : len(x)!=0).map(lambda x : x.split(';'))
    rddMovie = rdd.map(lambda x : x[3].split('，')).collect()
    rddLocation = rdd.collect()

    dataBase_Location = [i[0] for i in rddLocation if i[3]!='']
    dataBase_Movie = []
    dataset = []
    dataset_movie = {}
    for i in rddMovie:
        if i != ['']:
            for num in range(len(i)):
                i[num] = i[num].split(':')
                if(i[num] == ['']):
                    i.pop(num)
            dataBase_Movie.append(i)
    for i in dataBase_Movie:
        for j in i:
            if len(j) == 2:
                dic = {j[0]:int(j[1])}
                dataset_movie.update(dic)
        dataset.append(dataset_movie)
        dataset_movie = {}
    for i in dataset:
        i.pop('电影主页')

    return dataset



def parseUser():
    user = parseLines('./111')
    print(user)
    pass



def parseDatabase():
    
    database = parseLines('./011')
    #print(database)





if __name__ == "__main__":
    parseUser()