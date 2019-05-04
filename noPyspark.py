#-*- coding:utf-8 -*-
#from pyspark import SparkContext
#import sys
from enum import Enum

class suffle():
    #[~~~~,[{2541: ['我们俩', '8.7'] , 2542: ['听见天堂 Rosso come il cielo', '8.9']}]]
    def setMovie(self):
        movieFile = open('./save_movies.txt')
        movies = movieFile.readlines()
        movies = map(lambda x : [x.split(';')[0],x.split(';')[1]],movies)
        counter = 0
        movie = {}
        for i in movies:
            movie.update({i[0]:[counter,float(i[1])]})
            counter = counter+1
        return movie



    def parseLines(self,str):
        contentFile = open(str,'r')
        # sc = SparkContext()
        # x = sc.textFile(str)
        rdd = list(filter(lambda x : len(x)!=0,contentFile.readlines()))
        rddMovie = map(lambda x : x.split(';'),rdd)
        rddMovie = map(lambda x : x[3].split('，'),rddMovie)
        rddLocation = rdd

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
        contentFile.close()
        return dataset



    def parseUser(self):
        user = self.parseLines('./111')
        pass



    def parseDatabase(self):
        
        database = self.parseLines('./011')
        #print(database)
        return database


    def customlize(self,data):
        counter = 0
        userList = []
        for i in data:
            key = i.keys()
           #print(key)
            for k in key:
                if self.setMovie().get(k,None) != None:
                    userList.append([counter,self.setMovie().get(k,None)[0],self.setMovie().get(k,None)[1]])
            counter = counter + 1
        f = open('./temp','w+')
        for line in userList:
            f.writelines(str(line))
            f.write(',')
        f.close()
        return userList
                
        


if __name__ == "__main__":
    a = suffle()
    #print(a.setMovie())
    parsedD = a.parseDatabase()
    print(a.customlize(parsedD))
    #print(a.parseDatabase())