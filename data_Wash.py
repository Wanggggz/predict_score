#-*- coding:utf-8 -*-
#from pyspark import SparkContext
#import sys
import pymysql

class setUp():
    #[~~~~,[{2541: ['我们俩', '8.7'] , 2542: ['听见天堂 Rosso come il cielo', '8.9']}]]
    def setMovie(self):
        movieFile = open('./save_movies.txt')
        movies = movieFile.readlines()
        movies = map(lambda x : [x.split(';')[0],x.split(';')[1]],movies)
        counter = 0
        movie = {}
        for i in movies:
            movie.update({i[0]:[counter,i[1]]})
            counter = counter + 1
        return movie



    def parseLines(self,str):
        contentFile = open(str,'r')
        rdd = list(map(lambda x : x.split(';'),filter(lambda x : len(x)!=0,contentFile.readlines())))
        rddMovie = map(lambda x : x[3].split('，'),rdd[::])
        User_id = list(map(lambda x : x[1],filter(lambda x : x[3]!='',rdd[::])))
        User_Location = list(map(lambda x : x[0][3:],filter(lambda x : x[3]!='',rdd[::])))
        
        dataBase_Movie = []
        dataset = []
        for i in rddMovie:
            if i != ['']:
                for num in range(len(i)):
                    i[num] = i[num].split(':')
                    if(i[num] == ['']):
                        i.pop(num)
                dataBase_Movie.append(i)
        counter = 0
        dataset_movie = {}
        for i in dataBase_Movie:
            for j in i:
                if len(j) == 2:
                    dic = {j[0] : int(j[1])}
                    dataset_movie.update(dic)
            #print(dataset_movie)
            dataset.append([dataset_movie,User_id[counter],User_Location[counter]])
            #print(User_id)
            counter += 1
            dataset_movie = {}
        #print(dataset)
        contentFile.close()

        return dataset



    def parseDatabase(self):
        
        database = self.parseLines('./people_movie.txt')
        #print(database)
        return database


    def customlize(self,data):
        #print(data)
        counter = 0
        userList = []
        kMovie = self.setMovie()
        key = kMovie.keys()
        newkMovie = {}
        #print(kMovie)
        for i in key:
            newkMovie.update({i.split(' ')[0]:kMovie[i]})
        #print(newkMovie)
        kMovie = newkMovie
        for i in data:
            key = i[0].keys()
           #print(key)
            for k in key:
                tmp = kMovie.get(k,None)
                if tmp != None:
                    #print(k)
                    userList.append([counter,tmp[0],i[0][k],i[1],k,i[2]])

            counter = counter + 1
        #print(userList)
        db = pymysql.connect("123.207.154.167","root","123456wang","userBase" )
        cursor = db.cursor()
        cursor.execute('truncate table data;')
        sql="insert into data(user_id, movie_id,rate,user_name,movie_name,user_location) values (%s,%s,%s,%s,%s,%s);"
        
        cursor.executemany(sql,userList)
        

        db.commit()
        db.close()
        return userList

def setup(self):
    
    a = setUp()
    #print(a.setMovie())
    parsedD = a.parseDatabase()
    #print(parsedD)
    a.customlize(parsedD)
    return None
 