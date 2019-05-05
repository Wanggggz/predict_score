#-*- coding:utf-8 -*-
#from pyspark import SparkContext
#import sys
import pymysql

class suffle():
    #[~~~~,[{2541: ['我们俩', '8.7'] , 2542: ['听见天堂 Rosso come il cielo', '8.9']}]]
    def set_id(self):
        userFile = open('./011','r')
        reaD = userFile.readlines()
        userName = map(lambda x : x.split(';')[1])



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
        contentFile1 = open(str,'r')
        cf = contentFile.readlines() 
        rdd = list(filter(lambda x : len(x)!=0,contentFile1.readlines()))
       #print(rdd)
        rdd2 = list(filter(lambda x : len(x)!=0,cf))
        #print(list(rdd2))
        rddMovie = map(lambda x : x[3].split('，'),map(lambda x : x.split(';'),rdd))
        rddUser = map(lambda x : x[1],filter(lambda x : x[3]!='',map(lambda x : x.split(';'),rdd2)))
       
        rddLocation = rdd
        User_id = list(rddUser)
       
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
        counter = 0
        #print(dataBase_Movie)
        for i in dataBase_Movie:
            for j in i:
                if len(j) == 2:
                    dic = {j[0] : int(j[1])}
                    dataset_movie.update(dic)
            #print(dataset_movie)
            dataset.append([dataset_movie,User_id[counter]])
            #print(User_id)
            counter += 1
            dataset_movie = {}
        for i in dataset:
            i[0].pop('电影主页')
        #print(dataset)
        contentFile.close()
        contentFile1.close()
        return dataset



    def parseUser(self):
        user = self.parseLines('./111')
        pass



    def parseDatabase(self):
        
        database = self.parseLines('./011')
        #print(database)
        return database


    def customlize(self,data):
        #print(data)
        counter = 0
        userList = []
        kMovie = self.setMovie()
        key = kMovie.keys()
        newkMovie = {}
        print(kMovie)
        for i in key:
            newkMovie.update({i.split(' ')[0]:kMovie[i]})
        print(newkMovie)
        kMovie = newkMovie
        for i in data:
            key = i[0].keys()
           #print(key)
            for k in key:
                tmp = kMovie.get(k,None)
                if tmp != None:
                    #print(k)
                    userList.append([counter,tmp[0],i[0][k],i[1],k])

            counter = counter + 1
        #print(userList)
        db = pymysql.connect("123.207.154.167","root","123456wang","userBase" )
        cursor = db.cursor()
        cursor.execute('truncate table data;')
        sql="insert into data(user_id, movie_id,rate,user_name,movie_name) values (%s,%s,%s,%s,%s);"
        
        cursor.executemany(sql,userList)
        

        db.commit()
        db.close()
        return userList
                
        

if __name__ == "__main__":
    a = suffle()
    #print(a.setMovie())
    parsedD = a.parseDatabase()
    #print(parsedD)
    a.customlize(parsedD)
    #print(a.parseDatabase())