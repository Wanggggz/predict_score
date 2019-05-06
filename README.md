对豆瓣用户进行电影评分预测

环境:python3 

第三方包:pymysql

使用时需要在predict_Score.py和data_wash.py里将mysql地址和用户名密码数据库改成自己的

使用方法举例:

import Api

Setup('./people_movie.txt','./save_movies.txt')

print('modle : 用户：157353420 电影：飞驰人生 预测分数：'+str(doPredict('157353420','飞驰人生')))
