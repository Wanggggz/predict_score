#!/usr/bin/python3
 
import pymysql
 
# 打开数据库连接
db = pymysql.connect("123.207.154.167","root","123456wang","userBase" )
 
# 使用cursor()方法获取操作游标 
cursor = db.cursor()
 
# SQL 查询语句
sql = "SELECT * FROM data"
try:
   # 执行SQL语句
   cursor.execute(sql)
   # 获取所有记录列表
   results = cursor.fetchall()
   a = []
   for row in results:
      a.append([row[0],row[1],row[2]])
       # 打印结果
   print(a)
      
except:
   print ("Error: unable to fetch data")
 
# 关闭数据库连接
db.close()