import pymysql
import math
import sys

 # 打开数据库连接
db = pymysql.connect("123.207.154.167","root","123456wang","userBase" ) 
cursor = db.cursor()
sql = "SELECT * FROM data"
try:
   # 执行SQL语句
   cursor.execute(sql)
   # 获取所有记录列表
   results = cursor.fetchall()
   DataList = []
   for row in results:
      DataList.append([row[0],row[1],row[2],row[3],row[4]])
    
except:
   print ("Error: unable to fetch data")

# 关闭数据库连接
db.close()
dict_ = {}  #用户id,电影id，评分的字典
dict_id = {} # 用户id的字典,相当于用户id与用户的自然序号的对应
dict_movie = {} # 电影以及对应序号
for i in DataList:
    if dict_.get(i[0],None) == None:
        dict_.update({i[0]:{i[1]:i[2]}})
    else:
        dict_[i[0]].update({i[1]:i[2]})
for i in DataList:
    if dict_id.get(i[3],None) == None:
        dict_id.update({i[3]:i[0]})
for i in DataList:
    if dict_movie.get(i[4],None) == None:
        dict_movie.update({i[4]:i[1]})
# #计算余弦距离
# def calcCosDistSpe(user1,user2):
#     avg_x=0.0
#     avg_y=0.0
#     for key in user1:
#         avg_x+=key[1]
#     avg_x=avg_x/len(user1)

#     for key in user2:
#         avg_y+=key[1]
#     avg_y=avg_y/len(user2)

#     u1_u2=0.0
#     for key1 in user1:
#         for key2 in user2:
#             if key1[1] > avg_x and key2[1]>avg_y and key1[0]==key2[0]:
#                 u1_u2+=1
#     u1u2=len(user1)*len(user2)*1.0
#     sx_sy=u1_u2/math.sqrt(u1u2)
#     return sx_sy

#相似余弦距离
def calcSimlaryCosDist(dict_,userid,peopleid):
    sum_x=0.0
    sum_y=0.0
    sum_xy=0.0
    avg_x=0.0
    avg_y=0.0
    for key in dict_[userid].keys():
        avg_x+=dict_[userid][key]
    avg_x=avg_x/len(dict_[userid]) # user1的平均电影评分

    for key in dict_[peopleid].keys():
        avg_y+=dict_[peopleid][key]
    avg_y=avg_y/len(dict_[peopleid]) # user2的平均电影评分

    #在user1与user2中进行循环，挑出两者共同看过的电影
    for key1 in dict_[userid].keys():
        for key2 in dict_[peopleid].keys():
            if key1==key2 :
                sum_xy+=(dict_[userid][key1]-avg_x)*(dict_[peopleid][key2]-avg_y)
                sum_y+=(dict_[peopleid][key2]-avg_y)**2
        sum_x+=(dict_[userid][key1]-avg_x)**2

    if sum_xy == 0.0 :
        return 0,avg_x,avg_y
    sx_sy=math.sqrt(sum_x*sum_y) 
    return sum_xy/sx_sy,avg_x,avg_y


# 生成用户评分的数组
def createUserRankDic(rates):
    users_dic={}  # 用户打分表格，用户对应的是电影以及电影评分
    movie_dic={}   # 电影与用户对应的字典
    for i in DataList:
        user_rank=(i[1],i[2]) # 电影以及电影评分的数组
        if i[0] in users_dic:
             users_dic[i[0]].append(user_rank)
        else:
             users_dic[i[0]]=[user_rank]
        if i[1] in movie_dic:
             movie_dic[i[1]].append(i[0])
        else:
             movie_dic[i[1]]=[i[0]]
    return users_dic,movie_dic

# 计算与指定用户(userid)最相邻的邻居
# users_dic相当于用户打分字典
# movie_dic相当于电影对应的用户字典
# def calcNearestNeighbor(userid,users_dic,movie_dic):
#     # userid代表预测的人id
#     # movie 代表预测的人，由users_dic[userid]来访问
#     neighbors=[]
    
#     for movie in users_dic[userid]:
#         for neighbor in movie_dic[movie[0]]:# movie[0]代表电影，movie[1]代表评分，预测的人的评分与对比用户评分相等
#                                           # 上面表达式代表的是取出同样看过电影的人
#             if neighbor != userid and neighbor not in neighbors:
#                 if dict_[neighbor][movie[0]] == dict_[userid][movie[0]]:
#                     neighbors.append(neighbor)
    
#     neighbors_dist=[] # 返回与邻居最短距离的计算

#     for neighbor in neighbors:
#         dist=calcSimlaryCosDist(dict_[userid],dict_[neighbor]) 
#         neighbors_dist.append([dist,neighbor])
#     neighbors_dist.sort(reverse=True)
#     return  neighbors_dist,neighbors

# 找到邻居的函数
def find_your_neighbor(userid,users_dic,movie_dic):
    neighbors=[]
    for movie in users_dic[userid]:
        for neighbor in movie_dic[movie[0]]:# movie[0]代表电影，movie[1]代表评分，预测的人的评分与对比用户评分相等
                                          # 上面表达式代表的是取出同样看过电影的人
            if neighbor != userid and neighbor not in neighbors:
                if dict_[neighbor][movie[0]] == dict_[userid][movie[0]]:
                    neighbors.append(neighbor)
    return neighbors



# 利用相似度进行评分预测的函数，目的是求出userid对于movieid的预测评分
def predict_similary(userid,movieid):
    users_dic,movie_dic=createUserRankDic(DataList)
    neighbors=find_your_neighbor(userid,users_dic,movie_dic)
    sim_sum=0
    sim_sum2=0
    #print(users_dic[userid])
    #print(userid)
    #print(neighbors)
    for people in neighbors:
         sim_i_j,avg_i,avg_j = calcSimlaryCosDist(dict_,userid,people)
         print(avg_i)
         sim_sum += math.fabs(sim_i_j)
         temp = dict_[people].get(movieid,None)
         if temp!= None:
            sim_sum2 += sim_i_j*(dict_[people][movieid]-avg_j)
    if(sim_sum==0):
        #print(avg_i)
        pass
    else:
        grade=avg_i+(sim_sum2/sim_sum)
        print(grade)
    





#使用userfc进行推荐，需要的变量为 用户id,文件名称，邻居数量（代表着类似用户的数量）
def recommend(filename,userid,k):
    #格式化成字典数据
    #用户字典 dic[用户id]=[(电影id,电影评分),(电影id,电影评分)]
    #电影字典 dic[电影id]=[用户id1，用户id2]
    # test_dic 相当于用户字典
    # test_movie_to _user相当于电影对应的用户字典

    users_dic,movie_dic=createUserRankDic(DataList)
    
    #寻找邻居
    neighbors_dist,neighbors=calcNearestNeighbor(userid,users_dic,movie_dic)
    neighbors_dist =  neighbors_dist[:k]

    recommend_dic={}
    for neighbor in neighbors_dist:
        neighbor_user_id=neighbor[1]
        movies=users_dic[neighbor_user_id]
        for movie in movies:
            if movie[0] not in recommend_dic:
                recommend_dic[movie[0]]=neighbor[0]
            else:
                recommend_dic[movie[0]]+=neighbor[0]

    #建立推荐列表
    recommend_list=[]
    for key in recommend_dic:
        recommend_list.append([recommend_dic[key],key])

    recommend_list.sort(reverse=True)

    user_movies = [ i[0] for i in users_dic[userid]]
    return [i[1] for i in recommend_list],user_movies,movie_dic,neighbors_dist,neighbors



#主程序
if __name__ == "__main__":
    
    movies=DataList
    user = dict_id['yuminshuiqi']
    movie = dict_movie['拯救大兵瑞恩']
    predict_similary(user,movie)
    #print(dict_id)
    #predictUser_id = str(sys.argv[1])
    #print(predictUser_id)
    #predictUser_id = dict_id[predictUser_id]
    #predictMovie_id = str(sys.argv[2])
    #predictMovie_id = dict_movie[predictMovie_id]
    # recommend_list,user_movie,movie_dic,neighbors_dist,neighbors=recommend(DataList,predictUser_id,5)
    #neighbors_id=[ i[1] for i in neighbors_dist]
    # number=0
    # grade=0
    # for people in neighbors:
        
    #     if dict_[people].get(predictMovie_id,None) != None:
    #         grade +=dict_[people][predictMovie_id]
    #         number+=1
    # grade=grade/number

    # if grade==0:
    #     for j in dict_[predictUser_id].values():
    #         grade += j
    #     grade = grade/len(dict_[predictUser_id])
    # print(neighbors)
    # print(dict_[predictUser_id])
    # print(grade)

    #print(dict_)
    #print(dict_movie)