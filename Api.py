import predict_Score
import data_Wash


def setup():
    data_Wash.setUp()


def doPredict(username,moviename):
    '''
        usage : doPredict(username,moviename)
            when you use it,it will give you a predict
    '''
    fScore = predict_Score.doPredict(username,moviename)
    if(fScore - int(fScore)>0.5):
        return int(fScore + 0.5)
    else:
        return int(fScore)

if __name__ == '__main__':
    setup()
    print(doPredict('157353420','飞驰人生'))