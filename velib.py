#ZHANG Yuan 6/janvier/2018
import json
from pyspark import SparkContext
from pyspark.streaming import StreamingContext

sc = SparkContext()
ssc = StreamingContext(sc, 5)

#define functions used for different questions
#funnction used for Q2
def latest(x,y):
#for picking the latest updated data
    if x[0]>y[0]:
        return x
    else:
        return y

#funnction used for Q3
def checkchange(infos,currentinfo):
#if the no of available bikes changes during the last 5s, ifchang=1
#else ifchange=0
#return the latest value and add the information of 'ifchange'
    if not len(infos)==0:
        ifchange=0
        available_bikes=0
        #pick the values of'last_update' in the infos and regroup into a new list
        updatelst=[i[0] for i in infos]
        #pick the values of'available_bikes' in the infos and regroup into a new list
        availst=[i[1] for i in infos]
        #the latest value of 'available_bikes'
        latest_avai=availst[updatelst.index(max(updatelst))]
        if not(currentinfo is None):
            if (not len(set(availst))==1) or (latest_avai!=currentinfo[1]):
                ifchange=1
            else:
                ifchange=0
        return (max(updatelst),latest_avai,ifchange)
    else:
        return (currentinfo[0],currentinfo[1],0)

#funnction used for Q4
def culactivity(lst):
#the parameter "lst" is a list of the last 5min data of one station
#return the value of activity
    activity=0
    lst.sort(key=lambda k: k[0])
    if len(lst)>1:
        for i in range(len(lst)-1):
            activity+=abs(lst[i+1][1]-lst[i][1])
    return activity

#=========================================================
#Q1. reads Velib data from velib.behmo.com (port 9999)
stream = ssc.socketTextStream("velib.behmo.com", 9999)

#Q2. every 5s: print empty stations
emptystations = stream.map(lambda station: json.loads(station))\
    .map(lambda station: (station['name'],(station['last_update'],station['available_bikes'])))\
    .reduceByKey(lambda x,y: latest(x,y))\
    .filter(lambda station: station and (station[1][1]==0))\
    .map(lambda station:(station[0],'latest bikes_available:'+str(station[1][1])))\
    .pprint()

#Q3. every 5s: print the stations that have become empty
#here, I pick the stations whose no of 'available_bikes' changes during the last 5s
#and whose latest no of 'available_bikes'==0
newemptysts = stream.map(lambda station: json.loads(station))\
    .map(lambda station: (station['name'],(station['last_update'],station['available_bikes'],0)))\
    .updateStateByKey(checkchange)\
    .filter(lambda station: station and (station[1][1]==0 and station[1][2]==1))\
    .pprint()

#Q4.Every 1 min: print the stations that were most active during the last 5 min 
avastream=stream.map(lambda station: json.loads(station))\
    .map(lambda station: (station['name'],[(station['last_update'],station['available_bikes'],0)]))\

windowstream=avastream.reduceByKeyAndWindow(lambda c1, c2: c1+c2, None, 300, 60)\
    .map(lambda station: (station[0],culactivity(station[1])))\
    .transform(lambda rdd: rdd.sortBy(lambda wc: -wc[1]))\
    .map(lambda station: (station[0],'activity:'+str(station[1])))\
    .foreachRDD(lambda rdd: print(rdd.take(5),'\n'))


ssc.checkpoint("./checkpoint")
ssc.start()
ssc.awaitTermination()
