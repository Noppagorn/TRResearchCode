import datetime
import re
import pymysql
from pymysql import MySQLError

from datetime import datetime

from pandas import Series
import pandas as pd
import numpy as np
sql_username = "noppagorn"
sql_password = "Research705"
sql_main_database = "TR3_Class_test"
def reconnect():
    global conn
    conn = pymysql.connect(host='localhost', user=sql_username,
    passwd=sql_password, db=sql_main_database,
    autocommit=True,
    port=3306)
def getpersent(currentNum,maxNum):
	return "{:.2f}".format((100.0 * currentNum)/maxNum)
def qurryByString(string):
    try:
        reconnect()
        cursor = conn.cursor()
        row_count = cursor.execute(string)
        data =  pd.read_sql(string,conn)
        if data.empty:
            print("Emtry")
            return None
        else:
            return data
    except Exception as e:
        print(e)
keyQury = """
    SELECT cp.class_topic_id, cp.class, cp.topic, kcp_info.keyword FROM 
	(SELECT tmp.class_topic_id, class.class, tmp.topic FROM 
		(SELECT class_topic.class_topic_id, class_topic.class_id, topic.topic FROM `class_topic` 
			INNER JOIN topic ON class_topic.topic_id = topic.topic_id ) tmp 
			INNER JOIN class ON tmp.class_id = class.class_id ORDER BY class_topic_id DESC) cp 
            LEFT JOIN (SELECT kcp.class_topic_id, keyword.keyword FROM `keyword_class_topic` AS kcp 
					INNER JOIN keyword ON kcp.keyword_id = keyword.keyword_id) kcp_info ON kcp_info.class_topic_id = cp.class_topic_id 
						ORDER BY cp.class_topic_id ASC""" 
#arrKeyWord = arrKeyWord.split(";")
hemp = """ 
(title like \"%กัญชง%\" 
OR title like \"%hemp%\" 
OR title like \"%cannabis%\"
OR title like \"%sativa spp. sativa%\"
OR title like \"%cannabis%\"
OR title like \"%sativa subsp. sativa%\"
OR title like \"cannabis sativa subspicies sativa%\") 
"""
reconnect()

beginDT = datetime.now()

arrKeyWord = qurryByString(keyQury)
count = 0
maxNum = arrKeyWord.shape[0]
newData = pd.DataFrame(columns=["title","FiscalYear","keyword","topic","class"])
print("start search")
for index,x in arrKeyWord.iterrows():
	qurryTemp  = "select title,FiscalYear from TR3_Data_Migrated.item WHERE {1} AND title like \"%{0}%\" ;".format(x["keyword"],hemp)
	print(qurryTemp)
	frame = qurryByString(qurryTemp)
	if frame is not None:
		frame["keyword"] = x["keyword"]
		frame["topic"] = x["topic"]
		frame["class"] = x["class"]
		print(frame)
		newData = pd.concat([newData,frame])
	count+=1
	print("status :",getpersent(count,maxNum),"%")

print("drop duplicate")
newData = newData.drop_duplicates()
#DB Migrate
print("-----------------------------")
print(newData)
newData.to_csv("result_Migate_algro2_3_hemp.csv",encoding="utf-8-sig")

#Class TR3
endDT = datetime.now()
print(beginDT.strftime("%Y-%m-%d %H:%M:%S"))
print(endDT.strftime("%Y-%m-%d %H:%M:%S")) 

conn.close()
