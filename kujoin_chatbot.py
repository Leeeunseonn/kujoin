# !pip install xlrd
# !pip install openpyxl
# !pip install pandas

import json, unicodedata, re, ast, unicodedata, time, pprint, math

from elasticsearch import helpers, Elasticsearch, exceptions
import csv
import json
import pandas as pd
from datetime import datetime
from itertools import chain

import copy

# from total_funcs import ES_func
from openpyxl import load_workbook

# 로컬/개발/운영서버 별 자동으로 host(ip) 변경
import socket
curr_host_ip = socket.gethostbyname((socket.gethostname()))
# 운영서버 > 리눅스:127.0.0.1 / 127.0.1.1, 윈도우:10.1.0.4
if curr_host_ip in ["127.0.0.1", "127.0.1.1", "10.1.0.4"]:
    es_host = "10.1.0.4"
# 로컬/개발서버
else:
    es_host = "61.78.63.51"

# 엘라스틱서치 접속정보
es_host = "61.78.63.51"
es_port = 9200
es_http_auth = ("sycns", "rltnfdusrnth")
es_timeout = 3
es_max_retries = 0
es_retry_on_timeout = False
es_request_timeout=3

# 데이터 스크롤 options
es_scroll = "60m"
es_scroll_size = 10000
es_scroll_timeout = "60m"

index = "kujoin_chatbot_backupup"

should_list = {
    "무엇", "어떤", "선택", "어떻게",
    "의미", "뭔", "뭣", "뭐", "뭔가요", "왜", "어디서",
    "가능", "어디", "얼마나", "나요", "대해서", "대한",
    "이란", "란"
}

today =datetime.today()

chatbot_mapping = {
    "settings": {
        "index": {
            "analysis": {
                "tokenizer": {
                    "nori_tokenizer_mixed": {
                        "type": "nori_tokenizer",
                        "decompound_mode": "mixed"
                    }
                },

                "analyzer": {
                    "nori": {
                        "type": "custom",
                        "tokenizer": "nori_tokenizer_mixed",
                        "filter": ["lowercase", "my_pos_f"]
                    }
                },

                "filter": {
                    "my_pos_f": {
                        "type": "nori_part_of_speech",
                        "stoptags": [
                            "VA", "VX", "VCP", "VCN", "MAJ", "J", "XPN", "XSA", "XSN", "XSV", "SP", "SSC", "SSO",
                            "SC", "SE", "UNA"
                        ]
                    }
                }
            }
        }
    },

    "mappings": {
        "dynamic": False,

        "properties": {
            "CreateDate":{
                "type" : "date",
                "format" : "yyyy-MM-dd HH:mm:ss.SSSSSS"
            },
            "UpdateDate":{
                "type" : "date",
                "format" : "yyyy-MM-dd HH:mm:ss.SSSSSS"
            },
            "Data":{
                "properties":{
                    "Question": {
                        "type": "text",
                        "analyzer": "nori"
                    },
                    "Answer": {
                        "type": "text",
                        "analyzer": "nori"
                    },
                    "Url": {
                        "type": "text"
                    }
                }
            }
            
        }
    }
}

def get_es_conn():
    es = None
    try:
        es = Elasticsearch(
            host=es_host,
            port=es_port,
            http_auth=es_http_auth,
            timeout=es_timeout,
            max_retries=es_max_retries,
            retry_on_timeout=es_retry_on_timeout,
            request_timeout=es_request_timeout
        )

    except Exception as e:
        # print(e)
        pass
    return es
    
def get_data_from_es(index, query, es=None):
    Data = []
    try:
        data = es.search(index=index, body=query,size=10000,scroll=es_scroll)
        # print(data)
        if data["hits"]["total"]["value"] > 0:
            # 스크롤 시작
            # idx = 0
            sid = data.get("_scroll_id")
            size = len(data["hits"]["hits"])
            while size > 0:
                Data += data["hits"]["hits"]
                data = es.scroll(
                    scroll_id=sid, scroll=es_scroll
                )
                sid = data["_scroll_id"]
                size = len(data["hits"]["hits"])
                
            es.clear_scroll(scroll_id=sid)
        else:
            sid = data.get("_scroll_id")
            es.clear_scroll(scroll_id=sid)
            
    except Exception as ex:
        # print(ex)
        pass
    return Data

# 한글 형태소 분석기
def analyze_str(index=index, text=""):
    q = []
    es = get_es_conn()
    try:
        body = {"analyzer": "nori", "text": text}
        res = es.indices.analyze(index=index, body=body)
        q = [i["token"] for i in res["tokens"]]
    except Exception as e:
        # print(e)
        pass
    es.close()
    return q


#################################################################################


def index_setting():
    es = get_es_conn()

    if es.indices.exists(index=index):
        pass
    else:
        es.indices.create(index=index, body=chatbot_mapping)
    es.close()

def deleteIndex():
    es = get_es_conn()
    if es.indices.exists(index=index):
        es.indices.delete(index=index, ignore=[400, 404])
    else:
        pass
    es.close()

def csv_conn(xlsx_file): 
    flag="fail"
    try:

        es = get_es_conn()

        df = pd.read_excel(xlsx_file)
        df = df.fillna('')

        data=df.to_dict('records')


        if list(data[0].keys())==['Question', 'Answer', 'Url']:
            data=[{"CreateDate":str(today),"UpdateDate":str(today),"Data":i} for i in data if i['Question'] not in ["",None]]

        helpers.bulk(es, data, index=index)
        es.close()

        flag="success"

    except UnicodeDecodeError as ue:
        flag="UnicodeDecodeError"

    except Exception as ex:
        flag="fail"
        # print(ex)

    return flag


#################################################################################

def dup_check_query(ques):
    return {   
        "size": 1,
        "sort": {"UpdateDate":"desc"},
        "query": {           
            "bool":{
                "must":[{"match":{"Data.Question":ques}}]
            }
        }
    }





def csv_upload(xlsx_file): #csv-> elastic 으로 보냄
    
    flag="fail"

    try:
        es = get_es_conn()

        df = pd.read_excel(xlsx_file)
        df = df.fillna('')

        data=df.to_dict('records')
        # print(data)

        if list(data[0].keys())==['Question', 'Answer', 'Url']:
            data=[i for i in data if i['Question'] not in ["",None]]

        # print(data)

        try:
            for i in data:
                upsertFlag=upsert_chatbot_data(i)

                if ast.literal_eval(upsertFlag[0])['flag']=='fail':
                    flag='updatefail'
                    return str({"flag": flag}),
            
            flag="success"
    
        except Exception as ex:
            print(ex)
            flag="upsertfail"
            pass
    
    except UnicodeDecodeError as ue:
        # print(ue)
        flag="UnicodeDecodeError" #파일 확장자 올바르지않음
        pass
            
    except Exception as ex:
        # print(ex)
        flag="FileError" #양식 건드림
        pass

    return str({"flag": flag}),



# def csv_download(): # elastic-> csv 로 보냄

#     flag="fail"
#     try:
#         es = get_es_conn()

#         query= {   
#             "track_total_hits": True, 
#             "query": {           
#                 "match_all":{}
#             }
#         }

#         data= get_data_from_es(index=index, query=query, es=es)

#         data = [i['_source'] for i in data if i['_source']]

#         df= pd.DataFrame.from_dict(data)
        
#         df.to_csv(r'E:/sample/test.csv',encoding="utf-8-sig",index=False,header=True)
#         # C:/Users/user/eclipse-workspace/TechLab/KWSSDP/src/main/webapp/upload/kujoin_chatbot.csv
               
#         es.close()

#         flag="success"

#         return str({"flag": flag}),

#     except Exception as ex:
#         # print(ex)
#         pass

#     str({"flag": flag}),


#################################################################################

def search_chatbot_list(docID=None, value=None, size=None): #챗봇 리스트 조회

    result = {"flag": "fail", "Data": {}}

    # 전체검색화면 기준: 한 화면에 16개씩 데이터 출력
    try:
        size = int(size)
    except:
        size = 10

    try:
        if value: # 단어 검색

            query= {   
                "track_total_hits": True,
                "sort": {"UpdateDate":"desc"},
                "query": {           
                    "bool":{
                    }
                }
            }

            q, should = value.split(" "), list()

            # nori analyzer로 분석
            q += analyze_str(index=index, text=value)
            q = list(set(q))
            value = "*" + "* *".join(q) + "*"

            
            should.append({
                "query_string": {
                    "default_field": "Data.Question",
                    "query": value,
                    "default_operator": "OR"
                }
            })
            should.append({
                "query_string": {
                    "default_field": "Data.Answer",
                    "query": value,
                    "default_operator": "OR"
                }
            })
            should.append({
                "query_string": {
                "default_field": "Data.Url",
                "query": value,
                "default_operator": "OR"
                }
            })
            if should:
                query["query"]["bool"].update({"should": should})
                query["query"]["bool"].update({"minimum_should_match": 1})

            # print(query)


        else: #all 검색

            query= { 
                "track_total_hits": True,
                "sort": {"UpdateDate":"desc"},
                "query": {           
                    "match_all":{}
                }
            }
            # 몇 번째 페이지 호출인지 확인
            try:
                pageNo = int(docID)
            except:
                pageNo = 1
            
            from_ = size * (pageNo - 1)
            query.update({"from": from_})


        # 데이터 조회
        try:
            es = get_es_conn()

            res = es.search(
                index=index,
                body=query,
                track_total_hits=True
            )
            # print("===================")
            print(res)
            print("===================")

            # 구조변경하여 return

            if res['hits']['total']['value']:
                # 페이지 개수 계산
                total_num = res["hits"]["total"]["value"]
                total_page_num = int(math.ceil(total_num / size))
                
                dataList= [i['_source'] for i in res["hits"]["hits"] if i['_source']]

                for i in dataList:
                    i['Question']=i['Question'].replace("'","`")
                    i['Answer']=i['Answer'].replace("'","`")
                    i['Url']=i['Url'].replace("'","`")


                result["Data"] = {
                    "idList": total_page_num,
                    "dataList": dataList
                }
                result["flag"]="success"
                result=str(result).replace('"',"'")
                result=result.replace("`","\\\'")

                es.close()

                return result,
                
            else:
                result["flag"]="noData"
                es.close()

                return str(result),

        
        except Exception as e:
            # print(e)
            pass
        
    except Exception as ex:
        # print(ex)
        pass

    es.close()
    return str(result),
            
def upsert_chatbot_data(updatedict): # 개별 데이터 update, insert
    
    flag= "fail"
    try:
        es = get_es_conn()

        all_query= { 
            "track_total_hits": True,
            "sort": {"UpdateDate":"desc"},
            "query": {           
                "match_all":{}
            }
        }

        res = es.search(index=index, body=all_query)
        # print(res)

        if res['hits']['hits']:

            allDataDict={i['_id']:i['_source']['Data'] for i in res['hits']['hits']}
            find_ques=[k for k,v in allDataDict.items() if v['Question']==updatedict['Question']]

            if find_ques:   #수정인 경우
                updatedict={"doc":{"UpdateDate":str(today),"Data":updatedict}}
                result = es.update(index=index, id=find_ques[0], body=updatedict)
                
                if result["result"] == "updated":
                    flag = "success"
                else:
                    flag="fail"

            else: #새로 추가인 경우
                if updatedict['Question'] not in ["",None]:
                    result = es.index(index=index,body={"CreateDate":str(today),"UpdateDate":str(today),"Data":updatedict})

                    # 엘라스틱서치에 저장 성공시
                    if result["result"] == "created":
                        flag = "success"
                else:
                    flag="fail"

            
        # 엘라스틱서치 연결 종료
        es.close()

    except Exception as ex:
        # print(ex)
        pass

    return str({"flag": flag}),

def delete_chatbot_data(Ques):
    flag="fail"

    try:
        es = get_es_conn()

        query={   
            "size":100,
            "query": {           
                "bool":{
                    "must":[
                        {"match":{"Data.Question":Ques}}
                    ]
                }
            }
        }
        print(query)
        res = es.search(index=index, body=query)
        print(res)
        
        if res['hits']['hits']:

            find_id=res['hits']['hits'][0]['_id']
            result=es.delete_by_query(index=index, doc_type="_doc", body={"query":{"match":{"_id":find_id}}})

            if result['deleted']==1:
                flag = "success"
        
        es.close()
    
    except Exception as ex:
        # print(ex)
        pass

    return str({"flag": flag}),


# def searchData(input):
    
#     result = {"flag": None, "Data": None}
#     flag, data = "fail", []
    
#     try:

#         # 검색할 단어 추출
#         must, should = [], []

#         es = get_es_conn()

#         if es:
#             # 토큰 어널라이저
#             res_words = es.indices.analyze(index=index,
#                                            body={
#                                                "analyzer": "nori",
#                                                "text": [input]
#                                            }
#                                            )

#             # print(res_words)

#             if res_words["tokens"]:
#                 for w in res_words["tokens"]:
#                     if w["token"] in should_list:
#                         should.append(w["token"])
#                     else:
#                         must.append(w["token"])
#         else:
#             should = input.split(" ")

#         must = ' '.join(s for s in must)
#         should = ' '.join(s for s in should)

#         # refresh
#         es.indices.refresh(index=index)

#         # print("must:",must)
#         # print("should:",should)

#         # 검색
#         query = {
#             "size": 5,
#             "sort": {"_score": "desc"},
#             "query": {
#                 "bool": {
#                     "must": [{"match": {"Question": must}}],
#                     "should": [{"match": {"Question": should}}]
#                 }
#             }
#         }

#         # print("QUERY",query)

#         # 쿼리 실행
#         res = es.search(index=index, body=query,request_timeout=1)

#         # print(res)

#         # # 갯수
#         # print(res['hits']['total']['value'])

#         # 검색결과파싱
#         if res["hits"]["hits"]:

#             flag = "success"
#             data = [i['_source'] for i in res["hits"]["hits"]]

#             for i in data:
#                 i['Question']=i['Question'].strip().replace("'","`")
#                 i['Answer']=i['Answer'].strip().replace("'","`")
#                 i['Url']=i['Url'].strip().replace("'","`")

#         else:
#             flag = "noData"

#         # result dictionary
#         result['flag'] = flag
#         result['Data'] = data

#         result=str(result).replace('"',"'")
#         result=result.replace("`","\'")

#         # 엘라스틱서치 연결 종료
#         es.close()

#         return result,

#     except Exception as error:
#         # print(error)
        
#         # result dictionary
#         result['flag'] = 'fail'
#         result['Data'] = []

#         return str(result),
  
if __name__ =="__main__":

    # deleteIndex()
    # index_setting()
    # print(csv_conn('E:/sample/챗봇+질문답변+양식서.xlsx'))

    # print(csv_download())

    # print(datetime.today())
    # print(csv_upload('E:/sample/test.xlsx'))


    print(search_chatbot_list("2","지붕",10))

    # print(searchData("세윤씨앤에스"))
    # print(upsert_chatbot_data({"Question":"챗봇은 어떻게 실행하나요","Answer":"수정랭","Url":"https://unhappy.com"}))
    # print(delete_chatbot_data("챗봇은 어떻게 실행하나요"))
# es = get_es_conn()

# xlsx = pd.read_excel("2308_챗봇개발_j(2차수정분).xlsx")
# xlsx.to_csv("경로.csv")

# reader = csv.DictReader(f)
# helpers.bulk(es, reader, index=index)
