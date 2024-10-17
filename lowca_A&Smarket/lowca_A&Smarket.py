# -*- coding=utf-8 -*-

# 반환값에 배열(list)이 있는 경우 return 뒤에 쉼표(,) 붙여야함

############################### Library Import #######################################################
import copy
import datetime
from dateutil.relativedelta import relativedelta
import itertools

# pip install elasticsearch==7.13.3
from elasticsearch import Elasticsearch, helpers, exceptions

# pip install mysql-connector-python==8.0.27
import mysql.connector
import pymysql

import json, unicodedata, re, ast, unicodedata, time, pprint, math

############################### Host Setting #######################################################
# 로컬/개발/운영서버 별 자동으로 host(ip) 변경
import socket

curr_host_ip = socket.gethostbyname((socket.gethostname()))
# 운영서버 > 리눅스:127.0.0.1, 윈도우:10.1.0.4
if curr_host_ip in [
    "127.0.0.1", "127.0.1.1", "10.1.0.4"
    # , "192.168.10.95"
]:
    _market_data = "market_data"
    _program = "program"
    _question_data = "question_data"
    _lowca_market_data = "lowca_market_data"

    # 뷰 데이터 호출용
    es_host1 = "61.78.63.51"
    # 기타 데이터 호출용
    es_host2 = "61.78.63.51"

    # mysql 서버
    mysql_host = "20.194.17.221"
    mysql_user = "kwssdp"
    mysql_passwd = "kwss123!"
    mysql_database = "lowca"
    mysql_connect_timeout = 3600
    mysql_charset = 'utf8'
    autocommit=True

# 로컬/개발서버
else:
    _market_data = "market_data_backup"
    _program = "program_backup"
    _question_data = "question_data_backup"
    _lowca_market_data = "lowca_market_data_backup"

    # 뷰 데이터 호출용
    es_host1 = "61.78.63.51"
    # 기타 데이터 호출용
    es_host2 = "61.78.63.51"

    # mysql 서버
    mysql_host = "61.78.63.52"
    mysql_user = "kwssdp"
    mysql_passwd = "kwss123!"
    mysql_database = "lowca"
    mysql_connect_timeout = 3600
    mysql_charset = 'utf8'
    autocommit=True

############################### Options #######################################################

# 엘라스틱서치 접속정보
es_port = 9200
es_http_auth = ('sycns', 'rltnfdusrnth')
es_timeout = 36000
es_max_retries = 3
es_retry_on_timeout = True

# 데이터 스크롤 options
es_scroll = '60m'
es_scroll_size = 10000
es_scroll_timeout = '60m'

# 기본값 : 오늘 년월
today = datetime.datetime.today()
default_yyyymmdd = today.strftime("%Y%m%d")
default_yyyymm = today.strftime("%Y%m")
default_yyyy = today.strftime("%Y")

this_year = int(datetime.datetime.today().year)


# 데이터 호출
# input: 사업자번호(bizNo), 화면ID(viewID), 년월(yyyymm)
# output: (플래그:"success"/"noData"/"fail", 데이터)

############################### 엘라스틱서치 공통 함수 #######################################
# 엘라스틱서치 연결
def get_es_conn(host=es_host2):
    es = None
    try:
        es = Elasticsearch(
            host=host,
            port=es_port,
            http_auth=es_http_auth,
            timeout=es_timeout,
            max_retries=es_max_retries,
            retry_on_timeout=es_retry_on_timeout
        )
    except Exception as e:
        pass
        # print(e)
    return es


# 엘라스틱서치 인덱스 새로고침
def refresh_es(index):
    try:
        es = get_es_conn()
        es.indices.refresh(index=index)
        es.close()
    except Exception as e:
        pass
        # print(e)


# 엘라스틱서치 모든 데이터 호출
def get_data_from_es(index, query):
    # flag = "scroll"
    flag = "search after"

    Data = []
    es = get_es_conn()

    # search after
    if flag == "search after":
        # search after로 코드 업데이트 # 2023.01.25
        # s = time.time()
        try:
            if "sort" in query.keys():
                query["sort"].append({"_id": {"order": "asc"}})
            else:
                query.update({"sort": [{"_id": {"order": "asc"}}]})

            # 한번에 가져올 데이터 수 (사이즈가 작을수록 빠르게 처리)
            # 최초 엘라스틱서치 호출
            data = es.search(
                index=index,
                body=query,
                track_total_hits=True
            )
            total_num = int(data["hits"]["total"]["value"])
            # print("Total num of Data: {}".format(total_num))
            # print(data)

            while True:
                Data += data['hits']['hits']

                # search_after 업데이트
                search_after = data['hits']['hits'][-1]["sort"]
                query.update({"search_after": search_after})
                # print(len(Data), search_after)

                # 재검색
                data = es.search(
                    index=index,
                    body=query,
                    track_total_hits=True
                )
                # print("{} - {}".format(total_num, len(Data)))
        except Exception as e:
            # print(e)
            pass
        # e = time.time()
        # print(len(Data), e-s)

    # scroll 진행
    else:
        # s = time.time()
        try:
            # 한번에 가져올 데이터 수 (사이즈가 작을수록 빠르게 처리)
            # 엘라스틱서치 호출
            data = es.search(
                index=index,
                scroll=es_scroll,
                body=query,
                track_total_hits=True
            )
            total_num = int(data["hits"]["total"]["value"])
            # print("Total num of Data: {}".format(total_num))

            # print(json.dumps(data, indent=2, ensure_ascii=False))

            # 스크롤 시작
            # idx = 0
            sid = data['_scroll_id']
            while True:
                # idx += 1
                # print("{} Scrolling... {}".format(idx, scroll_size))
                # print(Data[-1])  # 데이터 검수
                Data += data['hits']['hits']

                # 스크롤할 id 업데이트
                data = es.scroll(
                    scroll_id=sid,
                    scroll=es_scroll
                )
                sid = data['_scroll_id']

                num = len(Data)
                # print(num)

                if num >= total_num:
                    break

            es.clear_scroll(scroll_id=sid)
        except Exception as e:
            # print(e)
            pass
        # e = time.time()
        # print(len(Data), e - s)

    es.close()
    return Data


# 엘라스틱서치 1개 데이터 호출
def get_data1_from_es(index, query):
    result = None
    try:
        es = get_es_conn()
        res = es.search(index=index, body=query, size=1)
        if res["hits"]["total"]["value"] > 0:
            result = res["hits"]["hits"][0]
        es.close()
    except Exception as e:
        # print(e)
        pass
    return result


# 한글 형태소 분석기
def analyze_str(index=_market_data, text=""):
    q = []
    es = get_es_conn()
    try:
        body = {"analyzer": "korean", "text": text}
        res = es.indices.analyze(index=index, body=body)
        q = [i["token"] for i in res["tokens"]]
    except Exception as e:
        # print(e)
        pass
    es.close()
    return q

############################### mysql 공통 함수 ############################################

def get_sql_conn():
    sql = None
    try:
        sql = pymysql.connect(
            host = mysql_host,
            user = mysql_user,
            password = mysql_passwd,
            db= mysql_database,
            charset=mysql_charset,
            connect_timeout=mysql_connect_timeout,
            autocommit=autocommit
        )
    except Exception as e:
        # print(e)
        pass
    return sql


############################### 형변환 #######################################################
# 데이터형 모두 str 변환 (None -> "")
def change_none_to_str(Data):
    data = copy.deepcopy(str(Data))
    try:
        while "None" in data:
            data = data.replace("None", """\"\"""")
        data = ast.literal_eval(data)
        # data = unicodedata.normalize("NFKD", json.dumps(data, ensure_ascii=False))  # 유니코드 normalize
        data = unicodedata.normalize("NFC", json.dumps(data, ensure_ascii=False))  # 한글 자음모음 합치기
        data = json.loads(data, strict=False)
    except Exception as e:
        # print(e)
        pass
    return data

# 데이터형 모두 None 변환 ("" -> None)
def change_str_to_none(Data):
    try:
        data = str(Data)
        while """\"\"""" in data:
            data = data.replace("""\"\"""", "None")
        while """\'\'""" in data:
            data = data.replace("""\'\'""", "None")
        data = ast.literal_eval(data)
        data=json.dumps(data, ensure_ascii=False)
        # data = unicodedata.normalize("NFKD", data)  # 유니코드 normalize
        data = unicodedata.normalize("NFC", data)  # 한글 자음모음 합치기
        Data = json.loads(data, strict=False)
    except Exception as e:
        # print(e)
        pass
    return Data

# str형을 dict/json 변환
def change_str_to_json(string, add_escape=True):
    try:
        if add_escape:
            string = string.replace("\\", "\\\\")
        else:
            string = string.replace("\\\\", "\\")
        # 작은따옴표 -> 큰따옴표 변환해야 json으로 인식
        while "\'" in string:
            string = string.replace("\'", "\"")
        string = string.replace("\"[", "[").replace("]\"", "]")
        string = json.loads(string, strict=False)
    except Exception as e:
        # print(e)
        pass
    return string

##########################샘플 데이터###########################################

sample_empno="230921"

fixed_data= {
    "CompName": "LOWCA_1134_J",
    "Field": "J",
    "LogoPath": [
        {
            "AtchMtrID":"레전드레전드ggggg",
            "RefName":"레 전 드 legend lengend ㅋㅋㅋㅋ ㅎㅎㅎㅎ ㅎㅎㅎㅎ ㅋㅋㅋㅋ",
            "RefPath":""
        },
    ],
    "BannerPath": [
        {
            "AtchMtrID":"배너'패쓰파일id1",
            "RefName":"배너패쓰원'본파일명1",
            "RefPath":"..\\project\\upload\\41\\image41.jpg"
        },
        
        
    ],
    "Address": "서울시 동대문'구' 서울시'립대로 28길 26 2층",
    "Email": "eunseon.lee@sycns.co.kr",
    "Tel": "02-572-8'662",
    "HomePageUrl": "",
    "Introduce": "",
    "FoundingDate": "2020-01-01",
    "CompType": ["중소기업", "구조인 디자인 연'구소", "디자인 연'구소", "구조인 디자인 연구소", "구조인 디자인 연구소"],
    "SearchKeyword": ["dfdf", "건축구조설계", "목구조설계", "구조디자인", "….설계"],
    "CertImagePath": [
        {
            "AtchMtrID":"",
            "RefName":"",
            "RefPath":""
        },
        
    ],
    "ImgAreaType": "3A",
    "ImgAreaPath": [
        {
            "AtchMtrID":"이미지에리아파일id1",
            "RefName":"이미지에리아원본파일명1",
            "RefPath":"..\\project\\upload\\41\\image41.jpg"
        },
        {
            "AtchMtrID":"이미지에리아파일id2",
            "RefName":"이미지에리아원본파일명2",
            "RefPath":"..\\project\\upload\\41\\image41.jpg"
        },
        {
            "AtchMtrID":"이미지에리아파일id3",
            "RefName":"이미지에리아원본파일명3",
            "RefPath":"..\\project\\upload\\41\\image41.jpg"
        },
    ],
    "ActiveYN": "1",


    "Category": [
        {			
            "CategoryType": "1",
            "CategoryName": "프로그'램",
            "CategoryList": [	 
                {
                    "Title" : "",
                    "Content" : "",
                    "ImgPath" : [
                        {
                            "AtchMtrID":"",
                            "RefName":"",
                            "RefPath":""
                        }
                    ],
                    "PriceType" : "",
                    "PriceInt" : "",
                    "PriceImg" : [{
                            "AtchMtrID":"",
                            "RefName":"",
                            "RefPath":""
                        }],
                    "PriceTxt" : "",
                    "ProgramID" : "3711909_20230207144525372177",
                    "Unit" : "",
                    "Standard" : "",
                    "TitleImgPath" :  [{
                            "AtchMtrID":"",
                            "RefName":"",
                            "RefPath":""
                        }],
                }

            ]
        },
        {			
            "CategoryType": "2",
            "CategoryName": "ㅇㅇㅇㅇ",
            "CategoryList": [	 
                {
                    "Title" : "타입2의 첫번째꺼 ㅇㅇㅇㅇ",
                    "Content" : "ㅇㅇㅇㅇㅇ타입2의 첫번째꺼 접합부의 설계' 근거한 목재 접합부 설계 프로그램",
                    "ImgPath" : [
                        
                        {
                            "AtchMtrID":"타입2의 첫번째꺼",
                            "RefName":"이미지원본파일명1",
                            "RefPath":"..\\project\\upload\\41\\image41.jpg"
                        },
                        {
                            "AtchMtrID":"타입2의 첫번째꺼",
                            "RefName":"이미지원본파일명1",
                            "RefPath":"..\\project\\upload\\41\\image41.jpg"
                        },
                        ],
                    "ArchiveID" : "",
                    # "ArchiveYN" : 0,
                    "PriceType" : "p1",
                    "PriceInt" : "1000000",
                    "PriceImg" : [
                        {
                            "AtchMtrID":"타입2의 첫번째꺼",
                            "RefName":"프라이스이미지원본파일명2",
                            "RefPath":"..\\project\\upload\\41\\image41.jpg"
                        },
                    ],
                    "PriceTxt" : "ㅋㅋㅋㅋㅋㅋㅋㅋㅋㅋㅋㅋㅋㅋ",
                    "ProgramID" : "",
                    "Unit" : "단위",
                    "Standard" : "",
                    "TitleImgPath" :  [
                        {
                            "AtchMtrID":"타입2의 첫번째꺼",
                            "RefName":"",
                            "RefPath":""
                        }
                    ],},
                
                {
                    "Title" : "타입2의 2번째꺼",
                    "Content" : "타입2의 2번째꺼5 '목구조 접합부의 설계' 근거한 목재 접합부 설계 프로그램",
                    "ImgPath" : [
                        {
                            "AtchMtrID":"",
                            "RefName":"",
                            "RefPath":""
                        },
                        
                    ],
                    "ArchiveID" : "333333",
                    # "ArchiveYN" : 1,
                    "PriceType" : "p1",
                    "PriceInt" : '1000000',
                    "PriceImg" : [
                        {
                            "AtchMtrID":"타입2의 2번째꺼",
                            "RefName":"타입2의 2번째꺼",
                            "RefPath":"..\\project\\upload\\41\\image41.jpg"
                        },
                    ],
                    "PriceTxt" : "ㅇㅇㅇ",
                    "ProgramID" : "",
                    "Unit" : "단위",
                    "Standard" : "",
                    "TitleImgPath" :  [
                        {
                            "AtchMtrID":"타입2의 2번째꺼",
                            "RefName":"타이틀이미지원본파일명1",
                            "RefPath":"..\\project\\upload\\41\\image41.jpg"
                        },
                    ],},
                
            ]
        },
        {			
            "CategoryType": "3",
            "CategoryName": "xn서비스",
            "CategoryList": [	 
                {
                    "Title" : "경골목구조의 서까래 설계gggggg",
                    "Content" : "건축구조기준 KDS 41 33 05 '목구조 접합부의 설계' 근거한 목재 접합부 설계 프로그램",
                    "ImgPath" : [
                        {
                            "AtchMtrID":"",
                            "RefName":"",
                            "RefPath":""
                        },
                    ],
                    "ArchiveID":"1212",
                    # "ArchiveYN" : "",
                    "PriceType" : "p1",
                    "PriceInt" : '1000000',
                    "PriceImg" : [
                        {
                            "AtchMtrID":"프라이스이미지파일id2",
                            "RefName":"프라이스이미지원본파일명2",
                            "RefPath":"..\\project\\upload\\41\\image41.jpg"
                        },
                    ],
                    "PriceTxt" : "dfddddddddddddddddddd",
                    "ProgramID" : "",
                    "Unit" : "단위",
                    "Standard" : "",
                    "TitleImgPath" : [
                        {
                            "AtchMtrID":"",
                            "RefName":"",
                            "RefPath":""
                        }],
                    } 
            ]
        },
        {			
            "CategoryType": "4",
            "CategoryName": "1제품",
            "CategoryList": [	 
                {
                    "Title" : "경골목구조의 서까래 설계",
                    "Content" : "건축구조기준 KDS 41 33 05 '목구조 접합부의 설계' 근거한 목재 접합부 설계 프로그램",
                    "ImgPath" : [
                        {
                            "AtchMtrID":"",
                            "RefName":"",
                            "RefPath":""
                        },
                    ],
                    "ArchiveID":"7777",
                    # "ArchiveYN":"",
                    "PriceType" : "p3",
                    "PriceInt" : "",
                    "PriceImg" : [
                        {
                            "AtchMtrID":"",
                            "RefName":"",
                            "RefPath":""
                        },
                    ],
                    "PriceTxt" : "5000만",
                    "ProgramID" : "",
                    "Unit" : "",
                    "Standard" : "규격",
                    "TitleImgPath" : [
                        {
                            "AtchMtrID":"",
                            "RefName":"",
                            "RefPath":""
                        },
                    ],
                } 
            ]
        }
        
    ]

}


question_data={
          "QuestionTitle": " 3711795가 1126에게 lowca 문의드립니다",
          "QuestionContext": "문의 드릴 것이 있어 연락드립니다.",
          "QuestionerEmpName": "홍길동",
          "QuestionerHP": "010-1234-1234",
          "QuestionerEmail": "email@email.com",
          "QuestionFilePath": [     
          {
                "AtchMtrID" : "000000155",
                "RefName" : "원본 파일명_55",
                "RefPath" : "..\\\\project\\\\upload\\\\55\\image55.jpg"
             }
          ]
     }

############################### 함수 ###########################################


def remove_quote(Data):
    import ast, json, unicodedata, pprint
    try:

        for key, value in Data.items():

            if type(value) != list and value != None and not isinstance(value, int):
                value = value.replace("'", "")
                Data[key] = value.replace('''"''', '')


            elif type(value) == list:

                for listvalue in value:
                    if type(listvalue) == dict:

                        for key2, value2 in listvalue.items():
                            if type(value2) != list and value2 != None and not isinstance(value2, int):
                                value2 = value2.replace("'", "")
                                listvalue[key2] = value2.replace('''"''', '')

                            elif type(value2) == list:
                                for listvalue2 in value2:
                                    if type(listvalue2) == dict:
                                        for key3, value3 in listvalue2.items():

                                            if type(value3) != list and value3 != None and not isinstance(value3, int):
                                                value3 = value3.replace("'", "")
                                                listvalue2[key3] = value3.replace('''"''', '')

                                            elif type(value3) == list:

                                                for listvalue3 in value3:
                                                    if type(listvalue3) == dict:
                                                        for key4, value4 in listvalue3.items():
                                                            if type(value4) != list and value4 != None and not isinstance(
                                                                    value4, int):
                                                                value4 = value4.replace("'", "")
                                                                listvalue3[key4] = value4.replace('''"''', '')
                                                                # print(value4)
                                            else:
                                                pass

                                    else:
                                        pass

                    elif type(listvalue) == str:
                        new_value = []
                        for a in value:
                            a = a.replace("'", "")
                            new_listvalue = a.replace('"', '')
                            new_value.append(new_listvalue)

                        Data[key] = new_value
                        break


                    else:
                        pass

            elif value == None:
                pass

    except Exception as e:
        # print(e)
        pass

    return Data

# 오늘 날짜
today = str(datetime.datetime.now()).replace("T", "")

def image_template():
    template = {
        "AtchMtrID": None,
        "RefName": None,
        "RefPath": None
    }
    return template

def comp_info_template():
    template = {
        "EmpNo": None,
        "DataType": None,
        "CreateDate": None,
        "UpdateDate": None,
        "KDisplayYN":None,
        "LDisplayYN":None,
        "Data": {
            "CompName": None,
            "Field": None,
            "LogoPath": [],
            "BannerPath": [],
            "Address": None,
            "Email": None,
            "Tel": None,
            "HomePageUrl": None,
            "Introduce": None,
            "FoundingDate": None,
            "CompType": None,
            "SearchKeyword": None,
            "CertImagePath": [],
            "ImgAreaType": None,
            "ImgAreaPath": [],
            "ActiveYN": None
        }

    }
    return template

def comp_detail_info_template():
    template = {
        "EmpNo": None,
        "DataType": None,
        "CreateDate": None,
        "UpdateDate": None,
        "KDisplayYN":None,
        "LDisplayYN":None,
        "Data": {
            "Category": [
                {
                    "CategoryType": "1",
                    "CategoryName": None,
                    "CategoryList": []

                },
                {
                    "CategoryType": "2",
                    "CategoryName": None,
                    "CategoryList": []

                },
                {
                    "CategoryType": "3",
                    "CategoryName": None,
                    "CategoryList": []

                },
                {
                    "CategoryType": "4",
                    "CategoryName": None,
                    "CategoryList": []

                }
            ]
        }

    }

    return template

def categoryList_temp():
    template = {
        "Title": None,
        "Content": None,
        "ImgPath": [],
        "ArchiveID":None,
        "PriceType": None,
        "PriceInt": None,
        "PriceImg": [],
        "PriceTxt": None,
        "ProgramID": None,
        "Unit": None,
        "Standard": None,
        "TitleImgPath": []
    }
    return template

################################################################################
# LOWCA_market_data index setting

def index_setting():
    
    es = get_es_conn()
    
    doc={
            "settings":{
                "index.number_of_shards": 1, 
                "index.number_of_replicas": 0, 
                "index": {
                    "analysis": {
                        "analyzer": {
                            "korean": {
                                "type": "nori",
                                "stopwords": "_korean_"
                            }
                        }
                    }
                }
            },
           
            "mappings" : {
            "properties" : {
                "CreateDate" : {
                "type" : "date",
                "format" : "yyyy-MM-dd HH:mm:ss.SSSSSS"
                },
                "KDisplayYN" : {
                    "type" : "text",
                    "fields" : {
                        "keyword" : {
                        "type" : "keyword",
                        "ignore_above" : 256
                        }
                    }
                    },
                "LDisplayYN" : {
                    "type" : "text",
                    "fields" : {
                        "keyword" : {
                        "type" : "keyword",
                        "ignore_above" : 256
                        }
                    }
                    },
                "Data" : {
                "properties" : {
                    "ActiveYN" : {
                    "type" : "keyword"
                    },
                    "Address" : {
                    "type" : "text",
                    "analyzer" : "korean"
                    },
                    "BannerPath" : {
                    "properties" : {
                        "AtchMtrID" : {
                        "type" : "keyword"
                        },
                        "RefName" : {
                        "type" : "keyword"
                        },
                        "RefPath" : {
                        "type" : "keyword"
                        }
                    }
                    },
                    "Category" : {
                    "properties" : {
                        "CategoryList" : {
                        "properties" : {
                            "ArchiveID" : {
                            "type" : "integer"
                            },
                            "Content" : {
                            "type" : "text",
                            "analyzer" : "korean"
                            },
                            "ImgPath" : {
                            "properties" : {
                                "AtchMtrID" : {
                                "type" : "keyword"
                                },
                                "RefName" : {
                                "type" : "keyword"
                                },
                                "RefPath" : {
                                "type" : "keyword"
                                }
                            }
                            },
                            "PriceImg" : {
                            "properties" : {
                                "AtchMtrID" : {
                                "type" : "keyword"
                                },
                                "RefName" : {
                                "type" : "keyword"
                                },
                                "RefPath" : {
                                "type" : "keyword"
                                }
                            }
                            },
                            "PriceInt" : {
                            "type" : "integer"
                            },
                            "PriceTxt" : {
                            "type" : "text",
                            "analyzer" : "korean"
                            },
                            "PriceType" : {
                            "type" : "keyword"
                            },
                            "ProgramID" : {
                            "type" : "text",
                            "fields" : {
                                "keyword" : {
                                "type" : "keyword"
                                }
                            },
                            "fielddata" : True
                            },
                            "Standard" : {
                            "type" : "text",
                            "fields" : {
                                "keyword" : {
                                "type" : "keyword"
                                }
                            },
                            "fielddata" : True
                            },
                            "Title" : {
                            "type" : "text",
                            "analyzer" : "korean"
                            },
                            "TitleImgPath" : {
                            "properties" : {
                                "AtchMtrID" : {
                                "type" : "keyword"
                                },
                                "RefName" : {
                                "type" : "keyword"
                                },
                                "RefPath" : {
                                "type" : "keyword"
                                }
                            }
                            },
                            "Unit" : {
                            "type" : "text",
                            "fields" : {
                                "keyword" : {
                                "type" : "keyword"
                                }
                            },
                            "fielddata" : True
                            }
                        }
                        },
                        "CategoryName" : {
                        "type" : "text",
                        "analyzer" : "korean"
                        },
                        "CategoryType" : {
                        "type" : "keyword"
                        }
                    }
                    },
                    "CertImagePath" : {
                    "properties" : {
                        "AtchMtrID" : {
                        "type" : "keyword"
                        },
                        "RefName" : {
                        "type" : "keyword"
                        },
                        "RefPath" : {
                        "type" : "keyword"
                        }
                    }
                    },
                    "CompName" : {
                    "type" : "text",
                    "fields" : {
                        "keyword" : {
                        "type" : "keyword"
                        }
                    },
                    "fielddata" : True
                    },
                    "CompType" : {
                    "type" : "text",
                    "fields" : {
                        "keyword" : {
                        "type" : "keyword"
                        }
                    },
                    "fielddata" : True
                    },
                    "Email" : {
                    "type" : "text"
                    },
                    "Field" : {
                    "type" : "keyword"
                    },
                    "FoundingDate" : {
                    "type" : "date",
                    "format" : "yyyy-MM-dd"
                    },
                    "HomePageUrl" : {
                    "type" : "keyword"
                    },
                    "ImgAreaPath" : {
                    "properties" : {
                        "AtchMtrID" : {
                        "type" : "keyword"
                        },
                        "RefName" : {
                        "type" : "keyword"
                        },
                        "RefPath" : {
                        "type" : "keyword"
                        }
                    }
                    },
                    "ImgAreaType" : {
                    "type" : "keyword"
                    },
                    "Introduce" : {
                    "type" : "text",
                    "analyzer" : "korean"
                    },
                    "LogoPath" : {
                    "properties" : {
                        "AtchMtrID" : {
                        "type" : "keyword"
                        },
                        "RefName" : {
                        "type" : "keyword"
                        },
                        "RefPath" : {
                        "type" : "keyword"
                        }
                    }
                    },
                    "SearchKeyword" : {
                    "type" : "text",
                    "fields" : {
                        "keyword" : {
                        "type" : "keyword"
                        }
                    },
                    "fielddata" : True
                    },
                    "Tel" : {
                    "type" : "text"
                    }
                }
                },
                "DataType" : {
                "type" : "keyword"
                },
                "EmpNo" : {
                "type" : "keyword"
                },
                "UpdateDate" : {
                "type" : "date",
                "format" : "yyyy-MM-dd HH:mm:ss.SSSSSS"
                }
            }
            }
        }
    
    if es.indices.exists(index=_lowca_market_data):
        pass
    else:
        es.indices.create(index=_lowca_market_data, body=doc)
    es.close()
    
    
def deleteIndex():
    
    es = get_es_conn()
    if es.indices.exists(index=_lowca_market_data):
        es.indices.delete(index=_lowca_market_data, ignore=[400, 404])
    else:
        pass
    es.close()

# lowca_question_data index setting

def ques_index_setting():
    es = get_es_conn()
    
    doc={
            "settings":{
                "index.number_of_shards": 1, 
                "index.number_of_replicas": 0, 
                "index": {
                    "analysis": {
                        "analyzer": {
                            "korean": {
                                "type": "nori",
                                "stopwords": "_korean_"
                            }
                        }
                    }
                }
            },
           
            "mappings" : {
                "properties" : {
                    "CreateDate" : {
                    "type" : "date",
                    "format" : "yyyy-MM-dd HH:mm:ss.SSSSSS"
                    },
                    "Data" : {
                    "properties" : {
                        "QuestionContext" : {
                        "type" : "text",
                        "analyzer" : "korean"
                        },
                        "QuestionFilePath" : {
                        "properties" : {
                            "AtchMtrID" : {
                            "type" : "keyword"
                            },
                            "RefName" : {
                            "type" : "keyword"
                            },
                            "RefPath" : {
                            "type" : "keyword"
                            }
                        }
                        },
                        "QuestionID" : {
                        "type" : "keyword"
                        },
                        "QuestionTitle" : {
                        "type" : "text",
                        "analyzer" : "korean"
                        },
                        "QuestionYN" : {
                        "type" : "keyword"
                        },
                        "QuestionerEmail" : {
                        "type" : "text"
                        },
                        "QuestionerEmpName" : {
                        "type" : "text",
                        "fields" : {
                            "keyword" : {
                            "type" : "keyword"
                            }
                        },
                        "fielddata" : True
                        },
                        "QuestionerHP" : {
                        "type" : "text"
                        }
                    }
                    },
                    "DataType" : {
                    "type" : "keyword"
                    },
                    "EmpNo" : {
                    "type" : "keyword"
                    },
                    "QuestionerEmpNo" : {
                    "type" : "keyword"
                    }
                }
            }
            
            
        }
    
    if es.indices.exists(index=_question_data):
        pass
    else:
        es.indices.create(index=_question_data, body=doc)
    es.close()



################################################################################


# 이수빈
# 페이징 기능 (id 리스트 반환)
# def get_id_list(index, query):
#     idList = list()
#     es = get_es_conn()
#     es.indices.refresh(index=index)
#     try:
#         data = es.search(
#             index=index,
#             body=query,
#             track_total_hits=True
#         )
#         total_num = int(data["hits"]["total"]["value"])
#         page_num = int(query["size"])
#
#         # print("Total num of Data: {}".format(total_num))
#         # print(data)
#
#         for page in range(0, total_num, page_num):
#             # print(query["size"])
#
#             # search_after 업데이트
#             search_after = data['hits']['hits'][-1]["sort"]
#             # print(data['hits']['hits'][-1])
#
#             idList.append(search_after)
#             # query.update({"search_after": search_after})
#
#             query.update({"from": page})
#             print(query)
#
#             # 재검색
#             es.indices.refresh(index=index)
#             data = es.search(
#                 index=index,
#                 body=query,
#                 track_total_hits=True
#             )
#             print("number of data -> {}".format(len(data["hits"]["hits"])))
#             print()
#     except Exception as e:
#         # print(e)
#         pass
#     es.close()
#     return idList




######### 자바 호출 : 건축과구조마켓 기업 조회 ###########

Field_mode={"allA":["A","B","C"],"allD":["D","E","F"],"allG":["G","I","J"]} #H는 기타
index_mode ={_market_data:"K",_lowca_market_data:"L"}

def search_comp_list(cond=None, docID=None, mode=None, value=None, size=None):

    flag = "fail"
    total_num, idList, dataList = 0, list(), list()
    # dataAll = {"A": list(), "B": list(), "E": list()}
    dataAll={i:list() for i in Field_mode}

    total_page_num = 0

    # 전체검색화면 기준: 한 화면에 16개씩 데이터 출력
    try:
        size = int(size)
    except:
        size = 16

    # 기본 쿼리
    query = {
        "track_total_hits": True,
        "size": size,
        "sort": [
            # {"_score": {"order": "desc"}},
            {"UpdateDate": {"order": "desc"}},
            {"_id": {"order": "asc"}}
        ],
        "query": {"bool": {
            "filter":[],
            "must": [
                {"match": {"DataType": "comp_info"}},
                # ACTV = 1인 기업들만 검색
                {"match": {"Data.ActiveYN": "1"}},
                {"match": {"LDisplayYN":"1"}}
            ]
        }}
    }

    # 몇 번째 페이지 호출인지 확인
    try:
        pageNo = int(docID)
    except:
        pageNo = 1
    from_ = size * (pageNo - 1)
    query.update({"from": from_})

    es = get_es_conn()

    # 검색조건 있다면
    # 검색조건: "A": 기업명, "B": 기업형태/검색키워드(태그)
    if value:
        q, should = value.split(" "), list()

        # nori analyzer로 분석
        q += analyze_str(index=_program, text=value)
        q = list(set(q))
        value = "*" + "* *".join(q) + "*"

        # 기업명 검색
        if mode == "A":
            should.append({
                "query_string": {
                    "default_field": "Data.CompName",
                    "query": value,
                    "default_operator": "OR"
                }
            })

        # 태그 검색
        elif mode == "B":
            should.append({
                "query_string": {
                    "default_field": "Data.SearchKeyword",
                    "query": value,
                    "default_operator": "OR"
                }
            })

        # 전체 검색
        else:
            should.append({
                "query_string": {
                    "default_field": "Data.CompName",
                    "query": value,
                    "default_operator": "OR"
                }
            })
            should.append({
                "query_string": {
                    "default_field": "Data.SearchKeyword",
                    "query": value,
                    "default_operator": "OR"
                }
            })
        if should:
            query["query"]["bool"].update({"should": should})
            query["query"]["bool"].update({"minimum_should_match": 1})
        # print(query)

    # 전체 검색화면
    if cond == "ALL":
        ##############################################
        # 2023-03-30 회의 이후
        # 최신순으로 4x4 출력
        ##############################################
        try:
            query_all = copy.deepcopy(query)

            # 한 화면에 6개씩 데이터 출력
            # size = 16
            # query_all["size"] = size

            # 필수데이터 존재하는 마켓만 조회
            query_all["query"]["bool"]["must"].append({"exists": {"field": "Data.ImgAreaPath.AtchMtrID"}})

            # 페이지번호에 해당하는 데이터부터 검색
            from_ = size*(pageNo-1)
            query_all.update({"from": from_})

            # 데이터 조회
            try:
                # print(query_all)
                res = es.search(
                    index=[_market_data,_lowca_market_data],
                    body=query_all,
                    track_total_hits=True
                )
                # print(res)
                # print(type(res))
                # print(len(res["hits"]["hits"]))

                # 구조변경하여 return
                if res:
                    total_num = res["hits"]["total"]["value"]
                    total_page_num = int(math.ceil(total_num/size))

                    for i in res['hits']['hits']:
                        i['_source']['Data']['IndexNm']=index_mode[i['_index']]


                    for item in res["hits"]["hits"]:
                        _source = item["_source"]
                        dataList.append(_source)

                flag = "success"
            except Exception as e:
                # print(e)
                pass
        except Exception as e:
            # print(e)
            pass

    # 분야별 검색화면
    else:
        try:
            query_all = copy.deepcopy(query)

            # 한 화면에 16개씩 데이터 출력
            # size = 16
            # query_all["size"] = size
            
            # 필수데이터 존재하는 마켓만 조회
            query_all["query"]["bool"]["must"].append({"exists": {"field": "Data.ImgAreaPath.AtchMtrID"}})
            
            #####should로 변경 allA allD allG 이거맞춰서
            query_all["query"]["bool"]["filter"].append({"terms": {"Data.Field": Field_mode[cond]}})

            # query_all["query"]["bool"]["must"].append({"match": {"Data.Field": cond}})
            from_ = size * (pageNo - 1)
            query_all.update({"from": from_})

            ###########################################################
            # doc ID 리스트 조회
            # idList = get_id_list(index=_lowca_market_data, query=copy.deepcopy(query_all))
            # print(idList)
            ###########################################################
            # docID 있는 경우 (페이징)
            # if docID:
            #     docID = [n for n in ast.literal_eval(str(docID))]
            #     if docID:
            #         query_all.update(
            #             {"search_after": [float(docID[0]), int(docID[1]), str(docID[2])]}
            #         )

            # 데이터 조회
            try:
                res = es.search(
                    index=[_market_data,_lowca_market_data],
                    body=query_all,
                    track_total_hits=True
                )
                # print(res)
                # print(type(res))
                # print(len(res["hits"]["hits"]))

                # 구조변경하여 return
                if res:
                    # 페이지 개수 계산
                    total_num = res["hits"]["total"]["value"]
                    total_page_num = int(math.ceil(total_num/size))

                    for i in res['hits']['hits']:
                        i['_source']['Data']['IndexNm']=index_mode[i['_index']]


                    for item in res["hits"]["hits"]:
                        _source = item["_source"]
                        dataList.append(_source)
                flag = "success"
            except Exception as e:
                # print(e)
                pass
        except Exception as e:
            # print(e)
            pass

    es.close()

    #협회사 회원정보 가져와서 붙이기
    try:
        if dataList:
            sql = get_sql_conn()
            cur = sql.cursor()
            
            #Y는 협회사 N은 일반
            sql_query = "SELECT DEPT_CODE,LOWCA_YN,GRADE FROM lowca.cmt_dept"
            
            cur.execute(sql_query)
            data= cur.fetchall()
            
            grade_dict={}
            
            for i in data:
                if i[1]!=None: #cmt_dept에 lowca_yn이 비어있으면 안됨
                    grade_dict[i[0]]=[i[1],i[2]]

            for i in dataList:
                if i['EmpNo'] in grade_dict.keys():
                    i['Data']['LowcaYN']=grade_dict[i['EmpNo']][0]
                    i['Data']['OrderGrade']=grade_dict[i['EmpNo']][1]
                else: #mysql에 없거나 lowca_yn 비어있는 경우 None 넣어줌
                    i['Data']['LowcaYN']=None
                    i['Data']['OrderGrade']=None

            cur.close()
            sql.close()

            flag = "success"
    except Exception as ex:
        # print(ex)
        flag = "fail"

    data = {
        "cond": cond,
        "idList": total_page_num,
        "dataAll": dataAll,
        "dataList": change_none_to_str(dataList)
    }

    # print(data)

    # data = unicodedata.normalize("NFKD", json.dumps(data, ensure_ascii=False))  # 유니코드 normalize
    data = unicodedata.normalize("NFC", json.dumps(data, ensure_ascii=False))  # 한글 자음모음 합치기
    data = json.loads(data, strict=False)
    return str({"flag": flag, "data": data}),


######### 자바 호출 : 관리자 문의내역 목록 조회 ###########
def search_ques_list(type="0", empNo=None, docID=None, size=None):
    flag = "fail"
    type_, total_num, idList, dataList = str(type), 0, list(), list()
    total_page_num = 0

    # 전체검색화면 기준: 한 화면에 20개씩 데이터 출력
    try:
        size = int(size)
    except:
        size = 20

    if empNo:
        # 기본 쿼리
        query = {
            "size": size,
            "sort": [
                {"_score": {"order": "desc"}},
                {"CreateDate": {"order": "desc"}},
                {"_id": {"order": "asc"}}
            ],
            "query": {"bool": {"must": [
                {"match": {"DataType": "comp_question"}}
                # 문의내역 미처리 기업들만 검색
                # ,{"match": {"Data.QuestionYN": "0"}}
            ]}}
        }

        # 몇 번째 페이지 호출인지 확인
        try:
            pageNo = int(docID)
        except:
            pageNo = 1

        from_ = size * (pageNo - 1)
        query.update({"from": from_})

        # 문의 받은/한 사람 쿼리 구분
        if type_ == "1":
            query["query"]["bool"]["must"].append({"match": {"QuestionerEmpNo": empNo}})
        # type_ == "0"
        else:
            query["query"]["bool"]["must"].append({"match": {"EmpNo": empNo}})

        ###########################################################
        # doc ID 리스트 조회
        # idList = get_id_list(index=_question_data, query=copy.deepcopy(query))
        # print(idList)
        ###########################################################
        # docID 있는 경우 (페이징)
        # if docID:
        #     docID = [n for n in ast.literal_eval(str(docID))]
        #     if docID:
        #         query.update(
        #             {"search_after": [float(docID[0]), int(docID[1]), str(docID[2])]}
        #         )

        es = get_es_conn()
        # print(query)

        # 데이터 조회
        try:
            res = es.search(
                index=_question_data,
                body=query,
                track_total_hits=True
            )
            # print(res)
            # print(type(res))
            # print(len(res["hits"]["hits"]))

            # 구조변경하여 return
            if res:
                # 페이지 개수 계산
                total_num = res["hits"]["total"]["value"]
                total_page_num = int(math.ceil(total_num / size))

                for item in res["hits"]["hits"]:
                    ques_info = item["_source"]
                    # 불필요한 파일 path 데이터 제거
                    del ques_info['Data']["QuestionFilePath"]

                    # 문의한 사람이면
                    if type_ == "1":
                        # 마켓 정보 조회
                        try:
                            _id = str(ques_info["EmpNo"]) + "_comp_info"
                            comp_info = es.get(index=_lowca_market_data, id=_id)
                            comp_info = comp_info["_source"]["Data"]
                            # 불필요한 파일 path 데이터 제거
                            del comp_info["LogoPath"]
                            del comp_info["BannerPath"]
                            del comp_info["CertImagePath"]
                            del comp_info["ImgAreaType"]
                            del comp_info["ImgAreaPath"]
                            # 마켓 정보 추가
                            ques_info["Data"].update(comp_info)
                        except Exception as e:
                            # print(e)
                            pass

                    dataList.append(change_none_to_str(ques_info))
            flag = "success"
        except Exception as e:
            # print(e)
            pass

        es.close()

    data = {
        "idList": total_page_num,
        "dataList": dataList
    }

    # data = unicodedata.normalize("NFKD", json.dumps(data, ensure_ascii=False))  # 유니코드 normalize
    data = unicodedata.normalize("NFC", json.dumps(data, ensure_ascii=False))  # 한글 자음모음 합치기
    data = json.loads(data, strict=False)

    return str({"flag": flag, "data": data}),


######### 자바 호출 : 관리자 문의내역 상세 조회 ###########
def search_ques_info(QuestionID=None):
    flag, data = "fail", dict()
    DataType = "comp_question"
    # 데이터호출
    es = get_es_conn()
    try:
        _id = str(QuestionID) + "_" + DataType
        res = es.get(index=_question_data, id=_id)

        data = change_none_to_str(res["_source"])
        data = change_str_to_json(data, add_escape=False)

        flag = "success"
    except Exception as e:
        # print(e)
        pass
    es.close()

    # data = unicodedata.normalize("NFKD", json.dumps(data, ensure_ascii=False))  # 유니코드 normalize
    data = unicodedata.normalize("NFC", json.dumps(data, ensure_ascii=False))  # 한글 자음모음 합치기
    data = json.loads(data, strict=False)

    return str({"flag": flag, "data": data}),


######### 자바 호출 : 관리자 문의내역 저장 ###########
def save_ques_info(empNo=None, questionerEmpNo=None, data=None):
    flag, DataType = "fail", "comp_question"
    if empNo and questionerEmpNo and data:
        try:
            now = datetime.datetime.now()
            n = now.strftime("%Y%d%m%H%M%S%f")
            d = str(now).replace("T", "")

            # 문의ID 생성 >>> 문의받은사람_문의한사람_seq
            QuestionID = str(empNo) + "_" + str(questionerEmpNo) + "_" + str(n)
            id_doc = QuestionID + "_" + DataType

            # "" -> None, 따옴표제거
            data = change_str_to_json(data, add_escape=True)

            # 전처리
            data["QuestionTitle"] = re.sub("\"", "", re.sub("\'", "", str(copy.deepcopy(data["QuestionTitle"]))))
            data["QuestionContext"] = re.sub("\"", "", re.sub("\'", "", str(copy.deepcopy(data["QuestionContext"]))))
            data["QuestionerEmpName"] = re.sub("\"", "",
                                               re.sub("\'", "", str(copy.deepcopy(data["QuestionerEmpName"]))))
            data["QuestionerHP"] = re.sub("\"", "", re.sub("\'", "", str(copy.deepcopy(data["QuestionerHP"]))))
            data["QuestionerEmail"] = re.sub("\"", "", re.sub("\'", "", str(copy.deepcopy(data["QuestionerEmail"]))))
            QuestionFilePath = copy.deepcopy(data["QuestionFilePath"])
            # print(QuestionFilePath)
            if QuestionFilePath:
                tmp_list = list()
                for q in QuestionFilePath:
                    tmp_dict = dict()
                    for k, v in q.items():
                        t = re.sub("\"", "", re.sub("\'", "", str(v)))
                        tmp_dict.update({k: t})
                    tmp_list.append(tmp_dict)
                data["QuestionFilePath"] = tmp_list

            es = get_es_conn()

            # 데이터 저장
            body = {
                "EmpNo": empNo,
                "QuestionerEmpNo": questionerEmpNo,
                "DataType": DataType,
                "CreateDate": d,
                "Data": {
                    "QuestionID": QuestionID,
                    "QuestionYN": "0"
                }
            }
            # print(id_doc, body)
            body["Data"].update(data)
            result = es.index(index=_question_data, body=body, id=id_doc)
            if result["result"] == "created":
                flag = "success"

            es.close()

        except Exception as e:
            # print(e)
            pass
    return str({"flag": flag}),


######### 자바 호출 : 기업 정보 수정 ###########
# 마켓쓴다는 걸 가정하고 분야는 mysql에도 있고
def update_active_info(empNo,Field,ActiveYN):
    flag, result= "fail", {}

    try:
        es = get_es_conn()

        # 기타 분야면 A&S마켓 운영불가 >>> 0 으로 저장
        if not Field:
            Field = "H"
        if Field not in list(itertools.chain(*Field_mode.values())):
            Field, ActiveYN = "H", 0

        if not ActiveYN:
            ActiveYN = '0'

        _id_comp_info = str(empNo) + "_" + "comp_info"
        _id_comp_detail_info = str(empNo) + "_" + "comp_detail_info"

        try:
            res = es.get(index=_lowca_market_data, id=_id_comp_info)

            # 기업 분야, A&S마켓등록여부 수정 후 저장
            if res["found"]:
                
                comp_info_body = {
                    "doc": {
                        "UpdateDate": today,
                        "LDisplayYN":str(ActiveYN),
                        "Data": {
                            "Field": Field,
                            "ActiveYN": ActiveYN
                        }
                    }
                }
                comp_detail_info_body = {
                    "doc": {
                        "UpdateDate": today,
                        "LDisplayYN":str(ActiveYN)
                    }
                }
            
                result1 = es.update(index=_lowca_market_data, id=_id_comp_info, body=comp_info_body)
                result2 = es.update(index=_lowca_market_data, id=_id_comp_detail_info, body=comp_detail_info_body)

                if result1["result"] == "updated" and result2["result"] == "updated":
                    flag = "success"

        except exceptions.NotFoundError:
            # 엘라스틱에 데이터 없는 경우
            # 애초에 mysql에 있는 걸 가정하고 update하기 때문에 elastic에 저장x
            # 이런 경우 그냥 넘어감
            flag = "success"
            pass

        # 엘라스틱서치 연결 종료
        es.close()

    except Exception as ex:
        # print(ex)
        pass

    return str({"flag": flag}),


######### 자바 호출 : A&S마켓 상세정보 저장 ###########
def save_comp_info(empNo, KDisplayYN ,data):
    flag, result = "fail", []
    try:
        es = get_es_conn(host=es_host2)

        if data:
            data=change_str_to_json(data)
            
            # 따옴표제거
            data = remove_quote(data)
            _id_comp_info = str(empNo) + "_" + "comp_info"
            template = comp_info_template()

            template["EmpNo"] = str(empNo)
            template["DataType"] = "comp_info"
            template["UpdateDate"]=today
            template["LDisplayYN"]=data["ActiveYN"]
            template["KDisplayYN"]=KDisplayYN

            template["Data"]["CompName"]=data["CompName"]
            template["Data"]["Field"] = data["Field"]

            if data["LogoPath"]:
                for i in data["LogoPath"]:
                    LogoPath_image_temp = copy.deepcopy(image_template())

                    LogoPath_image_temp["AtchMtrID"] = i["AtchMtrID"]
                    LogoPath_image_temp["RefName"] = i["RefName"]
                    LogoPath_image_temp["RefPath"] = i["RefPath"]

                    template["Data"]["LogoPath"].append(LogoPath_image_temp)
                
            if data["BannerPath"]:
                for i in data["BannerPath"]:
                    BannerPath_image_temp = copy.deepcopy(image_template())

                    BannerPath_image_temp["AtchMtrID"] = i["AtchMtrID"]
                    BannerPath_image_temp["RefName"] = i["RefName"]
                    BannerPath_image_temp["RefPath"] = i["RefPath"]

                    template["Data"]["BannerPath"].append(BannerPath_image_temp)

            template["Data"]["Address"] = data["Address"]
            template["Data"]["Email"] = data["Email"]
            template["Data"]["Tel"] = data["Tel"]
            template["Data"]["HomePageUrl"] = data["HomePageUrl"]
            template["Data"]["Introduce"] = data["Introduce"]
            template["Data"]["FoundingDate"] = data["FoundingDate"]

            template["Data"]["CompType"] = data["CompType"]
            template["Data"]["SearchKeyword"] = data["SearchKeyword"]

            if data["CertImagePath"]:
                for i in data["CertImagePath"]:
                    CertImagePath_image_temp = copy.deepcopy(image_template())
                    CertImagePath_image_temp["AtchMtrID"] = i["AtchMtrID"]
                    CertImagePath_image_temp["RefName"] = i["RefName"]
                    CertImagePath_image_temp["RefPath"] = i["RefPath"]
                    template["Data"]["CertImagePath"].append(CertImagePath_image_temp)

            template["Data"]["ImgAreaType"] = data["ImgAreaType"]

            if data["ImgAreaPath"]:
                for i in data["ImgAreaPath"]:
                    ImgAreaPath_image_temp = copy.deepcopy(image_template())

                    ImgAreaPath_image_temp["AtchMtrID"] = i["AtchMtrID"]
                    ImgAreaPath_image_temp["RefName"] = i["RefName"]
                    ImgAreaPath_image_temp["RefPath"] = i["RefPath"]

                    template["Data"]["ImgAreaPath"].append(ImgAreaPath_image_temp)

            template["Data"]["ActiveYN"] = data["ActiveYN"]
            
            ###########################################################
            
            _id_comp_detail_info = str(empNo) + "_" + "comp_detail_info"
            detail_temp = comp_detail_info_template()

            detail_temp["EmpNo"] = str(empNo)
            detail_temp["DataType"] = "comp_detail_info"
            detail_temp["CreateDate"]=today
            detail_temp["LDisplayYN"]=data["ActiveYN"]
            detail_temp["KDisplayYN"]=KDisplayYN

            
            if data["Category"]:

                detail_temp["Data"]["Category"][0]["CategoryName"] = data["Category"][0]["CategoryName"]
                detail_temp["Data"]["Category"][1]["CategoryName"] = data["Category"][1]["CategoryName"]
                detail_temp["Data"]["Category"][2]["CategoryName"] = data["Category"][2]["CategoryName"]
                detail_temp["Data"]["Category"][3]["CategoryName"] = data["Category"][3]["CategoryName"]

                for i in data["Category"][0]["CategoryList"]:
                    categoryList1_temp = copy.deepcopy(categoryList_temp())
                    categoryList1_temp["ImgPath"].append(copy.deepcopy(image_template()))
                    categoryList1_temp["PriceImg"].append(copy.deepcopy(image_template()))
                    categoryList1_temp["TitleImgPath"].append(copy.deepcopy(image_template()))  

                    # 프로그램ID가 None으로 넘어오면 저장x -> pass
                    if i["ProgramID"]:
                        categoryList1_temp["ProgramID"] = i["ProgramID"]

                        detail_temp["Data"]["Category"][0]["CategoryList"].append(categoryList1_temp)
                        
                for i in data["Category"][1]["CategoryList"]:
                    categoryList2_temp = copy.deepcopy(categoryList_temp())

                    # 아이템 제목이 None으로 넘어오면 저장x -> pass
                    if i["Title"]:
                        categoryList2_temp["Title"] = i["Title"]
                        categoryList2_temp["Content"] = i["Content"]

                        if i["ImgPath"]:
                            for j in i["ImgPath"]:
                                ImgPath_image_temp = copy.deepcopy(image_template())
                                ImgPath_image_temp["AtchMtrID"] = j["AtchMtrID"]
                                ImgPath_image_temp["RefName"] = j["RefName"]
                                ImgPath_image_temp["RefPath"] = j["RefPath"]
                                categoryList2_temp["ImgPath"].append(ImgPath_image_temp)
                        
                        if i["ArchiveID"]:
                            categoryList2_temp["ArchiveID"]=int(i["ArchiveID"])
                        
                        # if i["ArchiveYN"] not in ["",None]:
                        #     categoryList2_temp["ArchiveYN"]=int(i["ArchiveYN"])

                        categoryList2_temp["PriceType"] = i["PriceType"]
                        if i["PriceInt"]:
                            categoryList2_temp["PriceInt"] = int(i["PriceInt"])

                        if i["PriceImg"]:
                            for j in i["PriceImg"]:
                                PriceImg_image_temp = copy.deepcopy(image_template())
                                PriceImg_image_temp["AtchMtrID"] = j["AtchMtrID"]
                                PriceImg_image_temp["RefName"] = j["RefName"]
                                PriceImg_image_temp["RefPath"] = j["RefPath"]

                                categoryList2_temp["PriceImg"].append(PriceImg_image_temp)

                        categoryList2_temp["PriceTxt"] = i["PriceTxt"]
                        categoryList2_temp["Unit"] = i["Unit"]

                        if i["TitleImgPath"]:
                            for j in i["TitleImgPath"]:
                                TitleImgPath_image_temp = copy.deepcopy(image_template())
                                TitleImgPath_image_temp["AtchMtrID"] = j["AtchMtrID"]
                                TitleImgPath_image_temp["RefName"] = j["RefName"]
                                TitleImgPath_image_temp["RefPath"] = j["RefPath"]

                                categoryList2_temp["TitleImgPath"].append(TitleImgPath_image_temp)

                        detail_temp["Data"]["Category"][1]["CategoryList"].append(categoryList2_temp)

                for i in data["Category"][2]["CategoryList"]:
                    categoryList3_temp = copy.deepcopy(categoryList_temp())
                    categoryList3_temp["TitleImgPath"].append(copy.deepcopy(image_template()))

                    # 아이템 제목이 None으로 넘어오면 저장x -> pass
                    if i["Title"]:
                        categoryList3_temp["Title"] = i["Title"]
                        categoryList3_temp["Content"] = i["Content"]

                        if i["ImgPath"]:
                            for j in i["ImgPath"]:
                                ImgPath_image_temp = copy.deepcopy(image_template())
                                ImgPath_image_temp["AtchMtrID"] = j["AtchMtrID"]
                                ImgPath_image_temp["RefName"] = j["RefName"]
                                ImgPath_image_temp["RefPath"] = j["RefPath"]
                                categoryList3_temp["ImgPath"].append(ImgPath_image_temp)

                        if i["ArchiveID"]:
                            categoryList3_temp["ArchiveID"]=int(i["ArchiveID"])
                        
                        # if i["ArchiveYN"] not in ["",None]:
                        #     categoryList3_temp["ArchiveYN"]=int(i["ArchiveYN"])

                        categoryList3_temp["PriceType"] = i["PriceType"]
                        if i["PriceInt"]:
                            categoryList3_temp["PriceInt"] = int(i["PriceInt"])

                        if i["PriceImg"]:
                            for j in i["PriceImg"]:
                                PriceImg_image_temp = copy.deepcopy(image_template())
                                PriceImg_image_temp["AtchMtrID"] = j["AtchMtrID"]
                                PriceImg_image_temp["RefName"] = j["RefName"]
                                PriceImg_image_temp["RefPath"] = j["RefPath"]
                                categoryList3_temp["PriceImg"].append(PriceImg_image_temp)
                        
                        categoryList3_temp["PriceTxt"] = i["PriceTxt"]
                        categoryList3_temp["Unit"] = i["Unit"]

                        detail_temp["Data"]["Category"][2]["CategoryList"].append(categoryList3_temp)
                
                for i in data["Category"][3]["CategoryList"]:
                    categoryList4_temp = copy.deepcopy(categoryList_temp())

                    # 아이템 제목이 None으로 넘어오면 저장x -> pass
                    if i["Title"]:
                        categoryList4_temp["Title"] = i["Title"]
                        categoryList4_temp["Content"] = i["Content"]

                        if i["ImgPath"]:
                            for j in i["ImgPath"]:
                                ImgPath_image_temp = copy.deepcopy(image_template())
                                ImgPath_image_temp["AtchMtrID"] = j["AtchMtrID"]
                                ImgPath_image_temp["RefName"] = j["RefName"]
                                ImgPath_image_temp["RefPath"] = j["RefPath"]
                                categoryList4_temp["ImgPath"].append(ImgPath_image_temp)
                        
                        if i["ArchiveID"]:
                            categoryList4_temp["ArchiveID"]=int(i["ArchiveID"])
                        
                        # if i["ArchiveYN"] not in ["",None]:
                        #     categoryList4_temp["ArchiveYN"]=int(i["ArchiveYN"])

                        categoryList4_temp["PriceType"] = i["PriceType"]
                        if i["PriceInt"]:
                            categoryList4_temp["PriceInt"] = int(i["PriceInt"])

                        if i["PriceImg"]:
                            for j in i["PriceImg"]:
                                PriceImg_image_temp = copy.deepcopy(image_template())
                                PriceImg_image_temp["AtchMtrID"] = j["AtchMtrID"]
                                PriceImg_image_temp["RefName"] = j["RefName"]
                                PriceImg_image_temp["RefPath"] = j["RefPath"]
                                categoryList4_temp["PriceImg"].append(PriceImg_image_temp)

                        categoryList4_temp["PriceTxt"] = i["PriceTxt"]
                        categoryList4_temp["Standard"] = i["Standard"]

                        if i["TitleImgPath"]:
                            for j in i["TitleImgPath"]:
                                TitleImgPath_image_temp = copy.deepcopy(image_template())
                                TitleImgPath_image_temp["AtchMtrID"] = j["AtchMtrID"]
                                TitleImgPath_image_temp["RefName"] = j["RefName"]
                                TitleImgPath_image_temp["RefPath"] = j["RefPath"]
                                categoryList4_temp["TitleImgPath"].append(TitleImgPath_image_temp)

                        detail_temp["Data"]["Category"][3]["CategoryList"].append(categoryList4_temp)


            # 최초저장인지 확인
            try:
                res = es.get(index=_lowca_market_data, id=_id_comp_info)

                # 업데이트인 경우 >>> UpdateDate, data만 수정
                if res["found"]:

                    comp_info_body = {
                        "doc": {
                            "UpdateDate": today,
                            "LDisplayYN":template["LDisplayYN"],
                            "KDisplayYN":template["KDisplayYN"],
                            "Data": template["Data"]
                        }
                    }

                    comp_detail_info_body = {
                        "doc": {
                            "UpdateDate": today,
                            "LDisplayYN":template["LDisplayYN"],
                            "KDisplayYN":template["KDisplayYN"],
                            "Data": detail_temp["Data"]
                        }
                    }

                    result1 = es.update(index=_lowca_market_data, id=_id_comp_info, body=comp_info_body)
                    result2 = es.update(index=_lowca_market_data, id=_id_comp_detail_info, body=comp_detail_info_body)
                    
                    if result1["result"] == "updated" and result2["result"] == "updated":
                        flag = "success"

            except exceptions.NotFoundError:
                # 최초 저장인 경우
                
                template["CreateDate"] = today
                detail_temp["CreateDate"] = today

                result1 = es.index(
                    index=_lowca_market_data,
                    id=_id_comp_info,
                    body=template
                )

                result2 = es.index(
                    index=_lowca_market_data,
                    id=_id_comp_detail_info,
                    body=detail_temp
                )
                
                # 엘라스틱서치에 저장 성공시
                if result1["result"] == "created" and result2["result"] == "created":
                    flag = "success"
                

        # 엘라스틱서치 연결 종료
        es.close()

    except Exception as error:
        # print(error)
        pass

    return str({"flag": flag}),



######### 자바 호출 : A&S마켓 상세정보 조회 ###########
def search_comp_info(empNo,indexNm):
    import ast

    flag,KDisplayYN, result= "fail","0", {}

    try:
        
        es = get_es_conn(host=es_host2)

        _id_comp_info = str(empNo) + "_" + "comp_info"
        _id_comp_detail_info = str(empNo) + "_" + "comp_detail_info"

        query = {
            "size": 2,
            "query": {
                "terms": {
                    "_id": [_id_comp_info, _id_comp_detail_info]
                }
            },
            "sort": [{"_id": "desc"}],
        }

        if indexNm=="L": #인덱스가 lowca인 경우
            refresh_es(index=_lowca_market_data)
            res = es.search(index=_lowca_market_data, body=query)
            
        elif indexNm=="K":# 인덱스가 kujoin인 경우
            refresh_es(index=_market_data)
            res = es.search(index=_market_data, body=query)
            
        else:
            # flag="fail"
            # return str({"flag": flag,"KDisplayYN":KDisplayYN, "data": result}),
            refresh_es(index=_market_data)
            res = es.search(index=_market_data, body=query)
        
        # print(res)

        if res['hits']['hits']:
            for i in res['hits']['hits']:
                result.update(i['_source']['Data'])

                ########################################
                # elastic의 [] -> {"AtchMtrID":"","RefName":"","RefPath":""} 으로 변환
                if i['_source']['DataType']=='comp_info':
                    KDisplayYN=i['_source']['KDisplayYN']
                        
                    if not result['LogoPath']:  # 빈리스트면
                        result['LogoPath'].append(image_template())

                    if not result['BannerPath']:  # 빈리스트면
                        result['BannerPath'].append(image_template())

                    if not result['CertImagePath']:  # 빈리스트면
                        result['CertImagePath'].append(image_template())

                    if not result['ImgAreaPath']:  # 빈리스트면
                        result['ImgAreaPath'].append(image_template())
                if i['_source']['DataType']=='comp_detail_info':
                    for num in range(0, 4):
                        for i in result['Category'][num]['CategoryList']:

                            if not i['ImgPath']:  # 빈리스트면
                                i['ImgPath'].append(image_template())

                            if not i['PriceImg']:  # 빈리스트면
                                i['PriceImg'].append(image_template())

                            if not i['TitleImgPath']:  # 빈리스트면
                                i['TitleImgPath'].append(image_template())

            flag = "success"
        else:
            flag = "noData"

        # 엘라스틱서치 연결 종료
        es.close()
        
        result = change_none_to_str(result)

    except Exception as e:
        # print(e)
        pass

    return str({"flag": flag,"KDisplayYN":KDisplayYN, "data": result}),


# # field 바꾸는데만 쓰이는 함수, 자바에 안쓰임 # svn에 올리지 말기
# def change_field(index):

#     given_mapping={"A":["3712044","3712036","3712030","3711988","3712007",'3712003','3711995','3712000','3711951','3711950','3711999','28','9','3711936'],
#                    "B":['1','5','3711963'],"C":[],"D":['3711996','3711973'],"E":[],"F":[],"G":['3711980','3711970'],
#                    "I":[],"J":['3711974']}
    
#     flag, data = "fail", dict()
#     try:
#         es = get_es_conn()

#         query = {
#             "track_total_hits":True,
#             "size":10000,
#             "query": {"bool":{"must": [
#                 {"match": {"DataType":"comp_info"}}
#             ]}}}
    
#         result = get_data_from_es(index=index,query=query)
#         map_data={i['_source']['EmpNo']:i['_source']['Data']['Field'] for i in result}
#         # print(map_data)

#         for k,v in map_data.items():

#             #받은 매핑에 있으면 매핑에 있는대로 넣어주기
#             chosen=[k2 for k2,v2 in given_mapping.items() if k in v2]
#             if chosen:
#                 map_data[k]=chosen[0]
                
#             else: #정한 기준대로 넣기 #A->A, B->D, E->G, H->H
#                 if v=='A':
#                     map_data[k]='A'
#                 if v=='B':
#                     map_data[k]='D'
#                 if v=='E':
#                     map_data[k]='G'
#                 if v=='H':
#                     map_data[k]='H'
            
#             res1=es.update(index=index, id=k+'_comp_info', body={"doc":{"Data":{"Field":map_data[k]}}})
#             data[k]=map_data[k]
            
#             if res1["result"] == "updated":
#                 pass
#             elif res1["result"] == "noop":
#                 pass
#             else:
#                 return k+v+'error'
        
#         flag="success"


#     except Exception as ex:
#         # print(ex)
#         pass
    
#     return str({"flag": flag, "data": data}),

# # diplayYN field 추가
# def add_dispalyYN(index):

#     flag, data = "fail", dict()
#     try:
#         es = get_es_conn()

#         query = {
#             "track_total_hits":True,
#             "size":10000,
#             "query": {"bool":{"must": [
#                 {"match": {"DataType":"comp_info"}}
#             ]}}}
    
#         result = get_data_from_es(index=index,query=query)

#         map_data={i['_source']['EmpNo']:i['_source']['Data']['ActiveYN'] for i in result}
#         print(map_data)

#         for k,v in map_data.items():

#             comp_info_body = {
#                 "doc": {
#                     "LDisplayYN":'0',
#                     "KDisplayYN":str(v)
#                 }
#             }
#             try:

#                 res1 = es.get(index=index, id=k+"_comp_info")
                        
#                 if res1["found"]:
                    
#                     update_res1=es.update(index=index, id=k+'_comp_info', body=comp_info_body)
                    
#                     if update_res1["result"] == "updated":
#                         pass
#                     elif update_res1["result"] == "noop":
#                         pass
#                     else:
#                         return k+v+'error'
#             except exceptions.NotFoundError:
#                 return k+"comp_info가 없음"
            
                
#             try:
#                 res2 = es.get(index=index, id=k+"_comp_detail_info")
                        
#                 if res2["found"]:
                    
#                     update_res2=es.update(index=index, id=k+'_comp_detail_info', body=comp_info_body)
                    
#                     if update_res2["result"] == "updated":
#                         pass
#                     elif update_res2["result"] == "noop":
#                         pass
#                     else:
#                         return k+v+'error'
                    
#             except exceptions.NotFoundError:
#                 print(k+"는 detail이 없음")
#                 pass
                
            
                
            

        



        # for k,v in map_data.items():

        #     #받은 매핑에 있으면 매핑에 있는대로 넣어주기
        #     chosen=[k2 for k2,v2 in given_mapping.items() if k in v2]
        #     if chosen:
        #         map_data[k]=chosen[0]
                
        #     else: #정한 기준대로 넣기 #A->A, B->D, E->G, H->H
        #         if v=='A':
        #             map_data[k]='A'
        #         if v=='B':
        #             map_data[k]='D'
        #         if v=='E':
        #             map_data[k]='G'
        #         if v=='H':
        #             map_data[k]='H'
            
        #     res1=es.update(index=index, id=k+'_comp_info', body={"doc":{"Data":{"Field":map_data[k]}}})
        #     data[k]=map_data[k]
            
        #     if res1["result"] == "updated":
        #         pass
        #     elif res1["result"] == "noop":
        #         pass
        #     else:
        #         return k+v+'error'
        
        flag="success"


    except Exception as ex:
        # print(ex)
        pass
    
    return str({"flag": flag, "data": data}),



if __name__ =="__main__":
    # print(search_comp_info('1126', 'L'))
    ddata='''
        {'CompName': '건축 사무소 봄', 'Field': '', 'LogoPath': [{'AtchMtrID': 2061, 'RefPath': 'E:\\\\KWSSDP\\\\upload\\\\MPage1050\\\\1126\\\\1700185409023.jpg', 'RefName': '12123.jpg'}], 'BannerPath': [{'AtchMtrID': '', 'RefPath': '', 'RefName': ''}], 'Address': '충북 제천시 숭의로68 2층', 'Email': 'dmstjs1665@sycns.co.kr', 'Tel': '010-1234-1234', 'HomePageUrl': 'www.test112323.co.kr', 'Introduce': 'test111704_구조인_정회원관리 자_연동x', 'FoundingDate': '2006-11-08', 'CompType': ['대기업'], 'SearchKeyword': ['test102505', 'test110101_low'], 'CertImagePath': [{'AtchMtrID': '', 'RefPath': '', 'RefName': ''}], 'ImgAreaType': '2A', 'ImgAreaPath': [{'AtchMtrID': 2062, 'RefPath': 'E:\\\\KWSSDP\\\\upload\\\\MPage1050\\\\1126\\\\1700185476180.jpg', 'RefName': '12123.jpg'}, {'AtchMtrID': 2063, 'RefPath': 'E:\\\\KWSSDP\\\\upload\\\\MPage1050\\\\1126\\\\1700185479086.jpg', 'RefName': '12123.jpg'}], 
        'ActiveYN': '1', 
        'Category': [{'CategoryList': [], 'CategoryName': '웹프로그램', 'CategoryType': '1'}, 
                    {'CategoryList': [{'ImgPath': [{'AtchMtrID': 2078, 'RefPath': 'E:\\\\KWSSDP\\\\upload\\\\MPage1051\\\\3712110\\\\1701741204631.JPG', 'RefName': 'DSCN2852.JPG'}], 'PriceTxt': '', 'PriceInt': 1131000, 'Content': 'sdfsf', 'ArchiveID': '', 'Title': 'fdsdfsf', 'PriceImg': [{'AtchMtrID': '', 'RefPath': '', 'RefName': ''}], 'Standard': '', 'ProgramID': '', 'Unit': 'ㅇㄹㅇㄹ', 'TitleImgPath': [{'AtchMtrID': 2079, 'RefPath': 'E:\\\\KWSSDP\\\\upload\\\\MPage1050\\\\1126\\\\1701741213128.JPG', 'RefName': 'DSCN2852.JPG'}], 'PriceType': 'p1'}], 'CategoryName': 'test120501111', 'CategoryType': '2'},  
                    {'CategoryList': [{'ImgPath': [{'AtchMtrID': '', 'RefPath': '', 'RefName': ''}], 'PriceTxt': '', 'PriceInt': 1212, 'Content': 'sfds', 'ArchiveID': '', 'Title': 'dfs', 'PriceImg': [{'AtchMtrID': '', 'RefPath': '', 'RefName': ''}], 'Standard': '', 'ProgramID': '', 'Unit': '323', 'TitleImgPath': [{'AtchMtrID': '', 'RefPath': '', 'RefName': ''}], 'PriceType': 'p1'}], 'CategoryName': 'test1205012222', 'CategoryType': '3'}, 
                    {'CategoryList': [{'ImgPath': [{'AtchMtrID': '', 'RefPath': '', 'RefName': ''}], 'PriceTxt': '', 'PriceInt': 56000, 'Content': 'test120501', 'ArchiveID': '', 'Title': 'test120501', 'PriceImg': [{'AtchMtrID': '', 'RefPath': '', 'RefName': ''}], 'Standard': '규격 자유', 'ProgramID': '', 'Unit': '', 'TitleImgPath': [{'AtchMtrID': 2080, 'RefPath': 'E:\\\\KWSSDP\\\\upload\\\\MPage1050\\\\1126\\\\1701741226057.JPG', 'RefName': 'DSCN2852.JPG'}], 'PriceType': 'p1'}], 
                    'CategoryName': 'test120501333', 'CategoryType': '4'}]}}'''
    # print(save_comp_info('1126','0',ddata))
    # print(save_comp_info('1126', '0', '{"CompName":"건축사무소 봄","Field":"A","LogoPath":[{"AtchMtrID":2061,"RefName":"12123.jpg","RefPath":"E:\\KWSSDP\\upload\\MPage1050\\1126\\1700185409023.jpg"}],"BannerPath":[{"AtchMtrID":"","RefName":"","RefPath":""}],"Address":"충북 제천시 숭의로68 2층","Email":"dmstjs1665@sycns.co.kr","Tel":"010-1234-1234","HomePageUrl":"www.test112323.co.kr","Introduce":"test111704_구조인_정회원관리자_연동x","FoundingDate":"2006-11-08","CompType":["대기업"],"SearchKeyword":["test102505","test110101_low"],"CertImagePath":[{"AtchMtrID":"","RefName":"","RefPath":""}],"ImgAreaType":"2A","ImgAreaPath":[{"AtchMtrID":2062,"RefName":"12123.jpg","RefPath":"E:\\KWSSDP\\upload\\MPage1050\\1126\\1700185476180.jpg"},{"AtchMtrID":2063,"RefName":"12123.jpg","RefPath":"E:\\KWSSDP\\upload\\MPage1050\\1126\\1700185479086.jpg"}],"ActiveYN":"1","Category":[{"CategoryType":1,"CategoryName":"웹프로그램","CategoryList":[{"Title":"","Content":"","ArchiveID":"","ImgPath":[{"AtchMtrID":"","RefName":"","RefPath":""}],"PriceType":"","PriceInt":"","PriceImg":[{"AtchMtrID":"","RefName":"","RefPath":""}],"PriceTxt":"","ProgramID":"","Unit":"","Standard":"","TitleImgPath":[{"AtchMtrID":"","RefName":"","RefPath":""}]}]},{"CategoryType":2,"CategoryName":"test120501111","CategoryList":[{"Title":"fdsdfsf","Content":"sdfsf","ArchiveID":"","ImgPath":[{"AtchMtrID":2078,"RefName":"DSCN2852.JPG","RefPath":"E:\\KWSSDP\\upload\\MPage1051\\3712110\\1701741204631.JPG"}],"PriceType":"p1","PriceInt":1131000,"PriceImg":[{"AtchMtrID":"","RefName":"","RefPath":""}],"PriceTxt":"","ProgramID":"","Unit":"ㅇㄹㅇㄹ","Standard":"","TitleImgPath":[{"AtchMtrID":2079,"RefName":"DSCN2852.JPG","RefPath":"E:\\KWSSDP\\upload\\MPage1050\\1126\\1701741213128.JPG"}]}]},{"CategoryType":3,"CategoryName":"test1205012222","CategoryList":[{"Title":"dfs","Content":"sfds","ArchiveID":"","ImgPath":[{"AtchMtrID":"","RefName":"","RefPath":""}],"PriceType":"p1","PriceInt":1212,"PriceImg":[{"AtchMtrID":"","RefName":"","RefPath":""}],"PriceTxt":"","ProgramID":"","Unit":"323","Standard":"","TitleImgPath":[{"AtchMtrID":"","RefName":"","RefPath":""}]}]},{"CategoryType":4,"CategoryName":"test120501333","CategoryList":[{"Title":"test120501","Content":"test120501","ArchiveID":"","ImgPath":[{"AtchMtrID":"","RefName":"","RefPath":""}],"PriceType":"p1","PriceInt":56000,"PriceImg":[{"AtchMtrID":"","RefName":"","RefPath":""}],"PriceTxt":"","ProgramID":"","Unit":"","Standard":"규격 자유","TitleImgPath":[{"AtchMtrID":2080,"RefName":"DSCN2852.JPG","RefPath":"E:\\KWSSDP\\upload\\MPage1050\\1126\\1701741226057.JPG"}]}]}]}'))

    # print(change_field("market_data_backup"))
    print(add_dispalyYN("market_data_backup"))
