# -*- coding=utf-8 -*-

# 반환값에 배열(list)이 있는 경우 return 뒤에 쉼표(,) 붙여야함

############################### Library Import #######################################################
import json
import copy
import datetime
from dateutil.relativedelta import relativedelta
import itertools

# pip install elasticsearch==7.13.3
from elasticsearch import Elasticsearch, helpers, exceptions

# pip install mysql-connector-python==8.0.27
import mysql.connector

import json
import unicodedata
import re
import ast
import unicodedata
import time
import pprint
import math

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
    mysql_database = "kwssdp"
    mysql_connect_timeout = 3600

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
    mysql_database = "kwssdp"
    mysql_connect_timeout = 3600

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

Field_mode={"allA":["A","B","C"],"allD":["D","E","F"],"allG":["G","I","J"]} #H는 기타
index_mode ={_market_data:"K",_lowca_market_data:"L"}

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


############################### 형변환 #######################################################
# 데이터형 모두 str 변환 (None -> "")
def change_none_to_str(Data):
    data = copy.deepcopy(str(Data))
    try:
        while "None" in data:
            data = data.replace("None", """\"\"""")
        data = ast.literal_eval(data)
        # data = unicodedata.normalize("NFKD", json.dumps(data, ensure_ascii=False))  # 유니코드 normalize
        data = unicodedata.normalize("NFC", json.dumps(
            data, ensure_ascii=False))  # 한글 자음모음 합치기
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
        data = json.dumps(data, ensure_ascii=False)
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


############################### 함수 #######################################################
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
                {"match": {"KDisplayYN":"1"}}
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
        # 2023-03-30 회의 이전
        # 기업명만 목록으로 카테고리별 20개 출력
        ##############################################
        # try:
        #     for field in ["A", "B", "E"]:
        #         query_all = copy.deepcopy(query)
        #         query_all["query"]["bool"]["must"].append({"match": {"Data.Field": field}})
        #         # print(query_all)
        #
        #         query_all["query"]["bool"]["must"].append({"exists": {"field": "Data.ImgAreaPath.AtchMtrID"}})
        #
        #         tmp_all = es.search(
        #             index=_market_data,
        #             body=query_all,
        #             track_total_hits=True
        #         )
        #         # print(tmp_all)
        #
        #         # 구조변경하여 return
        #         if tmp_all:
        #             dataAll[field] = [
        #                 {"EmpNo": str(item["_source"]["EmpNo"]), "CompName": item["_source"]["Data"]["CompName"]}
        #                 for item in tmp_all["hits"]["hits"]
        #             ]
        #     flag = "success"
        # except Exception as e:
        #     # print(e)
        #     pass

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
            query_all["query"]["bool"]["must"].append(
                {"exists": {"field": "Data.ImgAreaPath.AtchMtrID"}})

            # 페이지번호에 해당하는 데이터부터 검색
            from_ = size*(pageNo-1)
            query_all.update({"from": from_})

            ###########################################################
            # doc ID 리스트 조회
            # idList = get_id_list(index=_market_data, query=copy.deepcopy(query_all))
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
                        dataList.append(change_none_to_str(_source))

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
            # idList = get_id_list(index=_market_data, query=copy.deepcopy(query_all))
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
                        dataList.append(change_none_to_str(_source))
                flag = "success"
            except Exception as e:
                # print(e)
                pass
        except Exception as e:
            # print(e)
            pass

    es.close()

    data = {
        "cond": cond,
        "idList": total_page_num,
        "dataAll": dataAll,
        "dataList": dataList
    }
    # print(len(dataList))
    # print([i["Data"]["CompName"] for i in dataList])
    # print(total_page_num)
    # print(type(data), data)

    # data = unicodedata.normalize("NFKD", json.dumps(data, ensure_ascii=False))  # 유니코드 normalize
    data = unicodedata.normalize("NFC", json.dumps(
        data, ensure_ascii=False))  # 한글 자음모음 합치기
    data = json.loads(data, strict=False)
    return str({"flag": flag, "data": data}),


######### 자바 호출 : 공유 프로그램 목록 조회 ###########
def search_program_list(empNo=None, docID=None, mode=None, value=None, size=None):
    flag = "fail"
    total_num, idList, dataList, data = 0, list(), list(), dict()
    total_page_num = 0

    # 한 화면에 10개씩 데이터 출력
    try:
        size = int(size)
    except:
        size = 10

    # 기본 쿼리
    query = {
        "track_total_hits": True,
        "size": size,
        "sort": [
            {"_score": {"order": "desc"}},
            {"UpdateDate": {"order": "desc"}},
            {"_id": {"order": "asc"}}
        ],
        "query": {"bool": {
            "must": [
                {"match": {"EmpNo": empNo}},
                {"match": {"DataType": "program_info"}}
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

    # 검색조건 있다면
    # 검색조건: "A": 제목, "B": 태그
    if value:
        q, should = value.split(" "), list()

        # nori analyzer로 분석
        q += analyze_str(index=_program, text=value)
        q = list(set(q))
        value = "*" + "* *".join(q) + "*"
        # print(value)

        # 기업명 검색
        if mode == "A":
            should.append({
                "query_string": {
                    "default_field": "Data.ProgramTitle",
                    "query": value,
                    "default_operator": "OR"
                }
            })

        # 태그 검색
        elif mode == "B":
            should.append({
                "query_string": {
                    "default_field": "Data.Tag",
                    "query": value,
                    "default_operator": "OR"
                }
            })

        # 전체 검색
        else:
            should.append({
                "query_string": {
                    "default_field": "Data.ProgramTitle",
                    "query": value,
                    "default_operator": "OR"
                }
            })
            should.append({
                "query_string": {
                    "default_field": "Data.Tag",
                    "query": value,
                    "default_operator": "OR"
                }
            })
            should.append({
                "query_string": {
                    "default_field": "Data.ProgramTxt",
                    "query": value,
                    "default_operator": "OR"
                }
            })

        if should:
            query["query"]["bool"].update({"should": should})
            query["query"]["bool"].update({"minimum_should_match": 1})
        # print(query)

    # ACTIVE=1(활성)만 호출 (관리자 프로그램 A&S마켓 등록 팝업)
    if mode == "ALL":
        query["query"]["bool"]["must"].append({"match": {"Data.Active": "1"}})
    # ACTIVE 0(삭제) 제외 모두 조회
    else:
        query["query"]["bool"].update(
            {"must_not": [{"match": {"Data.Active": "0"}}]})

    ###########################################################
    # doc ID 리스트 조회
    # idList = get_id_list(index=_program, query=copy.deepcopy(query))
    # data.update({"idList": idList})
    # print(len(idList))
    ###########################################################

    # docID 있는 경우 (페이징)
    # if docID:
    #     docID = [n for n in ast.literal_eval(str(docID))]
    #     if docID:
    #         query.update(
    #             {"search_after": [float(docID[0]), int(docID[1]), str(docID[2])]}
    #         )
    # print(query)

    # 데이터 조회
    es = get_es_conn()
    try:
        res = es.search(
            index=_program,
            body=query,
            track_total_hits=True
        )
        # print(res)

        # 구조변경하여 return
        if res:
            # 페이지 개수 계산
            total_num = res["hits"]["total"]["value"]
            total_page_num = int(math.ceil(total_num / size))
            # 반환할 데이터
            dataList = [change_none_to_str(item["_source"])
                        for item in res["hits"]["hits"]]
        flag = "success"
    except Exception as e:
        # print(e)
        pass
    es.close()

    data.update({"idList": total_page_num})
    data.update({"dataList": dataList})

    # data = unicodedata.normalize("NFKD", json.dumps(data, ensure_ascii=False))  # 유니코드 normalize
    data = unicodedata.normalize("NFC", json.dumps(
        data, ensure_ascii=False))  # 한글 자음모음 합치기
    data = json.loads(data, strict=False)
    return str({"flag": flag, "data": data}),


######### 자바 호출 : 공유 프로그램 데이터 조회 ###########
def search_program_info(programID=None, type=None):
    flag = "fail"
    data = {"comp_info": dict(), "program_info": dict(
    ), "program_input": dict(), "program_output": dict()}

    # 데이터호출
    type = str(type)
    if type == "1":
        dataTypeList = ["program_info"]
    elif type == "2":
        dataTypeList = ["program_input", "program_output"]
    else:
        dataTypeList = ["program_info", "program_input", "program_output"]

    es = get_es_conn()
    try:
        empNo = programID.split("_")[0]
        try:
            for DataType in dataTypeList:
                try:
                    _id = str(empNo) + "_" + programID + "_" + DataType
                    res = es.get(index=_program, id=_id)

                    _source = change_none_to_str(res["_source"])
                    _source = change_str_to_json(_source, add_escape=False)
                    data[DataType] = _source

                except exceptions.NotFoundError:
                    pass
            DataType = "comp_info"
            # 기업정보 전달
            _id = str(empNo) + "_" + DataType
            res = es.get(index=_market_data, id=_id)

            _source = change_none_to_str(res["_source"])
            _source = change_str_to_json(_source, add_escape=False)
            data[DataType] = _source

        except exceptions.NotFoundError:
            pass
        flag = "success"

    except Exception as e:
        # print(e)
        pass
    es.close()

    # data = unicodedata.normalize("NFKD", json.dumps(data, ensure_ascii=False))  # 유니코드 normalize
    data = unicodedata.normalize("NFC", json.dumps(
        data, ensure_ascii=False))  # 한글 자음모음 합치기
    data = json.loads(data, strict=False)

    return str({"flag": flag, "data": data}),


######### 자바 호출 : 공유 프로그램 기본정보 저장(업데이트) ###########
def save_program_info(programID=None, data=None):
    flag, DataType = "fail", "program_info"
    ProgramID = copy.deepcopy(programID)
    if programID and data:
        try:
            now = datetime.datetime.now()
            n = now.strftime("%Y%d%m%H%M%S%f")

            # 프로그램ID 존재 -> 업데이트
            if "_" in programID:
                EmpNo = ProgramID.split("_")[0]
                id_doc = EmpNo + "_" + ProgramID + "_" + DataType
            # 프로그램ID 미존재 -> 신규저장
            else:
                EmpNo = copy.deepcopy(programID)
                ProgramID = EmpNo + "_" + str(n)
                id_doc = EmpNo + "_" + ProgramID + "_" + DataType

            # "" -> None, 따옴표제거
            data = change_str_to_json(data, add_escape=True)

            data["ProgramTitle"] = re.sub("\"", "", re.sub(
                "\'", "", str(copy.deepcopy(data["ProgramTitle"]))))
            data["ProgramTxt"] = re.sub("\"", "", re.sub(
                "\'", "", str(copy.deepcopy(data["ProgramTxt"]))))
            data["Active"] = re.sub("\"", "", re.sub(
                "\'", "", str(copy.deepcopy(data["Active"]))))

            try:
                Price = str(copy.deepcopy(data["Price"])).replace(",", "")
                Price = int(Price)
                if Price < 0:
                    Price = 0
            except:
                Price = 0
            data["Price"] = Price

            try:
                Tag = [
                    re.sub("\"", "", re.sub("\'", "", str(i)))
                    for i in copy.deepcopy(data["Tag"])
                ]
            except:
                Tag = list()
            data["Tag"] = Tag

            ProgramImg = copy.deepcopy(data["ProgramImg"])
            if ProgramImg:
                tmp_list = list()
                for d in ProgramImg:
                    tmp_dict = dict()
                    for k, v in d.items():
                        t = re.sub("\"", "", re.sub("\'", "", str(v)))
                        tmp_dict.update({k: t})
                    tmp_list.append(tmp_dict)
                data["ProgramImg"] = tmp_list

            ProgramFile = copy.deepcopy(data["ProgramFile"])
            if ProgramFile:
                tmp_list = list()
                for d in ProgramFile:
                    tmp_dict = dict()
                    for k, v in d.items():
                        t = re.sub("\"", "", re.sub("\'", "", str(v)))
                        tmp_dict.update({k: t})
                    tmp_list.append(tmp_dict)
                data["ProgramFile"] = tmp_list

            ManualFile = copy.deepcopy(data["ManualFile"])
            if ManualFile:
                tmp_list = list()
                for d in ManualFile:
                    tmp_dict = dict()
                    for k, v in d.items():
                        t = re.sub("\"", "", re.sub("\'", "", str(v)))
                        tmp_dict.update({k: t})
                    tmp_list.append(tmp_dict)
                data["ManualFile"] = tmp_list

            es = get_es_conn()

            # 최초저장인지 확인
            try:
                res = es.get(index=_program, id=id_doc)
                # 업데이트인 경우 >>> 수정일자, data만 수정
                if res["found"]:
                    _source = res["_source"]
                    body = {
                        "doc": {
                            "UpdateDate": str(now).replace("T", ""),
                            "Data": data
                        }
                    }
                    # print(body)
                    result = es.update(index=_program, id=id_doc, body=body)
                    if result["result"] == "updated":
                        flag = "success"

            # 최초 저장인 경우 >>> Active(활성여부)=2, ViewCnt(조회수)=0
            except exceptions.NotFoundError:
                body = {
                    "EmpNo": EmpNo,
                    "DataType": DataType,
                    "CreateDate": str(now).replace("T", ""),
                    "UpdateDate": str(now).replace("T", ""),
                    "Data": {
                        "ProgramID": ProgramID,
                        "ViewCnt": 0
                    }
                }
                # print(body)
                body["Data"].update(data)
                result = es.index(index=_program, body=body, id=id_doc)
                if result["result"] == "created":
                    flag = "success"
            es.close()
        except Exception as e:
            # print(e)
            pass
    return str({"flag": flag, "programID": ProgramID}),


######### 자바 호출 : 공유 프로그램 입출력정보 저장(업데이트) ###########
def save_program_param(programID=None, active=None, input=None, output=None):
    flag = "fail"
    if programID:
        es = get_es_conn()
        # input, output 덮어쓰기 + program_info의 Active(활성여부)
        try:
            ProgramID = copy.deepcopy(programID)
            EmpNo = ProgramID.split("_")[0]

            # 입력 파라미터 저장(업데이트)
            input_ = list()
            input = [n for n in ast.literal_eval(input)]
            if input:
                for target in input:
                    tmp = dict()
                    for k, v in target.items():
                        t = re.sub("\"", "", re.sub(
                            "\'", "", str(copy.deepcopy(v))))
                        tmp.update({k: t})
                    input_.append(tmp)

            DataType, Data = "program_input", input_
            id_doc = EmpNo + "_" + ProgramID + "_" + DataType
            body = {
                "EmpNo": EmpNo,
                "ProgramID": ProgramID,
                "DataType": DataType,
                "Data": Data
            }
            result = es.index(index=_program, id=id_doc, body=body)
            if result["result"] in ["created", "updated"]:
                flag = "success"

            # 출력 파라미터 저장(업데이트)
            output_ = list()
            output = [n for n in ast.literal_eval(output)]
            if output:
                for target in output:
                    tmp = dict()
                    for k, v in target.items():
                        t = re.sub("\"", "", re.sub(
                            "\'", "", str(copy.deepcopy(v))))
                        tmp.update({k: t})
                    output_.append(tmp)

            DataType, Data = "program_output", output_
            id_doc = EmpNo + "_" + ProgramID + "_" + DataType
            body = {
                "EmpNo": EmpNo,
                "ProgramID": ProgramID,
                "DataType": DataType,
                "Data": Data
            }
            result = es.index(index=_program, id=id_doc, body=body)
            if result["result"] in ["created", "updated"]:
                flag = "success"

            # program_info의 Active(활성여부) 업데이트
            try:
                DataType = "program_info"
                id_doc = EmpNo + "_" + ProgramID + "_" + DataType
                res = es.get(index=_program, id=id_doc)
                # 업데이트인 경우 >>> 수정일자, data만 수정
                if res["found"]:
                    now = str(datetime.datetime.now()).replace("T", "")
                    body = {
                        "doc": {
                            "UpdateDate": now,
                            "Data": {"Active": str(active)}
                        }
                    }
                    # print(body)
                    result = es.update(index=_program, id=id_doc, body=body)
                    if result["result"] == "updated":
                        flag = "success"
            except Exception as e:
                # print(e)
                pass
        except Exception as e:
            # print(e)
            flag = "fail"
        es.close()
    return str({"flag": flag, "programID": programID}),


######### 자바 호출 : 공유 프로그램 활성여부 업데이트 ###########
def update_program_active(programID=None, active=None):
    flag, DataType = "fail", "program_info"
    if programID:
        es = get_es_conn()
        try:
            ProgramID = copy.deepcopy(programID)
            EmpNo = ProgramID.split("_")[0]

            # program_info의 Active(활성여부) 업데이트
            id_doc = EmpNo + "_" + ProgramID + "_" + DataType
            res = es.get(index=_program, id=id_doc)

            # 업데이트인 경우 >>> 수정일자, data만 수정
            if res["found"]:
                now = str(datetime.datetime.now()).replace("T", "")
                body = {
                    "doc": {
                        "UpdateDate": now,
                        "Data": {"Active": str(active)}
                    }
                }
                # print(body)
                result = es.update(index=_program, id=id_doc, body=body)
                if result["result"] == "updated":
                    flag = "success"
        except Exception as e:
            # print(e)
            flag = "fail"
        es.close()
    return str({"flag": flag, "programID": programID}),


######### 자바 호출 : 공유 프로그램 삭제 ###########
def delete_program(programID=None):
    flag = "fail"
    if programID:
        ProgramID = copy.deepcopy(programID)
        EmpNo = ProgramID.split("_")[0]
        es = get_es_conn()
        try:
            for DataType in ["program_info", "program_input", "program_output"]:
                id_doc = EmpNo + "_" + ProgramID + "_" + DataType
                result = es.delete(index=_program, id=id_doc)
                if result["result"] in ["deleted"]:
                    flag = "success"
        except exceptions.NotFoundError:
            pass
        except Exception as e:
            # print(e)
            flag = "fail"
            pass
        es.close()
    return str({"flag": flag}),


######### 자바 호출 : 기술공유마켓 화면에서 공유기술찾기 조회 ###########
def search_program(docID=None, value="", size=None):
    flag = "fail"
    total_num, idList, dataList, data = 0, list(), list(), dict()
    total_page_num = 0

    # 한 화면에 10개씩 데이터 출력
    try:
        size = int(size)
    except:
        size = 10

    # 기본 쿼리
    query = {
        "size": size,
        "sort": [
            {"_score": {"order": "desc"}},
            {"UpdateDate": {"order": "desc"}},
            {"_id": {"order": "asc"}}
        ],
        "query": {"bool": {"must": [
            {"match": {"DataType": "program_info"}},
            {"match": {"Data.Active": "1"}}
        ]}}
    }

    # 몇 번째 페이지 호출인지 확인
    try:
        pageNo = int(docID)
    except:
        pageNo = 1
    from_ = size * (pageNo - 1)
    query.update({"from": from_})

    # 검색조건 있다면
    if value:
        q, should = value.split(" "), list()

        # nori analyzer로 분석
        q += analyze_str(index=_program, text=value)
        q = list(set(q))
        value = "*" + "* *".join(q) + "*"
        # print(q)
        should.append({
            "query_string": {
                "default_field": "Data.ProgramTitle",
                "query": value,
                "default_operator": "OR"
            }
        })
        should.append({
            "query_string": {
                "default_field": "Data.Tag",
                "query": value,
                "default_operator": "OR"
            }
        })
        should.append({
            "query_string": {
                "default_field": "Data.ProgramTxt",
                "query": value,
                "default_operator": "OR"
            }
        })

        if should:
            query["query"]["bool"].update({"should": should})
            query["query"]["bool"].update({"minimum_should_match": 1})
        # print(query)

    ###########################################################
    # doc ID 리스트 조회
    # idList = get_id_list(index=_program, query=copy.deepcopy(query))
    # data.update({"idList": idList})
    # print(len(idList))
    ###########################################################
    # docID 있는 경우 (페이징)
    # if docID:
    #     docID = [n for n in ast.literal_eval(str(docID))]
    #     if docID:
    #         query.update(
    #             {"search_after": [float(docID[0]), int(docID[1]), str(docID[2])]}
    #         )
    # print(query)

    # 데이터 조회
    es = get_es_conn()
    try:
        res = es.search(
            index=_program,
            body=query,
            track_total_hits=True
        )
        # print(res)

        # 구조변경하여 return
        if res:
            # 페이지 개수 계산
            total_num = res["hits"]["total"]["value"]
            total_page_num = int(math.ceil(total_num / size))

            for item in res["hits"]["hits"]:
                _source = item["_source"]
                EmpNo = copy.deepcopy(_source["EmpNo"])
                try:
                    # 기업정보 전달
                    DataType = "comp_info"
                    _id = str(EmpNo) + "_" + DataType
                    comp_res = es.get(index=_market_data, id=_id)

                    comp_info = comp_res["_source"]["Data"]
                    # 불필요한 파일 path 데이터 제거
                    del comp_info["LogoPath"]
                    del comp_info["BannerPath"]
                    del comp_info["CertImagePath"]
                    del comp_info["ImgAreaType"]
                    del comp_info["ImgAreaPath"]

                    _source["Data"].update(comp_info)
                except exceptions.NotFoundError:
                    pass

                _source = change_none_to_str(_source)
                dataList.append(_source)

        flag = "success"
    except Exception as e:
        # print(e)
        pass
    es.close()

    data.update({"idList": total_page_num})
    data.update({"dataList": dataList})

    # data = unicodedata.normalize("NFKD", json.dumps(data, ensure_ascii=False))  # 유니코드 normalize
    data = unicodedata.normalize("NFC", json.dumps(
        data, ensure_ascii=False))  # 한글 자음모음 합치기
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
            query["query"]["bool"]["must"].append(
                {"match": {"QuestionerEmpNo": empNo}})
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
                            comp_info = es.get(index=_market_data, id=_id)
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
    data = unicodedata.normalize("NFC", json.dumps(
        data, ensure_ascii=False))  # 한글 자음모음 합치기
    data = json.loads(data, strict=False)

    return str({"flag": flag, "data": data}),


######### 자바 호출 : 관리자 문의내역 상세 조회 ###########
def search_ques_info(questionerQuestionID=None):
    flag, data = "fail", dict()
    DataType = "comp_question"
    # 데이터호출
    es = get_es_conn()
    try:
        _id = str(questionerQuestionID) + "_" + DataType
        res = es.get(index=_question_data, id=_id)

        data = change_none_to_str(res["_source"])
        data = change_str_to_json(data, add_escape=False)

        flag = "success"
    except Exception as e:
        # print(e)
        pass
    es.close()

    # data = unicodedata.normalize("NFKD", json.dumps(data, ensure_ascii=False))  # 유니코드 normalize
    data = unicodedata.normalize("NFC", json.dumps(
        data, ensure_ascii=False))  # 한글 자음모음 합치기
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
            data["QuestionTitle"] = re.sub("\"", "", re.sub(
                "\'", "", str(copy.deepcopy(data["QuestionTitle"]))))
            data["QuestionContext"] = re.sub("\"", "", re.sub(
                "\'", "", str(copy.deepcopy(data["QuestionContext"]))))
            data["QuestionerEmpName"] = re.sub("\"", "",
                                               re.sub("\'", "", str(copy.deepcopy(data["QuestionerEmpName"]))))
            data["QuestionerHP"] = re.sub("\"", "", re.sub(
                "\'", "", str(copy.deepcopy(data["QuestionerHP"]))))
            data["QuestionerEmail"] = re.sub("\"", "", re.sub(
                "\'", "", str(copy.deepcopy(data["QuestionerEmail"]))))
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


# 이태윤
############################### 조회수 업데이트 ############################
def view_count_increase(programId=None):
    flag, error_code = "fail", ""

    try:
        if programId:
            es = get_es_conn()
            q = {
                "script": {"source": "ctx._source.Data.ViewCnt++", "lang": "painless"},
                "query": {
                    "bool": {
                        "must": [{"match": {"Data.ProgramID": str(programId)}}]
                    }
                },
            }
            result = es.update_by_query(index=_program, body=q)
            if result['updated'] >= 1:
                flag = "success"
            error_code = flag
    except Exception as e:
        error_code = str(e)

    # error_code = unicodedata.normalize("NFKD", json.dumps(error_code, ensure_ascii=False))  # 유니코드 normalize
    error_code = unicodedata.normalize("NFC", json.dumps(
        error_code, ensure_ascii=False))  # 한글 자음모음 합치기
    error_code = json.loads(error_code, strict=False)
    return str({"flag": flag, "data": error_code}),


################################# 유저정보update ###################################
def update_user_info(empNo=None, field=None, activeYn=None, name=None, compName=None, tel=None, email=None):
    flag, error_code = "fail", ""
    if empNo:
        # # 빈 값이면 0으로 저장
        # if activeYn:
        #     activeYn = "1"
        # else:
        #     activeYn = "0"

        # 기타 분야면 A&S마켓 운영불가 >>> 0 으로 저장
        if not field:
            field = "H"
        if field not in list(itertools.chain(*Field_mode.values())):
            field, activeYn = "H", '0'

        if not activeYn:
            activeYn = '0'
        
        _id = str(empNo) + "_comp_info"
        now = str(datetime.datetime.now()).replace("T", "")

        es = get_es_conn()
        try:
            res = es.get(index=_market_data, id=_id)
            # 회원정보수정
            if res["found"]:
                # _source = res["_source"]
                comp_info_body = {
                    "doc": {
                        "UpdateDate": now,
                        "KDisplayYN":str(activeYn),
                        "Data": {
                            # "CompName": compName,
                            # "Email": email,
                            # "Tel": tel,
                            "Field": field,
                            "ActiveYN": str(activeYn),
                        }
                    }
                }
                # result = es.update(index=_program, id=id_doc, body=body)
                result = es.update(index=_market_data, id=_id, body=comp_info_body)
                # if result["result"] == "updated":
                #     flag = "success"
            try:
                res2 = es.get(index=_market_data, id=str(empNo) + "_comp_detail_info")

                if res2["found"]:
                    comp_detail_info_body = {
                        "doc": {
                            "UpdateDate": now,
                            "KDisplayYN":str(activeYn)
                        }}                   
                    result2 = es.update(index=_market_data, id=str(empNo) + "_comp_detail_info", body=comp_detail_info_body)
                    if result["result"] == "updated" and result2["result"] == "updated":
                        flag = "success"

            except exceptions.NotFoundError:
                if result["result"] == "updated":
                    flag = "success"
                pass
        
        # 회원가입
        except exceptions.NotFoundError:
            if not compName:
                compName = copy.deepcopy(name)
            path = [{
                "AtchMtrID": None,
                "RefPath": None,
                "RefName": None
            }]
            body = {
                "EmpNo": str(empNo),
                "DataType": "comp_info",
                "CreateDate": now,
                "UpdateDate": None,
                "KDisplayYN":str(activeYn),
                "LDisplayYN":"0",
                "Data": {
                    "CompName": compName,
                    "Field": field,
                    "LogoPath": path,
                    "BannerPath": path,
                    "Address": None,
                    "Email": email,
                    "Tel": tel,
                    "HomePageUrl": None,
                    "Introduce": None,
                    "FoundingDate": None,
                    "CompType": [],
                    "SearchKeyword": [],
                    "CertImagePath": path,
                    "ImgAreaType": "2A",
                    "ImgAreaPath": path,
                    "ActiveYN": str(activeYn)
                }
            }
            result = es.index(index=_market_data, id=_id, body=body)
            # 엘라스틱서치에 저장 성공시
            if result["result"] == "created":
                # mysql 데이터 a&s market 0으로 비활성 update
                try:
                    # sql = "update CMT_EMP " \
                    #       "set AS_MARKET_YN=0 " \
                    #       "where EMP_NO=%s"
                    # val = (empNo,)
                    # conn = mysql.connector.connect(
                    #     host=mysql_host,
                    #     user=mysql_user,
                    #     passwd=mysql_passwd,
                    #     database=mysql_database,
                    #     connect_timeout=mysql_connect_timeout
                    # )
                    # cur = conn.cursor()
                    # cur.execute(sql, val)
                    # conn.commit()
                    # cur.close()
                    # conn.close()
                    flag = "success"
                except:
                    flag = "fail"
        except Exception as e:
            # print(e)
            # error_code = e
            flag = "fail"
        es.close()
    # error_code = unicodedata.normalize("NFKD", json.dumps(error_code, ensure_ascii=False))  # 유니코드 normalize
    # error_code = unicodedata.normalize("NFC", error_code)  # 한글 자음모음 합치기
    # error_code = json.loads(error_code, strict=False)
    # return str({"flag": flag, "data": error_code}),
    return str({"flag": flag}),


# 이은선
############################상세정보화면 데이터 조회,저장#################################


def remove_quote(Data):
    import ast
    import json
    import unicodedata
    import pprint
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
                                                value3 = value3.replace(
                                                    "'", "")
                                                listvalue2[key3] = value3.replace(
                                                    '''"''', '')

                                            elif type(value3) == list:

                                                for listvalue3 in value3:
                                                    if type(listvalue3) == dict:
                                                        for key4, value4 in listvalue3.items():
                                                            if type(value4) != list and value4 != None and not isinstance(
                                                                    value4, int):
                                                                value4 = value4.replace(
                                                                    "'", "")
                                                                listvalue3[key4] = value4.replace(
                                                                    '''"''', '')
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
        "ArchiveID": None,
        # "ArchiveYN":None,
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


def program_info_template():
    template = {
        # "CompName": None,
        "ProgramID": None,
        "ProgramTitle": None,
        "Price": None,
        "Tag": [],
    }
    return template


def save_comp_info(empNo,LDisplayYN, data):
    flag, result = "fail", []
    try:
        es = get_es_conn(host=es_host2)

        data = change_str_to_json(data)

        # 따옴표제거
        data = remove_quote(data)
        _id_comp_info = str(empNo) + "_" + "comp_info"
        template = comp_info_template()

        template["EmpNo"] = str(empNo)
        template["DataType"] = "comp_info"
        # template["UpdateDate"] = today
        template["LDisplayYN"]=LDisplayYN
        template["KDisplayYN"]=data["ActiveYN"]

        template["Data"]["CompName"] = data["CompName"]
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
                template["Data"]["CertImagePath"].append(
                    CertImagePath_image_temp)

        template["Data"]["ImgAreaType"] = data["ImgAreaType"]

        if data["ImgAreaPath"]:
            for i in data["ImgAreaPath"]:
                ImgAreaPath_image_temp = copy.deepcopy(image_template())

                ImgAreaPath_image_temp["AtchMtrID"] = i["AtchMtrID"]
                ImgAreaPath_image_temp["RefName"] = i["RefName"]
                ImgAreaPath_image_temp["RefPath"] = i["RefPath"]

                template["Data"]["ImgAreaPath"].append(ImgAreaPath_image_temp)

        template["Data"]["ActiveYN"] = data["ActiveYN"]

        ##########################################

        _id_comp_detail_info = str(empNo) + "_" + "comp_detail_info"

        detail_temp = comp_detail_info_template()

        detail_temp["EmpNo"] = str(empNo)
        detail_temp["DataType"] = "comp_detail_info"
        # detail_temp["UpdateDate"] = today
        detail_temp["LDisplayYN"]=LDisplayYN
        detail_temp["KDisplayYN"]=data["ActiveYN"]


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

                    detail_temp["Data"]["Category"][0]["CategoryList"].append(
                        categoryList1_temp)

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
                            categoryList2_temp["ImgPath"].append(
                                ImgPath_image_temp)

                    if i["ArchiveID"]:
                        categoryList2_temp["ArchiveID"] = int(i["ArchiveID"])

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

                            categoryList2_temp["PriceImg"].append(
                                PriceImg_image_temp)

                    categoryList2_temp["PriceTxt"] = i["PriceTxt"]
                    categoryList2_temp["Unit"] = i["Unit"]

                    if i["TitleImgPath"]:
                        for j in i["TitleImgPath"]:
                            TitleImgPath_image_temp = copy.deepcopy(image_template())
                            TitleImgPath_image_temp["AtchMtrID"] = j["AtchMtrID"]
                            TitleImgPath_image_temp["RefName"] = j["RefName"]
                            TitleImgPath_image_temp["RefPath"] = j["RefPath"]

                            categoryList2_temp["TitleImgPath"].append(
                                TitleImgPath_image_temp)

                    detail_temp["Data"]["Category"][1]["CategoryList"].append(
                        categoryList2_temp)

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
                            categoryList3_temp["ImgPath"].append(
                                ImgPath_image_temp)

                    if i["ArchiveID"]:
                        categoryList3_temp["ArchiveID"] = int(i["ArchiveID"])

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
                            categoryList3_temp["PriceImg"].append(
                                PriceImg_image_temp)

                    categoryList3_temp["PriceTxt"] = i["PriceTxt"]
                    categoryList3_temp["Unit"] = i["Unit"]

                    detail_temp["Data"]["Category"][2]["CategoryList"].append(
                        categoryList3_temp)

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
                            categoryList4_temp["ImgPath"].append(
                                ImgPath_image_temp)

                    if i["ArchiveID"]:
                        categoryList4_temp["ArchiveID"] = int(i["ArchiveID"])

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
                            categoryList4_temp["PriceImg"].append(
                                PriceImg_image_temp)

                    categoryList4_temp["PriceTxt"] = i["PriceTxt"]
                    categoryList4_temp["Standard"] = i["Standard"]

                    if i["TitleImgPath"]:
                        for j in i["TitleImgPath"]:
                            TitleImgPath_image_temp = copy.deepcopy(image_template())
                            TitleImgPath_image_temp["AtchMtrID"] = j["AtchMtrID"]
                            TitleImgPath_image_temp["RefName"] = j["RefName"]
                            TitleImgPath_image_temp["RefPath"] = j["RefPath"]
                            categoryList4_temp["TitleImgPath"].append(
                                TitleImgPath_image_temp)

                    detail_temp["Data"]["Category"][3]["CategoryList"].append(
                        categoryList4_temp)

        else:
            pass

        # 최초저장인지 확인
        try:
            res1 = es.get(index=_market_data, id=_id_comp_info)
            # res2= es.get(index=_market_data, id=_id_comp_detail_info)
            
            if res1["found"]:

                try:
                    res2= es.get(index=_market_data, id=_id_comp_detail_info)

                    if res2:
                        # 기존 데이터 있고 둘다 업데이트인 경우 >>> UpdateDate, data만 수정
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

                        result1 = es.update(index=_market_data, id=_id_comp_info, body=comp_info_body)
                        result2 = es.update(index=_market_data, id=_id_comp_detail_info, body=comp_detail_info_body)
                        
                        if result1["result"] == "updated" and result2["result"] == "updated":
                            flag = "success"
                        # else:
                        #     flag="happ2y"

                # 회원가입때문에 compinfo만 있고 다 새로 저장인경우>>> comp_info는 update, comp_detail_info는 create, update
                except exceptions.NotFoundError:
                    comp_info_body = {
                        "doc": {
                            "UpdateDate": today,
                            "LDisplayYN":template["LDisplayYN"],
                            "KDisplayYN":template["KDisplayYN"],
                            "Data": template["Data"]
                        }
                    }

                    detail_temp['CreateDate']=today
                    detail_temp['UpdateDate']=today

                    result1 = es.update(index=_market_data, id=_id_comp_info, body=comp_info_body)
                    result2 = es.index(index=_market_data, id=_id_comp_detail_info, body=detail_temp)

                    if result1["result"] == "updated" and result2["result"] == "created":
                        flag = "success"
                    # else:
                    #     flag="happy1"
    

        except exceptions.NotFoundError:
            # 최초 저장인 경우
            
            template["CreateDate"] = today
            detail_temp["CreateDate"] = today

            result1 = es.index(
                index=_market_data,
                id=_id_comp_info,
                body=template
            )

            result2 = es.index(
                index=_market_data,
                id=_id_comp_detail_info,
                body=detail_temp
            )
            
            # 엘라스틱서치에 저장 성공시
            if result1["result"] == "created" and result2["result"] == "created":
            # if result1["result"] == "created" :
                flag = "success"


        # 엘라스틱서치 연결 종료
        es.close()

    except Exception as error:
        # flag=str(error)
        pass

    return str({"flag": flag}),


def search_comp_info(empNo,indexNm):
    import ast

    flag, LDisplayYN, result = "fail", "0", {}

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
            flag="fail"
            return str({"flag": flag,"LDisplayYN":LDisplayYN, "data": result}),

        if res['hits']['hits'][0]['_source'].get('LDisplayYN'):
            LDisplayYN=res['hits']['hits'][0]['_source']['LDisplayYN']


       
        if res["hits"]["total"]["value"] > 0:

            for i in res['hits']['hits']:
                result.update(i['_source']['Data'])

                ########################################
                # elastic의 [] -> {"AtchMtrID":"","RefName":"","RefPath":""} 으로 변환
                if i['_source']['DataType'] == 'comp_info':

                    if not result['LogoPath']:  # 빈리스트면
                        result['LogoPath'].append(image_template())

                    if not result['BannerPath']:  # 빈리스트면
                        result['BannerPath'].append(image_template())

                    if not result['CertImagePath']:  # 빈리스트면
                        result['CertImagePath'].append(image_template())

                    if not result['ImgAreaPath']:  # 빈리스트면
                        result['ImgAreaPath'].append(image_template())
                if i['_source']['DataType'] == 'comp_detail_info':
                    for num in range(0, 4):
                        for i in result['Category'][num]['CategoryList']:

                            if not i['ImgPath']:  # 빈리스트면
                                i['ImgPath'].append(image_template())

                            if not i['PriceImg']:  # 빈리스트면
                                i['PriceImg'].append(image_template())

                            if not i['TitleImgPath']:  # 빈리스트면
                                i['TitleImgPath'].append(image_template())

            ########################################
            # promgram index에서 programID에 해당하는 샤드 리스트형태로 반환
            result['programIDInfoList'] = []
            programid_list = []

            if indexNm=="K":
                if "Category" in result:
                    for i in result["Category"]:
                        if i["CategoryType"] == "1":
                            for j in i["CategoryList"]:
                                programid_list.append(j["ProgramID"])

                program_query = {
                    "query": {
                        "bool": {
                            "must": [
                                {"terms": {"Data.ProgramID": programid_list}},
                                {"term": {"Data.Active": "1"}}
                            ]
                        }
                    },
                    "sort": ["_score", {"UpdateDate": "desc"}],
                }

                refresh_es(index=_program)
                program_res = get_data_from_es(_program, program_query)

                for i in program_res:
                    program_temp = program_info_template()

                    # program_temp["CompName"]=result['CompName']
                    program_temp["ProgramID"] = i["_source"]["Data"]["ProgramID"]
                    program_temp["ProgramTitle"] = i["_source"]["Data"]["ProgramTitle"]
                    program_temp["Price"] = i["_source"]["Data"]["Price"]
                    program_temp["Tag"] = i["_source"]["Data"]["Tag"]

                    result['programIDInfoList'].append(program_temp)

            flag = "success"

        elif res["hits"]["total"]["value"] == 0:
            flag = "success"

        # 엘라스틱서치 연결 종료
        es.close()

        result = change_none_to_str(result)

    except Exception as e:
        # print(e)
        pass
    
    return str({"flag": flag,"LDisplayYN":LDisplayYN, "data": result}),


#####################################################################

def EmpNo_query(empNo):
    return {
        "size": 1,
        "query": {
            "term": {
                "DataType": "comp_info"
            },
            "term": {
                "EmpNo": empNo
            },
        },
        "sort": [{"_id": "desc"}],
    }


def search_program_popular():

    import itertools

    flag, data = "fail", []
    try:
        es = get_es_conn(host=es_host2)
        query = {
            "size": 3,
            "query": {
                "term": {
                    "DataType": "program_info"
                },
                "term": {
                    "Data.Active": 1
                }
            },
            "sort": [{"Data.ViewCnt": "desc"}, {"UpdateDate": "desc"}, ],
        }

        refresh_es(index=_program)
        res = es.search(index=_program, body=query)

        if res["hits"]["total"]["value"] > 0:
            data = [i['_source'] for i in res['hits']['hits']]
            empno_list = [i['EmpNo'] for i in data]

            for i in data:
                i['CompName'] = None

            ########################################
            # compname 추가
            m_query = [
                ({"index": _market_data}, EmpNo_query(empno)) for empno in empno_list
            ]  # tuple 로 묶은걸
            m_query = list(itertools.chain(*m_query))  # 하나의 list로 풀기
            empno_data = es.msearch(body=m_query)

            empno_dict = {string: None for string in empno_list}

            for i in empno_data['responses']:
                if i['hits']['total']['value'] > 0:
                    empno_dict[i['hits']['hits'][0]['_source']['EmpNo']
                               ] = i['hits']['hits'][0]['_source']['Data']['CompName']

            for i in data:
                i['CompName'] = empno_dict[i['EmpNo']]

            data = change_none_to_str(data)

            # ########################################
            # # elastic의 [] -> {"AtchMtrID":"","RefName":"","RefPath":""} 으로 변환

            for i in data:
                if not i['Data']['ManualFile']:
                    i['Data']['ManualFile'].append(image_template())
                if not i['Data']['ProgramImg']:
                    i['Data']['ProgramImg'].append(image_template())
                if not i['Data']['ProgramFile']:
                    i['Data']['ProgramFile'].append(image_template())

            flag = "success"

    except Exception as e:
        # print(e)
        pass

    return str({"flag": flag, "data": data}),


def update_archive_info(archiveID, existing_empNo, new_empNo):
    flag, result = "fail", []
    try:
        es = get_es_conn(host=es_host2)

        if existing_empNo:
            if new_empNo:
                if existing_empNo != new_empNo:
                    result = search_comp_info(existing_empNo,index_mode[_market_data])
                    result = eval(result[0])

                    if result['flag'] == 'success':
                        data = result['data']

                        if data['Category']:
                            for i in data['Category']:
                                if i['CategoryType'] != '1':
                                    for j in i['CategoryList']:
                                        if j['ArchiveID']:
                                            if int(j['ArchiveID']) == int(archiveID):
                                                j['ArchiveID'] = None

                    res=save_comp_info(existing_empNo,result['DisplayYN'], data)
                    res = eval(res[0])

                    if result['flag'] == 'success':
                        flag = "success"
            else:
                result = search_comp_info(existing_empNo,index_mode[_market_data])
                result = eval(result[0])

                if result['flag'] == 'success':
                    data = result['data']

                    if data['Category']:
                        for i in data['Category']:
                            if i['CategoryType'] != '1':
                                for j in i['CategoryList']:
                                    if j['ArchiveID']:
                                        if int(j['ArchiveID']) == int(archiveID):
                                            j['ArchiveID'] = None

                res=save_comp_info(existing_empNo,result['DisplayYN'], data)
                res = eval(res[0])

                if result['flag'] == 'success':
                    flag = "success"
        else:
            flag = "success"

    except Exception as error:
        # print(error)
        pass

    return str({"flag": flag}),


if __name__ =="__main__":
    # fixed_data= '{"CompName":"system_테스트입니다.아침!","Field":"E","LogoPath":[{"AtchMtrID":1970,"RefName":"12123.jpg","RefPath":"D:\\KWSSDP\\upload\\MPage1050\\3712073\\1698647010809.jpg"}],"BannerPath":[{"AtchMtrID":"","RefName":"","RefPath":""}],"Address":"test103004_테스트입니다. 3시23분","Email":"test103004@naver.com","Tel":"02-1234-1234","HomePageUrl":"","Introduce":"test103004","FoundingDate":"2023-10-01","CompType":["중견기업"],"SearchKeyword":["test103004","test103004test"],"CertImagePath":[{"AtchMtrID":"","RefName":"","RefPath":""}],"ImgAreaType":"2A","ImgAreaPath":[{"AtchMtrID":1971,"RefName":"12123.jpg","RefPath":"D:\\KWSSDP\\upload\\MPage1050\\3712073\\1698647044262.jpg"},{"AtchMtrID":1972,"RefName":"12123.jpg","RefPath":"D:\\KWSSDP\\upload\\MPage1050\\3712073\\1698647046308.jpg"}],"ActiveYN":"1","Category":[{"CategoryType":1,"CategoryName":"웹프로그램","CategoryList":[{"Title":"","Content":"","ArchiveID":"","ImgPath":[{"AtchMtrID":"","RefName":"","RefPath":""}],"PriceType":"","PriceInt":"","PriceImg":[{"AtchMtrID":"","RefName":"","RefPath":""}],"PriceTxt":"","ProgramID":"","Unit":"","Standard":"","TitleImgPath":[{"AtchMtrID":"","RefName":"","RefPath":""}]}]},{"CategoryType":2,"CategoryName":"서비스스스스111","CategoryList":[{"Title":"ㅎㅇㅎㅇㅎ","Content":"ㄴㅇㄴㄴㅇㅎㅎ","ArchiveID":"","ImgPath":[{"AtchMtrID":1973,"RefName":"12123.jpg","RefPath":"D:\\KWSSDP\\upload\\MPage1051\\3712073\\1698647068560.jpg"}],"PriceType":"p1","PriceInt":1131000,"PriceImg":[{"AtchMtrID":"","RefName":"","RefPath":""}],"PriceTxt":"","ProgramID":"","Unit":"ㅇㄹㅇㄹ","Standard":"","TitleImgPath":[{"AtchMtrID":1976,"RefName":"12123.jpg","RefPath":"D:\\KWSSDP\\upload\\MPage1050\\3712073\\1698647159172.jpg"}]}]},{"CategoryType":3,"CategoryName":"서비수수ㅜㅅ수수2222","CategoryList":[{"Title":"안뇽","Content":"ㅎㅎㅎ안뇽..","ArchiveID":"","ImgPath":[{"AtchMtrID":"","RefName":"","RefPath":""}],"PriceType":"p2","PriceInt":"","PriceImg":[{"AtchMtrID":"","RefName":"","RefPath":""}],"PriceTxt":"","ProgramID":"","Unit":"1322222자나","Standard":"","TitleImgPath":[{"AtchMtrID":"","RefName":"","RefPath":""}]}]},{"CategoryType":4,"CategoryName":"제품이자나~","CategoryList":[{"Title":"제품","Content":"제품","ArchiveID":"","ImgPath":[{"AtchMtrID":1974,"RefName":"12123.jpg","RefPath":"D:\\KWSSDP\\upload\\MPage1051\\3712073\\1698647129380.jpg"}],"PriceType":"p3","PriceInt":"","PriceImg":[{"AtchMtrID":"","RefName":"","RefPath":""}],"PriceTxt":"21ㅇㄹㅇㄴㅇㄹㄴ","ProgramID":"","Unit":"","Standard":"규겨ㅛㄱ","TitleImgPath":[{"AtchMtrID":1975,"RefName":"12123.jpg","RefPath":"D:\\KWSSDP\\upload\\MPage1050\\3712073\\1698647149070.jpg"}]}]}]}'

    # res=save_comp_info('3711795','1',fixed_data)
    # print(res)

    # uai=update_archive_info(archiveID=1,existing_empNo="3712072",new_empNo="999999")
    # print(uai)

    # sci=search_comp_info("3712072","K")
    # print(sci)

    # res=update_user_info(empNo="999999", field="G", activeYn="1", name="흑흑", compName="회사흑흑", tel="010-101-00", email="ggggg@gmail.com")
    # # print(res)

    # sci=search_comp_info('3711910', 'K')
    # print(sci)

    # res=save_comp_info('00001212', '1', '{"CompName":"엥.....머야","Field":"A","LogoPath":[{"AtchMtrID":1980,"RefName":"12123.jpg","RefPath":"D:\\KWSSDP\\upload\\MPage1050\\3712078\\1698728733075.jpg"}],"BannerPath":[{"AtchMtrID":"","RefName":"","RefPath":""}],"Address":"test103104test103104_newtest2","Email":"test103104test103104_newtest2@naver.com","Tel":"052-1231-1231","HomePageUrl":"","Introduce":"test103104test103104_newtest2","FoundingDate":"2023-10-02","CompType":["대기업"],"SearchKeyword":["test103104test","test103104test"],"CertImagePath":[{"AtchMtrID":"","RefName":"","RefPath":""}],"ImgAreaType":"2A","ImgAreaPath":[{"AtchMtrID":1981,"RefName":"12123.jpg","RefPath":"D:\\KWSSDP\\upload\\MPage1050\\3712078\\1698728747173.jpg"},{"AtchMtrID":1982,"RefName":"12123.jpg","RefPath":"D:\\KWSSDP\\upload\\MPage1050\\3712078\\1698728749399.jpg"}],"ActiveYN":"1","Category":[]}')
    # print(res)

    # res=update_user_info('0455200' , '' , '0' , 'test103106' , '' , '011-572-1231' , 'test103106@dreamwiz.com')

    # print(res)

    # res=save_comp_info('778899', '0', '{"CompName":"371209037120903712090","Field":"B","LogoPath":[{"AtchMtrID":2002,"RefName":"12123.jpg","RefPath":"D:\\KWSSDP\\upload\\MPage1050\\3712089\\1698735953867.jpg"}],"BannerPath":[{"AtchMtrID":"","RefName":"","RefPath":""}],"Address":"test103116test103116test103116","Email":"test103116@gmail.com","Tel":"063-4444-4444","HomePageUrl":"","Introduce":"test103116test103116test103116","FoundingDate":"2023-10-01","CompType":["중견기업"],"SearchKeyword":["test103116"],"CertImagePath":[{"AtchMtrID":"","RefName":"","RefPath":""}],"ImgAreaType":"2A","ImgAreaPath":[{"AtchMtrID":2003,"RefName":"12123.jpg","RefPath":"D:\\KWSSDP\\upload\\MPage1050\\3712089\\1698735969809.jpg"},{"AtchMtrID":2004,"RefName":"12123.jpg","RefPath":"D:\\KWSSDP\\upload\\MPage1050\\3712089\\1698735972377.jpg"}],"ActiveYN":"1","Category":[]}')

    # print(res)

    # res=update_user_info('3712100' , 'A' , '1' , 'test12345677' , '' , '010-5555-5555' , 'test103116@lycos.co.kr')
    # print(res)

    # res=save_comp_info('1', '1', '{"CompName":"주_구조인디자인연구소","Field":"A","LogoPath":[{"AtchMtrID":2002,"RefName":"12123.jpg","RefPath":"D:\\KWSSDP\\upload\\MPage1050\\3712089\\1698735953867.jpg"}],"BannerPath":[{"AtchMtrID":"","RefName":"","RefPath":""}],"Address":"test103116test103116test103116","Email":"test103116@gmail.com","Tel":"063-4444-4444","HomePageUrl":"","Introduce":"test103116test103116test103116","FoundingDate":"2023-10-01","CompType":["중견기업"],"SearchKeyword":["test103116"],"CertImagePath":[{"AtchMtrID":"","RefName":"","RefPath":""}],"ImgAreaType":"2A","ImgAreaPath":[{"AtchMtrID":2003,"RefName":"12123.jpg","RefPath":"D:\\KWSSDP\\upload\\MPage1050\\3712089\\1698735969809.jpg"},{"AtchMtrID":2004,"RefName":"12123.jpg","RefPath":"D:\\KWSSDP\\upload\\MPage1050\\3712089\\1698735972377.jpg"}],"ActiveYN":"1","Category":[]}')

    # print(res)

    # res=save_comp_info('121312', '0', '{"CompName":"test103118test103118test103118test103118","Field":"A","LogoPath":[{"AtchMtrID":2005,"RefName":"12123.jpg","RefPath":"D:\\KWSSDP\\upload\\MPage1050\\3712092\\1698737067069.jpg"}],"BannerPath":[{"AtchMtrID":"","RefName":"","RefPath":""}],"Address":"test103118test103118test103118test103118","Email":"test103118@naver.com","Tel":"063-4444-5555","HomePageUrl":"","Introduce":"test103118test103118test103118","FoundingDate":"2023-10-01","CompType":["대기업"],"SearchKeyword":["test103118"],"CertImagePath":[{"AtchMtrID":"","RefName":"","RefPath":""}],"ImgAreaType":"2A","ImgAreaPath":[{"AtchMtrID":2006,"RefName":"12123.jpg","RefPath":"D:\\KWSSDP\\upload\\MPage1050\\3712092\\1698737080673.jpg"},{"AtchMtrID":2007,"RefName":"12123.jpg","RefPath":"D:\\KWSSDP\\upload\\MPage1050\\3712092\\1698737082615.jpg"}],"ActiveYN":"1","Category":[]}')

    # print(res)

    # print(search_comp_list('allG', '', '', '', '32' ))

    res=search_ques_list(type="0", empNo="3711795", docID=None, size=None)
    print(res)

    # print(search_ques_info("3711795_3711795_20231907141512394873","K"))
