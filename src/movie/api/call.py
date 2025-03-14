import os
import requests
import pandas as pd 

BASE_URL="http://kobis.or.kr/kobisopenapi/webservice/rest/boxoffice/searchDailyBoxOfficeList.json"
KEY=os.getenv("MOVIE_KEY")

def gen_url(dt="20120101", url_param={}):
    "호출 URL 생성, url_param이 입력되면 multiMovieYn, repNationCd 처리"
    url = f"{BASE_URL}?key={KEY}&targetDt={dt}"
    # TODO = url_param 처리
    if url_param:
        for k, v in url_param.items():
            url += f"&{k}={v}"
    return url


def call_api(dt="20120101", url_param={}):
    respond = requests.get(gen_url(dt, url_param={}))
    data = respond.json()
    return data['boxOfficeResult']["dailyBoxOfficeList"]




def list2df(data : list , dt : str):
    l = call_api()
    # TODO = list를 DataFrame으로 변환
    df = pd.DataFrame(l)
    #df['dt'] = dt 도 가능 (맨 뒤에 생김)
    df.insert(0, "dt", dt)
    return df