import os
import requests
import pandas as pd 
import pyarrow

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


def list2df(data : list , dt : str, url_param={}):
    l = call_api()
    df = pd.DataFrame(l)
    df['dt'] = dt #도 가능 (맨 뒤에 생김)
    #df.insert(0, "dt", dt)
    for k, v in url_param.items():
        df[k] = v

    num_cols = ['rnum', 'rank', 'rankInten', 'salesAmt', 'audiCnt',
                'audiAcc', 'scrnCnt', 'showCnt', 'salesShare', 'salesInten',
                'salesChange', 'audiInten', 'audiChange']
    for col_name in num_cols:
        df[col_name] = pd.to_numeric(df[col_name])
    # df[num_cols] = df[num_cols].apply(pd.to_numeric) - 이것도 가능
    
    return df


def save_df(df, base_path, partitions=['dt']):
    df.to_parquet(base_path, partition_cols=partitions)
    save_path = base_path
    for p in partitions:
        save_path = save_path + f"/{p}={df[p][0]}"
    return save_path
