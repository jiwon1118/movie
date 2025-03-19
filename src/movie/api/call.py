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
    for k, v in url_param.items():
        url += f"&{k}={v}"
        
    return url


def call_api(dt="20120101", url_param={}):
    respond = requests.get(gen_url(dt, url_param))
    data = respond.json()
    return data['boxOfficeResult']["dailyBoxOfficeList"]


def list2df(data : list , dt : str, url_param={}):
    df = pd.DataFrame(data)
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


def fill_na_with_column(origin_df, c_name):
    df = origin_df.copy()
    for i, row in df.iterrows():
        if pd.isna(row[c_name]):
            same_movie_df = df[df["movieCd"] == row["movieCd"]]
            notna_idx = same_movie_df[c_name].dropna().first_valid_index()
            if notna_idx is not None:
                df.at[i, c_name] = df.at[notna_idx, c_name]
    return df


def create_unique_ranked_df(df, drop_columns):
    df_unique = df.drop(columns=drop_columns).drop_duplicates()    
    return df_unique


def re_ranking(df):
    # create_unique_ranked_df에서 중복을 제거한 후 순위 매기기  
    df = df.sort_values(by="audiCnt", ascending=False).reset_index(drop=True)
    df['rnum'] = df['audiCnt'].rank(ascending=False).astype(int)
    df['rank'] = df['rnum']  # 'rnum'과 'rank'가 동일하도록 설정
    return df


def fill_unique_ranking(df: pd.DataFrame, dt:str) -> pd.DataFrame:
    df1 = fill_na_with_column(df, 'multiMovieYn')
    df2 = fill_na_with_column(df1, 'repNationCd')
    drop_columns=['rnum', 'rank', 'rankInten', 'salesShare']
    unique_df = create_unique_ranked_df(df=df2, drop_columns=drop_columns)
    new_ranking_df = re_ranking(unique_df)
    new_ranking_df['dt'] = dt
    return new_ranking_df