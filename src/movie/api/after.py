import pandas as pd
import os


def read_df(parquet_path: str) -> pd.DataFrame:
    if os.path.exists(parquet_path):
        df = pd.read_parquet(parquet_path)
    else:
        df = None
        
    return df


def fillna_meta(previous_df: pd.DataFrame, current_df: pd.DataFrame) -> pd.DataFrame:
    
    if previous_df is None:
        return current_df
    
    merged_df = previous_df.merge(current_df, on="movieCd", how="outer", suffixes=("_A", "_B"))

    merged_df["multiMovieYn"] = merged_df["multiMovieYn_A"].combine_first(merged_df["multiMovieYn_B"])
    merged_df["repNationCd"] = merged_df["repNationCd_A"].combine_first(merged_df["repNationCd_B"])

    merged_df = merged_df[["movieCd", "multiMovieYn", "repNationCd"]]
    return(merged_df)


def save_gen(df: pd.DataFrame, parquet_path: str, partitions: list = []) -> str:
    os.makedirs(os.path.dirname(parquet_path), exist_ok=True)

    if partitions:
        df.to_parquet(parquet_path, partition_cols=partitions)
    else:
        df.to_parquet(parquet_path)

    # 저장된 파일의 절대 경로 반환
    save_path = os.path.abspath(parquet_path)    
    return save_path

# def save_with_mkdir(df: pd.DataFrame, parquet_path: str) -> str:
#     """디렉토리가 없으면 생성하고, DataFrame을 parquet 파일로 저장한 후 경로 반환"""
#     os.makedirs(os.path.dirname(parquet_path), exist_ok=True)
    
#     # DataFrame을 parquet 형식으로 저장
#     df.to_parquet(parquet_path)
    
#     # 저장된 파일의 절대 경로 반환
#     absolute_path = os.path.abspath(parquet_path)
#     print(f"파일이 성공적으로 저장되었습니다: {absolute_path}")
    
#     return absolute_path