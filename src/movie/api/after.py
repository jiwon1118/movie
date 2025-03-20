import pandas as pd
import os

def fillna_meta(previous_df: pd.DataFrame, current_df: pd.DataFrame) -> pd.DataFrame:
    
    if previous_df is None:
        return current_df
    
    merged_df = previous_df.merge(current_df, on="movieCd", how="outer", suffixes=("_A", "_B"))

    merged_df["multiMovieYn"] = merged_df["multiMovieYn_A"].combine_first(merged_df["multiMovieYn_B"])
    merged_df["repNationCd"] = merged_df["repNationCd_A"].combine_first(merged_df["repNationCd_B"])

    merged_df = merged_df[["movieCd", "multiMovieYn", "repNationCd"]]
    return(merged_df)


def save_meta(df: pd.DataFrame, parquet_path: str, partitions) -> str:
    os.makedirs(os.path.dirname(parquet_path), exist_ok=True)
  
    for p in partitions:
        parquet_path = parquet_path + f"/{p}"

    df.to_parquet(parquet_path)
    
    # 저장된 파일의 절대 경로 반환
    save_path = os.path.abspath(parquet_path)
    print(f"파일이 성공적으로 저장되었습니다: {save_path}")
    
    return save_path
