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


def gen_meta(base_path, ds_nodash):
        import os
        
        PATH = os.path.expanduser("~/data/movies/merge/dailyboxoffice/dt=")
        save_path = f"{base_path}/meta/meta.parquet"
        if not os.path.exists(f"{base_path}/meta"):
            os.makedirs(f"{base_path}/meta")
        else:
            pass
        
        previous_df = read_df(save_path)
        current_df = read_df(f"{PATH}{ds_nodash}")
        
        r_df = fillna_meta(previous_df, current_df)
        # TODO f"{base_path}/meta/meta.parquet -> 경로로 저장
        r_df.to_parquet(save_path)
        
        return(save_path)


def save_gen(df: pd.DataFrame, parquet_path: str, partitions: list = []) -> str:
    os.makedirs(os.path.dirname(parquet_path), exist_ok=True)

    if partitions:
        df.to_parquet(parquet_path, partition_cols=partitions, index=False)
    else:
        df.to_parquet(parquet_path, index=False)

    # 저장된 파일의 절대 경로 반환
    save_path = os.path.abspath(parquet_path)    
    return save_path