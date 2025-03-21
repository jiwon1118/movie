import pandas as pd
from movie.api.after import fillna_meta, save_gen


def test_fillna_meta():
    previous_df = pd.DataFrame(
        {
            "movieCd": ["1001", "1002", "1003"],
            "multiMovieYn": ["Y", "Y", "N"],
            "repNationCd": ["K", "F", None],
        }
    )

    current_df = pd.DataFrame(
        {
            "movieCd": ["1001", "1003", "1004"],
            "multiMovieYn": [None, "Y", "Y"],
            "repNationCd": [None, "F", "K"],
        }
    )

    r_df = fillna_meta(previous_df, current_df)

    assert not r_df.isnull().values.any(), "결과 데이터프레임에 NaN 또는 None 값이 있습니다!"
    
    
def test_fillna_meta_none_previous_df():
    previous_df = None

    current_df = pd.DataFrame(
        {
            "movieCd": ["1001", "1003", "1004"],
            "multiMovieYn": [None, "Y", "Y"],
            "repNationCd": [None, "F", "K"],
        }
    )

    r_df = fillna_meta(previous_df, current_df)

    assert r_df.equals(current_df), "r_df는 current_df와 동일해야 합니다!"
    
    
def test_save_gen():
    current_df = pd.DataFrame(
        {
            "movieCd": ["1001", "1003", "1004"],
            "multiMovieYn": [None, "Y", "Y"],
            "repNationCd": [None, "F", "K"],
        }
    )
    
    save_gen(current_df, '/home/jiwon/temp/1/2/3/meta.parquet')
    
    