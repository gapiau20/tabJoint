import pandas as pd
from functools import reduce
from pathlib import Path


def read_file(file:str,**kwargs):
    readers={
        '.csv':lambda x:pd.read_csv(x,**kwargs),
        '.tsv':lambda x:pd.read_tsv(x,**kwargs),
        '.xlsx':lambda x:pd.read_excel(x,**kwargs),
        '.xls':lambda x:pd.read_excel(x,**kwargs),
    }
    extension=Path(file).suffix.lower()

    if extension in readers.keys():
        return readers[extension](file,**kwargs)

    #if reader not available
    return None

def load_files(file_paths,reader_kwargs={}):
    """
    Load a list of csv files into pd Dataframe
    """
    dfs = []
    for path in file_paths:
        df=read_file(path,**reader_kwargs)
        dfs.append(df)
    return dfs


def merge_patient_files(dfs, patient_col="Patient"):
    """
    Concat Dataframe with overlapping patients/variables.
    For each patient, use the first non-nan value if it exists.
    """

    #Vertical stacking
    combined = pd.concat(dfs, axis=0, ignore_index=True)
    #groubby patient+pick first on-nan if exists
    merged = combined.groupby(patient_col, as_index=False).first()
    return merged


def merge_files(file_paths, output_path, patient_col="Patient"):
    """
    Read, merge, save
    """
    dfs = load_files(file_paths)
    merged_df = merge_patient_files(dfs, patient_col=patient_col)
    merged_df.to_csv(output_path, index=False)
    return merged_df



if __name__=='__main__':
    files = ["test_data\\table1.csv","test_data\\table2.csv","test_data\\table3.csv"]
    final_df = merge_files(file_paths=files,output_path="test_data\\patients_final.csv")
    print(final_df)

