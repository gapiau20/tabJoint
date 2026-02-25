import pandas as pd
from functools import reduce
from pathlib import Path
import os
import argparse

def write_table(df:pd.DataFrame, filepath:str, **kwargs):
    extension=Path(filepath).suffix.lower()
    writers={
        ".csv": df.to_csv,
        ".xlsx": df.to_excel
        }
    if extension in writers:
        writers[extension](filepath, **kwargs)

def get_readers(**kwargs):
    return{
            '.csv':lambda x:pd.read_csv(x,**kwargs),
            '.tsv':lambda x:pd.read_tsv(x,**kwargs),
            '.xlsx':lambda x:pd.read_excel(x,**kwargs),
            '.xls':lambda x:pd.read_excel(x,**kwargs),
        }
        


def extract_files(input_directory)->list[str]:
    dir_path=Path(input_directory)
    if not os.path.isdir(input_directory):
        raise ValueError('{} is not a valid directory'.format(input_directory))
    allowed_exts=set(get_readers().keys())

    files=[str(p)for p in dir_path.rglob("*")if p.suffix.lower() in allowed_exts]
    return files
    
def read_file(file:str,**kwargs):
    readers=get_readers(**kwargs)
    extension=Path(file).suffix.lower()

    if extension in readers.keys():
        return readers[extension](file)

    #if reader not available
    return None

def load_files(file_paths:list[str],add_filenamecolumn:bool,filename_column='TABLENAME',**reader_kwargs)->list[pd.DataFrame]:
    """
    Load a list of csv files into pd Dataframe
    """
    dfs = []
    for path in file_paths:
        try: 
            df=read_file(path,**reader_kwargs)
        except ValueError:
            print('Error Reading {}'.format(path))
        if add_filenamecolumn:
            df[filename_column]=Path(path).name
        dfs.append(df)
    return dfs


def merge_patient_dataframes(dfs, patient_col="Patient")->pd.DataFrame:
    """
    Concat Dataframe with overlapping patients/variables.
    For each patient, use the first non-nan value if it exists.
    """

    #Vertical stacking
    combined = pd.concat(dfs, axis=0, ignore_index=True)
    #groubby patient+pick first on-nan if exists
    merged = combined.groupby(patient_col, as_index=False).first()
    return merged


def merge_files(file_paths, output_path, patient_col="Patient",add_filenamecolumn=True,filename_column='TABLENAME',**reader_kwargs)->pd.DataFrame:
    """
    Read, merge, save
    """
    dfs = load_files(file_paths,add_filenamecolumn,filename_column,**reader_kwargs)
    merged_df = merge_patient_dataframes(dfs, patient_col=patient_col)
    write_table(merged_df,output_path,index=False)
    return merged_df

def normalize_str_series(s:pd.Series)->pd.Series:
    return (
        s.astype(str)
        .str.casefold() #lower case and ß->ss
        .str.replace('-',' ',regex=False)#umlaut
        .str.replace("ä", "ae", regex=False)
        .str.replace("ö", "oe", regex=False)
        .str.replace("ü", "ue", regex=False)
        .str.replace("-", " ", regex=False)
        .str.replace(r"\s+"," ",regex=True)#multiple spaces
        .str.strip()#start and end of line
    )



def sanity_check_dataframes(df1:pd.DataFrame,df2:pd.DataFrame,joint_keys:list[str])->bool:
    '''Check if all values of overlapping columns between df1 and df2 are equal'''
    #common columns outside joint keys
    col_intersec=df1.columns.intersection(df2.columns).difference(joint_keys)
    if len(col_intersec)<=0:
        return True #no common column to compare
    #joint
    joint=df1.merge(df2,on=joint_keys,suffixes=("_df1", "_df2"), how="inner")
    if joint.empty:
        return False #nothing common

    comparisons=[]
    for col in col_intersec:
        s1=joint[col+'_df1']
        s2=joint[col+'_df2']

        # numeric case
        if pd.api.types.is_numeric_dtype(s1) and pd.api.types.is_numeric_dtype(s2):
            #check if when both are non nan value is equal
            equal= (s1==s2) | s1.isna() | s2.isna()
        #str case : first we need to normalize the str
        elif s1.dtype=='str':
            equal=(normalize_str_series(s1)==normalize_str_series(s2)) | s1.isna() | s2.isna()
        else:
            equal = (s1 == s2) | s1.isna() | s2.isna()
        comparisons.append(equal)
        conflict=~equal
        if conflict.any():
            has_conflict = True

            print(f"\nConflict detected in column: {col}")
            print(
                joint.loc[conflict, joint_keys + 
                          [f"{col}_df1", f"{col}_df2"]]
            )
            raise ValueError('Debug')

    return pd.concat(comparisons,axis=1).all().all()

def parse_conflicts(df1:pd.DataFrame,df2:pd.DataFrame,joint_keys:list[str])->bool:
    '''Check if all values of overlapping columns between df1 and df2 are equal
    When conflict detected, parse to decide manually whether to ignore the conflict

    returns False if no conflict is detected, or if the user validates the consistency of the dataframes
    '''
    if df1.empty or df2.empty: 
        print('Empty dataframe')
        return False
    #common columns outside joint keys
    col_intersec=df1.columns.intersection(df2.columns).difference(joint_keys)
    if len(col_intersec)<=0:
        return True #no common column to compare
    #joint
    joint=df1.merge(df2,on=joint_keys,suffixes=("_df1", "_df2"), how="inner")
    if joint.empty:
        return False #nothing common

    comparisons=[]
    for col in col_intersec:
        s1=joint[col+'_df1']
        s2=joint[col+'_df2']
        #check if when both are non nan value is equal
        

        conflict = (~s1.isna()) & (~s2.isna()) & (s1 != s2)

        if conflict.any():
            has_conflict = True

            print(f"\nConflict detected in column: {col}")
            print(
                joint.loc[conflict, joint_keys + 
                          [f"{col}_df1", f"{col}_df2"]]
            )
            proceed=input('Is conflict OK or not Y/N')
            if proceed.lower()=='y': 
                conflict[:]=False
            else:
                conflict[:]==True
        comparisons.append(conflict)

    return pd.concat(comparisons,axis=1).all().all()

def main():
    parser=argparse.ArgumentParser(
        description="Merge multiple tabular datasets into a csv file"
    )
    parser.add_argument("-i",'--inputs', nargs='+',help='Input files to merge')
    parser.add_argument("-d", "--directory",help="Directory containing input files")
    parser.add_argument('-o','--output',required=True,help='Output file')
    parser.add_argument('--joint-col',default='Patient',help='Name of the column to joint the files (default: Patient)')
    parser.add_argument('-v','--verbose',action="store_true",help='Whether to display the extracted files')
    args=parser.parse_args() 

    if args.directory: 
        inputs=extract_files(args.directory)
    elif args.inputs:
        inputs=args.inputs
    else: 
        raise ValueError('At least directory (-d) or a list of input files should be specified') 
    
    if args.verbose: 
        print('Extracted {} files: \n {}'.format(len(inputs),str(inputs)))
    merge_files(inputs,args.output,args.joint_col)
    return

if __name__=='__main__':
    main()
    # print(extract_files('test_data'))
    # merge_files(extract_files('test_data'),'output.csv')


