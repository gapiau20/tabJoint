import pandas as pd
from pathlib import Path
import csv
import os
import argparse

HEADER_SEARCH_ROWS=50 #number of rows scanned to find the header line
CSV_DELIMITERS=",;\t|"
ENCODINGS=("utf-8-sig","latin-1") #utf-8-sig removes the BOM

def write_table(df:pd.DataFrame, filepath:str, **kwargs):
    extension=Path(filepath).suffix.lower()
    writers={
        ".csv": df.to_csv,
        ".tsv": lambda path,**kw: df.to_csv(path,sep='\t',**kw),
        ".xlsx": df.to_excel
        }
    if extension not in writers:
        raise ValueError('Unsupported output format {} (allowed: {})'.format(extension,', '.join(writers)))
    writers[extension](filepath, **kwargs)

def get_readers(**kwargs):
    return{
            '.csv':lambda x:pd.read_csv(x,**kwargs),
            '.tsv':lambda x:pd.read_csv(x,**{'sep':'\t',**kwargs}),
            '.xlsx':lambda x:pd.read_excel(x,**kwargs),
            '.xls':lambda x:pd.read_excel(x,**kwargs),
        }



def extract_files(input_directory,exclude:list[str]=None)->list[str]:
    dir_path=Path(input_directory)
    if not os.path.isdir(input_directory):
        raise ValueError('{} is not a valid directory'.format(input_directory))
    allowed_exts=set(get_readers().keys())
    excluded={Path(p).resolve() for p in (exclude or [])}

    files=[str(p) for p in dir_path.rglob("*")
           if p.is_file()
           and p.suffix.lower() in allowed_exts
           and not p.name.startswith('~$') #excel lock files
           and p.resolve() not in excluded] #e.g. previous output
    return files

def _same_name(cell,name:str)->bool:
    return str(cell).replace('﻿','').strip().casefold()==name.strip().casefold()

def _read_text_sample(file:str)->tuple[str,str]:
    '''Return (encoding,first lines) of a text file'''
    for encoding in ENCODINGS:
        try:
            with open(file,encoding=encoding,newline='') as f:
                lines=[line for _,line in zip(range(HEADER_SEARCH_ROWS),f)]
            return encoding,''.join(lines)
        except UnicodeDecodeError:
            continue
    raise ValueError('Cannot decode {}'.format(file))

def _sniff_delimiter(sample:str,default:str)->str:
    try:
        return csv.Sniffer().sniff(sample,delimiters=CSV_DELIMITERS).delimiter
    except csv.Error:
        return default

def _find_header_row(rows,key_col:str):
    '''Index of the first row containing key_col, None if not found'''
    for i,row in enumerate(rows):
        if any(_same_name(cell,key_col) for cell in row):
            return i
    return None

def read_file(file:str,key_col:str=None,**kwargs):
    '''
    Read a tabular file. If key_col is given, the header is the first line containing key_col
    (handles title/blank lines above the header). Returns None if the format is not supported.
    '''
    readers=get_readers(**kwargs)
    extension=Path(file).suffix.lower()
    if extension not in readers.keys():
        return None

    options={}
    if extension in ('.csv','.tsv'):
        encoding,sample=_read_text_sample(file)
        default_sep='\t' if extension=='.tsv' else ','
        lines=sample.splitlines()
        #title lines above the header would mislead the delimiter detection
        start=0
        if key_col is not None:
            start=next((i for i,line in enumerate(lines) if key_col.casefold() in line.casefold()),0)
        sep=_sniff_delimiter('\n'.join(lines[start:]),default_sep)
        options.update(encoding=encoding,sep=sep,skipinitialspace=True)
        if key_col is not None:
            header_row=_find_header_row(csv.reader(lines,delimiter=sep),key_col)
            if header_row is not None:
                options['skiprows']=header_row
    else:
        if key_col is not None:
            preview=pd.read_excel(file,header=None,nrows=HEADER_SEARCH_ROWS,**kwargs)
            header_row=_find_header_row(preview.itertuples(index=False),key_col)
            if header_row is not None:
                options['header']=header_row

    options.update(kwargs) #explicit user options take precedence
    return get_readers(**options)[extension](file)

def clean_dataframe(df:pd.DataFrame)->pd.DataFrame:
    '''Strip column names and string cells, turn empty strings into NaN, drop empty unnamed columns'''
    df=df.copy()
    df.columns=[str(c).replace('﻿','').strip() for c in df.columns]
    for col in df.columns[df.dtypes.eq(object)]:
        df[col]=df[col].map(lambda v: v.strip() if isinstance(v,str) else v)
    df=df.replace('',pd.NA)
    #trailing separators create empty unnamed columns
    empty_unnamed=[c for c in df.columns if (c=='' or c.startswith('Unnamed:')) and df[c].isna().all()]
    df=df.drop(columns=empty_unnamed)
    duplicated=df.columns[df.columns.duplicated()]
    if len(duplicated)>0:
        print('Warning: duplicated columns {}'.format(list(duplicated)))
    return df

def normalize_key_series(s:pd.Series)->pd.Series:
    '''Make keys comparable between files: 1, 1.0, " 1" -> "1"'''
    def normalize(v):
        if pd.isna(v):
            return pd.NA
        if isinstance(v,float) and v.is_integer():
            v=int(v)
        return str(v).strip()
    return s.map(normalize)

def load_files(file_paths:list[str],add_filenamecolumn:bool,filename_column='TABLENAME',key_col:str=None,**reader_kwargs)->list[pd.DataFrame]:
    """
    Load a list of tabular files into pd Dataframe.
    Files that cannot be read, or that do not contain key_col, are skipped.
    """
    dfs = []
    for path in file_paths:
        try:
            df=read_file(path,key_col,**reader_kwargs)
        except Exception as e:
            print('Error Reading {}: {}'.format(path,e))
            continue
        if df is None:
            print('Unsupported format, skipped: {}'.format(path))
            continue
        df=clean_dataframe(df)
        if key_col is not None:
            if key_col not in df.columns:
                print('Column "{}" not found in {}, file skipped. Columns read: {}'.format(key_col,path,list(df.columns)[:10]))
                continue
            df[key_col]=normalize_key_series(df[key_col])
        if add_filenamecolumn:
            df[filename_column]=Path(path).name
        dfs.append(df)
    return dfs


def merge_patient_dataframes(dfs, patient_col="Patient",filename_column=None)->pd.DataFrame:
    """
    Concat Dataframe with overlapping patients/variables.
    For each patient, use the first non-nan value if it exists.
    If filename_column is given, it lists all the files where the patient appears.
    """
    if len(dfs)==0:
        raise ValueError('No table to merge')

    #Vertical stacking
    combined = pd.concat(dfs, axis=0, ignore_index=True)
    missing_key=combined[patient_col].isna()
    if missing_key.any():
        print('Warning: {} rows without {} are ignored'.format(missing_key.sum(),patient_col))
        combined=combined[~missing_key]
    #groubby patient+pick first non-nan if exists
    merged = combined.groupby(patient_col, as_index=False, sort=False).first()
    if filename_column is not None and filename_column in combined.columns:
        sources=combined.groupby(patient_col,sort=False)[filename_column].agg(lambda s:';'.join(dict.fromkeys(s.dropna())))
        merged[filename_column]=merged[patient_col].map(sources)
    return merged


def merge_files(file_paths, output_path, patient_col="Patient",add_filenamecolumn=True,filename_column='TABLENAME',**reader_kwargs)->pd.DataFrame:
    """
    Read, merge, save
    """
    dfs = load_files(file_paths,add_filenamecolumn,filename_column,key_col=patient_col,**reader_kwargs)
    merged_df = merge_patient_dataframes(dfs, patient_col=patient_col,
                                         filename_column=filename_column if add_filenamecolumn else None)
    write_table(merged_df,output_path,index=False)
    return merged_df

def normalize_str_series(s:pd.Series)->pd.Series:
    return (
        s.astype(str)
        .str.casefold() #lower case and ß->ss
        .str.replace("ä", "ae", regex=False)#umlaut
        .str.replace("ö", "oe", regex=False)
        .str.replace("ü", "ue", regex=False)
        .str.replace("-", " ", regex=False)
        .str.replace(r"\s+"," ",regex=True)#multiple spaces
        .str.strip()#start and end of line
    )

def _equal_values(s1:pd.Series,s2:pd.Series)->pd.Series:
    '''True when values are equal, or when at least one of them is nan'''
    either_na=s1.isna() | s2.isna()
    if pd.api.types.is_numeric_dtype(s1) and pd.api.types.is_numeric_dtype(s2):
        equal= s1==s2
    elif pd.api.types.is_numeric_dtype(s1) or pd.api.types.is_numeric_dtype(s2):
        #mixed types, e.g. 1 vs "1"
        equal= normalize_key_series(s1)==normalize_key_series(s2)
    else:
        equal= normalize_str_series(s1)==normalize_str_series(s2)
    return (equal.fillna(False).astype(bool)) | either_na

def _joint_common_columns(df1:pd.DataFrame,df2:pd.DataFrame,joint_keys):
    if isinstance(joint_keys,str):
        joint_keys=[joint_keys]
    col_intersec=df1.columns.intersection(df2.columns).difference(joint_keys)
    joint=df1.merge(df2,on=joint_keys,suffixes=("_df1", "_df2"), how="inner")
    return joint_keys,col_intersec,joint

def _print_conflict(joint:pd.DataFrame,conflict:pd.Series,joint_keys:list[str],col:str):
    print(f"\nConflict detected in column: {col}")
    print(joint.loc[conflict, joint_keys + [f"{col}_df1", f"{col}_df2"]])


def sanity_check_dataframes(df1:pd.DataFrame,df2:pd.DataFrame,joint_keys:list[str])->bool:
    '''Check if all values of overlapping columns between df1 and df2 are equal'''
    joint_keys,col_intersec,joint=_joint_common_columns(df1,df2,joint_keys)
    if len(col_intersec)<=0:
        return True #no common column to compare
    if joint.empty:
        return False #nothing common

    consistent=True
    for col in col_intersec:
        conflict=~_equal_values(joint[col+'_df1'],joint[col+'_df2'])
        if conflict.any():
            consistent=False
            _print_conflict(joint,conflict,joint_keys,col)
    return consistent

def parse_conflicts(df1:pd.DataFrame,df2:pd.DataFrame,joint_keys:list[str])->bool:
    '''Check if all values of overlapping columns between df1 and df2 are equal
    When conflict detected, parse to decide manually whether to ignore the conflict

    returns False if no conflict is detected, or if the user validates the consistency of the dataframes
    '''
    if df1.empty or df2.empty:
        print('Empty dataframe')
        return False
    joint_keys,col_intersec,joint=_joint_common_columns(df1,df2,joint_keys)
    if len(col_intersec)<=0 or joint.empty:
        return False #nothing to compare

    has_conflict=False
    for col in col_intersec:
        conflict=~_equal_values(joint[col+'_df1'],joint[col+'_df2'])
        if conflict.any():
            _print_conflict(joint,conflict,joint_keys,col)
            proceed=input('Is conflict OK or not Y/N ')
            if proceed.strip().lower()!='y':
                has_conflict=True

    return has_conflict

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
        inputs=extract_files(args.directory,exclude=[args.output])
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
