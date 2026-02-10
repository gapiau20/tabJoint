import pandas as pd
from functools import reduce
from pathlib import Path
import os
import argparse

def get_readers(**kwargs):
    return{
            '.csv':lambda x:pd.read_csv(x,**kwargs),
            '.tsv':lambda x:pd.read_tsv(x,**kwargs),
            '.xlsx':lambda x:pd.read_excel(x,**kwargs),
            '.xls':lambda x:pd.read_excel(x,**kwargs),
        }

def extract_files(input_directory):
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


