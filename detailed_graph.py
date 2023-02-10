import networkx as nx
import pandas as pd 
import numpy as np 
import logging 
import semantic_version as sv 

def long(df: pd.DataFrame) -> pd.DataFrame:
    '''Melt the dataframe pivoting on pkg_id, latest_version, & publish_time. 
    Dep_type stores type of dependency'''
    return pd.melt(
        df, id_vars=['pkg_id','latest_version','publish_time'], 
        var_name='dep_type', value_name='dep_value'
    )

def semver_unwrapper(row) -> str:
    '''Checks if the semver is compatible with the latest version of the dependency'''
    try:
        return str(sv.NpmSpec(row.dep_semver).select(row.version_obj))
    except TypeError as e:
        return 'Not in all_versions'
    except Exception as e:
        with open('semver_unwrapping_errors.txt', 'a') as f:
            f.write(f'{type(e).__name__}, {e.args}, {row.dep_name}, {row.dep_semver}\n')
        return 'Parsing dep_semver'

def open_graph(graph_path: str) -> pd.DataFrame:
    '''Read the graph from the path provided'''
    return pd.read_pickle(graph_path)

def handle_node_npm(df: pd.DataFrame) -> pd.DataFrame:
    '''Append 'node_' to front of dep_value for rows where dep_type == node and 'node_' to front of dep_value for rows where dep_type == npm'''
    df.loc[df['dep_type'] == 'node', 'dep_value'] = 'node_' + df['dep_value'].astype(str)
    df.loc[df['dep_type'] == 'npm', 'dep_value'] = 'npm_' + df['dep_value'].astype(str)
    return df

def convert_columns_to_dict(df: pd.DataFrame) -> pd.DataFrame:
    df.loc[
        (df.dep_type=='dependencies') | (df.dep_type=='dev') |
        (df.dep_type=='peer') | (df.dep_type=='optional'), 
        'dep_value'
    ] = df.loc[
        (df.dep_type=='dependencies') | (df.dep_type=='dev') |
        (df.dep_type=='peer') | (df.dep_type=='optional'),
        'dep_value'
    ].apply(lambda x: try_literal_eval(x))
    return df

def unpack_dependencies(df: pd.DataFrame) -> pd.DataFrame:
    '''Convert the columns to dictionaries'''
    df = convert_columns_to_dict(df)
    df['dep_id'] = df.loc[
        (df.dep_type=='dependencies') | (df.dep_type=='dev') |
        (df.dep_type=='peer') | (df.dep_type=='optional'), 'dep_value'
    ].apply(lambda x: create_dep_id(x))
    df.loc[(df.dep_type=='node') | (df.dep_type=='npm'), 'dep_id'] = df.loc[
        (df.dep_type=='node') | (df.dep_type=='npm'), 'dep_value'
    ].apply(lambda x: splitter(x))
    return df 

def explode_dependencies(df: pd.DataFrame) -> pd.DataFrame:
    df = unpack_dependencies(df)
    return df.explode('dep_id')

def split_name_semver(df: pd.DataFrame) -> pd.DataFrame:
    df['dep_name'] = df['dep_id'].str.split('_').str[0]
    df['dep_semver'] = df['dep_id'].str.split('_').str[1].str.replace(' ', '')
    return df

def latest_version_deps(df):
    tracker = {}
    for dep in df['dep_name'].unique():
        # Find the latest version for the dep 
        max_version = max(df[df['dep_name']==dep]['latest_version'], default='0.0.0')
        tracker[dep] = max_version
    # Bulk update the dataframe
    df['dep_latest_version'] = df.apply(lambda x: tracker[x['dep_name']], axis=1)
    return df 

def replace_underscore(df: pd.DataFrame) -> pd.DataFrame:
    df['dep_value'] = df['dep_value'].apply(lambda x: {k.replace('_', '-'): v.replace('_', '-') for k,v in x.items()})
    return df

def get_latest_versions(df: pd.DataFrame) -> pd.DataFrame:
    return df[['pkg_name','latest_version']].drop_duplicates()

def get_publish_times(df: pd.DataFrame) -> pd.DataFrame:
    return df[['pkg_id','publish_time']].drop_duplicates()

def convert_to_sv_version(x)->list:
    try:
        return sv.Version(x)
    except Exception as ex:
        with open('errors.txt', 'a') as f:
            f.write(f'{x}, {type(ex).__name__}, {ex.args}\n')

def get_versions_dep(df,x):
    '''Returns a list of versions for a dependency'''
    try:
        return df[df.pkg_name==x]['versions'].apply(convert_to_sv_version).values
    except:
        raise ValueError(f'No versions, or pkg_name missing column, or pkg missing from df: {x}')

def get_all_versions() -> pd.DataFrame:
    '''Returns a list of available versions for a package'''
    import io 
    with io.open(f'../pickles/all_pkg_versions.pkl', 'rb') as f:
        versions = pd.read_pickle(f)
    versions['version_obj'] = versions.versions.apply(convert_to_sv_version)
    versions = versions[['pkg_name', 'version_obj']]
    versions = versions[versions.version_obj.notnull()]
    versions = versions.groupby('pkg_name').agg({'version_obj': lambda x: list(x)})#.reset_index().set_index('pkg_name')
    return versions 

def clean(df,col) -> pd.DataFrame:
    '''Clean the dataframe'''
    df = df[df[col].notnull()]
    df = df[df[col].notna()]
    df = df[df[col]!='']
    return df

def semver_handler(df: pd.DataFrame) -> pd.DataFrame:
    '''Semver pipeline'''
    df = df[['dep_id','dep_name','dep_semver']].drop_duplicates()
    print(f'After dropping duplicates, now {len(df)}, Getting all_versions')

    all_versions = get_all_versions()
    print(f'Got all versions, now {len(all_versions)}')

    df = df.merge(all_versions, how='left', left_on='dep_name', right_index=True)
    print(f'After merging, now {len(df)}')

    df['highest_ver'] = df.apply(lambda x: semver_unwrapper(x), axis=1)
    print(df.shape)
    del all_versions
    df = df[df['highest_ver'] != 'Parsing dep_semver']
    df = df[df['highest_ver'] != 'Not in all_versions']
    return df  

def orchestrator(graph_path: str, logger: logging.Logger) -> nx.Graph:
    '''Orchestrator function to run the entire extraction pipeline'''
    # 1. Open the graph & store latest version, publish time separately
    print(f'Opening graph')
    df = open_graph(graph_path)
    #df = df.head(50000)
    df['pkg_name'] = df['pkg_id'].str.split('_').str[0]
    #latest_versions = get_latest_versions(df)
    publish_times = get_publish_times(df)
    
    # 2. Make it long, based on each type of dependency & the node/npm versions
    print(f'Converting to long format, now {len(df)}')
    df = clean(long(df), 'dep_value')

    # 3. Handle node/npm versions
    print(f'Handling node / npm, now {len(df)}')
    df = handle_node_npm(df)

    # 4. Explode the dependencies column, since each dependency is a list
    print(f'Exploding dependencies, now {len(df)}')
    df = explode_dependencies(df)
    
    # 5. Adjust columns
    print(f'Dropping columns, now {len(df)}')
    df = clean(df[['pkg_id','dep_id','dep_type']].drop_duplicates(), 'dep_id')
    df = split_name_semver(df)

    # 6. Handle semantic version
    print(f'Starting semantic version process, now {len(df)}')
    deps = semver_handler(df)
    df = pd.merge(df, deps[['dep_id', 'highest_ver']], how='left', on='dep_id')

    # 7. Add final columns -- publish time, dependency id
    print(f'Adding final columns, now {len(df)}')
    df['dependency_id'] = df['dep_name'] + '_' + df['highest_ver']
    df = pd.merge(df, publish_times, how='left', left_on='dependency_id', right_on='pkg_id')
    df = df.drop_duplicates()
    # If publish_times is na or null, then replace with 0
    df['publish_time'] = df['publish_time'].fillna(pd.Timestamp.now())
    df['exp_age_500'] = np.exp(df['publish_time'].apply(lambda x: (pd.Timestamp.now() - x).days)/500)
    
    # 8. Drop duplicates & clean
    print(f'Cleaning, now {len(df)}')
    df = df[['pkg_id_x','dependency_id','dep_type','dep_semver','exp_age_500']].drop_duplicates()
    df.rename(columns={'pkg_id_x':'pkg_id','dependency_id':'dep_id'}, inplace=True)
    df = clean(df,'dep_id')
    
    # 9. Remove any records where the dependency is the same as the package
    print(f'Removing self dependencies, now {len(df)}')
    df = df[df['pkg_id'] != df['dep_id']]
    print(f'Done, now {len(df)}')
    return df

def try_literal_eval(x):
    from ast import literal_eval
    import numpy as np 
    try:
        return literal_eval(x)
    except ValueError:
        print(x)
        return np.nan
    except SyntaxError:
        return np.nan
    except Exception:
        return np.nan 

def create_dep_id(x):
    try:
        return [f'{k}_{v}' for k,v in x.items()]
    except Exception:
        print(x, type(x).__name__)
    return np.nan

def splitter(x):
    try:
        return x.split(',')
    except Exception:
        print(x, type(x).__name__)
    return np.nan
    