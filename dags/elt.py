def schema1_definition():
    from snowflake.snowpark import types as T
    load_schema1 = T.StructType([T.StructField("TRIPDURATION", T.StringType()),
                             T.StructField("STARTTIME", T.StringType()), 
                             T.StructField("STOPTIME", T.StringType()), 
                             T.StructField("START_STATION_ID", T.StringType()),
                             T.StructField("START_STATION_NAME", T.StringType()), 
                             T.StructField("START_STATION_LATITUDE", T.StringType()),
                             T.StructField("START_STATION_LONGITUDE", T.StringType()),
                             T.StructField("END_STATION_ID", T.StringType()),
                             T.StructField("END_STATION_NAME", T.StringType()), 
                             T.StructField("END_STATION_LATITUDE", T.StringType()),
                             T.StructField("END_STATION_LONGITUDE", T.StringType()),
                             T.StructField("BIKEID", T.StringType()),
                             T.StructField("USERTYPE", T.StringType()), 
                             T.StructField("BIRTH_YEAR", T.StringType()),
                             T.StructField("GENDER", T.StringType())])
    return load_schema1

def schema2_definition():
    from snowflake.snowpark import types as T
    load_schema2 = T.StructType([T.StructField("ride_id", T.StringType()), 
                             T.StructField("rideable_type", T.StringType()), 
                             T.StructField("STARTTIME", T.StringType()), 
                             T.StructField("STOPTIME", T.StringType()), 
                             T.StructField("START_STATION_NAME", T.StringType()), 
                             T.StructField("START_STATION_ID", T.StringType()),
                             T.StructField("END_STATION_NAME", T.StringType()), 
                             T.StructField("END_STATION_ID", T.StringType()),
                             T.StructField("START_STATION_LATITUDE", T.StringType()),
                             T.StructField("START_STATION_LONGITUDE", T.StringType()),
                             T.StructField("END_STATION_LATITUDE", T.StringType()),
                             T.StructField("END_STATION_LONGITUDE", T.StringType()),
                             T.StructField("USERTYPE", T.StringType())])
    return load_schema2

def conformed_schema():
    from snowflake.snowpark import types as T
    trips_table_schema = T.StructType([T.StructField("STARTTIME", T.StringType()), 
                             T.StructField("STOPTIME", T.StringType()), 
                             T.StructField("START_STATION_NAME", T.StringType()), 
                             T.StructField("START_STATION_ID", T.StringType()),
                             T.StructField("END_STATION_NAME", T.StringType()), 
                             T.StructField("END_STATION_ID", T.StringType()),
                             T.StructField("START_STATION_LATITUDE", T.StringType()),
                             T.StructField("START_STATION_LONGITUDE", T.StringType()),
                             T.StructField("END_STATION_LATITUDE", T.StringType()),
                             T.StructField("END_STATION_LONGITUDE", T.StringType()),
                             T.StructField("USERTYPE", T.StringType())])
    return trips_table_schema

def extract_trips_to_stage(session, files_to_download: list, download_base_url: str, load_stage_name:str):
    import os 
    import requests
    from zipfile import ZipFile
    import gzip
    from datetime import datetime
    from io import BytesIO
    
    schema1_download_files = list()
    schema2_download_files = list()
    schema2_start_date = datetime.strptime('202102', "%Y%m")

    for file_name in files_to_download:
        file_start_date = datetime.strptime(file_name.split("-")[0], "%Y%m")
        if file_start_date < schema2_start_date:
            schema1_download_files.append(file_name)
        else:
            schema2_download_files.append(file_name)
         
        
    schema1_load_stage = load_stage_name+'/schema1/'
    schema1_files_to_load = list()
    for zip_file_name in schema1_download_files:

        url = download_base_url+zip_file_name

        print('Downloading and unzipping: '+url)
        r = requests.get(url)
        file = ZipFile(BytesIO(r.content))
        csv_file_name=file.namelist()[0]
        file.extract(csv_file_name)
        file.close()

        print('Putting '+csv_file_name+' to stage: '+schema1_load_stage)
        session.file.put(local_file_name=csv_file_name, 
                         stage_location=schema1_load_stage, 
                         source_compression='NONE', 
                         overwrite=True)
        schema1_files_to_load.append(csv_file_name)
        os.remove(csv_file_name)

        
    schema2_load_stage = load_stage_name+'/schema2/'
    schema2_files_to_load = list()
    for zip_file_name in schema2_download_files:

        url = download_base_url+zip_file_name

        print('Downloading and unzipping: '+url)
        r = requests.get(url)
        file = ZipFile(BytesIO(r.content))
        csv_file_name=file.namelist()[0]
        file.extract(csv_file_name)
        file.close()

        print('Putting '+csv_file_name+' to stage: '+schema2_load_stage)
        session.file.put(local_file_name=csv_file_name, 
                         stage_location=schema2_load_stage, 
                         source_compression='NONE', 
                         overwrite=True)
        schema2_files_to_load.append(csv_file_name)
        os.remove(csv_file_name)
        
    load_stage_names = {'schema1' : schema1_load_stage, 'schema2' : schema2_load_stage}
    files_to_load = {'schema1': schema1_files_to_load, 'schema2': schema2_files_to_load}

    return load_stage_names, files_to_load
    
def load_trips_to_raw(session, files_to_load:dict, load_stage_names:dict, load_table_name:str):
    from snowflake.snowpark import functions as F
    from snowflake.snowpark import types as T
    from datetime import datetime
    
    csv_file_format_options = {"FIELD_OPTIONALLY_ENCLOSED_BY": "'\"'", "skip_header": 1}

    if len(files_to_load['schema1']) > 0:
        load_schema1 = schema1_definition()
        loaddf = session.read.option("SKIP_HEADER", 1)\
                         .option("FIELD_OPTIONALLY_ENCLOSED_BY", "\042")\
                         .option("COMPRESSION", "GZIP")\
                         .option("NULL_IF", "\\\\N")\
                         .option("NULL_IF", "NULL")\
                         .schema(load_schema1)\
                         .csv('@'+load_stage_names['schema1'])\
                         .copy_into_table(load_table_name+'schema1', 
                                          files=files_to_load['schema1'],
                                          format_type_options=csv_file_format_options)
                              
    if len(files_to_load['schema2']) > 0:
        load_schema2 = schema2_definition()
        loaddf = session.read.option("SKIP_HEADER", 1)\
                         .option("FIELD_OPTIONALLY_ENCLOSED_BY", "\042")\
                         .option("COMPRESSION", "GZIP")\
                         .option("NULL_IF", "\\\\N")\
                         .option("NULL_IF", "NULL")\
                         .schema(load_schema2)\
                         .csv('@'+load_stage_names['schema2'])\
                         .copy_into_table(load_table_name+'schema2', 
                                          files=files_to_load['schema2'],
                                          format_type_options=csv_file_format_options)
        
    load_table_names = {'schema1' : load_table_name+str('schema1'), 
                         'schema2' : load_table_name+str('schema2')}
                         
    return load_table_names
    
def transform_trips(session, stage_table_names:dict, trips_table_name:str):
    from snowflake.snowpark import functions as F
        
    #Change all dates to YYYY-MM-DD HH:MI:SS format
    date_format_match = "^([0-9]?[0-9])/([0-9]?[0-9])/([0-9][0-9][0-9][0-9]) ([0-9]?[0-9]):([0-9][0-9])(:[0-9][0-9])?.*$"
    date_format_repl = "\\3-\\1-\\2 \\4:\\5\\6"

    trips_table_schema = conformed_schema()
                         
    trips_table_schema_names = [field.name for field in trips_table_schema.fields]
                         
    transdf1 = session.table(stage_table_names['schema1'])[trips_table_schema_names]
    transdf2 = session.table(stage_table_names['schema2'])[trips_table_schema_names]
                         
    transdf = transdf1.union_by_name(transdf2)\
                      .with_column('STARTTIME', F.regexp_replace(F.col('STARTTIME'),
                                                                F.lit(date_format_match), 
                                                                F.lit(date_format_repl)))\
                      .with_column('STARTTIME', F.to_timestamp('STARTTIME'))\
                      .with_column('STOPTIME', F.regexp_replace(F.col('STOPTIME'),
                                                               F.lit(date_format_match), 
                                                               F.lit(date_format_repl)))\
                      .with_column('STOPTIME', F.to_timestamp('STOPTIME'))\
                      .write.mode('overwrite').save_as_table(trips_table_name)

    return trips_table_name

def reset_database(session, state_dict:dict, prestaged=False):
    _ = session.sql('CREATE OR REPLACE DATABASE '+state_dict['connection_parameters']['database']).collect()
    _ = session.sql('CREATE SCHEMA '+state_dict['connection_parameters']['schema']).collect() 

    if prestaged:
        sql_cmd = 'CREATE OR REPLACE STAGE '+state_dict['load_stage_name']+\
                  ' url='+state_dict['connection_parameters']['download_base_url']+\
                  " credentials=(aws_role='"+state_dict['connection_parameters']['download_role_ARN']+"')"
        _ = session.sql(sql_cmd).collect()
    else: 
        _ = session.sql('CREATE STAGE IF NOT EXISTS '+state_dict['load_stage_name']).collect()

    load_schema1=schema1_definition()
    session.createDataFrame([[None]*len(load_schema1.names)], schema=load_schema1)\
           .na.drop()\
           .write\
           .saveAsTable(state_dict['load_table_name']+'schema1')

    load_schema2=schema2_definition()
    session.createDataFrame([[None]*len(load_schema2.names)], schema=load_schema2)\
       .na.drop()\
       .write\
       .saveAsTable(state_dict['load_table_name']+'schema2')
