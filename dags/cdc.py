def schema1_spoc_str(procedure_name:str, interim_target_table_name:str, stream_name:str) -> str:
    create_spoc_sql="CREATE OR REPLACE PROCEDURE "+procedure_name+"() " + \
                    "RETURNS VARCHAR " + \
                    "LANGUAGE SQL " + \
                    "AS " + \
                    "$$ " + \
                    "    BEGIN " + \
                    "        MERGE INTO " + interim_target_table_name + \
                    "           AS T USING (SELECT * FROM " + stream_name + ") \
                                AS S ON concat(T.BIKEID, T.STARTTIME, T.STOPTIME) = concat(S.BIKEID, S.STARTTIME, S.STOPTIME) \
                                WHEN MATCHED AND S.metadata$action = 'INSERT' \
                                             AND S.metadata$isupdate \
                                  THEN UPDATE SET T.TRIPDURATION = S.TRIPDURATION, \
                                                T.STARTTIME = S.STARTTIME, \
                                                T.STOPTIME = S.STOPTIME, \
                                                T.START_STATION_ID = S.START_STATION_ID, \
                                                T.START_STATION_NAME = S.START_STATION_NAME, \
                                                T.START_STATION_LATITUDE = S.START_STATION_LATITUDE, \
                                                T.START_STATION_LONGITUDE = S.START_STATION_LONGITUDE, \
                                                T.END_STATION_ID = S.END_STATION_ID, \
                                                T.END_STATION_NAME = S.END_STATION_NAME, \
                                                T.END_STATION_LATITUDE = S.END_STATION_LATITUDE, \
                                                T.END_STATION_LONGITUDE = S.END_STATION_LONGITUDE, \
                                                T.BIKEID = S.BIKEID, \
                                                T.USERTYPE = S.USERTYPE, \
                                                T.BIRTH_YEAR = S.BIRTH_YEAR, \
                                                T.GENDER = S.GENDER \
                                WHEN MATCHED AND S.metadata$action = 'DELETE' \
                                  THEN DELETE \
                                WHEN NOT MATCHED AND S.metadata$action = 'INSERT' \
                                  THEN INSERT (TRIPDURATION, \
                                                STARTTIME , \
                                                STOPTIME , \
                                                START_STATION_ID , \
                                                START_STATION_NAME , \
                                                START_STATION_LATITUDE , \
                                                START_STATION_LONGITUDE , \
                                                END_STATION_ID , \
                                                END_STATION_NAME , \
                                                END_STATION_LATITUDE , \
                                                END_STATION_LONGITUDE , \
                                                BIKEID , \
                                                USERTYPE , \
                                                BIRTH_YEAR , \
                                                GENDER) \
                                        VALUES (S.TRIPDURATION, \
                                                S.STARTTIME , \
                                                S.STOPTIME , \
                                                S.START_STATION_ID , \
                                                S.START_STATION_NAME , \
                                                S.START_STATION_LATITUDE , \
                                                S.START_STATION_LONGITUDE , \
                                                S.END_STATION_ID , \
                                                S.END_STATION_NAME , \
                                                S.END_STATION_LATITUDE , \
                                                S.END_STATION_LONGITUDE , \
                                                S.BIKEID , \
                                                S.USERTYPE , \
                                                S.BIRTH_YEAR , \
                                                S.GENDER); " + \
                    "    END; " + \
                    "$$"
    
    return create_spoc_sql

def schema2_spoc_str(procedure_name:str, interim_target_table_name:str, stream_name:str) -> str:
    create_spoc_sql="CREATE OR REPLACE PROCEDURE "+procedure_name+"() " + \
                    "RETURNS VARCHAR " + \
                    "LANGUAGE SQL " + \
                    "AS " + \
                    "$$ " + \
                    "    BEGIN " + \
                    "        MERGE INTO " + interim_target_table_name + \
                    "           AS T USING (SELECT * FROM " + stream_name + ") \
                                AS S ON T.RIDE_ID = S.RIDE_ID \
                                WHEN MATCHED AND S.metadata$action = 'INSERT' \
                                             AND S.metadata$isupdate \
                                  THEN UPDATE SET T.RIDE_ID = S.RIDE_ID, \
                                                  T.RIDEABLE_TYPE = S.RIDEABLE_TYPE, \
                                                  T.STARTTIME = S.STARTTIME, \
                                                  T.STOPTIME = S.STOPTIME, \
                                                  T.START_STATION_NAME = S.START_STATION_NAME, \
                                                  T.START_STATION_ID = S.START_STATION_ID, \
                                                  T.END_STATION_NAME = S.END_STATION_NAME, \
                                                  T.END_STATION_ID = S.END_STATION_ID, \
                                                  T.START_STATION_LATITUDE = S.START_STATION_LATITUDE, \
                                                  T.START_STATION_LONGITUDE = S.END_STATION_LATITUDE, \
                                                  T.END_STATION_LONGITUDE = S.END_STATION_LONGITUDE, \
                                                  T.USERTYPE = S.USERTYPE \
                                WHEN MATCHED AND S.metadata$action = 'DELETE' \
                                  THEN DELETE \
                                WHEN NOT MATCHED AND S.metadata$action = 'INSERT' \
                                  THEN INSERT (RIDE_ID, \
                                               RIDEABLE_TYPE, \
                                               STARTTIME, \
                                               STOPTIME, \
                                               START_STATION_NAME, \
                                               START_STATION_ID, \
                                               END_STATION_NAME, \
                                               END_STATION_ID, \
                                               START_STATION_LATITUDE, \
                                               END_STATION_LATITUDE, \
                                               END_STATION_LONGITUDE, \
                                               USERTYPE) \
                                        VALUES (S.RIDE_ID, \
                                                S.RIDEABLE_TYPE, \
                                                S.STARTTIME, \
                                                S.STOPTIME, \
                                                S.START_STATION_NAME, \
                                                S.START_STATION_ID, \
                                                S.END_STATION_NAME, \
                                                S.END_STATION_ID, \
                                                S.START_STATION_LATITUDE, \
                                                S.END_STATION_LATITUDE, \
                                                S.END_STATION_LONGITUDE, \
                                                S.USERTYPE); " + \
                    "    END; " + \
                    "$$"
    return create_spoc_sql

def load_trips_from_raw_to_interim_target_cdc(session, 
                                      stage_table_names:list, 
                                      cdc_task_warehouse_name:str):
    from datetime import datetime
    interim_target_table_names = list()
    for stage_table_name in stage_table_names:
        schema = stage_table_name.split("_")[1]
        if schema == 'schema1':
            interim_target_table_name = 'INTERIM_schema1'
            stream_name = 'STREAM_schema1'
            task_name = 'TRIPSCDCTASK_schema1'
            procedure_name = 'TRIPSCDCPROC_schema1'
            create_processcdc_procedure_statement = schema1_spoc_str(procedure_name, 
                                                                     interim_target_table_name, 
                                                                     stream_name)
            
        elif schema == 'schema2':
            interim_target_table_name = 'INTERIM_schema2'
            stream_name = 'STREAM_schema2'
            task_name = 'TRIPSCDCTASK_schema2'
            procedure_name = 'TRIPSCDCPROC_schema2'
            create_processcdc_procedure_statement = schema2_spoc_str(procedure_name, 
                                                                     interim_target_table_name, 
                                                                     stream_name)
        
        #outside the if else condition but still inside the for loop
        interim_target_table_names.append(interim_target_table_name)
        create_stream_sql ='CREATE OR REPLACE STREAM ' + stream_name + \
                       ' ON TABLE ' + stage_table_name + \
                       ' APPEND_ONLY = FALSE SHOW_INITIAL_ROWS = TRUE'
        
        create_interim_target_table_sql = 'CREATE OR REPLACE TABLE ' + interim_target_table_name +\
                                    ' LIKE ' + stage_table_name
        create_task_statement = "CREATE OR REPLACE TASK " + task_name + \
                            " WAREHOUSE='" + cdc_task_warehouse_name +"'"+ \
                            " SCHEDULE = '1 minute'"+ \
                            " WHEN SYSTEM$STREAM_HAS_DATA('" + stream_name + "')"+\
                            " AS CALL " + procedure_name + "()"
        resume_task_statement = "ALTER TASK " + task_name + " RESUME"
        
        _ = session.sql(create_stream_sql).collect()
        _ = session.sql(create_interim_target_table_sql).collect() 
        _ = session.sql(create_processcdc_procedure_statement).collect()
        _ = session.sql(create_task_statement).collect()
        _ = session.sql(resume_task_statement).collect()

    return interim_target_table_names
    
    

def cdc_elt(session, load_stage_name, files_to_download, download_base_url, load_table_name, trips_table_name) -> str:
    from citibike_ml import elt as ELT

    load_stage_name, files_to_load = ELT.extract_trips_to_stage(session=session, 
                                                                files_to_download=files_to_download, 
                                                                download_base_url=download_base_url, 
                                                                load_stage_name=load_stage_name)
    stage_table_names = ELT.load_trips_to_raw(session, 
                                              files_to_load=files_to_load, 
                                              load_stage_name=load_stage_name, 
                                              load_table_name=load_table_name)
    
    interim_target_table_names = load_trips_from_raw_to_target_cdc(session, files_to_load, load_table_name, cdc_target_table_name, stream_name, cdc_task_warehouse_name, procedure_name, full_task_name)
    
    trips_table_name = ELT.transform_trips(session=session, 
                                           stage_table_names=interim_target_table_names, 
                                           trips_table_name=trips_table_name)
    return trips_table_name

