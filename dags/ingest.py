def incremental_elt(session, 
                    state_dict:dict, 
                    files_to_ingest:list, 
                    download_base_url,
                    use_prestaged=False) -> str:
    
    import dags.elt as ELT
    from datetime import datetime

    load_stage_name=state_dict['load_stage_name']
    load_table_name=state_dict['load_table_name']
    trips_table_name=state_dict['trips_table_name']
    
    if use_prestaged:
        print("Skipping extract.  Using provided bucket for pre-staged files.")
        
        schema1_download_files = list()
        schema2_download_files = list()
        schema2_start_date = datetime.strptime('202102', "%Y%m")

        for file_name in files_to_ingest:
            file_start_date = datetime.strptime(file_name.split("-")[0], "%Y%m")
            if file_start_date < schema2_start_date:
                schema1_download_files.append(file_name.replace('.zip','.gz'))
            else:
                schema2_download_files.append(file_name.replace('.zip','.gz'))
        
        
        load_stage_names = {'schema1':load_stage_name+'/schema1/', 'schema2':load_stage_name+'/schema2/'}
        files_to_load = {'schema1': schema1_download_files, 'schema2': schema2_download_files}
    else:
        print("Extracting files from public location.")
        load_stage_names, files_to_load = ELT.extract_trips_to_stage(session=session, 
                                                                    files_to_download=files_to_ingest, 
                                                                    download_base_url=download_base_url, 
                                                                    load_stage_name=load_stage_name)
        
        files_to_load['schema1']=[file+'.gz' for file in files_to_load['schema1']]
        files_to_load['schema2']=[file+'.gz' for file in files_to_load['schema2']]


    print("Loading files to raw.")
    stage_table_names = ELT.load_trips_to_raw(session=session, 
                                              files_to_load=files_to_load, 
                                              load_stage_names=load_stage_names, 
                                              load_table_name=load_table_name)    
    
    print("Transforming records to trips table.")
    trips_table_name = ELT.transform_trips(session=session, 
                                           stage_table_names=stage_table_names, 
                                           trips_table_name=trips_table_name)
    return trips_table_name

def bulk_elt(session, 
             state_dict:dict,
             download_base_url, 
             use_prestaged=False) -> str:
    
    #import dags.elt as ELT
    from dags.ingest import incremental_elt
    
    import pandas as pd
    from datetime import datetime

    #Create a list of filenames to download based on date range
    #For files like 201306-citibike-tripdata.zip
    date_range1 = pd.period_range(start=datetime.strptime("201306", "%Y%m"), 
                                 end=datetime.strptime("201612", "%Y%m"), 
                                 freq='M').strftime("%Y%m")
    file_name_end1 = '-citibike-tripdata.zip'
    files_to_extract = [date+file_name_end1 for date in date_range1.to_list()]

    #For files like 201701-citibike-tripdata.csv.zip
    date_range2 = pd.period_range(start=datetime.strptime("201701", "%Y%m"), 
                                 end=datetime.strptime("201912", "%Y%m"), 
                                 freq='M').strftime("%Y%m")
    
    file_name_end2 = '-citibike-tripdata.csv.zip'
    
    files_to_extract = files_to_extract + [date+file_name_end2 for date in date_range2.to_list()]        

    trips_table_name = incremental_elt(session=session, 
                                       state_dict=state_dict, 
                                       files_to_ingest=files_to_extract, 
                                       use_prestaged=use_prestaged,
                                       download_base_url=download_base_url)
    
    return trips_table_name
