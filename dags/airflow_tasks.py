
from airflow.decorators import task

@task()
def snowpark_database_setup(state_dict:dict)-> dict: 
    import snowflake.snowpark.functions as F
    from dags.snowpark_connection import snowpark_connect
    from dags.elt import reset_database

    session, _ = snowpark_connect('./include/state.json')
    reset_database(session=session, state_dict=state_dict, prestaged=True)

    _ = session.sql('CREATE STAGE '+state_dict['model_stage_name']).collect()
    _ = session.sql('CREATE TAG model_id_tag').collect()

    session.close()

    return state_dict

@task()
def incremental_elt_task(state_dict: dict, files_to_download:list)-> dict:
    from dags.ingest import incremental_elt
    from dags.snowpark_connection import snowpark_connect

    session, _ = snowpark_connect()

    print('Ingesting '+str(files_to_download))
    download_role_ARN=state_dict['connection_parameters']['download_role_ARN']
    download_base_url=state_dict['connection_parameters']['download_base_url']

    _ = session.use_warehouse(state_dict['compute_parameters']['load_warehouse'])

    _ = incremental_elt(session=session, 
                        state_dict=state_dict, 
                        files_to_ingest=files_to_download,
                        download_role_ARN=download_role_ARN,
                        download_base_url=download_base_url)

    #_ = session.sql('ALTER WAREHOUSE IF EXISTS '+state_dict['compute_parameters']['load_warehouse']+\
    #                ' SUSPEND').collect()

    session.close()
    return state_dict

@task()
def initial_bulk_load_task(state_dict:dict)-> dict:
    from dags.snowpark_connection import snowpark_connect
    from dags.ingest import bulk_elt
    from dags.elt import schema1_definition, schema2_definition

    session, _ = snowpark_connect()
    
    download_role_ARN=state_dict['connection_parameters']['download_role_ARN']
    download_base_url=state_dict['connection_parameters']['download_base_url']

    print('Running initial bulk ingest from '+download_base_url)
    
    #create empty ingest tables
    load_schema1 = schema1_definition()
    session.create_dataframe([[None]*len(load_schema1.names)], schema=load_schema1)\
           .na.drop()\
           .write\
           .save_as_table(state_dict['load_table_name']+'schema1')

    load_schema2 = schema2_definition()
    session.create_dataframe([[None]*len(load_schema2.names)], schema=load_schema2)\
           .na.drop()\
           .write\
           .save_as_table(state_dict['load_table_name']+'schema2')

    _ = session.use_warehouse(state_dict['compute_parameters']['load_warehouse'])

    _ = bulk_elt(session=session, 
                 state_dict=state_dict, 
                 download_role_ARN=download_role_ARN,
                 download_base_url=download_base_url)

    #_ = session.sql('ALTER WAREHOUSE IF EXISTS '+state_dict['compute_parameters']['load_warehouse']+\
    #                ' SUSPEND').collect()

    session.close()
    return state_dict

@task()
def materialize_holiday_task(state_dict: dict)-> dict:
    from dags.snowpark_connection import snowpark_connect
    from dags.mlops_pipeline import materialize_holiday_table

    print('Materializing holiday table.')
    session, _ = snowpark_connect()

    _ = materialize_holiday_table(session=session, 
                                  holiday_table_name=state_dict['holiday_table_name'])

    session.close()
    return state_dict

@task()
def materialize_weather_task(state_dict: dict)-> dict:
    from dags.snowpark_connection import snowpark_connect
    from dags.mlops_pipeline import materialize_weather_table

    print('Materializing weather table')
    session, _ = snowpark_connect()

    _ = materialize_weather_table(session=session,
                                  weather_table_name=state_dict['weather_table_name'])
    session.close()
    return state_dict

@task()
def deploy_model_udf_task(state_dict:dict)-> dict:
    from dags.snowpark_connection import snowpark_connect
    from dags.mlops_pipeline import deploy_pred_train_udf

    print('Deploying station model')
    session, _ = snowpark_connect()

    _ = session.sql('CREATE STAGE IF NOT EXISTS ' + state_dict['model_stage_name']).collect()

    _ = deploy_pred_train_udf(session=session, 
                              udf_name=state_dict['train_udf_name'],
                              function_name=state_dict['train_func_name'],
                              model_stage_name=state_dict['model_stage_name'])
    session.close()
    return state_dict

@task()
def deploy_eval_udf_task(state_dict:dict)-> dict:
    from dags.snowpark_connection import snowpark_connect
    from dags.mlops_pipeline import deploy_eval_udf

    print('Deploying station model')
    session, _ = snowpark_connect()

    _ = session.sql('CREATE STAGE IF NOT EXISTS ' + state_dict['model_stage_name']).collect()

    _ = deploy_eval_udf(session=session, 
                        udf_name=state_dict['eval_udf_name'],
                        function_name=state_dict['eval_func_name'],
                        model_stage_name=state_dict['model_stage_name'])
    session.close()
    return state_dict

@task()
def generate_feature_table_task(state_dict:dict, 
                                holiday_state_dict:dict, 
                                weather_state_dict:dict)-> dict:
    from dags.snowpark_connection import snowpark_connect
    from dags.mlops_pipeline import create_feature_table

    print('Generating features for all stations.')
    session, _ = snowpark_connect()

    session.use_warehouse(state_dict['compute_parameters']['fe_warehouse'])

    _ = session.sql("CREATE OR REPLACE TABLE "+state_dict['clone_table_name']+\
                    " CLONE "+state_dict['trips_table_name']).collect()
    _ = session.sql("ALTER TABLE "+state_dict['clone_table_name']+\
                    " SET TAG model_id_tag = '"+state_dict['model_id']+"'").collect()

    _ = create_feature_table(session, 
                             trips_table_name=state_dict['clone_table_name'], 
                             holiday_table_name=state_dict['holiday_table_name'], 
                             weather_table_name=state_dict['weather_table_name'],
                             feature_table_name=state_dict['feature_table_name'])

    _ = session.sql("ALTER TABLE "+state_dict['feature_table_name']+\
                    " SET TAG model_id_tag = '"+state_dict['model_id']+"'").collect()

    session.close()
    return state_dict

@task()
def generate_forecast_table_task(state_dict:dict, 
                                 holiday_state_dict:dict, 
                                 weather_state_dict:dict)-> dict: 
    from dags.snowpark_connection import snowpark_connect
    from dags.mlops_pipeline import create_forecast_table

    print('Generating forecast features.')
    session, _ = snowpark_connect()

    _ = create_forecast_table(session, 
                              trips_table_name=state_dict['trips_table_name'],
                              holiday_table_name=state_dict['holiday_table_name'], 
                              weather_table_name=state_dict['weather_table_name'], 
                              forecast_table_name=state_dict['forecast_table_name'],
                              steps=state_dict['forecast_steps'])

    _ = session.sql("ALTER TABLE "+state_dict['forecast_table_name']+\
                    " SET TAG model_id_tag = '"+state_dict['model_id']+"'").collect()

    session.close()
    return state_dict

@task()
def bulk_train_predict_task(state_dict:dict, 
                            feature_state_dict:dict, 
                            forecast_state_dict:dict)-> dict: 
    from dags.snowpark_connection import snowpark_connect
    from dags.mlops_pipeline import train_predict

    state_dict = feature_state_dict

    print('Running bulk training and forecast.')
    session, _ = snowpark_connect()

    session.use_warehouse(state_dict['compute_parameters']['train_warehouse'])

    pred_table_name = train_predict(session, 
                                    station_train_pred_udf_name=state_dict['train_udf_name'], 
                                    feature_table_name=state_dict['feature_table_name'], 
                                    forecast_table_name=state_dict['forecast_table_name'],
                                    pred_table_name=state_dict['pred_table_name'])

    _ = session.sql("ALTER TABLE "+state_dict['pred_table_name']+\
                    " SET TAG model_id_tag = '"+state_dict['model_id']+"'").collect()
    #_ = session.sql('ALTER WAREHOUSE IF EXISTS '+state_dict['compute_parameters']['train_warehouse']+\
    #                ' SUSPEND').collect()

    session.close()
    return state_dict

@task()
def eval_station_models_task(state_dict:dict, 
                             pred_state_dict:dict,
                             run_date:str)-> dict:

    from dags.snowpark_connection import snowpark_connect
    from dags.mlops_pipeline import evaluate_station_model

    print('Running eval UDF for model output')
    session, _ = snowpark_connect()

    eval_table_name = evaluate_station_model(session, 
                                             run_date=run_date, 
                                             eval_model_udf_name=state_dict['eval_udf_name'], 
                                             pred_table_name=state_dict['pred_table_name'], 
                                             eval_table_name=state_dict['eval_table_name'])

    _ = session.sql("ALTER TABLE "+state_dict['eval_table_name']+\
                    " SET TAG model_id_tag = '"+state_dict['model_id']+"'").collect()
    session.close()
    return state_dict                                               

@task()
def flatten_tables_task(pred_state_dict:dict, state_dict:dict)-> dict:
    from dags.snowpark_connection import snowpark_connect
    from dags.mlops_pipeline import flatten_tables

    print('Flattening tables for end-user consumption.')
    session, _ = snowpark_connect()

    flat_pred_table, flat_forecast_table, flat_eval_table = flatten_tables(session,
                                                                           pred_table_name=state_dict['pred_table_name'], 
                                                                           forecast_table_name=state_dict['forecast_table_name'], 
                                                                           eval_table_name=state_dict['eval_table_name'])
    state_dict['flat_pred_table'] = flat_pred_table
    state_dict['flat_forecast_table'] = flat_forecast_table
    state_dict['flat_eval_table'] = flat_eval_table

    _ = session.sql("ALTER TABLE "+flat_pred_table+" SET TAG model_id_tag = '"+state_dict['model_id']+"'").collect()
    _ = session.sql("ALTER TABLE "+flat_forecast_table+" SET TAG model_id_tag = '"+state_dict['model_id']+"'").collect()
    _ = session.sql("ALTER TABLE "+flat_eval_table+" SET TAG model_id_tag = '"+state_dict['model_id']+"'").collect()

    return state_dict
