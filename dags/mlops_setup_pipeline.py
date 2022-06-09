from dags.mlops_tasks import snowpark_database_setup
from dags.mlops_tasks import initial_bulk_load_task
from dags.mlops_tasks import materialize_holiday_task
from dags.mlops_tasks import subscribe_to_weather_data_task
from dags.mlops_tasks import create_weather_view_task
from dags.mlops_tasks import deploy_model_udf_task
from dags.mlops_tasks import deploy_eval_udf_task
from dags.mlops_tasks import generate_feature_table_task
from dags.mlops_tasks import generate_forecast_table_task
from dags.mlops_tasks import bulk_train_predict_task
from dags.mlops_tasks import eval_station_models_task 
from dags.mlops_tasks import flatten_tables_task

def citibikeml_setup_taskflow(run_date:str):
    """
    End to end Snowflake ML Demo
    """
    import uuid
    import json

    with open('./include/state.json') as sdf:
        state_dict = json.load(sdf)
    
    model_id = str(uuid.uuid1()).replace('-', '_')

    state_dict.update({'model_id': model_id})
    state_dict.update({'run_date': run_date})
    state_dict.update({'weather_database_name': 'WEATHER_NYC'})
    state_dict.update({'load_table_name': 'RAW_',
                       'trips_table_name': 'TRIPS',
                       'load_stage_name': 'LOAD_STAGE',
                       'model_stage_name': 'MODEL_STAGE',
                       'weather_table_name': state_dict['weather_database_name']+'.ONPOINT_ID.HISTORY_DAY',
                       'weather_view_name': 'WEATHER_NYC_VW',
                       'holiday_table_name': 'HOLIDAYS',
                       'clone_table_name': 'CLONE_'+model_id,
                       'feature_table_name' : 'FEATURE_'+model_id,
                       'pred_table_name': 'PRED_'+model_id,
                       'eval_table_name': 'EVAL_'+model_id,
                       'forecast_table_name': 'FORECAST_'+model_id,
                       'forecast_steps': 30,
                       'train_udf_name': 'station_train_predict_udf',
                       'train_func_name': 'station_train_predict_func',
                       'eval_udf_name': 'eval_model_output_udf',
                       'eval_func_name': 'eval_model_func'
                      })
    
    #Task order - one-time setup
    setup_state_dict = snowpark_database_setup(state_dict)
    load_state_dict = initial_bulk_load_task(setup_state_dict)
    holiday_state_dict = materialize_holiday_task(setup_state_dict)
    subscribe_state_dict = subscribe_to_weather_data_task(setup_state_dict)
    weather_state_dict = create_weather_view_task(subscribe_state_dict)
    model_udf_state_dict = deploy_model_udf_task(setup_state_dict)
    eval_udf_state_dict = deploy_eval_udf_task(setup_state_dict)
    feature_state_dict = generate_feature_table_task(load_state_dict, holiday_state_dict, weather_state_dict) 
    foecast_state_dict = generate_forecast_table_task(load_state_dict, holiday_state_dict, weather_state_dict)
    pred_state_dict = bulk_train_predict_task(model_udf_state_dict, feature_state_dict, foecast_state_dict)
    eval_state_dict = eval_station_models_task(eval_udf_state_dict, pred_state_dict, run_date)  
    state_dict = flatten_tables_task(pred_state_dict, eval_state_dict)

    return state_dict
