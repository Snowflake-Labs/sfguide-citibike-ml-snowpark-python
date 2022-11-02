
from dags.mlops_tasks import incremental_elt_task
from dags.mlops_tasks import generate_feature_table_task
from dags.mlops_tasks import generate_forecast_table_task
from dags.mlops_tasks import bulk_train_predict_task
from dags.mlops_tasks import eval_station_models_task 
from dags.mlops_tasks import flatten_tables_task

def citibikeml_monthly_taskflow(files_to_download:list, run_date:str):
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
                       'train_udf_name': 'station_train_predict_udtf',
                       'train_func_name': 'StationTrainPredictFunc',
                       'eval_udf_name': 'eval_model_output_udf',
                       'eval_func_name': 'eval_model_func'
                      })
    
    #Task order - monthlyl incremental
    incr_state_dict = incremental_elt_task(state_dict, files_to_download)
    feature_state_dict = generate_feature_table_task(incr_state_dict, incr_state_dict, incr_state_dict) 
    forecast_state_dict = generate_forecast_table_task(incr_state_dict, incr_state_dict, incr_state_dict)
    pred_state_dict = bulk_train_predict_task(feature_state_dict, feature_state_dict, forecast_state_dict)
    eval_state_dict = eval_station_models_task(pred_state_dict, pred_state_dict, run_date)
    state_dict = flatten_tables_task(pred_state_dict, eval_state_dict)

    return state_dict
