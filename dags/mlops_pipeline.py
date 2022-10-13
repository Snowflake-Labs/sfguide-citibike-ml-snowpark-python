
def materialize_holiday_table(session, holiday_table_name:str) -> str:
    from dags.feature_engineering import generate_holiday_df
    
    holiday_df = generate_holiday_df(session=session, holiday_table_name=holiday_table_name)
    holiday_df.write.mode('overwrite').saveAsTable(holiday_table_name)
    
    return holiday_table_name

def subscribe_to_weather_data(session, 
                              weather_database_name:str, 
                              weather_listing_id:str) -> str:
    
    session.sql("CREATE DATABASE IF NOT EXISTS "+weather_database_name+\
                " FROM SHARE "+weather_listing_id).collect()
    
    return weather_database_name

def create_weather_view(session, weather_table_name:str, weather_view_name:str) -> str:
    from dags.feature_engineering import generate_weather_df

    weather_df = generate_weather_df(session=session, weather_table_name=weather_table_name)
    
    weather_df.create_or_replace_view(weather_view_name)
    
    return weather_view_name

def deploy_pred_train_udf(session, udf_name:str, function_name:str, model_stage_name:str) -> str:
    from dags.station_train_predict import StationTrainPredictFunc
    from snowflake.snowpark import types as T
    from snowflake.snowpark.functions import udtf

    session.clear_packages()
    session.clear_imports()
    dep_packages=["pandas==1.3.5", "pytorch==1.10.2", "scipy==1.7.1", "scikit-learn==1.0.2", "setuptools==58.0.4", "cloudpickle==2.0.0"]
    dep_imports=['./include/pytorch_tabnet.zip', 'dags']

    station_train_predict_udtf = udtf(StationTrainPredictFunc,
                                                      session=session,
                                                     name="station_train_predict_udtf",
                                                     is_permanent=True,
                                                     stage_location='@'+str(model_stage_name), 
                                                     imports=dep_imports,
                                                     packages=dep_packages,
                                                     input_types=[T.DateType(), 
                                                                  T.FloatType(), 
                                                                  T.FloatType(),
                                                                  T.FloatType(),
                                                                  T.FloatType(),
                                                                  T.FloatType(),
                                                                  T.FloatType(),
                                                                  T.FloatType(),
                                                                  T.FloatType(),
                                                                  T.ArrayType(),
                                                                  T.StringType(),
                                                                  T.FloatType(), 
                                                                  T.FloatType(), 
                                                                  T.ArrayType(), 
                                                                  T.ArrayType(),
                                                                  T.ArrayType()],
                                                     output_schema=T.StructType([T.StructField("PRED_DATA", T.VariantType())]),
                                                     replace=True)
    
    return station_train_predict_udtf.name


def deploy_eval_udf(session, udf_name:str, function_name:str, model_stage_name:str) -> str:
    from dags.model_eval import eval_model_func
    from snowflake.snowpark import types as T

    session.clear_packages()
    session.clear_imports()
    dep_packages=['pandas==1.3.5', 'scikit-learn==1.0.2', "cloudpickle==2.0.0"]
    dep_imports=['./include/rexmex.zip', 'dags']

    eval_model_output_udf = session.udf.register(eval_model_func, 
                                                 name=udf_name,
                                                 is_permanent=True,
                                                 stage_location='@'+str(model_stage_name), 
                                                 imports=dep_imports,
                                                 packages=dep_packages,
                                                 input_types=[T.StringType(), 
                                                              T.StringType(), 
                                                              T.StringType()],
                                                 return_type=T.VariantType(),
                                                 replace=True)
    return eval_model_output_udf.name

def create_forecast_table(session, 
                          trips_table_name:str,
                          holiday_table_name:str, 
                          weather_view_name:str, 
                          forecast_table_name:str,
                          steps:int):
    
    from dags.feature_engineering import generate_holiday_df
    from datetime import timedelta, datetime
    from snowflake.snowpark import functions as F 
    
    start_date = session.table(trips_table_name)\
                        .select(F.to_date(F.max('STARTTIME'))).collect()[0][0]+timedelta(days=1)
    end_date = start_date+timedelta(days=steps)

    #check if it tables already materialized, otherwise generate DF
    holiday_df = session.table(holiday_table_name)
    try: 
        _ = holiday_df.columns
    except:
        holiday_df = generate_holiday_df(session, holiday_table_name)
        
    weather_df = session.table(weather_view_name)
        
    forecast_df = holiday_df.join(weather_df[['DATE','PRECIP','TEMP']], 'DATE', join_type='right')\
                            .na.fill({'HOLIDAY':0})\
                            .filter((F.col('DATE') >= start_date) &\
                                    (F.col('DATE') <= end_date))\
                            .sort('DATE', ascending=True)
    
    forecast_df.write.mode('overwrite').save_as_table(forecast_table_name)
    
    return forecast_table_name


def create_feature_table(session, 
                         trips_table_name:str, 
                         holiday_table_name:str, 
                         weather_view_name:str,
                         feature_table_name:str) -> list:

    import snowflake.snowpark as snp
    from snowflake.snowpark import functions as F 
    from dags.feature_engineering import generate_holiday_df, generate_weather_df
    
    #check if it tables already materialized, otherwise generate DF
    holiday_df = session.table(holiday_table_name)
    try: 
        _ = holiday_df.columns
    except:
        holiday_df = generate_holiday_df(session, holiday_table_name)
        
    weather_df = session.table(weather_view_name)
    
    sid_date_window = snp.Window.partition_by(F.col('STATION_ID')).order_by(F.col('DATE').asc())
    sid_window = snp.Window.partition_by(F.col('STATION_ID'))
    latest_date = session.table(trips_table_name).select(F.to_char(F.to_date(F.max('STARTTIME')))).collect()[0][0]
    
    feature_df = session.table(trips_table_name)\
                        .select(F.to_date(F.col('STARTTIME')).alias('DATE'),
                                F.col('START_STATION_ID').alias('STATION_ID'))\
                        .group_by(F.col('STATION_ID'), F.col('DATE'))\
                                .count()\
                        .with_column('LAG_1', F.lag(F.col('COUNT'), offset=1).over(sid_date_window))\
                        .with_column('LAG_7', F.lag(F.col('COUNT'), offset=7).over(sid_date_window))\
                        .with_column('LAG_90', F.lag(F.col('COUNT'), offset=90).over(sid_date_window))\
                        .with_column('LAG_365', F.lag(F.col('COUNT'), offset=365).over(sid_date_window))\
                            .na.drop()\
                        .join(holiday_df, 'DATE', join_type='left').na.fill({'HOLIDAY':0})\
                        .join(weather_df[['DATE','PRECIP','TEMP']], 'DATE', 'inner')\
                        .with_column('DAY_COUNT', F.count(F.col('DATE')).over(sid_window))\
                            .filter(F.col('DAY_COUNT') >= 365*2)\
                        .with_column('MAX_DATE', F.max('DATE').over(sid_window))\
                            .filter(F.col('MAX_DATE') == latest_date)\
                        .drop(['DAY_COUNT', 'MAX_DATE'])
    
    feature_df.write.mode('overwrite').save_as_table(feature_table_name)
    
    return feature_table_name

def train_predict(session, 
                  station_train_pred_udf_name:str, 
                  feature_table_name:str, 
                  forecast_table_name:str,
                  pred_table_name:str) -> list:
    
    from snowflake.snowpark import functions as F
    
    cutpoint=365
    max_epochs = 10
    target_column = 'COUNT'
    lag_values=[1,7,90,365]
    lag_values_array = F.array_construct(*[F.lit(x) for x in lag_values])
    
    historical_df = session.table(feature_table_name)
    historical_column_list = historical_df.columns
    historical_column_list.remove('STATION_ID')
    historical_column_names = F.array_construct(*[F.lit(x) for x in historical_column_list])

    forecast_df = session.table(forecast_table_name)
    forecast_column_list = forecast_df.columns
    forecast_df = forecast_df.select(F.array_agg(F.array_construct(F.col('*'))).alias('FORECAST_DATA'))
    
    train_df = historical_df.join(forecast_df)
    train_df.write.mode('overwrite').save_as_table('udtf_input', table_type='temporary')
    
    sql_txt = "with input as ( \
                select * from udtf_input \
                ) \
            select res.*, station_id from input, \
            table(station_train_predict_udtf(input.DATE, to_double(input.COUNT), \
                                            to_double(input.LAG_1), \
                                            to_double(input.LAG_7), \
                                            to_double(input.LAG_90), \
                                            to_double(input.LAG_365), \
                                            to_double(input.HOLIDAY), \
                                            to_double(input.PRECIP), \
                                            to_double(input.TEMP), \
                                            {}, \
                                            '{}', \
                                            to_double({}), \
                                            to_double({}), \
                                            {}, \
                                            input.FORECAST_DATA, \
                                            {}) \
            over (partition by STATION_ID)) res".format(historical_column_list, target_column, cutpoint, max_epochs, forecast_column_list, lag_values)
    
    pred_df = session.sql(sql_txt)

    pred_df.write.mode('overwrite')\
            .save_as_table(pred_table_name)

    return pred_table_name

def evaluate_station_model(session, 
                           run_date:str, 
                           eval_model_udf_name:str, 
                           pred_table_name:str, 
                           eval_table_name:str):
    from snowflake.snowpark import functions as F
    from datetime import datetime
    
    y_true_name='COUNT'
    y_score_name='PRED'
    run_date=datetime.strptime(run_date, '%Y_%m_%d').date()

    session.table(pred_table_name)\
           .select('STATION_ID',
                   F.call_udf(eval_model_udf_name,
                              F.parse_json(F.col('PRED_DATA')[0]),
                              F.lit(y_true_name),
                              F.lit(y_score_name)).alias('EVAL_DATA'))\
           .with_column('RUN_DATE', F.to_date(F.lit(run_date)))\
           .write.mode('overwrite')\
           .save_as_table(eval_table_name)
    
    return eval_table_name

def flatten_tables(session, pred_table_name:str, forecast_table_name:str, eval_table_name:str):
    from snowflake.snowpark import functions as F
    
    session.table(pred_table_name)\
           .select('STATION_ID', F.parse_json(F.col('PRED_DATA')[0]).alias('PRED_DATA'))\
           .flatten('PRED_DATA').select('STATION_ID', F.col('VALUE').alias('PRED_DATA'))\
           .select('STATION_ID', 
                   F.to_date(F.col('PRED_DATA')['DATE']).alias('DATE'),
                   F.as_integer(F.col('PRED_DATA')['COUNT']).alias('COUNT'),
                   F.as_integer(F.col('PRED_DATA')['LAG_1']).alias('LAG_1'),
                   F.as_integer(F.col('PRED_DATA')['LAG_7']).alias('LAG_7'),
                   F.as_integer(F.col('PRED_DATA')['LAG_90']).alias('LAG_90'),
                   F.as_integer(F.col('PRED_DATA')['LAG_365']).alias('LAG_365'),
                   F.as_integer(F.col('PRED_DATA')['HOLIDAY']).alias('HOLIDAY'),
                   F.as_decimal(F.col('PRED_DATA')['PRECIP']).alias('PRECIP'),
                   F.as_decimal(F.col('PRED_DATA')['TEMP']).alias('TEMP'),
                   F.as_decimal(F.col('PRED_DATA')['PRED']).alias('PRED'),
                   F.as_decimal(F.col('PRED_DATA')['EXPL_LAG_1']).alias('EXPL_LAG_1'),
                   F.as_decimal(F.col('PRED_DATA')['EXPL_LAG_7']).alias('EXPL_LAG_7'),
                   F.as_decimal(F.col('PRED_DATA')['EXPL_LAG_90']).alias('EXPL_LAG_90'),
                   F.as_decimal(F.col('PRED_DATA')['EXPL_LAG_365']).alias('EXPL_LAG_365'),
                   F.as_decimal(F.col('PRED_DATA')['EXPL_HOLIDAY']).alias('EXPL_HOLIDAY'),
                   F.as_decimal(F.col('PRED_DATA')['EXPL_PRECIP']).alias('EXPL_PRECIP'),
                   F.as_decimal(F.col('PRED_DATA')['EXPL_TEMP']).alias('EXPL_TEMP'))\
           .write.mode('overwrite').save_as_table('flat_PRED')

    #forecast are in position 2 of the pred_table
    session.table(pred_table_name)\
           .select('STATION_ID', F.parse_json(F.col('PRED_DATA')[1]).alias('PRED_DATA'))\
           .flatten('PRED_DATA').select('STATION_ID', F.col('VALUE').alias('PRED_DATA'))\
           .select('STATION_ID', 
                   F.to_date(F.col('PRED_DATA')['DATE']).alias('DATE'),
                   F.as_integer(F.col('PRED_DATA')['COUNT']).alias('COUNT'),
                   F.as_integer(F.col('PRED_DATA')['LAG_1']).alias('LAG_1'),
                   F.as_integer(F.col('PRED_DATA')['LAG_7']).alias('LAG_7'),
                   F.as_integer(F.col('PRED_DATA')['LAG_90']).alias('LAG_90'),
                   F.as_integer(F.col('PRED_DATA')['LAG_365']).alias('LAG_365'),
                   F.as_integer(F.col('PRED_DATA')['HOLIDAY']).alias('HOLIDAY'),
                   F.as_decimal(F.col('PRED_DATA')['PRECIP']).alias('PRECIP'),
                   F.as_decimal(F.col('PRED_DATA')['TEMP']).alias('TEMP'),
                   F.as_decimal(F.col('PRED_DATA')['PRED']).alias('PRED'),
                   F.as_decimal(F.col('PRED_DATA')['EXPL_LAG_1']).alias('EXPL_LAG_1'),
                   F.as_decimal(F.col('PRED_DATA')['EXPL_LAG_7']).alias('EXPL_LAG_7'),
                   F.as_decimal(F.col('PRED_DATA')['EXPL_LAG_90']).alias('EXPL_LAG_90'),
                   F.as_decimal(F.col('PRED_DATA')['EXPL_LAG_365']).alias('EXPL_LAG_365'),
                   F.as_decimal(F.col('PRED_DATA')['EXPL_HOLIDAY']).alias('EXPL_HOLIDAY'),
                   F.as_decimal(F.col('PRED_DATA')['EXPL_PRECIP']).alias('EXPL_PRECIP'),
                   F.as_decimal(F.col('PRED_DATA')['EXPL_TEMP']).alias('EXPL_TEMP'))\
           .write.mode('overwrite').save_as_table('flat_FORECAST')

    session.table(eval_table_name)\
           .select('RUN_DATE', 'STATION_ID', F.parse_json(F.col('EVAL_DATA')).alias('EVAL_DATA'))\
           .flatten('EVAL_DATA').select('RUN_DATE', 'STATION_ID', F.col('VALUE').alias('EVAL_DATA'))\
           .select('RUN_DATE', 'STATION_ID', 
                   F.as_decimal(F.col('EVAL_DATA')['mae'], 10, 2).alias('mae'),
                   F.as_decimal(F.col('EVAL_DATA')['mape'], 10, 2).alias('mape'),
                   F.as_decimal(F.col('EVAL_DATA')['mse'], 10, 2).alias('mse'),
                   F.as_decimal(F.col('EVAL_DATA')['r_squared'], 10, 2).alias('r_squared'),
                   F.as_decimal(F.col('EVAL_DATA')['rmse'], 10, 2).alias('rmse'),
                   F.as_decimal(F.col('EVAL_DATA')['smape'], 10, 2).alias('smape'),)\
           .write.mode('append').save_as_table('flat_EVAL')
    
    return 'flat_PRED', 'flat_FORECAST', 'flat_EVAL'
        
