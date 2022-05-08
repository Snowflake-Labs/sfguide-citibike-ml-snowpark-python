import sys, os
sys.path.append(os.getcwd()+'/dags')

from snowflake.snowpark import functions as F
from snowpark_connection import snowpark_connect
import streamlit as st
import pandas as pd
from datetime import timedelta, datetime
from dateutil.relativedelta import *
import calendar
import altair as alt
import requests
from requests.auth import HTTPBasicAuth
import time 
import json
import logging

logging.basicConfig(level=logging.WARN)
logging.getLogger().setLevel(logging.WARN)

def update_forecast_table(forecast_df, stations:list, start_date, end_date):
#     explainer_columns = [col for col in forecast_df.schema.names if 'EXP' in col]
    explainer_columns=['EXPL_LAG_1', 'EXPL_LAG_7','EXPL_LAG_365','EXPL_HOLIDAY','EXPL_TEMP']
    explainer_columns_new=['DAY', 'DAY_OF_WEEK', 'DAY_OF_YEAR','US_HOLIDAY', 'TEMPERATURE']

    cond = "F.when" + ".when".join(["(F.col('" + c + "') == F.col('EXPLAIN'), F.lit('" + c + "'))" for c in explainer_columns])

    df = forecast_df.filter((forecast_df['STATION_ID'].in_(stations)) &
                       (F.col('DATE') >= start_date) & 
                       (F.col('DATE') <= end_date))\
                .select(['STATION_ID', 
                         F.to_char(F.col('DATE')).alias('DATE'), 
                         'PRED', 
                         'HOLIDAY',
                         *explainer_columns])\
                .with_column('EXPLAIN', F.greatest(*explainer_columns))\
                .with_column('REASON', eval(cond))\
                .select(F.col('STATION_ID'), 
                        F.col('DATE'), 
                        F.col('PRED'), 
                        F.col('REASON'), 
                        F.col('EXPLAIN'), 
                        F.col('EXPL_LAG_1').alias('DAY'),
                        F.col('EXPL_LAG_7').alias('DAY_OF_WEEK'),
                        F.col('EXPL_LAG_365').alias('DAY_OF_YEAR'),
                        F.col('EXPL_HOLIDAY').alias('US_HOLIDAY'),
                        F.col('EXPL_TEMP').alias('TEMPERATURE'),
                       )\
                .to_pandas()
    
    df['REASON'] = pd.Categorical(df['REASON'])
    df['REASON_CODE']=df['REASON'].cat.codes
        
    rect = alt.Chart(df).mark_rect().encode(alt.X('DATE:N'), 
                                        alt.Y('STATION_ID:N'), 
                                        alt.Color('REASON'),
                                        tooltip=explainer_columns_new)
    text = rect.mark_text(baseline='middle').encode(text='PRED:Q', color=alt.value('white'))

    l = alt.layer(
        rect, text
    )

    st.write("### Forecast")
    st.altair_chart(l, use_container_width=True)
        
    return None

def update_eval_table(eval_df, stations:list):
    df = eval_df.select('STATION_ID', F.to_char(F.col('RUN_DATE')).alias('RUN_DATE'), 'RMSE')\
                .filter(eval_df['STATION_ID'].in_(stations))\
                .to_pandas()

    data = df.pivot(index="RUN_DATE", columns="STATION_ID", values="RMSE")
    data = data.reset_index().melt('RUN_DATE', var_name='STATION_ID', value_name='RMSE')

    nearest = alt.selection(type='single', nearest=True, on='mouseover',
                            fields=['RUN_DATE'], empty='none')

    line = alt.Chart(data).mark_line(interpolate='basis').encode(
        x='RUN_DATE:N',
        y='RMSE:Q',
        color='STATION_ID:N'
    )

    selectors = alt.Chart(data).mark_point().encode(
        x='RUN_DATE:N',
        opacity=alt.value(0)
    ).add_selection(
        nearest
    )

    points = line.mark_point().encode(
        opacity=alt.condition(nearest, alt.value(1), alt.value(0))
    )

    text = line.mark_text(align='left', dx=5, dy=-5).encode(
        text=alt.condition(nearest, 'RMSE:Q', alt.value(' '))
    )

    rules = alt.Chart(data).mark_rule(color='gray').encode(
        x='RUN_DATE:N',
    ).transform_filter(
        nearest
    )

    l = alt.layer(
        line, selectors, points, rules, text
    ).properties(
        width=600, height=300
    )
    st.write("### Model Monitor")
    st.altair_chart(l, use_container_width=True)
    
    return None

def trigger_ingest(download_file_name, run_date):    
    dag_url='http://localhost:8080/api/v1/dags/citibikeml_monthly_taskflow/dagRuns'
    json_payload = {"conf": {"files_to_download": [download_file_name], "run_date": run_date}}
    
    response = requests.post(dag_url, 
                            json=json_payload,
                            auth = HTTPBasicAuth('admin', 'admin'))

    run_id = json.loads(response.text)['dag_run_id']
    #run_id = 'manual__2022-04-07T15:02:29.166108+00:00'

    state=json.loads(requests.get(dag_url+'/'+run_id, auth=HTTPBasicAuth('admin', 'admin')).text)['state']

    st.snow()

    with st.spinner('Ingesting file: '+download_file_name):
        while state != 'success':
            time.sleep(5)
            state=json.loads(requests.get(dag_url+'/'+run_id, auth=HTTPBasicAuth('admin', 'admin')).text)['state']
    st.success('Ingested file: '+download_file_name+' State: '+str(state))

#Main Body    
session, state_dict = snowpark_connect('./include/state.json')
forecast_df = session.table('FLAT_FORECAST')
eval_df = session.table('FLAT_EVAL')
trips_df = session.table('TRIPS')

st.header('Citibike Forecast Application')
st.write('In this application we leverage deep learning models to predict the number of trips started from '+
         'a given station each day.  After selecting the stations and time range desired the application '+\
         'displays not only the forecast but also explains which features of the model were most used in making '+\
         'the prediction. Additionally users can see the historical performance of the deep learning model to '+\
         'monitor predictive capabilities over time.')

last_trip_date = trips_df.select(F.to_date(F.max('STARTTIME'))).collect()[0][0]
st.write('Data provided as of '+str(last_trip_date))

#Create a sidebar for input
min_date=forecast_df.select(F.min('DATE')).collect()[0][0]
max_date=forecast_df.select(F.max('DATE')).collect()[0][0]

start_date = st.sidebar.date_input('Start Date', value=min_date, min_value=min_date, max_value=max_date)
show_days = st.sidebar.number_input('Number of days to show', value=7, min_value=1, max_value=30)
end_date = start_date+timedelta(days=show_days)

stations_df=forecast_df.select(F.col('STATION_ID')).distinct().to_pandas()

sample_stations = ["519", "497", "435", "402", "426", "285", "293"]

stations = st.sidebar.multiselect('Choose stations', stations_df['STATION_ID'], sample_stations)
if not stations:
    stations = stations_df['STATION_ID']

update_forecast_table(forecast_df, stations, start_date, end_date)

update_eval_table(eval_df, stations)


next_ingest = last_trip_date+relativedelta(months=+1)
next_ingest = next_ingest.replace(day=1)       

if next_ingest <= datetime.strptime("2016-12-01", "%Y-%m-%d").date():
    download_file_name=next_ingest.strftime('%Y%m')+'-citibike-tripdata.zip'
else:
    download_file_name=next_ingest.strftime('%Y%m')+'-citibike-tripdata.csv.zip'
    
run_date = next_ingest+relativedelta(months=+1)
run_date = run_date.strftime('%Y_%m_%d')

st.write('Next ingest for '+str(next_ingest))

st.button('Run Ingest Taskflow', on_click=trigger_ingest, args=(download_file_name, run_date))
