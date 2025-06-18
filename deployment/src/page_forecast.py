import dill as pickle
import json
import pandas as pd
import numpy as np
from my_models import ModelWrapper
from datetime import datetime
import streamlit as st

# def model_filename(komoditas_id, provinsi_id):
#     return f'./src/models/best_model_komoditas_{komoditas_id}_provinsi_{provinsi_id}.pkl'

import logging

logging.basicConfig(level=logging.INFO)
logging.info("Ini info log")
logging.warning("Ini warning log")

is_model_loaded = False

def run():
    global is_model_loaded
    global komoditas_ids
    global provinsi_ids
    global wrapper
    
    hf_repo_id = 'elangcergasp/bakoscope-models'

    # Import JSON Files
    if is_model_loaded != True:
        with open('./src/lib/komoditas_id.json', 'r') as f:
            komoditas_ids = json.load(f)

        with open('./src/lib/provinsi_id.json', 'r') as f:
            provinsi_ids = json.load(f)

        wrapper = ModelWrapper.load_models_from_directory("./src/models", komoditas_ids=komoditas_ids, provinsi_ids=provinsi_ids, hf_repo_id=hf_repo_id)
        
        is_model_loaded = True


    # Begin App
    st.subheader('BakoScope - Forecast')

    # FORM
    inputs = {}
    with st.form(key='bakoscope-2025'):
        # Pilih Komoditas
        inputs['komoditas'] = st.selectbox('Komoditas', komoditas_ids.values(), help='Pilih Komoditas')
        
        # Pilih Provinsi
        inputs['provinsi'] = st.selectbox('Provinsi', provinsi_ids.values(), help='Pilih Provinsi')
        
        # Pilih Tanggal
        inputs['date'] = st.date_input("Tanggal", datetime.now())
        
        st.markdown('---')
        submitted = st.form_submit_button('Predict')
    
    komoditas = inputs['komoditas']
    provinsi = inputs['provinsi']
    
    target_date = pd.Timestamp(inputs['date'])
    try:
        forecast_steps = 0
        forecast_dates = pd.date_range(start=target_date, periods=forecast_steps + 1, freq='D')
        y_pred_inf = wrapper.predict(komoditas, provinsi, start=forecast_dates[0], end=forecast_dates[-1])
        print(y_pred_inf)
        y_pred_inf_value = y_pred_inf[0]
    except KeyError as e:
        g = (komoditas, provinsi)
        print(wrapper.models[g].model)
        print(wrapper.models[g].model.data)
        print(wrapper.models[g].model.data.orig_endog)
        print(wrapper.models[g].model.data.orig_endog.index)
        last_date = wrapper.models[g].model.data.orig_endog.index[-1]

        step_size = pd.infer_freq(wrapper.models[g].model.data.row_labels)
        if step_size is None:
            step_size = "D"  # fallback to daily if unknown

        date_range = pd.date_range(start=last_date + pd.Timedelta(days=1), end=target_date, freq=step_size)
        steps_ahead = len(date_range)
        
        forecast = wrapper.forecast(steps_ahead)

        y_pred_inf_value = forecast.iloc[-1]

    st.write(f'### Harga {komoditas} di {provinsi} : ')
    st.write('### Rp. ' + f'{y_pred_inf_value:,.0f}'.replace(',', '.'))

if __name__ == '__main__':
    run()
