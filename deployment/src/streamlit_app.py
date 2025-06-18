import streamlit as st
import page_eda
import page_forecast
import dill as pickle
from PIL import Image
import json

with st.sidebar:
    # st.sidebar.image("./src/images/bakoscope.png", use_container_width=True)
    # st.sidebar.image("./src/images/bakoscope.png", width=48)
    # Create two columns
    col1, col2 = st.columns([1, 3])  # Adjust ratio to control spacing

    with col1:
        st.image("./src/images/bakoscope.png", use_container_width=True)

    with col2:
        st.title('BakoScope')

    page = st.radio('Pilih Halaman', ('EDA', 'Forecasting'))

st.title('BakoScope')

if page == 'EDA':
    page_eda.run()
if page == 'Forecasting':
    page_forecast.run()

st.write('---')
st.code('Arcana Anggreliya Klau Rissa - Aris Trisnawan - Elang Cergas Pembrani - 2025')
