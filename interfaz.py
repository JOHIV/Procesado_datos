#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Mar 13 16:07:25 2025

@author: hitoshi
"""

import streamlit as st
import pandas as pd
import numpy as np
from io import StringIO, BytesIO

def process_file(uploaded_file):
    if uploaded_file is not None:
        df = pd.read_csv(uploaded_file, delimiter=",", skiprows=[0, 2, 3])
        dff = df.copy()
        
        # Convertir columnas a numérico
        columnas_object = dff.select_dtypes(include=['object']).columns
        columnas_a_convertir = columnas_object.drop('TIMESTAMP', errors='ignore')
        dff[columnas_a_convertir] = dff[columnas_a_convertir].apply(pd.to_numeric, errors='coerce')
        dff['TIMESTAMP'] = pd.to_datetime(dff['TIMESTAMP'])
        
        # Completar rango de fechas
        rango_fechas = pd.date_range(start=dff['TIMESTAMP'].min(), end=dff['TIMESTAMP'].max(), freq='H')
        df_completo = pd.DataFrame({'TIMESTAMP': rango_fechas})
        dff_completo = pd.merge(df_completo, dff, on='TIMESTAMP', how='left')
        
        # Filtrar columnas relevantes
        prefijo = ("PM25", "PM10")
        columnas_extra = ["TIMESTAMP"]
        columnas_prefijo = [col for col in dff_completo.columns if col.startswith(prefijo)]
        df_nuevo = dff_completo[columnas_extra + columnas_prefijo].copy()
        
        # Definir límites
        limites = {
            'PM25_CONC_Avg': (0.01, 500),  
            'PM25_AMB_TEMP_Avg': (15, 35),
            'PM25_AMB_RH_Avg': (0, 100),
            'PM25_BARO_PRES_Avg': (None, 1013.25),
            'PM25_TAPE_COUNTER': (None, 649.9)
        }
        
        def verificar_limites(row):
            for col, (lim_inf, lim_sup) in limites.items():
                valor = row.get(col, np.nan)
                if pd.isna(valor):
                    return np.nan 
                if (lim_inf is not None and valor < lim_inf) or (lim_sup is not None and valor > lim_sup):
                    return "no cumple"
            return "si cumple"
        
        df_nuevo["ETAPA_1_PM25"] = df_nuevo.apply(verificar_limites, axis=1)
        
        def marcar_dudoso(serie):
            condiciones = (serie == serie.shift(1)) & (serie == serie.shift(2))
            return ['no cumple' if cond else 'si cumple' for cond in condiciones]
        
        df_nuevo['ETAPA_2_PM25'] = marcar_dudoso(df_nuevo['PM25_CONC_Avg'])
        df_nuevo['ETAPA_2_PM10'] = marcar_dudoso(df_nuevo['PM10_CONC_Avg'])
        
        df_nuevo['ratio'] = df_nuevo['PM25_CONC_Avg'] / df_nuevo['PM10_CONC_Avg']
        df_nuevo['ETAPA_3_PM25'] = df_nuevo['ETAPA_3_PM10'] = df_nuevo['ratio'].apply(lambda x: 'no cumple' if x > 1 else 'si cumple')
        
        df_final = df_nuevo[["TIMESTAMP", "PM25_CONC_Avg", "PM10_CONC_Avg", "ETAPA_1_PM25", "ETAPA_2_PM25", "ETAPA_3_PM25"]].copy()
        
        def definir_estado(row):
            if pd.isna(row['PM25_CONC_Avg']):
                return 'ND'
            elif row['ETAPA_1_PM25'] == 'no cumple':
                return 'M'
            elif 'no cumple' in [row['ETAPA_2_PM25'], row['ETAPA_3_PM25']]:
                return 'D'
            else:
                return 'C'
        
        df_final['estado_pm25'] = df_final.apply(definir_estado, axis=1)
        
        output = BytesIO()
        df_final.to_csv(output, index=False, encoding="utf-8")
        output.seek(0)
        return output, df_final
    return None, None

st.title("Procesador de Datos de Calidad del Aire")
uploaded_file = st.file_uploader("Sube un archivo CSV para procesar", type=["csv", "dat"])

if uploaded_file is not None:
    output_file, df_final = process_file(uploaded_file)
    
    if df_final is not None:
        st.write("Vista previa de los datos procesados:")
        st.dataframe(df_final.head())
        st.download_button("Descargar archivo procesado", data=output_file, file_name="df_final.csv", mime="text/csv")
