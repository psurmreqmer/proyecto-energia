import gradio as gr
import pandas as pd
import numpy as np
import xgboost as xgb
import joblib

# =====================================================================
# 1. CARGA DE MODELOS Y ESCALADORES DESDE LA CARPETA 'MODELOS'
# =====================================================================
# Modelo Ganador (Con Lags) y su Escalador
modelo_bueno = xgb.XGBRegressor()
modelo_bueno.load_model("modelos/mejor_modelo_xgboost.json")
scaler_bueno = joblib.load("modelos/escalador_datos.pkl")

# Modelo Amnésico (Sin Lags) y su Escalador
modelo_malo = xgb.XGBRegressor()
modelo_malo.load_model("modelos/modelo_sin_lags_xgboost.json")
scaler_malo = joblib.load("modelos/escalador_datos_sin_lags.pkl")


# =====================================================================
# 2. FUNCIONES DE PREDICCIÓN PARA CADA ESCENARIO
# =====================================================================

# FUNCIÓN 1: Para la Pestaña Histórica (Utiliza ambos modelos para comparar)
def prediccion_historica(indice_fila):
    try:
        idx = int(indice_fila)
        if idx < 0 or idx >= len(test_df):
            return f"Error: El índice debe estar entre 0 y {len(test_df)-1}"
        
        # Extraemos la fila real seleccionada del test_df
        fila_real = test_df.iloc[[idx]]
        
        # 1. Predicción con el MODELO BUENO (Con Lags)
        datos_buenos = pd.DataFrame(scaler_bueno.transform(fila_real[variables_ganadoras]), columns=variables_ganadoras)
        pred_buena = modelo_bueno.predict(datos_buenos)[0]
        
        # 2. Predicción con el MODELO MALO (Sin Lags)
        features_sin_lags = ['hour', 'month', 'day_of_week', 'temp_madrid', 'temp_barcelona', 'temp_seville', 'is_holiday', 'is_extreme_temp']
        datos_malos = pd.DataFrame(scaler_malo.transform(fila_real[features_sin_lags]), columns=features_sin_lags)
        pred_mala = modelo_malo.predict(datos_malos)[0]
        
        # 3. Consumo Real
        consumo_real = fila_real['total_load_actual'].values[0]
        
        # Formateamos el resultado para mostrarlo bonito
        resultado = (
            f"📊 REVISANDO FILA HISTÓRICA Nº {idx}\n"
            f"-----------------------------------------\n"
            f"🔌 Consumo REAL registrado: {consumo_real:.2f} MW\n\n"
            f"🏆 MODELO GANADOR (Con memoria / Lags): {pred_buena:.2f} MW\n"
            f"   👉 Error: {abs(consumo_real - pred_buena):.2f} MW\n\n"
            f"🧠 MODELO AMNÉSICO (Sin Lags / Solo Clima): {pred_mala:.2f} MW\n"
            f"   👉 Error: {abs(consumo_real - pred_mala):.2f} MW\n"
        )
        return resultado
    except Exception as e:
        return f"Ocurrió un error al procesar la fila: {str(e)}"


# FUNCIÓN 2: Para la Pestaña Libre/Futuro (Solo usa el modelo sin lags, no pide pasado)
def prediccion_libre(hora, mes, dia_semana, t_madrid, t_barcelona, t_seville, es_festivo, es_extrema):
    # Traducimos los textos de la interfaz a números binarios (0 o 1)
    is_holiday = 1 if es_festivo == "Sí" else 0
    is_extreme_temp = 1 if es_extrema == "Sí" else 0
    
    # Mapeo del día de la semana (Lunes=0, Domingo=6)
    dias = {"Lunes": 0, "Martes": 1, "Miércoles": 2, "Jueves": 3, "Viernes": 4, "Sábado": 5, "Domingo": 6}
    day_of_week = dias[dia_semana]
    
    # Creamos el diccionario con el orden EXACTO que espera el modelo amnésico
    features_sin_lags = ['hour', 'month', 'day_of_week', 'temp_madrid', 'temp_barcelona', 'temp_seville', 'is_holiday', 'is_extreme_temp']
    input_dict = {
        'hour': [int(hora)],
        'month': [int(mes)],
        'day_of_week': [day_of_week],
        'temp_madrid': [float(t_madrid)],
        'temp_barcelona': [float(t_barcelona)],
        'temp_seville': [float(t_seville)],
        'is_holiday': [is_holiday],
        'is_extreme_temp': [is_extreme_temp]
    }
    
    # Convertimos a DataFrame y escalamos con el scaler de la ablación
    nuevo_escenario = pd.DataFrame(input_dict)
    nuevo_escenario_scaled = pd.DataFrame(scaler_malo.transform(nuevo_escenario), columns=features_sin_lags)
    
    # Predecimos con el modelo entrenado sin lags
    prediccion = modelo_malo.predict(nuevo_escenario_scaled)[0]
    
    return f"🔮 Demanda estimada para este escenario: {prediccion:.2f} MW"


# =====================================================================
# 3. DISEÑO DE LA INTERFAZ GRÁFICA CON GRADIO
# =====================================================================
with gr.Blocks(theme=gr.themes.Soft()) as demo:
    gr.Markdown("# ⚡ Sistema Inteligente de Predicción de Demanda Eléctrica")
    gr.Markdown("Prototipo para el despliegue del modelo XGBoost optimizado. Desarrollado por Marta Requejo.")
    
    # PESTAÑA 1: Simulación con datos existentes
    with gr.Tab("🎯 Simulación Histórica (Comparativa de Modelos)"):
        gr.Markdown("### Evalúa el impacto de la memoria temporal (*Lags*) usando registros reales del conjunto de pruebas.")
        with gr.Row():
            with gr.Column(scale=1):
                input_idx = gr.Number(value=0, label=f"Introduce el número de fila (0 a {len(test_df)-1})")
                btn_hist = gr.Button("Comparar Modelos", variant="primary")
            with gr.Column(scale=2):
                output_hist = gr.Textbox(label="Resultados del Análisis", lines=10, placeholder="Los resultados aparecerán aquí...")
        
        btn_hist.click(fn=prediccion_historica, inputs=input_idx, outputs=output_hist)
        
    # PESTAÑA 2: Simulación de cualquier fecha fuera del rango
    with gr.Tab("🔮 Escenario Manual Libre (Cualquier Fecha / Futuro)"):
        gr.Markdown("### Predice el consumo para cualquier escenario futuro donde no se disponen de registros históricos (*Lags*). El sistema utilizará automáticamente el modelo entrenado mediante ablación temporal.")
        with gr.Row():
            with gr.Column():
                in_hora = gr.Slider(0, 23, value=12, step=1, label="Hora del día")
                in_mes = gr.Slider(1, 12, value=6, step=1, label="Mes del año")
                in_dia = gr.Dropdown(["Lunes", "Martes", "Miércoles", "Jueves", "Viernes", "Sábado", "Domingo"], value="Lunes", label="Día de la Semana")
                in_festivo = gr.Radio(["No", "Sí"], value="No", label="¿Es un día Festivo?")
            with gr.Column():
                in_madrid = gr.Slider(-5, 45, value=22, label="Temperatura Madrid (°C)")
                in_barcelona = gr.Slider(-5, 45, value=22, label="Temperatura Barcelona (°C)")
                in_seville = gr.Slider(-5, 45, value=25, label="Temperatura Sevilla (°C)")
                in_extrema = gr.Radio(["No", "Sí"], value="No", label="¿Hay Alerta por Temperatura Extrema?")
        
        btn_libre = gr.Button("Calcular Predicción Futura", variant="secondary")
        output_libre = gr.Textbox(label="Resultado de la Inferencia", placeholder="La demanda estimada aparecerá aquí...")
        
        btn_libre.click(
            fn=predict_demand_libre, 
            inputs=[in_hora, in_mes, in_dia, in_madrid, in_barcelona, in_seville, in_festivo, in_extrema], 
            outputs=output_libre
        )

# Lanzar la aplicación interactiva
demo.launch()