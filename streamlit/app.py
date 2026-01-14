import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
import pydeck as pdk

st.set_page_config(page_title="Ônibus SPTrans", layout="wide")


engine = create_engine("trino://trino@trino:8080/delta/trusted")

query = """
SELECT
    codigo_linha,
    codigo_linha_id,
    prefixo,
    sentido,
    latitude,
    longitude,
    timestamp_posicao
FROM sptrans_position
WHERE ativo = true
"""

df = pd.read_sql(query, engine)

st.title("🚌 Localização de Ônibus por Parada")

# 1. Filtro de Parada
lista_paradas = df['codigo_parada'].unique()
parada_selecionada = st.selectbox("Selecione o Código da Parada:", lista_paradas)

# 2. Filtrar DataFrame
dados_filtrados = df[df['codigo_parada'] == parada_selecionada]

if not dados_filtrados.empty:
    # Coordenadas da Parada (pega a primeira linha do filtro)
    lat_p = dados_filtrados.iloc[0]['latitude_parada']
    lon_p = dados_filtrados.iloc[0]['longitude_parada']

    # Criar DataFrames separados para o mapa
    df_parada = pd.DataFrame({'lat': [lat_p], 'lon': [lon_p], 'nome': ['Parada Selecionada']})
    df_onibus = dados_filtrados[['latitude_veiculo', 'longitude_veiculo', 'letreiro', 'prefixo_veiculo']].rename(
        columns={'latitude_veiculo': 'lat', 'longitude_veiculo': 'lon'}
    )

    # 3. Visualização com Pydeck (mais profissional que o st.map)
    view_state = pdk.ViewState(latitude=lat_p, longitude=lon_p, zoom=14, pitch=0)

    layer_parada = pdk.Layer(
        "ScatterplotLayer",
        df_parada,
        get_position="[lon, lat]",
        get_color="[255, 0, 0, 200]", # Vermelho para a parada
        get_radius=50,
    )

    layer_onibus = pdk.Layer(
        "ScatterplotLayer",
        df_onibus,
        get_position="[lon, lat]",
        get_color="[0, 0, 255, 200]", # Azul para os ônibus
        get_radius=30,
        pickable=True,
    )

    st.pydeck_chart(pdk.Deck(
        map_style='mapbox://styles/mapbox/light-v9',
        initial_view_state=view_state,
        layers=[layer_parada, layer_onibus],
        tooltip={"text": "Linha: {letreiro}\nVeículo: {prefixo_veiculo}"}
    ))

    # 4. Tabela de Detalhes
    st.subheader("Ônibus chegando nesta parada")
    st.dataframe(dados_filtrados[['letreiro', 'origem', 'destino', 'horario_previsto', 'prefixo_veiculo']])
else:
    st.warning("Nenhum dado encontrado para esta parada.")
