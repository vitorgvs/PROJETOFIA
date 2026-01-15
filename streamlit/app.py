import streamlit as st
import duckdb
import connectorx as cx  
import pandas as pd 

st.set_page_config(page_title="Ônibus SPTrans", layout="wide")


CONN_STR = "trino://trino@trino:8080/delta/refined"

@st.cache_resource(ttl=30)  
def load_data_to_duckdb():
    query = "SELECT * FROM delta.refined.vw_bi_sptrans_prevision_stop"
    

    arrow_table = cx.read_sql(CONN_STR, query, return_type="arrow")
    

    con = duckdb.connect(database=':memory:')
    

    con.register('tb_onibus', arrow_table)
    
    return con


try:
    con = load_data_to_duckdb()
    
    st.title("🚌 Localização de Ônibus por Parada (Engine: DuckDB)")


    df_paradas = con.execute("SELECT DISTINCT nome_parada FROM tb_onibus ORDER BY nome_parada").fetch_df()
    lista_paradas = df_paradas['nome_parada'].tolist()
    
    parada_selecionada = st.selectbox("Selecione o Código da Parada:", lista_paradas)


    query_filtrada = f"""
        SELECT 
            latitude_parada, 
            longitude_parada, 
            latitude_veiculo, 
            longitude_veiculo, 
            letreiro, 
            prefixo_veiculo,
            veiculo_sentido, 
            horario_previsto
        FROM tb_onibus 
        WHERE nome_parada = '{parada_selecionada}'
    """
    dados_filtrados = con.execute(query_filtrada).fetch_df()

    if not dados_filtrados.empty:

        lat_p = dados_filtrados.iloc[0]['latitude_parada']
        lon_p = dados_filtrados.iloc[0]['longitude_parada']

  
        df_parada = pd.DataFrame({'lat': [lat_p], 'lon': [lon_p], 'nome': ['Parada Selecionada']})
        
        df_onibus = dados_filtrados[['latitude_veiculo', 'longitude_veiculo', 'letreiro', 'prefixo_veiculo', 'horario_previsto']].rename(
            columns={'latitude_veiculo': 'lat', 'longitude_veiculo': 'lon'}
        )

        view_state = pdk.ViewState(latitude=lat_p, longitude=lon_p, zoom=14, pitch=0)

        layer_parada = pdk.Layer(
            "ScatterplotLayer",
            df_parada,
            get_position="[lon, lat]",
            get_color="[255, 0, 0, 200]",
            get_radius=50,
        )

        layer_onibus = pdk.Layer(
            "ScatterplotLayer",
            df_onibus,
            get_position="[lon, lat]",
            get_color="[0, 0, 255, 200]",
            get_radius=30,
            pickable=True,
        )

        st.pydeck_chart(pdk.Deck(
            map_style=None,
            initial_view_state=view_state,
            layers=[layer_parada, layer_onibus],
            tooltip={"text": "Linha: {letreiro}\nVeículo: {prefixo_veiculo}\nChegada: {horario_previsto}"}
        ))

        st.subheader("Ônibus chegando nesta parada")
        

        display_cols = ['letreiro', 'veiculo_sentido', 'horario_previsto', 'prefixo_veiculo']
        st.dataframe(
            dados_filtrados[display_cols]
            .drop_duplicates()
            .sort_values(by='horario_previsto', ascending=True)
        )
    else:
        st.warning("Nenhum dado encontrado para esta parada.")

except Exception as e:
    st.error(f"Erro ao carregar dados do Trino: {e}")
    st.info("Verifique se o container Trino está rodando e se a URL de conexão está correta.")