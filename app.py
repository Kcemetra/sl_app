import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import requests
import aiohttp
import asyncio
import time
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime


# Вычисление скользящего среднего, стандартного отклонения и аномалий для выбранного города
def process_city_data(df_city):
    df_city = df_city.sort_values('timestamp').copy()
    df_city['rolling_mean'] = df_city['temperature'].rolling(window=30, min_periods=1).mean()
    df_city['rolling_std'] = df_city['temperature'].rolling(window=30, min_periods=1).std().fillna(0)
    upper_bound = df_city['rolling_mean'] + 2 * df_city['rolling_std']
    lower_bound = df_city['rolling_mean'] - 2 * df_city['rolling_std']
    df_city['is_anomaly'] = (df_city['temperature'] > upper_bound) | (df_city['temperature'] < lower_bound)
    return df_city

# Синхронная обработка всех городов
@st.cache_data
def analyze_data_sync(df):
    return pd.concat([process_city_data(df[df['city'] == city]) for city in df['city'].unique()])

# Параллельная обработка всех городов
@st.cache_data
def analyze_data_parallel(df):
    cities = df['city'].unique()
    city_dfs = [df[df['city'] == city] for city in cities]
    with ProcessPoolExecutor() as executor:
        results = list(executor.map(process_city_data, city_dfs))
    return pd.concat(results)

# Среднее и std для каждого сезона в городе
def get_seasonal_stats(df_city):
    return df_city.groupby('season')['temperature'].agg(['mean', 'std']).reset_index()

# Синхронный запрос погоды для одного города
def get_current_weather_sync(city, api_key):
    url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}&units=metric"
    response = requests.get(url)
    return response.json(), response.status_code

# Асинхронный запрос погоды для одного города
async def fetch_weather_async(session, city, api_key):
    url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}&units=metric"
    async with session.get(url) as response:
        return await response.json(), response.status

# Асинхронный запрос погоды для всех городов
async def get_all_weather_async(cities, api_key):
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_weather_async(session, city, api_key) for city in cities]
        return await asyncio.gather(*tasks)


# Сделал красивые карточки для сравнения методов
def render_benchmark_card(title, time_val, is_winner, delta_time=0):
    bg_color = "#d4edda" if is_winner else "#f8f9fa"
    border_color = "#c3e6cb" if is_winner else "#dee2e6"
    text_color = "#155724" if is_winner else "#495057"
    delta_html = f"<div style='font-size: 13px; margin-top: 4px;'>🚀 Быстрее на {delta_time:.3f} с</div>" if is_winner and delta_time > 0 else "<div style='font-size: 13px; margin-top: 4px;'>&nbsp;</div>"
    html = f"""
    <div style='background-color: {bg_color}; border: 1px solid {border_color}; border-radius: 8px; padding: 15px; text-align: center; color: {text_color};'>
        <div style='font-size: 14px; margin-bottom: 5px;'>{title}</div>
        <div style='font-size: 24px; font-weight: bold;'>{time_val:.3f} сек</div>
        {delta_html}
    </div>
    """
    st.markdown(html, unsafe_allow_html=True)


def main():
    st.set_page_config(page_title="Анализ погоды", layout="wide")
    st.title("Анализ температурных данных и мониторинг текущей температуры через OpenWeatherMap API")

    col_file, col_api = st.columns(2)
    with col_file:
        uploaded_file = st.file_uploader("Загрузите файл .CSV", type="csv")

    # тестовый запрос, чтобы проверить ключ и показать статус
    with col_api:
        api_key = st.text_input("OpenWeatherMap API Key")
        is_api_valid = False
        if api_key:
            _, test_status = get_current_weather_sync("London", api_key)
            if test_status == 200:
                is_api_valid = True
                st.caption("✅ Ключ валиден и работает")
            else:
                st.caption("❌ Введен некорректный ключ")

    st.markdown("---")

    if uploaded_file is not None:
        raw_data = pd.read_csv(uploaded_file)
        raw_data['timestamp'] = pd.to_datetime(raw_data['timestamp'])

        processed_data = analyze_data_parallel(raw_data)
        cities_list = sorted(processed_data['city'].unique())
        st.subheader("Сравнение производительности")

        b_col1, b_col2 = st.columns(2)

        # Бенчмарк проведения анализа
        with b_col1:
            st.markdown("**Сравниваем скорость выполнения анализа с распараллеливанием и без него**")
            if st.button("Сравнить", key="btn_calc"):
                with st.spinner("..."):
                    start = time.time()
                    _ = analyze_data_sync.__wrapped__(raw_data)
                    t_sync = time.time() - start

                    start = time.time()
                    _ = analyze_data_parallel.__wrapped__(raw_data)
                    t_par = time.time() - start

                    c1, c2 = st.columns(2)
                    with c1:
                        render_benchmark_card("Синхронно", t_sync, t_sync < t_par, t_par - t_sync)
                    with c2:
                        render_benchmark_card("Параллельно", t_par, t_par <= t_sync, t_sync - t_par)

        # Бенчмарк запросов к api
        with b_col2:
            st.markdown("**Сравниваем синхронные и асинхронные методы запросов к API**")
            if st.button("Сравнить API", key="btn_api"):
                if not is_api_valid:
                    st.error("Для теста API нужен валидный ключ!")
                else:
                    with st.spinner("Запрашиваем погоду для 10 городов..."):
                        test_cities = cities_list[:10]

                        start = time.time()
                        for c in test_cities:
                            get_current_weather_sync(c, api_key)
                        t_api_sync = time.time() - start

                        start = time.time()
                        asyncio.run(get_all_weather_async(test_cities, api_key))
                        t_api_async = time.time() - start

                        c1, c2 = st.columns(2)
                        with c1:
                            render_benchmark_card("Requests (Цикл)", t_api_sync, t_api_sync < t_api_async, t_api_async - t_api_sync)
                        with c2:
                            render_benchmark_card("Aiohttp (Asyncio)", t_api_async, t_api_async <= t_api_sync, t_api_sync - t_api_async)

        st.markdown("---")

        selected_city = st.selectbox("Выберите город для детального анализа", cities_list)
        city_data = processed_data[processed_data['city'] == selected_city]

        # Блок исторической статистики
        st.subheader(f"📊 Историческая статистика: {selected_city}")
        stat1, stat2, stat3, stat4 = st.columns(4)
        stat1.metric("Мин. температура", f"{city_data['temperature'].min():.1f} °C")
        stat2.metric("Макс. температура", f"{city_data['temperature'].max():.1f} °C")
        stat3.metric("Средняя температура", f"{city_data['temperature'].mean():.1f} °C")
        stat4.metric("Количество аномалий", f"{city_data['is_anomaly'].sum()}")

        # График временного ряда
        fig_ts = go.Figure()
        fig_ts.add_trace(
            go.Scatter(x=city_data['timestamp'], y=city_data['temperature'], mode='lines', name='Температура',
                       line=dict(color='lightgray')))
        fig_ts.add_trace(
            go.Scatter(x=city_data['timestamp'], y=city_data['rolling_mean'], mode='lines', name='Скользящее среднее',
                       line=dict(color='blue')))
        anomalies = city_data[city_data['is_anomaly']]
        fig_ts.add_trace(
            go.Scatter(x=anomalies['timestamp'], y=anomalies['temperature'], mode='markers', name='Аномалии',
                       marker=dict(color='red', size=6)))
        fig_ts.update_layout(height=400, margin=dict(l=0, r=0, t=30, b=0), hovermode="x unified")
        st.plotly_chart(fig_ts)

        # Текущая погода
        st.subheader("⛅ Мониторинг текущей температуры")
        if not api_key:
            st.info("Введите API ключ в верхней части страницы для проверки текущей погоды.")
        else:
            weather_data, status_code = get_current_weather_sync(selected_city, api_key)

            if status_code == 200:
                current_temp = weather_data['main']['temp']

                # Определение сезона
                month = datetime.now().month
                season_map = {12: "winter", 1: "winter", 2: "winter", 3: "spring", 4: "spring", 5: "spring",
                              6: "summer", 7: "summer", 8: "summer", 9: "autumn", 10: "autumn", 11: "autumn"}
                current_season = season_map[month]

                # Историческая норма
                season_stats = get_seasonal_stats(city_data)
                stats_row = season_stats[season_stats['season'] == current_season].iloc[0]
                mean_temp, std_temp = stats_row['mean'], stats_row['std']
                lower_bound, upper_bound = mean_temp - 2 * std_temp, mean_temp + 2 * std_temp
                is_normal = lower_bound <= current_temp <= upper_bound

                # Метрики
                mc1, mc2, mc3 = st.columns(3)
                mc1.metric("Текущая температура", f"{current_temp} °C")
                mc2.metric("Текущий сезон", current_season.capitalize())
                mc3.metric("Историческая норма", f"от {lower_bound:.1f} до {upper_bound:.1f} °C")

                if is_normal:
                    st.success(f"Температура в городе {selected_city} в норме для сезона {current_season}.")
                else:
                    st.error(
                        f"Температура в городе {selected_city} выходит за пределы нормы для сезона {current_season}.")

            elif status_code == 401:
                st.error("Введен некорректный API ключ. Ответ сервера:")
                st.code(
                    '{"cod":401, "message": "Invalid API key. Please see https://openweathermap.org/faq#error401 for more info."}',
                    language="json")
            else:
                st.error(f"Произошла ошибка при обращении к API: {weather_data.get('message', 'Неизвестная ошибка')}")

        st.markdown("---")

        # Сезонные профили
        st.subheader("🍂 Исторические сезонные профили")
        season_stats_all = get_seasonal_stats(city_data)
        fig_season = px.bar(season_stats_all, x='season', y='mean', error_y='std',
                            labels={'season': 'Сезон', 'mean': 'Средняя температура (°C)'},
                            color='season', height=400)
        fig_season.update_layout(margin=dict(l=0, r=0, t=10, b=0))
        st.plotly_chart(fig_season)

    else:
        st.info("Пожалуйста, загрузите файл с историческими данными (temperature_data.csv) для начала работы.")


if __name__ == "__main__":
    main()