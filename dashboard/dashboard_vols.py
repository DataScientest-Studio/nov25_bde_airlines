import os
import streamlit as st
import pandas as pd
import psycopg2
import plotly.express as px
import joblib
from streamlit_autorefresh import st_autorefresh

st.set_page_config(page_title="Dashboard retards vols", layout="wide")

# 🔄 Auto-refresh toutes les 30 minutes
st_autorefresh(
    interval=30 * 60 * 1000,  # 30 minutes en ms
    key="airlines_30min_refresh"
)


# =====================
# CONFIG via ENV VARS
# =====================
PG_HOST = os.getenv("PG_HOST", "postgres_data")
PG_DB = os.getenv("PG_DB", "flight")
PG_USER = os.getenv("PG_USER", "postgres")
PG_PASSWORD = os.getenv("PG_PASSWORD", "postgres")
PG_PORT = int(os.getenv("PG_PORT") or 5432)
MODEL_PATH = os.getenv("MODEL_PATH", "/app/models/delay_tree_regressor.pkl")

# =====================
# Connection à la DB 
# =====================
def get_conn():
    return psycopg2.connect(
        host=PG_HOST,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD,
        port=PG_PORT,
    )

@st.cache_resource
def load_model_artifact(path: str, mtime: float):
    if not os.path.exists(path):
        return None
    return joblib.load(path)


# =====================
# TABS
# =====================
tab_dash, tab_pred = st.tabs(["📊 Dashboard", "🔮 Prédiction"])

# =====================
# DASHBOARD
# =====================
with tab_dash:
    conn = get_conn()

    # ============================
    # REQUÊTE SQL PRINCIPALE
    # ============================
    query_main = """
    SELECT
        a.iata_code AS airport,
        a.name AS airport_name,
        al.iata_code AS airline,
        al.name AS airline_name,
        COUNT(fd.delay_id) AS nb_vols,
        AVG(fd.total_delay) AS avg_delay
    FROM flight_delays fd
    JOIN flights f ON fd.flight_id = f.flight_id
    JOIN airports a ON f.dep_airport_id = a.airport_id
    JOIN airlines al ON f.airline_id = al.airline_id
    WHERE fd.total_delay IS NOT NULL
      AND al.name IS NOT NULL
      AND al.iata_code IS NOT NULL
    GROUP BY a.iata_code, a.name, al.iata_code, al.name
    ORDER BY avg_delay DESC;
    """

    df = pd.read_sql(query_main, conn)

    # ============================
    # REQUÊTE SQL POUR COURBE PAR HEURE
    # ============================
    query_hour = """
    SELECT
        a.iata_code AS airport,
        al.iata_code AS airline,
        EXTRACT(HOUR FROM fd.dep_time_utc) AS dep_hour,
        fd.total_delay AS delay
    FROM flight_delays fd
    JOIN flights f ON fd.flight_id = f.flight_id
    JOIN airports a ON f.dep_airport_id = a.airport_id
    JOIN airlines al ON f.airline_id = al.airline_id
    WHERE fd.total_delay IS NOT NULL
      AND al.name IS NOT NULL
      AND al.iata_code IS NOT NULL;
    """

    df_hour = pd.read_sql(query_hour, conn)
    conn.close()

    # ============================
    # CRÉATION DES LABELS
    # ============================
    df["airport_filter"] = df["airport_name"] + " (" + df["airport"] + ")"
    df["airline_filter"] = df["airline_name"] + " (" + df["airline"] + ")"

    df["airport_plot"] = df["airport"]
    df["airline_plot"] = df["airline"]

    # ============================
    # SIDEBAR : FILTRES
    # ============================
    st.sidebar.header("Filtres")

    airports = df["airport_filter"].dropna().unique()
    selected_airports = st.sidebar.multiselect(
        "Aéroports",
        airports,
        default=list(airports)
    )

    search_airline = st.sidebar.text_input("Rechercher une compagnie", "")
    airline_labels = df["airline_filter"].dropna().unique()

    if search_airline:
        airlines_filtered_list = [
            a for a in airline_labels if search_airline.lower() in str(a).lower()
        ]
    else:
        airlines_filtered_list = airline_labels

    selected_airlines = st.sidebar.multiselect(
        "Compagnies aériennes",
        airlines_filtered_list,
        default=list(airlines_filtered_list)
    )

    if df.empty:
        st.warning("Aucune donnée à afficher.")
        st.stop()

    min_delay, max_delay = st.sidebar.slider(
        "Retard moyen (min)",
        float(df["avg_delay"].min()),
        float(df["avg_delay"].max()),
        (float(df["avg_delay"].min()), float(df["avg_delay"].max()))
    )

    # ============================
    # APPLICATION DES FILTRES
    # ============================
    df_filtered = df[
        (df["airport_filter"].isin(selected_airports)) &
        (df["airline_filter"].isin(selected_airlines)) &
        (df["avg_delay"] >= min_delay) &
        (df["avg_delay"] <= max_delay)
    ]

    df_hour_filtered = df_hour[
        (df_hour["airport"].isin(df_filtered["airport_plot"])) &
        (df_hour["airline"].isin(df_filtered["airline_plot"]))
    ]

    # ============================
    # TITRE
    # ============================
    st.title("Dashboard des retards de vols")

    # ============================
    # KPI
    # ============================
    col1, col2, col3, col4 = st.columns(4)

    col1.metric("Aéroports analysés", int(df_filtered["airport_plot"].nunique()))
    col2.metric("Compagnies analysées", int(df_filtered["airline_plot"].nunique()))
    col3.metric("Total vols", int(df_filtered["nb_vols"].sum()) if not df_filtered.empty else 0)
    col4.metric("Retard moyen total", round(float(df_filtered["avg_delay"].mean()), 1) if not df_filtered.empty else 0)

    st.markdown("---")

    # ============================
    # 1) RETARD MOYEN PAR AÉROPORT
    # ============================
    st.subheader("Retard moyen par aéroport")

    df_airport = df_filtered.groupby("airport_plot", as_index=False).agg({
        "avg_delay": "mean",
        "nb_vols": "sum"
    })

    fig1 = px.bar(
        df_airport,
        x="airport_plot",
        y="avg_delay",
        title="Retard moyen par aéroport",
    )
    st.plotly_chart(fig1, use_container_width=True)

    # ============================
    # 2) RETARD MOYEN PAR COMPAGNIE
    # ============================
    st.subheader("Retard moyen par compagnie aérienne")

    df_airline = df_filtered.groupby("airline_plot", as_index=False).agg({
        "avg_delay": "mean",
        "nb_vols": "sum"
    })

    fig2 = px.bar(
        df_airline,
        x="airline_plot",
        y="avg_delay",
        title="Retard moyen par compagnie",
    )
    st.plotly_chart(fig2, use_container_width=True)

    # ============================
    # 3) HEATMAP AÉROPORT × COMPAGNIE
    # ============================
    st.subheader("Heatmap : retard moyen par aéroport × compagnie")

    pivot = df_filtered.pivot_table(
        index="airport_plot",
        columns="airline_plot",
        values="avg_delay",
        aggfunc="mean"
    )

    fig3 = px.imshow(
        pivot,
        aspect="auto",
        title="Heatmap des retards"
    )
    st.plotly_chart(fig3, use_container_width=True)

    # ============================
    # 4) TOP 10 DES PAIRES AÉROPORT × COMPAGNIE
    # ============================
    st.subheader("Top 10 des combinaisons aéroport × compagnie les plus en retard")

    top10 = df_filtered.sort_values("avg_delay", ascending=False).head(10)

    fig4 = px.bar(
        top10,
        x="airport_plot",
        y="avg_delay",
        color="airline_plot",
        title="Top 10 des retards"
    )
    st.plotly_chart(fig4, use_container_width=True)

    # ============================
    # 5) COURBE DES RETARDS PAR HEURE (FILTRÉE)
    # ============================
    st.subheader("Évolution des retards par heure (après filtres)")

    if df_hour_filtered.empty:
        st.info("Pas assez de données pour tracer la courbe horaire avec ces filtres.")
    else:
        df_hour_grouped = df_hour_filtered.groupby("dep_hour", as_index=False).agg({
            "delay": "mean"
        })

        fig5 = px.line(
            df_hour_grouped,
            x="dep_hour",
            y="delay",
            markers=True,
            title="Retard moyen par heure de la journée"
        )
        fig5.update_layout(
            xaxis_title="Heure (UTC)",
            yaxis_title="Retard moyen (minutes)",
            xaxis=dict(dtick=1)
        )
        st.plotly_chart(fig5, use_container_width=True)

    # ============================
    # TABLEAUX DÉTAILLÉS
    # ============================
    st.subheader("Données détaillées")

    tab1, tab2 = st.tabs(["Aéroports", "Compagnies"])

    with tab1:
        st.dataframe(df_airport.sort_values("avg_delay", ascending=False), use_container_width=True)

    with tab2:
        st.dataframe(df_airline.sort_values("avg_delay", ascending=False), use_container_width=True)


# =====================
# PREDICTION
# ===================
with tab_pred:
    st.title("Prédiction du retard au départ (dep_delay)")

    mtime = os.path.getmtime(MODEL_PATH) if os.path.exists(MODEL_PATH) else 0.0
    artifact = load_model_artifact(MODEL_PATH, mtime)


    if artifact is None:
        st.error("Modèle introuvable. Vérifie le montage du volume ./models et la variable MODEL_PATH.")
        st.code(f"MODEL_PATH = {MODEL_PATH}")
        st.stop()

    
    if isinstance(artifact, dict) and "pipeline" in artifact:
        pipe = artifact["pipeline"]
        metrics = artifact.get("metrics", {})
        target = artifact.get("target", "dep_delay")
        feature_cols = artifact.get("features", None)
    else:
        #
        pipe = artifact
        metrics = {}
        target = "dep_delay"
        feature_cols = None

    # Afficher métriques si dispo
    if metrics:
        c1, c2, c3 = st.columns(3)
        if "mae" in metrics:
            c1.metric("MAE (min)", f"{metrics['mae']:.2f}")
        if "rmse" in metrics:
            c2.metric("RMSE (min)", f"{metrics['rmse']:.2f}")
        if "r2" in metrics:
            c3.metric("R²", f"{metrics['r2']:.3f}")

    st.write("Renseigne les paramètres du vol, puis clique sur **Prédire**.")

    # 
   
    try:
        airline_opts = sorted(df["airline_plot"].dropna().unique().tolist())
        airport_opts = sorted(df["airport_plot"].dropna().unique().tolist())
    except Exception:
        airline_opts = []
        airport_opts = []

    if not airline_opts:
        # fallback simple
        airline_opts = ["AF", "BA", "LH"]
    if not airport_opts:
        airport_opts = ["CDG", "JFK", "LHR"]

    with st.form("predict_form_v2"):
        colA, colB, colC = st.columns(3)
        with colA:
            airline = st.selectbox("Compagnie (IATA)", options=airline_opts, index=0)
        with colB:
            origin_airport = st.selectbox("Aéroport départ (IATA)", options=airport_opts, index=0)
        with colC:
            destination_airport = st.selectbox("Aéroport arrivée (IATA)", options=airport_opts, index=min(1, len(airport_opts) - 1))

        col1, col2, col3, col4 = st.columns(4)
        with col1:
            scheduled_dep_hour = st.number_input("Heure départ (0-23)", min_value=0, max_value=23, value=12, step=1)
        with col2:
            day_of_week = st.number_input("Jour semaine (0=Dim ... 6=Sam)", min_value=0, max_value=6, value=1, step=1)
        with col3:
            month = st.number_input("Mois (1-12)", min_value=1, max_value=12, value=1, step=1)
        with col4:
            duration = st.number_input("Durée (minutes)", min_value=1, max_value=2000, value=120, step=5)

        submitted = st.form_submit_button("Prédire")

    if submitted:
        X_pred = pd.DataFrame([{
            "airline": str(airline).strip().upper(),
            "origin_airport": str(origin_airport).strip().upper(),
            "destination_airport": str(destination_airport).strip().upper(),
            "scheduled_dep_hour": int(scheduled_dep_hour),
            "day_of_week": int(day_of_week),
            "month": int(month),
            "duration": int(duration),
        }])

        # 
        if feature_cols:
            X_pred = X_pred.reindex(columns=feature_cols)

        try:
            pred = float(pipe.predict(X_pred)[0])
            pred = max(0.0, pred)  # éviter retard négatif
            st.success(f"{target} prédit : **{pred:.1f} minutes**")

            with st.expander("Détails (features envoyées au modèle)"):
                st.dataframe(X_pred, use_container_width=True)

        except Exception as e:
            st.error(f"Erreur lors de la prédiction: {e}")
            st.info("Astuce: vérifie que MODEL_PATH pointe vers le bon modèle (dep_delay_model.pkl).")
