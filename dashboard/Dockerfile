FROM python:3.11-slim

WORKDIR /app

# Dépendances système minimales (souvent utiles)
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
  && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY dashboard_vols.py /app/dashboard_vols.py

EXPOSE 8501

CMD ["streamlit", "run", "/app/dashboard_vols.py", "--server.address=0.0.0.0", "--server.port=8501"]

