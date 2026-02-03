Nov25_bde_airlines
==============================


Project Organization
------------


    ├── airflow/
    │   ├── dags/
    │   │   ├── airlines_collect_etl.py
    │   │   ├── airlines_train_daily.py
    │   ├── scripts/
    │   │   ├── Collect_data.py
    │   │   ├── mongo_to_sql.py
    │   │   ├── train_ml.py
    │   ├── plugins/
    │   └── logs/
    ├── airflow-image/
    │   ├── Dockerfile
    │   └── requirements.txt
    ├── dashboard/
    │   ├── Dockerfile
    │   ├── dashboard_vols.py
    │   └── requirements.txt
    ├── infra/
    │   ├── init.sql
    ├── docker-compose.yml
    ├── README.md
    ├── .gitignore
    └── .env.example
--------

<p><small>Project based on the <a target="_blank" href="https://drivendata.github.io/cookiecutter-data-science/">cookiecutter data science project template</a>. #cookiecutterdatascience</small></p>
