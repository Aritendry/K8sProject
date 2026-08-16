"""
DAG Airflow — Pipeline ETL EcomDash.

Question métier : « combien y a-t-il de données vendues durant ce mois ? »
Le DAG lit le CSV des ventes (Extract), calcule les indicateurs (Transform)
puis écrit le résultat en JSON consommé par le dashboard web (Load).

Seul Airflow orchestre l'ETL (pas de CronJob Kubernetes).
Déployé dans le cluster : copier ce fichier dans dags/ d'Airflow, les données
dans k8s/site/ étant montées dans /workspace.
"""
from datetime import datetime, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator

CSV = "/workspace/sales.csv"
OUT = "/workspace/etl_result.json"
PERIOD = "2026-08"


def etl():
    import csv
    import json

    # Extract
    rows = []
    with open(CSV, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            rows.append(r)

    # Transform : ventes du mois, CA, panier moyen
    total = sum(float(r["total"]) for r in rows)
    metrics = {
        "total_sales": len(rows),
        "total_revenue": round(total, 2),
        "avg_order_value": round(total / len(rows), 2) if rows else 0,
    }

    # Load : écrit le JSON consommé par le site web (démo "données tirées vers le serveur")
    with open(OUT, "w", encoding="utf-8") as f:
        json.dump({"period": PERIOD, **metrics}, f, ensure_ascii=False, indent=2)
    print(metrics)


default_args = {
    "owner": "ecommerce",
    "start_date": datetime(2026, 8, 1, tzinfo=timezone.utc),
}

with DAG("ecommerce_etl", default_args=default_args, schedule_interval="0 6 * * 1", catchup=False) as dag:
    PythonOperator(task_id="etl", python_callable=etl)
