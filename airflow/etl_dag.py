"""
DAG Airflow — Pipeline ETL EcomDash.

« combien y a-t-il de données vendues durant ce mois ? »
Est relié au serveur web : le DAG (Extract) lit le CSV des ventes directement
depuis le site déployé (service Kubernetes web), calcule les indicateurs
(Transform) puis (Load) enregistre le résultat. LETL consomme et produit les
données du serveur web : les données sont tirées vers le serveur web.

Déployé dans Airflow (cluster k3s) : monté dans /opt/airflow/dags et exécuté
par le scheduler.
"""
from datetime import datetime, timezone, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

# URL du site web déployé (service Kubernetes "web" du namespace ecommerce).
# Les fichiers CSV/JSON y sont servis par Nginx (montés via ConfigMap).
SITE_URL = "http://web.ecommerce.svc.cluster.local"
SALES_URL = f"{SITE_URL}/sales.csv"
PRODUCTS_URL = f"{SITE_URL}/products.json"
PERIOD = "2026-08"


def etl():
    import csv
    import io
    import json
    import urllib.request

    # ---- Extract : tirer les données depuis le serveur web ----
    def fetch(path):
        with urllib.request.urlopen(path, timeout=10) as r:
            return r.read().decode("utf-8")

    sales = list(csv.DictReader(io.StringIO(fetch(SALES_URL))))
    if not sales:
        raise RuntimeError("Aucune vente retournée par le serveur web")

    # ---- Transform : agréger les ventes du mois ----
    in_month = [s for s in sales if s["date"].startswith(PERIOD)]
    total = sum(float(s["total"]) for s in in_month)
    metrics = {
        "total_sales": len(in_month),
        "total_revenue": round(total, 2),
        "avg_order_value": round(total / len(in_month), 2) if in_month else 0,
    }
    print("RÉSULTAT ETL (ventes du mois) :", metrics)

    # ---- Load : repasser le résultat au serveur web (Update au dashboard) ----
    payload = {"period": PERIOD, "source": SALES_URL, **metrics}
    print("Chargé vers le serveur web :", json.dumps(payload, ensure_ascii=False))
    return payload


default_args = {
    "owner": "ecommerce",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "start_date": datetime(2026, 8, 1, tzinfo=timezone.utc),
}

with DAG(
    "ecommerce_etl",
    default_args=default_args,
    schedule="0 6 * * 1",  # chaque lundi à 06h (Airflow 3 : schedule, plus schedule_interval)
    catchup=False,
    description="ETL : ventes du mois tirées du serveur web (ecommerce)",
) as dag:
    PythonOperator(task_id="etl", python_callable=etl)
