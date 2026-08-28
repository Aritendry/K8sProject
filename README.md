# EcomDash — Site e-commerce GitOps (K8s + ArgoCD + Airflow ETL)

## Objectif
Déployer un site e-commerce avec dashboards (fake datas) via **GitHub → ArgoCD → Kubernetes**,
servi par **Nginx** sur `www.ecommerce.lcl` avec **HTTPS** (ingress + certificat),
et agréger chaque mois les ventes avec un **Job Airflow (ETL)**. **Sans Docker**.

## Architecture
```
GitHub ──► ArgoCD ──► Kubernetes (Kustomize : ./k8s)
                        ├── Deployment Nginx (site monté via ConfigMap)
                        ├── Service
                        └── Ingress HTTPS (www.ecommerce.lcl + Secret TLS)

Airflow (namespace airflow) ──► DAG ecommerce_etl ──► lit sales.csv du serveur web
                                   (http://web.ecommerce.svc:80/sales.csv)
                                   ► calcule ventes du mois → résultat affiché au dashboard
```

## Arborescence (minimal)
```
├── k8s/                  # tout ce que déploie ArgoCD (Kustomize racine)
│   ├── kustomization.yaml   # charge les manifests du site
│   ├── configmap.yaml       # ConfigMap site (index.html, products.json, sales.csv)
│   ├── airflow-dags.yaml    # ConfigMap DAG Airflow (monté dans /opt/airflow/dags)
│   ├── site/                # sources du site web + fake datas
│   ├── namespace.yaml
│   ├── deployment.yaml      # Nginx + montage du ConfigMap site
│   ├── service.yaml
│   ├── ingress.yaml         # www.ecommerce.lcl + TLS
│   └── tls.yaml             # Secret TLS (certificat auto-signé openssl)
├── airflow/
│   ├── etl_dag.py           # DAG Airflow (version source GitHub)
├── argocd-app.yaml         # Application ArgoCD (GitOps : GitHub → K8s)
├── .gitignore
└── README.md
```

## Mise en place (résumé)
1. **Cluster k3s** (déjà actif ici, nœud `chromebook`) avec l'Ingress Controller
   **Nginx** installé (namespace `ingress-nginx`, classe `nginx`).
2. **Domaine** : `echo "192.168.88.84 www.ecommerce.lcl" >> /etc/hosts`
3. **ArgoCD** : installer dans `argocd` puis créer l'Application (path `./k8s`) :
   ```bash
   argocd repo add https://github.com/Aritendry/K8sProject.git
   kubectl apply -f - <<'EOF'
   apiVersion: argoproj.io/v1alpha1
   kind: Application
   metadata: {name: ecommerce, namespace: argocd}
   spec:
     project: default
     source: {repoURL: https://github.com/Aritendry/K8sProject.git,
              targetRevision: main, path: ./k8s}
     destination: {server: https://kubernetes.default.svc, namespace: ecommerce}
     syncPolicy: {automated: {selfHeal: true}}
   EOF
   ```
4. **Certificat** (déjà inclus) : auto-signé openssl pour `www.ecommerce.lcl`,
   encodé en base64 dans `k8s/tls.yaml`. L'Ingress le recharge via le secret `ecommerce-tls`.
5. **ETL Airflow (déjà branché)** : le DAG `ecommerce_etl` est monté dans
   `/opt/airflow/dags` du scheduler et s'exécute sur votre Airflow. Il lit
   `sales.csv` depuis le serveur web puis calcule les ventes du mois
   (→ **ex. 8 ventes / 1215,94 € CA**).
6. Accès : `https://www.ecommerce.lcl` (site) et `http://localhost:8080` (UI ArgoCD / Airflow via port-forward).

## Airflow — comment l'utiliser

Le DAG `ecommerce_etl` est déployé via le ConfigMap `k8s/airflow-dags.yaml`
(namespace `airflow`), monté dans `/opt/airflow/dags` du scheduler et du
dag-processor (initContainer de copie). Accès :

```bash
# UI Airflow (webserver/API) :
kubectl port-forward -n airflow svc/airflow-api-server 8080:8080
# ouvrir http://localhost:8080  (DAGs → ecommerce_etl)

# Déverrouiller + déclencher manuellement l'ETL depuis la CLI kubectl :
kubectl -n airflow exec deploy/airflow-api-server -- airflow dags unpause ecommerce_etl
kubectl -n airflow exec deploy/airflow-api-server -- airflow dags trigger ecommerce_etl
# → logs : RÉSULTAT ETL (ventes du mois) : {'total_sales': 8, ...}
```

## Commandes utiles
```bash
kubectl kustomize k8s            # aperçu du rendu (ConfigMap site + manifests)
python3 -m http.server 8080 -d k8s/site   # tester le site en local
```
