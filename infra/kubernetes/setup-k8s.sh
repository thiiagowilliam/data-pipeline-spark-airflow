#!/bin/bash
# infra/kubernetes/setup-k8s.sh

# Exit immediately if a command exits with a non-zero status.
set -e

echo "--- Starting Kubernetes Infrastructure Setup ---"

# --- 1. Add required Helm Repositories ---
echo "--- Step 1: Adding Helm Repositories ---"
helm repo add spark-operator https://kubeflow.github.io/spark-operator &> /dev/null || echo "spark-operator repo already exists."
helm repo add apache-airflow https://airflow.apache.org &> /dev/null || echo "apache-airflow repo already exists."
helm repo add bitnami https://charts.bitnami.com &> /dev/null || echo "bitnami repo already exists."
helm repo update
echo "Helm repositories are up to date."

# --- 2. Create Namespaces ---
echo "--- Step 2: Creating Kubernetes Namespaces ---"
kubectl create namespace spark-operator --dry-run=client -o yaml | kubectl apply -f -
kubectl create namespace airflow --dry-run=client -o yaml | kubectl apply -f -
kubectl create namespace minio --dry-run=client -o yaml | kubectl apply -f -
kubectl create namespace postgres --dry-run=client -o yaml | kubectl apply -f -
kubectl create namespace spark-jobs --dry-run=client -o yaml | kubectl apply -f -
echo "Namespaces created."

# --- 2.1. Create Secrets ---
echo "--- Step 2.1: Creating Kubernetes Secrets ---"
# Apply secrets before deploying charts that depend on them.
# Note: The secrets.yaml file itself specifies the namespace for each secret.
kubectl apply -f infra/kubernetes/secrets.yaml
echo "Secrets created."

# --- 3. Deploy Spark Operator ---
echo "--- Step 3: Deploying Spark Operator ---"
helm upgrade --install spark-operator spark-operator/spark-operator \
  --namespace spark-operator \
  --values infra/kubernetes/values/spark-operator-values.yaml
echo "Spark Operator deployment initiated."

# --- 4. Deploy MinIO (Data Lake Storage) ---
echo "--- Step 4: Deploying MinIO ---"
helm upgrade --install minio bitnami/minio \
  --namespace minio \
  --set auth.existingSecret=minio-credentials-secret \
  --set persistence.enabled=true \
  --set persistence.size=10Gi \
  --set buckets[0].name=bronze \
  --set buckets[1].name=silver \
  --set buckets[2].name=gold \
  --set buckets[3].name=spark-logs
echo "MinIO deployment initiated."

# --- 5. Deploy PostgreSQL (Source Database) ---
echo "--- Step 5: Deploying PostgreSQL ---"
# Create a temporary values file to handle the multi-line init script correctly.
cat > temp-postgres-values.yaml <<-EOF
auth:
  existingSecret: "postgres-credentials-secret"
persistence:
  enabled: true
  size: 10Gi
primary:
  initdb:
    scripts:
      01_create_sales_table.sql: |
        CREATE TABLE public.sales (
            id SERIAL PRIMARY KEY,
            order_id VARCHAR(50) NOT NULL,
            customer_id VARCHAR(50) NOT NULL,
            product_id VARCHAR(50) NOT NULL,
            quantity INTEGER NOT NULL,
            unit_price NUMERIC(10, 2) NOT NULL,
            order_date TIMESTAMP NOT NULL,
            status VARCHAR(20)
        );
        INSERT INTO public.sales (order_id, customer_id, product_id, quantity, unit_price, order_date, status) VALUES
        ('ORD-001', 'CUST-101', 'PROD-A', 2, 15.50, '2026-01-15 10:00:00', 'COMPLETED'),
        ('ORD-002', 'CUST-102', 'PROD-B', 1, 120.00, '2026-01-15 11:30:00', 'COMPLETED'),
        ('ORD-003', 'CUST-101', 'PROD-C', 5, 5.00, '2026-01-16 14:00:00', 'COMPLETED'),
        ('ORD-001', 'CUST-101', 'PROD-A', 2, 15.50, '2026-01-15 10:00:00', 'RETURNED');
EOF

# Install the chart using the temporary values file.
helm upgrade --install postgres bitnami/postgresql \
  --namespace postgres \
  -f temp-postgres-values.yaml

# Clean up the temporary file.
rm temp-postgres-values.yaml
echo "PostgreSQL deployment initiated."

# --- 6. Deploy Airflow ---
echo "--- Step 6: Deploying Airflow ---"
helm upgrade --install airflow apache-airflow/airflow \
  --namespace airflow \
  --values infra/kubernetes/values/airflow-values.yaml
echo "Airflow deployment initiated."

echo ""
echo "--- Kubernetes Infrastructure Setup Complete ---"
echo "--- Use 'kubectl get pods -A -w' to monitor pod readiness. ---"