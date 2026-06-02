set shell := ["bash", "-cu"]

cluster_name := "mintrcc"

# クラスタ作成
cluster-up:
  kind create cluster --config kind-config.yaml

# クラスタ削除
cluster-down:
  kind delete cluster --name {{cluster_name}}

# Skaffold 開発モード（hot reload + port-forward）
dev:
  skaffold dev --port-forward=user

# 1度だけビルド + デプロイ
deploy:
  skaffold run --port-forward=user

# 全リソース削除
undeploy:
  skaffold delete

# Goビルドの簡易チェック
build-go:
  cd services && go build ./...

# Go vet
vet:
  cd services && go vet ./...

# UI開発サーバ（ホストで実行、APIをport-forwardすること）
ui-dev:
  cd ui && npm install && npm run dev

# 状態確認
status:
  kubectl get pods,svc,jobs

# Worker Job 一覧
jobs:
  kubectl get jobs -l app=worker

# ===========================
# Nnamespace
# ===========================

namespaces:
  kubectl apply -f k8s/namespaces.yaml

# ===========================
# LGTM stack
# ===========================

o11y-up: helm-repos namespaces helm-minio helm-loki helm-mimir helm-tempo helm-alloy helm-otel-collector helm-otel-gateway helm-grafana

o11y-down:
  -helm uninstall grafana --namespace o11y
  -helm uninstall tempo --namespace o11y
  -helm uninstall otel-gateway --namespace o11y
  -helm uninstall otel-collector --namespace o11y
  -helm uninstall mimir --namespace o11y
  -helm uninstall alloy --namespace o11y
  -helm uninstall loki --namespace o11y
  -helm uninstall minio --namespace o11y
  -kubectl delete -f k8s/config/
  -kubectl delete -f k8s/namespaces.yaml

helm-repos:
  helm repo add grafana           https://grafana.github.io/helm-charts                      2>/dev/null || true
  helm repo add grafana-community https://grafana-community.github.io/helm-charts            2>/dev/null || true
  helm repo add minio             https://charts.min.io/                                     2>/dev/null || true
  helm repo add open-telemetry    https://open-telemetry.github.io/opentelemetry-helm-charts 2>/dev/null || true
  helm repo update

helm-minio:
  kubectl apply -f k8s/config/minio-secret.yaml
  helm upgrade --install minio minio/minio \
    --namespace o11y \
    --version 5.4.0 \
    --values k8s/helm/values/minio.yaml \
    --wait --timeout 5m

helm-loki:
  helm upgrade --install loki grafana-community/loki \
    --namespace o11y \
    --version 6.55.0 \
    --values k8s/helm/values/loki.yaml \
    --wait --timeout 5m

helm-alloy:
  helm upgrade --install alloy grafana/alloy \
    --namespace o11y \
    --version 1.7.0 \
    --values k8s/helm/values/alloy.yaml \
    --wait --timeout 5m

helm-mimir:
  helm upgrade --install mimir grafana/mimir-distributed \
    --namespace o11y \
    --version 6.0.6 \
    --values k8s/helm/values/mimir.yaml \
    --wait --timeout 5m

helm-otel-collector:
  helm upgrade --install otel-collector open-telemetry/opentelemetry-collector \
    --namespace o11y \
    --version 0.150.1 \
    --values k8s/helm/values/otel-collector.yaml \
    --wait --timeout 5m

helm-otel-gateway:
  helm upgrade --install otel-gateway open-telemetry/opentelemetry-collector \
    --namespace o11y \
    --version 0.150.1 \
    --values k8s/helm/values/otel-gateway.yaml \
    --wait --timeout 5m

helm-tempo:
  helm upgrade --install tempo grafana-community/tempo \
    --namespace o11y \
    --version 1.26.7 \
    --values k8s/helm/values/tempo.yaml \
    --wait --timeout 5m

helm-grafana:
  kubectl apply -f k8s/config/grafana-secret.yaml
  kubectl create configmap grafana-dashboards \
    --from-file=k8s/helm/dashboards/ \
    --namespace o11y \
    --dry-run=client -o yaml | kubectl apply -f -
  helm upgrade --install grafana grafana-community/grafana \
    --namespace o11y \
    --version 12.1.1 \
    --values k8s/helm/values/grafana.yaml \
    --wait --timeout 5m
