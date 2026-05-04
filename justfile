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
