# minimum-trocco-alpha

troccoライクなジョブ実行基盤の最小実装。アーキテクチャ検証用。  
詳細は [.claude/plans/spec.md](.claude/plans/spec.md) を参照。

## 構成

```
User → ui (nginx)
     → api (Deployment) → ElasticMQ (jobs queue)
                        → PostgreSQL
manager (Deployment)    → ElasticMQ をポーリング
                        → k8s Job (worker) を作成
                        → k8s Job をポーリングして DB に反映
worker (k8s Job)        → ダミー処理 (sleep + 確率失敗)
```

## 必要なもの

- Docker
- kind
- kubectl
- skaffold
- just (任意)
- Go 1.26
- Node.js 22+ (UI 開発時)

## 起動

```bash
just cluster-up
just dev          # skaffold dev: ビルド + デプロイ + port-forward
```

ブラウザで http://localhost:3000 を開く。

## クリーンアップ

```bash
just undeploy
just cluster-down
```
