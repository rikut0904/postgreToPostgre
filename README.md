# PostgreSQL to PostgreSQL Migration Tool

PostgreSQL 間のデータ移行を Web UI で行う汎用ツール。
接続情報を入力するだけで、スキーマ・テーブル・データをまとめて移行できる。

## 特徴

- **Web UI 操作** - ブラウザ上で接続設定から移行実行まで完結
- **並行処理** - goroutine ワーカープールによるテーブル単位の並行コピー / インデックス・制約の並行作成
- **リアルタイム進捗** - WebSocket でテーブルごとの進捗率・経過時間をライブ表示
- **フル移行** - スキーマ、型、テーブル、データ、インデックス、制約、ビュー、関数、シーケンス、トリガーに対応
- **失敗テーブル表示** - 失敗したテーブルをエラー内容付きで一覧表示し、他のテーブルは続行

## クイックスタート

```bash
# ビルド
go build -o migrate .

# 起動
./migrate
# → http://localhost:8080 でアクセス
```

### 必要環境

- Go 1.22+
- PostgreSQL 12+（移行元・移行先）

## 使い方

### Step 1: 接続設定

移行元 (Source) と移行先 (Destination) の接続情報を入力し、「テスト接続」で疎通確認する。
URL 形式 (`postgres://user:pass@host:5432/dbname`) またはフィールド個別入力に対応。

### Step 2: 移行対象選択

「スキーマを取得」ボタンで移行元の構成を表示。
チェックボックスでテーブル・ビュー・関数・シーケンスを個別選択できる。

### Step 3: 移行オプション

| オプション | 選択肢 | デフォルト |
|-----------|--------|-----------|
| 既存テーブルの扱い | `DROP して再作成` / `既存をそのまま使う` | 既存をそのまま使う |
| 既存データの扱い | `TRUNCATE 後に INSERT` / `追記` / `UPSERT` | TRUNCATE |
| バッチサイズ | 数値 (行数) | 1000 |
| 並行ワーカー数 | 数値 (0 = 自動) | 0 (自動: min(テーブル数, 8)) |
| FK 制約無効化 | チェックボックス | 有効 |
| ドライラン | チェックボックス | 無効 |

### Step 4: 実行・進捗

「移行開始」で実行。テーブルごとの進捗・全体進捗率・経過時間がリアルタイム表示される。
失敗したテーブルがある場合、赤いパネルにエラー内容が一覧表示される。

## 並行処理アーキテクチャ

```
テーブルリスト → Channel → [Worker 1] ─→ src接続 / dst接続 / テーブルトランザクション
                         → [Worker 2] ─→ src接続 / dst接続 / テーブルトランザクション
                         → [Worker N] ─→ src接続 / dst接続 / テーブルトランザクション
```

- 各ワーカーは独自の src/dst DB 接続を持つ（`*pgx.Conn` は goroutine 非安全のため）
- ワーカー数は `min(テーブル数, 8)` で自動決定、手動指定も可能
- テーブル単位トランザクションで、失敗テーブルは記録して次のテーブルへ続行
- `sync/atomic` で全体進捗カウントをスレッドセーフに管理
- インデックス・制約作成も同様にワーカープールで並行処理

## 移行順序

依存関係を考慮し、以下の順序で実行する:

1. スキーマ (`CREATE SCHEMA`)
2. データ型 (ENUM / Composite)
3. テーブル構造 (`CREATE TABLE`)
4. **データコピー** (並行処理 / `COPY` or `INSERT` or `UPSERT`)
5. **インデックス** (並行処理 / `CREATE INDEX`)
6. **制約** (並行処理 / `ALTER TABLE ADD CONSTRAINT`)
7. ビュー (`CREATE VIEW`)
8. 関数 (`CREATE FUNCTION`)
9. シーケンス (`CREATE SEQUENCE`)
10. トリガー (`CREATE TRIGGER`)

## API

### REST

| メソッド | パス | 説明 |
|---------|------|------|
| POST | `/api/test-connection` | 接続テスト |
| POST | `/api/browse` | スキーマ・オブジェクト一覧取得 |
| POST | `/api/table-info` | テーブル詳細 (カラム, 行数) |
| POST | `/api/migrate` | 移行開始 |
| POST | `/api/migrate/cancel` | 移行中断 |
| GET | `/api/migrate/status` | 移行状態取得 |

### WebSocket

| パス | 説明 |
|------|------|
| `/ws/progress` | 進捗リアルタイム通知 |

## ディレクトリ構成

```
postgreToPostgre/
├── main.go                        # エントリーポイント (port 8080)
├── go.mod / go.sum
├── docs/
│   ├── ABOUT.md                   # ツール概要
│   └── SPEC.md                    # 詳細仕様書
├── internal/
│   ├── server/
│   │   ├── server.go              # HTTP ルーティング
│   │   └── handlers.go            # API ハンドラー
│   ├── migration/
│   │   ├── types.go               # データ型定義
│   │   ├── manager.go             # 移行ジョブ管理 (排他制御)
│   │   ├── migrator.go            # 移行ロジック (並行処理)
│   │   └── progress.go            # 進捗管理 (WebSocket 配信)
│   ├── database/
│   │   ├── connection.go          # DB 接続
│   │   ├── inspector.go           # スキーマ情報取得
│   │   └── query.go               # クエリユーティリティ
│   └── websocket/
│       ├── hub.go                 # WebSocket ハブ
│       └── client.go              # WebSocket クライアント
└── web/
    ├── index.html                 # メイン画面
    ├── help.html                  # ヘルプページ
    ├── css/style.css              # スタイル
    └── js/app.js                  # フロントエンドロジック
```

## ライセンス

Private
