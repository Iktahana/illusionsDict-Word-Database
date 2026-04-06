# Genji Word Database

https://幻辞.com

幻辞.comで使用される全語彙データを収録した、オープンソースの日本語語彙データベースです。

本リポジトリでは、複数の信頼できるソースと独自のクローリングシステムを統合・加工した語彙データを、扱いやすい **SQLite 形式** で提供しています。

## 📦 特徴

- **SQLite 形式**: データベースファイルをダウンロードするだけで、すぐにアプリケーションに組み込み可能です。
- **自動更新**: Genji の自動クローリングシステムにより、不定期にデータがビルドされ、常に最新の語彙が反映されます。
- **軽量かつ高速**: インデックスが最適化されており、数十万件のデータから瞬時に検索が可能です。

## 📂 データソース (Data Sources)

本データベースは、以下の優れたリソースを統合し、独自の加工を施したものです：

1.  **[JMdict (yomidevs/jmdict-yomitan)](https://github.com/yomidevs/jmdict-yomitan)** - 広範な辞書定義および語彙データ。
2.  **[Japanese Word Frequency (hingston/japanese)](https://github.com/hingston/japanese)** - 語彙の頻度・優先順位データ。
3.  **Genji Crawler System** - 独自のクローリングシステムによる最新のトレンド語彙および語法データ。

## 🚀 使い方

### API

SQLite を直接使用できない環境向けに、Datasette ベースの REST API を提供しています。

**エンドポイント:** `https://dict-api.illusions.app`

#### 定型クエリ

| クエリ | URL | パラメータ |
|--------|-----|-----------|
| 見出し語で検索 | `/genji/lookup_by_entry.json?word=雪` | `word` |
| 読みで検索 | `/genji/lookup_by_reading.json?reading=ゆき` | `reading` |
| 見出し語・読みを全文検索 | `/genji/search_entries.json?q=食べ` | `q` |
| 語釈を全文検索 | `/genji/search_definitions.json?q=eat` | `q` |
| ランダムな語を取得 | `/genji/random_entries.json?count=5` | `count` |

#### テーブル直接アクセス

```
# 全エントリ一覧（ページング付き）
https://dict-api.illusions.app/genji/entries.json?_size=20

# 特定 UUID のエントリ
https://dict-api.illusions.app/genji/entries/UUID.json

# フィルタ付きクエリ
https://dict-api.illusions.app/genji/entries.json?entry=雪&_shape=array
```

詳細なクエリパラメータは [Datasette ドキュメント](https://docs.datasette.io/en/stable/json_api.html) を参照してください。

### SQLite を直接使用する

[Releases](/releases) ページから最新の `genji.db.gz` をダウンロードし、解凍して使用してください。

```bash
gunzip genji.db.gz
```

#### クエリ例
```sql
-- 見出し語で検索
SELECT raw_json FROM entries WHERE entry = '幻辞';

-- 読みで検索
SELECT e.entry, d.gloss FROM entries e
JOIN definitions d ON d.entry_uuid = e.uuid
WHERE e.reading_primary = 'ゆき';

-- 全文検索（FTS5）
SELECT e.entry, e.reading_primary FROM fts_entries fts
JOIN entries e ON e.uuid = fts.uuid
WHERE fts_entries MATCH '雪';

-- 頻度順に上位 10 件を取得する
SELECT entry, reading_primary, json_extract(meta, '$.freq_rank') AS freq
FROM entries WHERE freq IS NOT NULL
ORDER BY freq ASC LIMIT 10;
```

#### メタデータの確認

データベースにはビルド情報を格納する `_metadata` テーブルが含まれています。

```sql
SELECT * FROM _metadata;
-- version, commit, branch, repository, build_date, entry_count
```

### Docker

Docker イメージは GHCR で配布しています（`linux/amd64` / `linux/arm64` 対応）。

```bash
docker pull ghcr.io/iktahana/genji:latest
docker run -p 8001:8001 ghcr.io/iktahana/genji:latest
```

ローカルでビルドする場合、`genji.db` が無くてもコンテナ内で自動生成されます。

```bash
docker compose up -d --build
```

`http://localhost:8001` で Datasette API にアクセスできます。
