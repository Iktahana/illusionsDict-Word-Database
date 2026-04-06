# 幻辞 Datasette API サーバー

# --- ビルドステージ: genji.db が無い場合のみ生成 ---
FROM python:slim AS builder

ARG GENJI_COMMIT=""
ARG GENJI_BRANCH=""
ARG GENJI_REPO=""

COPY script/json_to_sqlite.py /build/script/json_to_sqlite.py
COPY data/ /build/data/
COPY genji.db* /build/

RUN if [ ! -f /build/genji.db ]; then \
      echo "genji.db not found, building from JSON..." && \
      cd /build && \
      GENJI_COMMIT="${GENJI_COMMIT}" \
      GENJI_BRANCH="${GENJI_BRANCH}" \
      GENJI_REPO="${GENJI_REPO}" \
      python script/json_to_sqlite.py; \
    else \
      echo "genji.db already exists, skipping build."; \
    fi

# --- 本番ステージ ---
FROM datasetteproject/datasette:latest

# CORS プラグインをインストール
RUN datasette install datasette-cors

# メタデータ設定をコピー
COPY metadata.yml /app/metadata.yml

# ビルドステージから DB をコピー
COPY --from=builder /build/genji.db /data/genji.db

EXPOSE 8001

# 読み取り専用モードで起動
CMD ["datasette", "serve", \
     "/data/genji.db", \
     "--metadata", "/app/metadata.yml", \
     "--host", "0.0.0.0", \
     "--port", "8001", \
     "--cors", \
     "--setting", "default_page_size", "20", \
     "--setting", "max_returned_rows", "1000", \
     "--setting", "sql_time_limit_ms", "5000"]
