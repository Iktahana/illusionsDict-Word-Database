# 幻辞 Datasette API サーバー
FROM datasetteproject/datasette:latest

# CORS プラグインをインストール
RUN datasette install datasette-cors

# メタデータ設定をコピー
COPY metadata.yml /app/metadata.yml

# コンパイル済み辞典 DB をイメージに内包
COPY illusions_dict.db /data/illusions_dict.db

EXPOSE 8001

# 読み取り専用モードで起動
CMD ["datasette", "serve", \
     "/data/illusions_dict.db", \
     "--metadata", "/app/metadata.yml", \
     "--host", "0.0.0.0", \
     "--port", "8001", \
     "--cors", \
     "--setting", "default_page_size", "20", \
     "--setting", "max_returned_rows", "1000", \
     "--setting", "sql_time_limit_ms", "5000"]
