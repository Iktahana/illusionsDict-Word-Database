# illusionsDict Word Database

幻辞.com: https://dict.illusions.app

IllusionsDict プロジェクトで使用される全語彙データを収録した、オープンソースの日本語語彙データベースです。

本リポジトリでは、複数の信頼できるソースと独自のクローリングシステムを統合・加工した語彙データを、扱いやすい **SQLite 形式** で提供しています。

## 📦 特徴

- **SQLite 形式**: データベースファイルをダウンロードするだけで、すぐにアプリケーションに組み込み可能です。
- **自動更新**: Illusions の自動クローリングシステムにより、不定期にデータがビルドされ、常に最新の語彙が反映されます。
- **軽量かつ高速**: インデックスが最適化されており、数十万件のデータから瞬時に検索が可能です。

## 📂 データソース (Data Sources)

本データベースは、以下の優れたリソースを統合し、独自の加工を施したものです：

1.  **[JMdict (yomidevs/jmdict-yomitan)](https://github.com/yomidevs/jmdict-yomitan)** - 広範な辞書定義および語彙データ。
2.  **[Japanese Word Frequency (hingston/japanese)](https://github.com/hingston/japanese)** - 語彙の頻度・優先順位データ。
3.  **Illusions Crawler System** - 独自のクローリングシステムによる最新のトレンド語彙および語法データ。

## 🚀 使い方

[Releases](/releases) ページから最新の `illusions-wordlist.db` をダウンロードして使用してください。

### SQLite でのクエリ例
```sql
-- 特定の単語を検索する
SELECT * FROM words WHERE kanji = '幻辞';

-- 頻度順に上位 10 件を取得する
SELECT * FROM words ORDER BY frequency_rank ASC LIMIT 10;
