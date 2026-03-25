
import os
import json
import re
import subprocess
import time
import sys
import signal
import threading
import atexit
from pathlib import Path
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

# ──────────────────────────────────────────────────────────
# 設定
# ──────────────────────────────────────────────────────────

SCRIPT_DIR   = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
DATA_ROOT    = PROJECT_ROOT / "data"
CHECKPOINT_DIR = PROJECT_ROOT / ".checkpoints"
MAX_WORKERS  = 8      # 並列実行エージェント数
REPORT_SEC   = 3.0    # 進捗表示の更新間隔（秒）
CHECKPOINT_SAVE_INTERVAL = 50  # N ファイル処理ごとに checkpoint を自動保存
BATCH_SIZE   = 12     # 一回のAPI呼び出しで処理する語彙数（トークン削減の核心）
FILE_CHUNK_SIZE = 120  # 一つのワーカーが担当するファイル数
AUTO_COMMIT_INTERVAL = 1800  # 自動コミットの間隔（秒 = 30分）

# AIモデル情報
MODELS = [
    "gemini-2.5-flash",
    "gemini-2.5-flash-lite",
    "gemini-2.5-pro",
    "gemini-3-flash-preview",
    "gemini-3.1-pro-preview"
]

class ModelManager:
    def __init__(self, models):
        self.models = models
        self.current_index = 0
        self.lock = threading.Lock()
        self.failures_in_row = 0

    def get_current_model(self):
        with self.lock:
            return self.models[self.current_index]

    def switch_to_next_model(self):
        with self.lock:
            self.current_index = (self.current_index + 1) % len(self.models)
            self.failures_in_row += 1
            model = self.models[self.current_index]
            if self.failures_in_row >= len(self.models):
                print(f"\n[INFO] 全モデルのクォータ制限に達した可能性があります。60秒待機します...", flush=True)
                time.sleep(60)
                self.failures_in_row = 0
            return model

    def reset_failure_count(self):
        with self.lock:
            self.failures_in_row = 0

model_manager = ModelManager(MODELS)

# ──────────────────────────────────────────────────────────
# Checkpoint管理（中断・再開機能）
# ──────────────────────────────────────────────────────────

class CheckpointManager:
    def __init__(self, checkpoint_dir: Path = CHECKPOINT_DIR):
        self.checkpoint_dir = checkpoint_dir
        self.checkpoint_dir.mkdir(exist_ok=True)
        self.checkpoint_file = self.checkpoint_dir / "progress.json"
        self.processed_files: set[str] = set()
        self.updated_files: set[str] = set()
        self.lock = threading.Lock()
        self._dirty_count = 0
        self._load_checkpoint()

    def _load_checkpoint(self):
        if self.checkpoint_file.exists():
            try:
                with open(self.checkpoint_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    self.processed_files = set(data.get('processed', []))
                    self.updated_files = set(data.get('updated', []))
                    timestamp = data.get('timestamp', 'Unknown')
                    print(f"✓ Checkpoint loaded: {len(self.processed_files)} files processed at {timestamp}")
            except Exception as e:
                print(f"! Checkpoint load failed: {e}. Starting fresh.")
                self.processed_files = set()
                self.updated_files = set()

    def add_processed(self, file_path: str):
        with self.lock:
            self.processed_files.add(file_path)
            self._dirty_count += 1
            should_save = self._dirty_count >= CHECKPOINT_SAVE_INTERVAL
        if should_save:
            self.save_checkpoint()

    def add_updated(self, file_path: str):
        with self.lock:
            self.updated_files.add(file_path)

    def is_processed(self, file_path: str) -> bool:
        with self.lock:
            return file_path in self.processed_files

    def save_checkpoint(self):
        try:
            with self.lock:
                self._dirty_count = 0
                data = {
                    'timestamp': datetime.now(timezone.utc).isoformat(),
                    'processed': sorted(self.processed_files),
                    'updated': sorted(self.updated_files),
                    'processed_count': len(self.processed_files),
                    'updated_count': len(self.updated_files)
                }
            tmp_file = self.checkpoint_file.with_suffix('.tmp')
            with open(tmp_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            tmp_file.replace(self.checkpoint_file)
        except Exception as e:
            print(f"! Checkpoint save failed: {e}")

    def clear_checkpoint(self):
        with self.lock:
            self.processed_files.clear()
            self.updated_files.clear()
            self._dirty_count = 0
        if self.checkpoint_file.exists():
            self.checkpoint_file.unlink()
        print("✓ Checkpoint cleared")

checkpoint_manager = CheckpointManager()

# 低品質なテンプレートの定義
_JUNK_PATTERN_STRINGS = [
    r"私たちの生活に欠かせません",
    r"ビジネスシーンでは.*重要です",
    r"科学的研究が進みました",
    r"物語の中心となって",
    r"学校の教室で.*学びました",
    r"医師から.*アドバイスを受けました",
    r"法律では.*定義されています",
    r"スポーツの試合では.*勝敗を決めました",
    r"自然界では.*見られる現象です",
    r"歴史的に.*重要な位置づけです",
    r"料理において.*重要な食材です",
    r"旅行中に.*見学することができました",
    r"朝食の時に.*いただきました",
    r"営業会議で.*議論されました",
    r"実験の結果、.*性質が明らかになりました",
    r"著者は.*象徴的に表現しています",
    r"教科書の第三章は.*內容です",
    r"健康診断で.*相談しました",
    r"法的な観点から.*重要な問題です",
    r"アスリートは.*訓練しています",
    r"例句\d+",
    r"この言葉は日常会話で頻繁に使用されます",
    r"文脈によって意味が変わることがあります",
    r"ビジネス会話では特に重要な表現です",
    r"日本の伝統文化に関連する言葉です",
    r"学校教育で教えられる基本的な言葉です",
    r"医学分野でも使用される専門用語です",
    r"法律文書でこの表現がよく見られます",
    r"スポーツ界でも一般的な言い回しです",
    r"環境問題に関する文脈で使用されます",
    r"料理や食文化の説明に用いられます",
    r"旅行会話で役立つ重要な言葉です",
    r"日本の歴史的背景を反映しています",
    r"社会問題の議論で言及されることが多いです",
    r"技術用語としても広く認識されています",
    r"地域によって方言的な変形があります",
    r"若い世代も自然に使用する一般的な言葉です",
    r"文語的な表現として古典に登場します",
    r"その語源は興味深い歴史があります",
    r"現代でも使用頻度が高い重要語彙です"
]

JUNK_PATTERNS = [re.compile(p) for p in _JUNK_PATTERN_STRINGS]

# ──────────────────────────────────────────────────────────
# 進捗表示（GitHub Actions 対応）
# ──────────────────────────────────────────────────────────

class Progress:
    IS_GHA = os.environ.get("GITHUB_ACTIONS") == "true"
    BAR_W  = 28

    @staticmethod
    def _bar(done: int, total: int) -> str:
        if total <= 0: return f"[{'░' * Progress.BAR_W}]  0.0%"
        pct    = min(done / total, 1.0)
        filled = round(pct * Progress.BAR_W)
        return f"[{'█' * filled}{'░' * (Progress.BAR_W - filled)}] {pct:5.1%}"

    @staticmethod
    def group(title: str) -> None:
        if Progress.IS_GHA: print(f"::group::{title}", flush=True)
        else: print(f"\n┌─ {title}", flush=True)

    @staticmethod
    def endgroup() -> None:
        if Progress.IS_GHA: print("::endgroup::", flush=True)

    @staticmethod
    def step(msg: str) -> None:
        print(f"  │  {msg}", flush=True)

    @staticmethod
    def ok(msg: str) -> None:
        print(f"  └✓ {msg}", flush=True)

    @staticmethod
    def bar_line(done: int, total: int, suffix: str = "") -> None:
        bar = Progress._bar(done, total)
        print(f"  │  {bar}  {suffix}", flush=True)

# ──────────────────────────────────────────────────────────
# ユーティリティ
# ──────────────────────────────────────────────────────────

progress_lock   = threading.Lock()
updated_count   = 0
processed_count = 0
last_report_t   = 0

_ANSI_RE = re.compile(r'\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])')

def clean_ansi(text: str) -> str:
    return _ANSI_RE.sub('', text)

def is_low_quality(examples: list) -> bool:
    if not examples: return True
    for ex in examples:
        txt = ex.get('text', '')
        if any(p.search(txt) for p in JUNK_PATTERNS): return True
    return False

def _to_checkpoint_key(file_path: Path) -> str:
    try:
        return str(file_path.relative_to(PROJECT_ROOT))
    except ValueError:
        return str(file_path)

# ──────────────────────────────────────────────────────────
# バッチAPI呼び出し（最適化の核心）
# ──────────────────────────────────────────────────────────

def generate_examples_batch(items: list) -> dict:
    """
    複数語の例文を一括生成（トークン節約の核心）。
    items: [(entry, reading, gloss, pos), ...] のリスト
    returns: {"1": [{"text":"...", "citation":{...}}], "2": [...], ...}

    Before: N語 × 400token指令 = N×400 tokens
    After:  1回 × 150token指令 + N語のデータ ≈ 150 + N×30 tokens
    """
    max_retries = len(MODELS) * 2

    for _ in range(max_retries):
        current_model = model_manager.get_current_model()

        word_lines = "\n".join([
            f"{i+1}. 表記:{entry} 読み:{reading} 品詞:{pos} 意味:{gloss}"
            for i, (entry, reading, gloss, pos) in enumerate(items)
        ])

        # 精簡プロンプト（~150 tokens vs 旧 ~400 tokens）
        prompt = (
            '以下の語の自然な例文を各3〜5個、JSONオブジェクト形式で出力。\n'
            'キーは番号（"1","2",...）。値は[{"text":"例文"}]の配列。\n'
            'テンプレ表現禁止（「生活に欠かせない」「重要です」等）。'
            '具体的な場面を想定。感動詞は会話文形式。\n\n'
            f'{word_lines}\n\n'
            '出力形式: {"1": [{"text": "..."}], "2": [...]}'
        )

        try:
            res = subprocess.run(
                ['gemini', '-m', current_model, '-p', prompt],
                capture_output=True, text=True, encoding='utf-8', timeout=180
            )

            out = clean_ansi(res.stdout).strip()
            err = clean_ansi(res.stderr).strip()

            if any(x in err for x in ("429", "Quota exceeded", "Rate limit", "ModelNotFoundError")):
                model_manager.switch_to_next_model()
                continue

            match = re.search(r'\{.*\}', out, re.DOTALL)
            if match:
                for attempt in [match.group(0), re.sub(r',\s*([}\]])', r'\1', match.group(0))]:
                    try:
                        result = json.loads(attempt)
                        # citation はコードで自動付与（AIに生成させない）
                        for key in result:
                            if isinstance(result[key], list):
                                for ex in result[key]:
                                    if isinstance(ex, dict) and 'citation' not in ex:
                                        ex['citation'] = {
                                            "source": "幻辭AI",
                                            "author": "Gemini",
                                            "note": current_model
                                        }
                        model_manager.reset_failure_count()
                        return result
                    except json.JSONDecodeError:
                        continue

            time.sleep(1)

        except subprocess.TimeoutExpired:
            model_manager.switch_to_next_model()
        except Exception:
            pass

    return {}

# ──────────────────────────────────────────────────────────
# ファイルチャンク処理
# ──────────────────────────────────────────────────────────

def process_file_chunk(file_paths: list) -> int:
    """
    ファイルのチャンクをまとめて処理。
    1. 全ファイルをロードして更新が必要な項目を収集
    2. バッチAPIで一括生成（大幅なトークン節約）
    3. 結果をファイルに書き戻す
    """
    global updated_count, processed_count

    # Step 1: ファイルロード & 保留項目の収集
    file_data = {}
    pending_items = []  # (fp, entry_idx, def_idx, entry_text, reading, gloss, pos)

    for fp in file_paths:
        try:
            with open(fp, 'r', encoding='utf-8') as f:
                data = json.load(f)
            file_data[fp] = data

            for ei, entry_obj in enumerate(data):
                entry_text = entry_obj.get('entry', '')
                reading    = entry_obj.get('reading', {}).get('primary', '')
                pos        = ",".join(entry_obj.get('grammar', {}).get('pos', []))

                for di, definition in enumerate(entry_obj.get('definitions', [])):
                    if 'examples' not in definition:
                        definition['examples'] = {'standard': [], 'literary': []}
                    std_examples = definition['examples'].get('standard', [])
                    if is_low_quality(std_examples):
                        pending_items.append(
                            (fp, ei, di, entry_text, reading, definition.get('gloss', ''), pos)
                        )
        except Exception as e:
            print(f"\n[エラー] {getattr(fp, 'name', str(fp))}: {e}")

    # Step 2: バッチAPIで一括生成
    api_results = {}  # (fp, ei, di) -> [examples]

    for i in range(0, len(pending_items), BATCH_SIZE):
        if _shutdown_requested.is_set():
            break

        batch = pending_items[i:i + BATCH_SIZE]
        # items形式: (entry, reading, gloss, pos)
        batch_input = [(item[3], item[4], item[5], item[6]) for item in batch]

        batch_results = generate_examples_batch(batch_input)

        for j, item in enumerate(batch):
            fp, ei, di = item[0], item[1], item[2]
            key = str(j + 1)
            if key in batch_results:
                api_results[(fp, ei, di)] = batch_results[key]

    # Step 3: 結果をファイルに適用して書き戻す
    modified_files = set()
    for (fp, ei, di), new_exs in api_results.items():
        valid_new = [
            ex for ex in new_exs
            if isinstance(ex, dict) and not any(p.search(ex.get('text', '')) for p in JUNK_PATTERNS)
        ]
        if valid_new:
            file_data[fp][ei]['definitions'][di]['examples']['standard'] = valid_new
            modified_files.add(fp)

    local_updated = 0
    for fp in file_paths:
        if fp not in file_data:
            continue  # ロード失敗したファイルはスキップ

        if fp in modified_files:
            if file_data[fp] and 'meta' in file_data[fp][0]:
                file_data[fp][0]['meta']['updated_at'] = (
                    datetime.now(timezone.utc).isoformat() + 'Z'
                )
            try:
                with open(fp, 'w', encoding='utf-8') as f:
                    json.dump(file_data[fp], f, ensure_ascii=False, indent=2)
                checkpoint_manager.add_updated(_to_checkpoint_key(fp))
                local_updated += 1
            except Exception as e:
                print(f"\n[エラー] 書き込み失敗 {fp.name}: {e}")

        checkpoint_manager.add_processed(_to_checkpoint_key(fp))

    with progress_lock:
        updated_count += local_updated
        processed_count += len(file_paths)

    return local_updated

# ──────────────────────────────────────────────────────────
# 自動コミット（30分ごと）
# ──────────────────────────────────────────────────────────

def auto_commit_worker():
    """30分ごとに進捗を自動コミット。失敗時はエラーを捕捉してスキップ（プロセスを止めない）。"""
    while not _shutdown_requested.wait(timeout=AUTO_COMMIT_INTERVAL):
        try:
            # dataディレクトリのみステージング（checkpoint等は除外）
            subprocess.run(
                ['git', 'add', 'data/'],
                capture_output=True, cwd=str(PROJECT_ROOT), timeout=60
            )

            # 変更があるか確認
            status = subprocess.run(
                ['git', 'status', '--porcelain', 'data/'],
                capture_output=True, text=True, cwd=str(PROJECT_ROOT), timeout=30
            )

            if not status.stdout.strip():
                continue  # 変更なし、スキップ

            ts = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')
            with progress_lock:
                cnt = updated_count

            commit = subprocess.run(
                ['git', 'commit', '-m', f'Auto-commit: {cnt} files updated ({ts})'],
                capture_output=True, text=True, cwd=str(PROJECT_ROOT), timeout=60
            )

            if commit.returncode == 0:
                print(f"\n[Auto-commit] ✓ {cnt}件を commit しました ({ts})", flush=True)
            else:
                err_msg = commit.stderr.strip()[:100]
                print(f"\n[Auto-commit] スキップ: {err_msg}", flush=True)

        except Exception as e:
            print(f"\n[Auto-commit] エラー (スキップ): {e}", flush=True)

# ──────────────────────────────────────────────────────────
# 実行
# ──────────────────────────────────────────────────────────

_shutdown_requested = threading.Event()

def main():
    def signal_handler(sig, frame):
        if _shutdown_requested.is_set():
            return
        _shutdown_requested.set()
        print("\n\n[INFO] 処理を中断しています...", flush=True)
        checkpoint_manager.save_checkpoint()
        print(f"[INFO] Checkpoint 保存完了: {len(checkpoint_manager.processed_files)} files processed", flush=True)
        print(f"[INFO] 次回実行時に処理中断地点から再開できます。", flush=True)
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    atexit.register(checkpoint_manager.save_checkpoint)

    # 自動コミットスレッドを起動
    commit_thread = threading.Thread(target=auto_commit_worker, daemon=True, name="auto-commit")
    commit_thread.start()

    Progress.group(
        f"例文の自動生成・品質改善プロセスを開始します "
        f"(並列エージェント数={MAX_WORKERS}, バッチサイズ={BATCH_SIZE})"
    )
    Progress.step(f"使用モデル候補: {', '.join(MODELS)}")
    Progress.step(f"自動コミット: {AUTO_COMMIT_INTERVAL // 60}分ごと")

    all_dirs  = sorted([d for d in DATA_ROOT.iterdir() if d.is_dir()], key=lambda x: x.name)
    all_files = []
    for d in all_dirs:
        all_files.extend(sorted(list(d.glob("*.json"))))

    total_files = len(all_files)

    all_file_keys = {_to_checkpoint_key(f) for f in all_files}
    valid_processed = checkpoint_manager.processed_files & all_file_keys
    already_processed = len(valid_processed)
    remaining_files = total_files - already_processed

    Progress.step(f"スキャン対象: {total_files:,} ファイル")
    if already_processed > 0:
        Progress.step(f"✓ 前回の進捗: {already_processed:,} ファイル既に処理済み")
        Progress.step(f"→ 残り: {remaining_files:,} ファイル")

    files_to_process = [f for f in all_files if not checkpoint_manager.is_processed(_to_checkpoint_key(f))]

    # ファイルをチャンクに分割
    file_chunks = [
        files_to_process[i:i + FILE_CHUNK_SIZE]
        for i in range(0, len(files_to_process), FILE_CHUNK_SIZE)
    ]
    Progress.step(f"チャンク数: {len(file_chunks):,} (各{FILE_CHUNK_SIZE}ファイル × {BATCH_SIZE}語/APIコール)")

    global last_report_t
    last_report_t = time.perf_counter()

    try:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {executor.submit(process_file_chunk, chunk): chunk for chunk in file_chunks}

            for future in as_completed(futures):
                if _shutdown_requested.is_set():
                    executor.shutdown(wait=False, cancel_futures=True)
                    break
                try:
                    future.result()
                except Exception as e:
                    print(f"\n[ERROR] チャンク処理中にエラー: {e}", flush=True)

                now = time.perf_counter()
                if now - last_report_t >= REPORT_SEC:
                    with progress_lock:
                        last_report_t = now
                        Progress.bar_line(
                            processed_count, total_files,
                            f"{processed_count:,} / {total_files:,} files (更新済み: {updated_count:,})"
                        )

        checkpoint_manager.save_checkpoint()
        Progress.ok(f"プロセス完了。合計 {updated_count:,} 件のファイルを更新・最適化しました。")
        Progress.step(f"Checkpoint 保存: {len(checkpoint_manager.processed_files)} files")
        Progress.endgroup()

    except Exception as e:
        checkpoint_manager.save_checkpoint()
        print(f"\n[ERROR] 予期しないエラーが発生しました: {e}", flush=True)
        print(f"[INFO] Checkpoint 保存完了。次回実行時に再開できます。", flush=True)
        raise

if __name__ == "__main__":
    if len(sys.argv) > 1:
        if sys.argv[1] == "--clear-checkpoint":
            checkpoint_manager.clear_checkpoint()
            sys.exit(0)
        elif sys.argv[1] == "--status":
            print(f"Processed files: {len(checkpoint_manager.processed_files)}")
            print(f"Updated files: {len(checkpoint_manager.updated_files)}")
            print(f"Checkpoint file: {checkpoint_manager.checkpoint_file}")
            sys.exit(0)
        elif sys.argv[1] in ("-h", "--help"):
            print("Usage: python generate_examples.py [OPTIONS]")
            print()
            print("Options:")
            print("  --status            Show checkpoint progress")
            print("  --clear-checkpoint  Reset checkpoint and start fresh")
            print("  -h, --help          Show this help message")
            sys.exit(0)

    main()
