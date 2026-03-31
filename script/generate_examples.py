
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
MAX_WORKERS  = 3      # 3 Flash モデル × 1 並発 = 3 workers
REPORT_SEC   = 3.0    # 進捗表示の更新間隔（秒）
CHECKPOINT_SAVE_INTERVAL = 20  # N ファイル処理ごとに checkpoint を自動保存
BATCH_SIZE   = 50     # 一回のAPI呼び出しで処理する語彙数（12→50: 4倍効率化）
AUTO_COMMIT_INTERVAL = 1800  # 自動コミットの間隔（秒 = 30分）
GLOBAL_RPM_INTERVAL  = 1.5   # グローバルリクエスト間隔（秒）≈ 40 RPM

# AIモデル情報（Flash のみ — Pro は 2026/3/25 以降無料利用不可）
MODELS = [
    "gemini-2.5-flash",
    "gemini-2.5-flash-lite",
    "gemini-3-flash-preview",
]

def parse_retry_delay(err: str) -> int:
    """
    429 エラーメッセージからリトライ待機時間（秒）を解析。
    見つからなければデフォルト 60 秒を返す。

    対応パターン例:
      retryDelay: "1m30s"  →  90
      retryDelay: "30s"    →  30
      retry after 60 seconds
      Retry-After: 45
    """
    # "1m30s" / "2m" / "45s" 形式
    m = re.search(r'retryDelay[^0-9]*(\d+)m(\d+)s', err)
    if m:
        return int(m.group(1)) * 60 + int(m.group(2))
    m = re.search(r'retryDelay[^0-9]*(\d+)m', err)
    if m:
        return int(m.group(1)) * 60
    m = re.search(r'retryDelay[^0-9]*(\d+)s', err)
    if m:
        return int(m.group(1))
    # "retry after N seconds" / "Retry-After: N"
    m = re.search(r'retry.{0,15}after[:\s]+(\d+)', err, re.IGNORECASE)
    if m:
        return int(m.group(1))
    m = re.search(r'Retry-After[:\s]+(\d+)', err, re.IGNORECASE)
    if m:
        return int(m.group(1))
    return 60  # fallback


class ModelManager:
    """
    複数 Gemini モデルのレート制限を管理。
    - 各モデルのクールダウン時刻を記録し、利用可能なモデルを自動選択
    - BoundedSemaphore で同一モデルへの同時リクエスト数を 1 に制限
    - グローバル token bucket で全モデル合計の RPM を制御（~40 RPM）
    """
    def __init__(self, models):
        self.models = models
        self.lock = threading.Lock()
        self.cooldowns: dict[str, float] = {m: 0.0 for m in models}
        self.semaphores: dict[str, threading.BoundedSemaphore] = {
            m: threading.BoundedSemaphore(1) for m in models
        }
        self.preferred_index = 0
        # グローバル限速: 全モデル合計で GLOBAL_RPM_INTERVAL 秒に 1 リクエスト
        self._last_global_request = 0.0

    def acquire_model(self) -> str:
        """
        利用可能なモデルのセマフォを獲得して返す。
        使い終わったら release_model() を呼ぶこと。
        """
        while True:
            if _shutdown_requested.is_set():
                return self.models[0]

            with self.lock:
                now = time.time()
                n = len(self.models)
                candidates = []
                for offset in range(n):
                    idx = (self.preferred_index + offset) % n
                    model = self.models[idx]
                    if self.cooldowns[model] <= now:
                        candidates.append((idx, model))

            # クールダウンしていないモデルのセマフォを非ブロッキングで試す
            for idx, model in candidates:
                if self.semaphores[model].acquire(blocking=False):
                    # グローバル限速: 前回のリクエストから最小間隔を空ける
                    with self.lock:
                        now = time.time()
                        wait = max(0.0, self._last_global_request + GLOBAL_RPM_INTERVAL - now)
                        self._last_global_request = now + wait  # スロットを予約
                        self.preferred_index = idx
                    if wait > 0:
                        time.sleep(wait)
                    return model

            # 全モデルがビジーまたはクールダウン中
            with self.lock:
                now = time.time()
                all_cooling = all(self.cooldowns[m] > now for m in self.models)
                if all_cooling:
                    soonest_model = min(self.models, key=lambda m: self.cooldowns[m])
                    wait_sec = max(0.0, self.cooldowns[soonest_model] - now)
                else:
                    wait_sec = 0.5

            if all_cooling:
                ts = datetime.now().strftime('%H:%M:%S')
                resume = datetime.fromtimestamp(self.cooldowns[soonest_model]).strftime('%H:%M:%S')
                print(
                    f"\n[Rate Limit] 全モデルがクールダウン中。"
                    f"{wait_sec:.0f}秒待機 ({ts} → {resume}) ...",
                    flush=True
                )
                deadline = time.time() + wait_sec
                while time.time() < deadline:
                    if _shutdown_requested.is_set():
                        return self.models[0]
                    time.sleep(min(5, max(0.1, deadline - time.time())))
            else:
                time.sleep(wait_sec)

    def release_model(self, model: str) -> None:
        """モデルのセマフォを解放"""
        try:
            self.semaphores[model].release()
        except ValueError:
            pass

    def mark_rate_limited(self, model: str, retry_after: int) -> None:
        """モデルをクールダウン状態にする"""
        with self.lock:
            self.cooldowns[model] = time.time() + retry_after
        ts = datetime.fromtimestamp(self.cooldowns[model]).strftime('%H:%M:%S')
        print(f"\n[Rate Limit] {model} → {retry_after}s 待機 (解除: {ts})", flush=True)

    def mark_success(self, model: str) -> None:
        """成功したモデルを preferred に設定"""
        with self.lock:
            try:
                self.preferred_index = self.models.index(model)
            except ValueError:
                pass

    def mark_unavailable(self, model: str) -> None:
        """利用不可モデルを長期クールダウン（1時間）に設定"""
        with self.lock:
            self.cooldowns[model] = time.time() + 3600

    def status(self) -> list[tuple[str, str]]:
        """各モデルの状態を返す（表示用）"""
        now = time.time()
        result = []
        with self.lock:
            for m in self.models:
                cd = self.cooldowns[m]
                if cd <= now:
                    result.append((m, "available"))
                else:
                    secs = int(cd - now)
                    result.append((m, f"cooldown {secs}s"))
        return result


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
    r"教科書の第三章は.*内容です",
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
total_batches   = 0
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
# スキャン & バッチ構築（扁平化パイプライン）
# ──────────────────────────────────────────────────────────

def scan_pending_items(files_to_process: list) -> list[tuple]:
    """
    全未処理ファイルをスキャンし、例文生成が必要な定義のリストを返す。
    更新不要のファイルはその場で processed マーク。
    returns: [(file_path, entry_idx, def_idx, entry_text, reading, gloss, pos), ...]
    """
    pending = []
    skipped = 0

    for fp in files_to_process:
        try:
            with open(fp, 'r', encoding='utf-8') as f:
                data = json.load(f)

            file_pending = []
            for ei, entry_obj in enumerate(data):
                entry_text = entry_obj.get('entry', '')
                reading    = entry_obj.get('reading', {}).get('primary', '')
                pos        = ",".join(entry_obj.get('grammar', {}).get('pos', []))

                for di, definition in enumerate(entry_obj.get('definitions', [])):
                    std_examples = definition.get('examples', {}).get('standard', [])
                    if is_low_quality(std_examples):
                        file_pending.append(
                            (fp, ei, di, entry_text, reading, definition.get('gloss', ''), pos)
                        )

            if file_pending:
                pending.extend(file_pending)
            else:
                # 更新不要 → 即座に processed マーク
                checkpoint_manager.add_processed(_to_checkpoint_key(fp))
                skipped += 1

        except Exception as e:
            print(f"\n[エラー] {fp}: {e}")

    if skipped > 0:
        Progress.step(f"スキップ（更新不要）: {skipped:,} files")

    return pending


def create_batches(pending_items: list) -> list[list[tuple]]:
    """
    pending_items を BATCH_SIZE で切分。
    同一ファイルの定義は同一バッチに収める（checkpoint の整合性のため）。
    """
    batches = []
    current_batch = []
    current_file = None

    for item in pending_items:
        fp = item[0]
        if len(current_batch) >= BATCH_SIZE and fp != current_file:
            batches.append(current_batch)
            current_batch = []
        current_batch.append(item)
        current_file = fp

    if current_batch:
        batches.append(current_batch)

    return batches

# ──────────────────────────────────────────────────────────
# バッチAPI呼び出し（-o json + 精簡プロンプト）
# ──────────────────────────────────────────────────────────

def generate_examples_batch(items: list) -> tuple[dict, str]:
    """
    複数語の例文を一括生成。
    items: [(entry, reading, gloss, pos), ...]
    returns: ({"1": [{"text":"..."}], ...}, model_name)
    """
    max_retries = len(MODELS) * 3

    word_lines = "\n".join([
        f"{i+1}. 表記:{entry} 読み:{reading} 品詞:{pos} 意味:{gloss}"
        for i, (entry, reading, gloss, pos) in enumerate(items)
    ])

    prompt = (
        '各語に自然な例文を3個、JSON出力。'
        'キー="1","2",...、値=[{"text":"例文"}]。'
        'テンプレ禁止。具体的場面。感動詞は会話文。\n\n'
        f'{word_lines}\n\n'
        '{"1":[{"text":"..."}],"2":[...]}'
    )

    for _ in range(max_retries):
        if _shutdown_requested.is_set():
            return {}, ""

        current_model = model_manager.acquire_model()
        acquired = not _shutdown_requested.is_set()
        if not acquired:
            return {}, ""
        try:
            res = subprocess.run(
                ['gemini', '-m', current_model, '-p', prompt, '-o', 'json'],
                capture_output=True, text=True, encoding='utf-8', timeout=180
            )

            raw_out = res.stdout.strip()
            err = res.stderr.strip()

            # レート制限・モデル不可エラーの処理（stderr で判定）
            is_rate_limit = any(x in err for x in ("429", "Quota exceeded", "Rate limit"))
            is_unavailable = any(x in err for x in ("ModelNotFoundError", "not found", "INVALID_ARGUMENT"))

            if is_rate_limit:
                delay = parse_retry_delay(err)
                model_manager.mark_rate_limited(current_model, delay)
                continue
            if is_unavailable:
                model_manager.mark_unavailable(current_model)
                continue

            # -o json の envelope をパース
            response_text = ""
            try:
                envelope = json.loads(raw_out)
                if "error" in envelope:
                    err_msg = envelope["error"].get("message", "")
                    if any(x in err_msg for x in ("429", "Quota", "Rate")):
                        delay = parse_retry_delay(err_msg)
                        model_manager.mark_rate_limited(current_model, delay)
                        continue
                response_text = envelope.get("response", "")
            except json.JSONDecodeError:
                # fallback: ANSI 除去して生テキストとして扱う
                response_text = clean_ansi(raw_out)

            # レスポンス内の JSON を抽出
            match = re.search(r'\{.*\}', response_text, re.DOTALL)
            if match:
                for attempt in [match.group(0), re.sub(r',\s*([}\]])', r'\1', match.group(0))]:
                    try:
                        result = json.loads(attempt)
                        model_manager.mark_success(current_model)
                        return result, current_model
                    except json.JSONDecodeError:
                        continue

            time.sleep(1)

        except subprocess.TimeoutExpired:
            model_manager.mark_rate_limited(current_model, 30)
        except Exception:
            pass
        finally:
            if acquired:
                model_manager.release_model(current_model)

    return {}, ""

# ──────────────────────────────────────────────────────────
# バッチ処理（1 バッチ = 1 API コール）
# ──────────────────────────────────────────────────────────

def process_batch(batch: list) -> int:
    """
    1 バッチの pending items を処理。
    API 呼び出し → 結果をファイルに書き戻し → checkpoint 更新。
    """
    global updated_count, processed_count

    if _shutdown_requested.is_set():
        return 0

    # API 呼び出し
    batch_input = [(item[3], item[4], item[5], item[6]) for item in batch]
    batch_results, model_name = generate_examples_batch(batch_input)

    # 結果をファイル別に整理
    file_updates: dict[Path, list] = {}
    for j, item in enumerate(batch):
        key = str(j + 1)
        if key in batch_results:
            fp, ei, di = item[0], item[1], item[2]
            file_updates.setdefault(fp, []).append((ei, di, batch_results[key]))

    # ファイルに書き戻し
    local_updated = 0
    all_files_in_batch = set(item[0] for item in batch)

    for fp, updates in file_updates.items():
        try:
            with open(fp, 'r', encoding='utf-8') as f:
                data = json.load(f)

            modified = False
            for ei, di, new_exs in updates:
                valid = [
                    ex for ex in new_exs
                    if isinstance(ex, dict) and not any(p.search(ex.get('text', '')) for p in JUNK_PATTERNS)
                ]
                if valid:
                    for ex in valid:
                        if 'citation' not in ex:
                            ex['citation'] = {
                                "source": "幻辭AI",
                                "author": "Gemini",
                                "note": model_name
                            }
                    if 'examples' not in data[ei]['definitions'][di]:
                        data[ei]['definitions'][di]['examples'] = {'standard': [], 'literary': []}
                    data[ei]['definitions'][di]['examples']['standard'] = valid
                    modified = True

            if modified:
                if data and 'meta' in data[0]:
                    data[0]['meta']['updated_at'] = datetime.now(timezone.utc).isoformat() + 'Z'
                with open(fp, 'w', encoding='utf-8') as f:
                    json.dump(data, f, ensure_ascii=False, indent=2)
                checkpoint_manager.add_updated(_to_checkpoint_key(fp))
                local_updated += 1

        except Exception as e:
            print(f"\n[エラー] {fp}: {e}")

    # batch 内の全ファイルを processed マーク
    for fp in all_files_in_batch:
        checkpoint_manager.add_processed(_to_checkpoint_key(fp))

    with progress_lock:
        updated_count += local_updated
        processed_count += 1  # バッチ数でカウント

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
            with checkpoint_manager.lock:
                cnt = len(checkpoint_manager.updated_files)

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
# 起動時チェック
# ──────────────────────────────────────────────────────────

def _probe_model(model: str) -> tuple[str, str]:
    """単一モデルに最小リクエストを送って状態を確認。(model, status_str) を返す。"""
    try:
        res = subprocess.run(
            ['gemini', '-m', model, '-p', '1'],
            capture_output=True, text=True, encoding='utf-8', timeout=20
        )
        err = clean_ansi(res.stderr).strip()
        out = clean_ansi(res.stdout).strip()

        if any(x in err for x in ("429", "Quota exceeded", "Rate limit")):
            delay = parse_retry_delay(err)
            model_manager.mark_rate_limited(model, delay)
            return model, f"⚠  Rate limited (reset in {delay}s)"
        if any(x in err for x in ("ModelNotFoundError", "not found", "INVALID_ARGUMENT")):
            model_manager.mark_unavailable(model)
            return model, "✗  Unavailable"
        if res.returncode == 0 and out:
            return model, "✓  OK"
        return model, f"?  Unknown (rc={res.returncode})"
    except subprocess.TimeoutExpired:
        return model, "✗  Timeout"
    except Exception as e:
        return model, f"✗  Error: {e}"


def startup_check() -> None:
    """起動時にモデル疎通確認と進捗 stats を表示。"""
    sep = "─" * 60
    print(f"\n{sep}", flush=True)
    print("  illusionsDict 例文生成スクリプト", flush=True)
    print(sep, flush=True)

    # ── Gemini 接続確認（並列）──
    print("\n[Gemini] モデル疎通確認中...", flush=True)
    results: dict[str, str] = {}
    with ThreadPoolExecutor(max_workers=len(MODELS)) as ex:
        futures = {ex.submit(_probe_model, m): m for m in MODELS}
        for fut in as_completed(futures):
            model, status = fut.result()
            results[model] = status

    for m in MODELS:
        print(f"  {m:<30} {results[m]}", flush=True)

    available = [m for m in MODELS if results[m].startswith("✓")]
    if not available:
        print("\n  [WARNING] 利用可能なモデルが見つかりません。クールダウン後に自動リトライします。", flush=True)

    # ── ファイル統計 ──
    print("\n[Stats]", flush=True)
    try:
        all_dirs  = sorted([d for d in DATA_ROOT.iterdir() if d.is_dir()], key=lambda x: x.name)
        total     = sum(len(list(d.glob("*.json"))) for d in all_dirs)
        processed = len(checkpoint_manager.processed_files)
        updated   = len(checkpoint_manager.updated_files)
        remaining = total - processed
        pct       = processed / total * 100 if total else 0
        print(f"  総ファイル数  : {total:>10,}", flush=True)
        print(f"  処理済み     : {processed:>10,}  ({pct:.1f}%)", flush=True)
        print(f"  更新済み     : {updated:>10,}", flush=True)
        print(f"  残り         : {remaining:>10,}", flush=True)
    except Exception as e:
        print(f"  (stats 取得失敗: {e})", flush=True)

    print(f"\n{sep}\n", flush=True)


# ──────────────────────────────────────────────────────────
# 実行
# ──────────────────────────────────────────────────────────

_shutdown_requested = threading.Event()

def main():
    def signal_handler(sig, frame):
        if _shutdown_requested.is_set():
            print("\n[INFO] 強制終了します。", flush=True)
            sys.exit(1)
        _shutdown_requested.set()
        print("\n\n[INFO] シャットダウン要求を受信。実行中のワーカーの完了を待っています...", flush=True)
        print("[INFO] もう一度 Ctrl+C で強制終了。", flush=True)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    atexit.register(checkpoint_manager.save_checkpoint)

    # 起動時チェック
    startup_check()

    # 自動コミットスレッド
    commit_thread = threading.Thread(target=auto_commit_worker, daemon=True, name="auto-commit")
    commit_thread.start()

    Progress.group(
        f"例文の自動生成・品質改善プロセスを開始します "
        f"(並列={MAX_WORKERS}, バッチ={BATCH_SIZE}語/APIコール, 限速=~{int(60/GLOBAL_RPM_INTERVAL)}RPM)"
    )
    Progress.step(f"使用モデル: {', '.join(MODELS)}")
    Progress.step(f"自動コミット: {AUTO_COMMIT_INTERVAL // 60}分ごと")

    # ── ファイル一覧 ──
    all_dirs  = sorted([d for d in DATA_ROOT.iterdir() if d.is_dir()], key=lambda x: x.name)
    all_files = []
    for d in all_dirs:
        all_files.extend(sorted(list(d.glob("*.json"))))

    total_files = len(all_files)

    all_file_keys = {_to_checkpoint_key(f) for f in all_files}
    valid_processed = checkpoint_manager.processed_files & all_file_keys
    already_processed = len(valid_processed)

    Progress.step(f"総ファイル数: {total_files:,}")
    if already_processed > 0:
        Progress.step(f"✓ 前回の進捗: {already_processed:,} ファイル既に処理済み")

    files_to_process = [f for f in all_files if not checkpoint_manager.is_processed(_to_checkpoint_key(f))]
    Progress.step(f"スキャン対象: {len(files_to_process):,} ファイル")

    # ── スキャンフェーズ ──
    Progress.step("ファイルスキャン中...")
    pending_items = scan_pending_items(files_to_process)

    if not pending_items:
        Progress.ok("全ファイル処理済みです。")
        Progress.endgroup()
        return

    unique_files = len(set(item[0] for item in pending_items))
    Progress.step(f"処理対象: {len(pending_items):,} definitions ({unique_files:,} files)")

    # ── バッチ構築 ──
    batches = create_batches(pending_items)
    global total_batches
    total_batches = len(batches)
    Progress.step(f"バッチ数: {total_batches:,} (各~{BATCH_SIZE}語/APIコール)")

    est_minutes = total_batches * GLOBAL_RPM_INTERVAL / 60
    Progress.step(f"推定所要時間: {est_minutes:.0f}分 (限速ベース)")

    # ── 実行フェーズ ──
    global last_report_t
    last_report_t = time.perf_counter()

    try:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {executor.submit(process_batch, batch): i for i, batch in enumerate(batches)}

            for future in as_completed(futures):
                if _shutdown_requested.is_set():
                    executor.shutdown(wait=False, cancel_futures=True)
                    break
                try:
                    future.result()
                except Exception as e:
                    print(f"\n[ERROR] バッチ処理中にエラー: {e}", flush=True)

                now = time.perf_counter()
                if now - last_report_t >= REPORT_SEC:
                    with progress_lock:
                        last_report_t = now
                        Progress.bar_line(
                            processed_count, total_batches,
                            f"batch {processed_count:,}/{total_batches:,} "
                            f"(更新: {updated_count:,} files)"
                        )

        checkpoint_manager.save_checkpoint()
        if _shutdown_requested.is_set():
            print(f"\n[INFO] 安全にシャットダウンしました。", flush=True)
            print(f"[INFO] Checkpoint 保存完了: {len(checkpoint_manager.processed_files)} files processed", flush=True)
            print(f"[INFO] 次回実行時に処理中断地点から再開できます。", flush=True)
        else:
            Progress.ok(f"プロセス完了。合計 {updated_count:,} 件のファイルを更新しました。")
            Progress.step(f"Checkpoint: {len(checkpoint_manager.processed_files)} files processed")
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
