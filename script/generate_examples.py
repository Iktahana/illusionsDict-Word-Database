
import os
import json
import re
import subprocess
from pathlib import Path
from datetime import datetime, timezone
import sys

# === 垃圾模板模式 (不合格判定基準) ===
JUNK_PATTERNS = [
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
    r"文語的表現として古典に登場します",
    r"その語源は興味深い歴史があります",
    r"現代でも使用頻度が高い重要語彙です"
]

def clean_ansi(text):
    ansi_escape = re.compile(r'\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])')
    return ansi_escape.sub('', text)

def is_unqualified(examples):
    """檢查例句是否不合格"""
    if not examples:
        return True
    for ex in examples:
        text = ex.get('text', '')
        for pattern in JUNK_PATTERNS:
            if re.search(pattern, text):
                return True
    return False

def generate_examples_jp(entry, reading, gloss, pos):
    """全日文提示詞調用 Gemini CLI"""
    prompt = f"""
以下の日本語単語について、辞書に掲載するのに適した具体的かつ高品質な例文を作成してください。

単語: {entry}
読み: {reading}
品詞: {pos}
意味: {gloss}

【作成ルール】
1. **「生活に欠かせない」「ビジネスで重要」「科学的研究」などのテンプレート的な表現は厳禁です。**
2. その単語が実際に使われる具体的なシーン（ニュース、専門現場、日常生活など）を想定してください。
3. 自然な日本語のコロケーション（語の繋がり）を重視してください。
4. 例文を読むだけで単語の意味が推測できるような、情報量の多い文にしてください。
5. **無理に20個作る必要はありません。5〜8個を目標とし、難解な語の場合は3個程度でも構いません。質を最優先してください。**
6. 感動詞や副詞は「」を用いた会話形式、固有名詞は背景知識に基づいた文にしてください。

【出力形式】
JSON配列形式のみを出力してください。解説やMarkdownの枠は不要です。
各オブジェクトは "text"（例文）と "source": "幻辞" を含めてください。

例:
[
  {{"text": "具体的な例文1", "source": "幻辞"}},
  {{"text": "具体的な例文2", "source": "幻辞"}}
]
"""
    try:
        result = subprocess.run(
            ['gemini', '-p', prompt],
            capture_output=True,
            text=True,
            encoding='utf-8',
            timeout=90
        )
        output = clean_ansi(result.stdout).strip()
        json_match = re.search(r'\[\s*\{.*\}\s*\]', output, re.DOTALL)
        if json_match:
            return json.loads(json_match.group(0))
        return None
    except Exception:
        return None

def process_file(file_path):
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        modified = False
        for entry_obj in data:
            entry_text = entry_obj.get('entry', '')
            reading = entry_obj.get('reading', {}).get('primary', '')
            pos = ",".join(entry_obj.get('grammar', {}).get('pos', []))
            
            for definition in entry_obj.get('definitions', []):
                if 'examples' not in definition:
                    definition['examples'] = {'standard': [], 'literary': []}
                
                std_examples = definition['examples'].get('standard', [])
                
                # 判定：是欠缺還是不合格
                if is_unqualified(std_examples):
                    new_exs = generate_examples_jp(entry_text, reading, definition.get('gloss', ''), pos)
                    if new_exs:
                        # 過濾新生成的內容
                        valid_new = [ex for ex in new_exs if not any(re.search(p, ex.get('text', '')) for p in JUNK_PATTERNS)]
                        if valid_new:
                            definition['examples']['standard'] = valid_new
                            modified = True
        
        if modified:
            if 'meta' in data[0]:
                data[0]['meta']['updated_at'] = datetime.now(timezone.utc).isoformat() + 'Z'
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            return True
    except Exception as e:
        print(f"\n[Error] {file_path.name}: {e}")
    return False

def main():
    print("=== 辞書例文の全件品質チェック & 最適化タスク啟動 ===")
    
    # 優先處理 か 行
    priority_order = ["か", "が", "き", "ぎ", "く", "ぐ", "け", "げ", "こ", "ご"]
    data_dir = Path("data")
    all_rows = sorted([d.name for d in data_dir.iterdir() if d.is_dir()])
    
    processing_queue = [r for r in priority_order if r in all_rows]
    processing_queue += [d for d in all_rows if d not in priority_order]
    
    for row in processing_queue:
        subdir = data_dir / row
        print(f"\nScanning: [{row}]")
        files = list(subdir.glob("*.json"))
        total = len(files)
        
        fixed = 0
        for i, file_path in enumerate(files):
            if (i + 1) % 5 == 1:
                sys.stdout.write(f"\r  Progress: {i+1}/{total} (Fixed/Updated: {fixed})")
                sys.stdout.flush()
            
            if process_file(file_path):
                fixed += 1
        print(f"\n  Row [{row}] completed.")

if __name__ == "__main__":
    main()
