import json
import os
from datetime import datetime

def update_json(file_path, new_examples_data):
    """
    new_examples_data: list of lists of lists.
    Outer list: per entry in the file.
    Middle list: per definition in the entry.
    Inner list: list of strings (the example sentences).
    """
    with open(file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    for entry_idx, entry_data in enumerate(data):
        if entry_idx >= len(new_examples_data):
            break
        
        entry_examples = new_examples_data[entry_idx]
        for def_idx, definition in enumerate(entry_data.get('definitions', [])):
            if def_idx < len(entry_examples):
                definition['examples']['standard'] = [
                    {"text": text, "source": "幻辞"} for text in entry_examples[def_idx]
                ]
        
        entry_data['meta']['updated_at'] = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
    
    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

if __name__ == "__main__":
    import sys
    file_path = sys.argv[1]
    examples_json = sys.stdin.read()
    new_examples_data = json.loads(examples_json)
    update_json(file_path, new_examples_data)
