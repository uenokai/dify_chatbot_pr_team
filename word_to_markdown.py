import glob
import os
from docx import Document

# --- 設定 (パスを更新) ---
INPUT_FOLDER_PATH = r'C:\Users\c3005\Documents\QA対応_原稿_dify\data_upload\01_row_file' 
OUTPUT_MD_PATH = r'C:\Users\c3005\Documents\QA対応_原稿_dify\data_upload\03_markdown_file\qa_from_word_20251021.md'
# ---------------

def convert_docx_to_md(input_folder, output_md_file):
    """
    指定フォルダ内の.docxファイルを読み込み、
    段落(Paragraphs)のテキストを抽出してまとめる。
    ★空白行を区切りとし、各QAセットの後にファイル名を挿入する。
    （表(Tables)の処理は行わない）
    """
    
    docx_files = glob.glob(os.path.join(input_folder, "*.docx"))
    
    if not docx_files:
        print(f"フォルダ '{input_folder}' に .docx ファイルが見つかりません。")
        return

    all_md_content = []
    print(f"合計 {len(docx_files)} 個のファイルを処理します...")
    
    for docx_file in docx_files:
        
        file_basename = os.path.basename(docx_file)
        
        if file_basename.startswith('~$'):
            print(f"スキップ (一時ファイル): {file_basename}")
            continue
            
        print(f"読み込み中: {file_basename}")
        
        # ▼▼▼ 修正点 ▼▼▼
        # ファイル名行の定義 (ファイル名の前に改行1つ、後に改行2つ)
        file_name_line = f"ファイル名：{file_basename}\n\n" 
        
        # QAブロックを一時的に溜めるバッファ
        current_qa_block = [] 
        # ▲▲▲ 修正ここまで ▲▲▲

        try:
            doc = Document(docx_file)
            
            # --- 1. 段落 (Paragraphs) の処理 ---
            for para in doc.paragraphs:
                text = para.text.strip()
                
                # ▼▼▼ 修正点：バッファリングロジック ▼▼▼
                if not text:
                    # 空白行を見つけた時の処理
                    if current_qa_block:
                        # バッファに何か入っていれば、QAセットとみなす
                        # 1. バッファの内容（QAテキスト）を書き出す
                        all_md_content.extend(current_qa_block) 
                        # 2. ファイル名を追加
                        all_md_content.append(file_name_line)
                        # 3. バッファをクリア
                        current_qa_block = [] 
                    # 連続する空白行は無視される
                else:
                    # テキスト行の処理
                    style_name = para.style.name
                    # スタイルに基づいてバッファに追加
                    if 'Heading 1' in style_name:
                        current_qa_block.append(f"# {text}\n")
                    elif 'Heading 2' in style_name:
                        current_qa_block.append(f"## {text}\n")
                    elif 'Heading 3' in style_name:
                        current_qa_block.append(f"### {text}\n")
                    elif 'Heading 4' in style_name:
                        current_qa_block.append(f"#### {text}\n")
                    elif 'List Bullet' in style_name:
                        current_qa_block.append(f"- {text}\n")
                    else:
                        current_qa_block.append(f"{text}\n")
                # ▲▲▲ 修正ここまで ▲▲▲
            
            # --- 2. 表 (Tables) の処理 (削除済み) ---

            # ▼▼▼ 修正点：ファイルの最後の処理 ▼▼▼
            # ループ終了後、バッファにまだテキストが残っているかチェック
            # (ファイルが空白行で終わっていない場合の最後のブロック)
            if current_qa_block:
                all_md_content.extend(current_qa_block)
                all_md_content.append(file_name_line)
            # ▲▲▲ 修正ここまで ▲▲▲

        except Exception as e:
            print(f"エラー: {docx_file} の処理中に問題が発生しました。 {e}")

    # --- 3. ファイルへの書き込み ---
    try:
        with open(output_md_file, 'w', encoding='utf-8') as f:
            f.writelines(all_md_content)
        print(f"\n完了: {output_md_file} に保存されました。")
    
    except IOError as e:
        print(f"エラー: ファイル '{output_md_file}' への書き込みに失敗しました。 {e}")

# --- 実行 ---
if __name__ == "__main__":
    if not os.path.exists(INPUT_FOLDER_PATH):
        print(f"エラー: 元ファイルのフォルダ '{INPUT_FOLDER_PATH}' が見つかりません。")
    else:
        convert_docx_to_md(INPUT_FOLDER_PATH, OUTPUT_MD_PATH)