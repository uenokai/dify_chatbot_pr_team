import os
import pandas as pd
import requests
import json
import re
import argparse
from dotenv import load_dotenv

# --- 📚 .envファイルから環境変数を読み込む ---
dotenv_loaded = load_dotenv()

# --- ⚙️ 設定 ---
AZURE_API_KEY = os.getenv("AZURE_OPENAI_API_KEY")
AZURE_OPENAI_ENDPOINT = os.getenv("AZURE_OPENAI_ENDPOINT")
AZURE_DEPLOYMENT = os.getenv("AZURE_OPENAI_DEPLOYMENT_NAME")
API_VERSION = os.getenv("AZURE_OPENAI_API_VERSION")

# LLMの挙動を制御するパラメータ
LLM_MAX_TOKENS = 2000
LLM_TEMPERATURE = 0

# --- 📂 パスとファイル名の設定 (ここを編集してください) ---
DEFAULT_INPUT_FOLDER = r"C:\Users\c3005\Documents\QA対応_原稿_dify\data_upload\01_row_file"
DEFAULT_OUTPUT_FOLDER = r"C:\Users\c3005\Documents\QA対応_原稿_dify\data_upload\03_markdown_file"
DEFAULT_OUTPUT_FILENAME = "test.md"

# --- 🤖 LLM呼び出し関数 ---
def call_llm(prompt: str, expect_json: bool = True) -> str or None:
    """Azure OpenAI APIを直接呼び出す汎用関数"""
    if not dotenv_loaded:
        raise ValueError("`.env`ファイルが見つからないか、読み込めませんでした。")
    if not AZURE_API_KEY:
        raise ValueError("`.env`ファイルから `AZURE_API_KEY` を読み込めませんでした。")

    url = f"{AZURE_OPENAI_ENDPOINT.rstrip('/')}/openai/deployments/{AZURE_DEPLOYMENT}/chat/completions?api-version={API_VERSION}"
    headers = {"api-key": AZURE_API_KEY, "Content-Type": "application/json"}
    
    json_data = {
        "messages": [{"role": "user", "content": prompt}],
        "max_tokens": LLM_MAX_TOKENS,
        "temperature": LLM_TEMPERATURE,
    }
    if expect_json:
        json_data["response_format"] = {"type": "json_object"}
        
    try:
        response = requests.post(url, headers=headers, json=json_data)
        response.raise_for_status()
        return response.json()["choices"][0]["message"]["content"]
    except requests.exceptions.RequestException as e:
        print(f"🚨 エラー: APIリクエストに失敗しました。{e}")
        return None
    except json.JSONDecodeError:
        print(f"🚨 エラー: APIからの応答がJSONではありませんでした。")
        return None

# --- 🛠️ ヘルパー関数 ---
def contains_japanese(text: str) -> bool:
    """文字列に日本語が含まれているかを判定する"""
    if not isinstance(text, str): return False
    return bool(re.search(r'[\u3040-\u3DFF\u4E00-\u9FFF]', text))

def clean_cell_for_markdown(text: str) -> str:
    """Difyのテーブル形式用に、セルのテキストをクリーニングし、改行を<br>タグに置換する"""
    cleaned_text = str(text).replace('\r', '').replace('|', '｜').strip()
    # \nを半角スペースではなく<br>に置換
    cleaned_text_with_br = cleaned_text.replace('\n', '<br>')
    # 連続する<br>を1つにまとめる (例: <br><br> -> <br>)
    cleaned_text_with_br = re.sub(r'(<br>\s*)+', '<br>', cleaned_text_with_br)
    return cleaned_text_with_br

# --- 🧠 LLM処理 ---
def extract_qa_columns(df: pd.DataFrame, file_info: str) -> dict:
    """LLMを使いQ&Aカラム名を抽出する"""
    print("---- Q&Aカラムの抽出開始 ----")
    df_sample_md = df.head(5).to_markdown(index=False)
    column_list = df.columns.tolist()

    prompt = f"""
    ファイル「{file_info}」のデータサンプルを分析し、「質問」と「回答」に該当するカラムを特定してください。
    # 利用可能なカラム名のリスト
    {column_list}
    # データサンプル
    {df_sample_md}
    # あなたのタスク
    上記の「利用可能なカラム名のリスト」の中から、「質問」に最もふさわしいカラム名を1つ選び、`question_column`の値としてください。
    同様に、「回答」に最もふさわしいカラム名を1つ選び、`answer_column`の値としてください。
    # 絶対的なルール
    1. 判断はカラム名だけでなく、データサンプルの内容を最優先してください。
    2. JSONで返す値は、「利用可能なカラム名のリスト」に存在する文字列と**完全に一致**している必要があります。絶対にリストにない文字列を生成しないでください。
    3. たとえカラム名が「質問内容」のように見えても、リストにあるのが「質問」であれば、必ず「質問」を返してください。
    応答は必ず、選択したカラム名を含むJSONオブジェクトのみとしてください。
    例: {{"question_column": "質問", "answer_column": "回答"}}
    """
    response_str = call_llm(prompt)
    try:
        return json.loads(response_str) if response_str else None
    except json.JSONDecodeError:
        print(f"🚨 エラー: カラム抽出AIの応答が不正なJSON形式でした。")
        return None

def translate_text(text: str) -> str:
    """テキストが日本語でなければ翻訳する。日本語ならそのまま返す"""
    if not text or contains_japanese(text): 
        return text
    
    # <br>タグを翻訳前に一時的に改行に戻す
    text_to_translate = text.replace('<br>', '\n')
    print(f"    - 翻訳中: \"{text_to_translate[:30]}...\"")
    prompt = f"以下の英語のテキストを自然な日本語に翻訳してください:\n\n{text_to_translate}"
    translated = call_llm(prompt, expect_json=False)
    
    # 翻訳後、表示用に再度<br>タグに変換
    return clean_cell_for_markdown(translated) if translated else text
# --- ▲ 修正箇所 2 ▲ ---

# --- 🚀 メイン実行ロジック ---
def process_sheet(df: pd.DataFrame, file_name: str, sheet_name: str) -> pd.DataFrame or None:
    """単一のExcelシートに対する全処理を統括する"""
    col_names = extract_qa_columns(df, f"{file_name} / {sheet_name}")
    if not col_names: return None
    
    q_col, a_col = col_names.get("question_column"), col_names.get("answer_column")
    if not all([q_col, a_col, q_col in df.columns, a_col in df.columns]):
        print(f"🚨 エラー: AIが指定したカラム '{q_col}' or '{a_col}' がデータ内に存在しません。")
        return None

    df = df.fillna("")
    # clean_cell_for_markdownが<br>タグを返すように修正されたため、ここは変更不要
    q_texts = df[q_col].astype(str).apply(clean_cell_for_markdown)
    a_texts = df[a_col].astype(str).apply(clean_cell_for_markdown)

    print("→ 必要に応じて質問・回答テキストの翻訳を実行します...")
    q_texts_translated = q_texts.apply(translate_text)
    a_texts_translated = a_texts.apply(translate_text)

    result_df = pd.DataFrame({
        "file": file_name, 
        "sheet": sheet_name, 
        "question": q_texts_translated, 
        "answer": a_texts_translated
    })
    result_df = result_df[result_df['question'].str.strip().astype(bool) & result_df['answer'].str.strip().astype(bool)]
    
    if result_df.empty:
        print("→ 抽出できる有効なQ&Aペアがありませんでした。"); return None
    
    print(f"✅ Q&Aを{len(result_df)}件抽出しました。")
    return result_df

def main():
    """コマンドライン引数を受け取り、全体の処理を実行する"""
    parser = argparse.ArgumentParser(description="指定フォルダ内のExcelからQ&Aを抽出し、Markdownファイルに変換します。")
    parser.add_argument("-i", "--input", type=str, default=DEFAULT_INPUT_FOLDER, help=f"処理対象のExcelフォルダ。デフォルト: {DEFAULT_INPUT_FOLDER}")
    parser.add_argument("-o", "--output", type=str, default=DEFAULT_OUTPUT_FOLDER, help=f"出力先フォルダ。デフォルト: {DEFAULT_OUTPUT_FOLDER}")
    parser.add_argument("-f", "--filename", type=str, default=DEFAULT_OUTPUT_FILENAME, help=f"出力ファイル名。デフォルト: {DEFAULT_OUTPUT_FILENAME}")
    args = parser.parse_args()

    excel_folder = args.input
    output_folder = args.output
    output_filename = args.filename
    
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
        print(f"📁 出力フォルダを作成しました: {output_folder}")
        
    output_md_path = os.path.join(output_folder, output_filename)
    all_valid_dfs = []
    
    if not os.path.isdir(excel_folder):
        print(f"🚨 エラー: フォルダが見つかりません: {excel_folder}"); return
    
    excel_files = [f for f in os.listdir(excel_folder) if f.endswith(".xlsx") and not f.startswith('~')]
    if not excel_files:
        print(f"🤷‍♀️ フォルダに処理対象のExcelファイル(.xlsx)が見つかりませんでした。"); return
        
    for file in excel_files:
        print(f"\n{'='*50}\n📂 処理ファイル: {file}\n{'='*50}")
        try:
            xls = pd.ExcelFile(os.path.join(excel_folder, file))
            for sheet in xls.sheet_names:
                print(f"\n📄 シート: {sheet}")
                df = pd.read_excel(xls, sheet_name=sheet, header=0) 
                if df.empty:
                    print("→ 空のシートのためスキップします。"); continue
                
                processed_df = process_sheet(df, file, sheet)
                if processed_df is not None and not processed_df.empty:
                    all_valid_dfs.append(processed_df)
                    print(f"🎉 シート '{sheet}' の処理が正常に完了。")
        except Exception as e:
            print(f"🚨 重大なエラー: ファイル '{file}' の処理中に予期せMぬエラーが発生しました: {e}")

    if all_valid_dfs:
        combined_df = pd.concat(all_valid_dfs, ignore_index=True)
        
        with open(output_md_path, "w", encoding="utf-8") as f:
            for index, row in combined_df.iterrows():
                # データを取得
                question = row["question"]
                answer = row["answer"]
                file_name = row["file"]
                sheet_name = row["sheet"]

                # 指定された形式で書き込む
                f.write(f"質問：{question}\n")
                f.write(f"回答：{answer}\n")
                f.write(f"ファイル名：{file_name}\n")
                f.write(f"シート名：{sheet_name}\n")
                
                # 最後の行以外、各エントリの後にセパレータを挿入
                if index < len(combined_df) - 1:
                    f.write("---\n")

        print(f"\n\n{'='*50}\n✨ 全処理完了 ✨\n合計 {len(combined_df)} 件のQ&Aを {output_md_path} に出力しました。\n{'='*50}")
    else:
        print("\n\n🤷‍♀️ 有効なQ&Aデータを1件も抽出できませんでした。")

if __name__ == "__main__":
    main()