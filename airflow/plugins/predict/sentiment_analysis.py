import json
import torch
import pandas as pd
import os
from transformers import AutoTokenizer, AutoModelForSequenceClassification
from langdetect import detect, DetectorFactory

# Set seed for reproducibility
DetectorFactory.seed = 0


def detect_language(comment):
    try:
        return detect(comment)
    except:
        return None


def merge_json_files(raw_path, temp_path):
    with open(raw_path, 'r', encoding='utf-8') as f:
        raw_data = json.load(f)

    with open(temp_path, 'r', encoding='utf-8') as f:
        temp_data = json.load(f)

    if isinstance(raw_data, dict) and isinstance(temp_data, dict):
        merged_data = {**raw_data, **temp_data}
    elif isinstance(raw_data, list) and isinstance(temp_data, list):
        merged_data = raw_data + temp_data
    else:
        raise ValueError("Không thể gộp JSON (kiểu dữ liệu không khớp)")

    with open(raw_path, 'w', encoding='utf-8') as f:
        json.dump(merged_data, f, indent=4, ensure_ascii=False)

    print(f"Đã gộp thành công: {os.path.basename(raw_path)}")


def process_sentiment_analysis_vi():
    json_path = os.path.expanduser('~/Documents/Airflow/raw/review/bus_reviews_temp.json')
    model_path = '5CD-AI/Vietnamese-Sentiment-visobert'
    output_json_temp = os.path.expanduser('~/Documents/Airflow/raw/sentiment/sentiment_vi_results_temp.json')
    output_json = os.path.expanduser('~/Documents/Airflow/raw/sentiment/sentiment_vi_results.json')

    with open(json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    df = pd.DataFrame(data)[['Bus_Name', 'Comment']].rename(columns={'Bus_Name': 'bus_name', 'Comment': 'comment'})
    df['lang'] = df['comment'].apply(detect_language)
    df_vi = df[df['lang'] == 'vi'].reset_index(drop=True)

    bus_names = df_vi['bus_name']
    sentences = df_vi['comment']

    try:
        tokenizer = AutoTokenizer.from_pretrained(model_path)
        model = AutoModelForSequenceClassification.from_pretrained(model_path)
    except Exception as e:
        print(f"Lỗi khi tải mô hình Vietnamese: {e}")
        return

    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    model.to(device)

    batch_size = 16
    results = []

    for start_idx in range(0, len(sentences), batch_size):
        batch_bus_names = bus_names[start_idx:start_idx + batch_size].tolist()
        batch_sentences = sentences[start_idx:start_idx + batch_size].tolist()

        inputs = tokenizer(batch_sentences, padding=True, truncation=True, return_tensors="pt")
        inputs = {key: value.to(device) for key, value in inputs.items()}

        with torch.no_grad():
            outputs = model(**inputs)
            scores = outputs.logits.softmax(dim=-1).cpu().numpy()

        for i, (bus_name, sentence) in enumerate(zip(batch_bus_names, batch_sentences)):
            results.append({
                'Bus_Name': bus_name,
                'Comment': sentence,
                'POS': round(scores[i][model.config.label2id['POS']], 4),
                'NEG': round(scores[i][model.config.label2id['NEG']], 4),
                'NEU': round(scores[i][model.config.label2id['NEU']], 4)
            })

        torch.cuda.empty_cache()

    pd.DataFrame(results).to_json(output_json_temp, orient="records", force_ascii=False)
    merge_json_files(output_json, output_json_temp)


def process_sentiment_analysis_en():
    json_path = os.path.expanduser('~/Documents/Airflow/raw/review/bus_reviews_temp.json')
    model_path = 'distilbert-base-uncased-finetuned-sst-2-english'
    output_json_temp = os.path.expanduser('~/Documents/Airflow/raw/sentiment/sentiment_en_results_temp.json')
    output_json = os.path.expanduser('~/Documents/Airflow/raw/sentiment/sentiment_en_results.json')

    with open(json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    df = pd.DataFrame(data)[['Bus_Name', 'Comment']].rename(columns={'Bus_Name': 'bus_name', 'Comment': 'comment'})
    df['lang'] = df['comment'].apply(detect_language)
    df_en = df[df['lang'] == 'en'].reset_index(drop=True)

    bus_names = df_en['bus_name']
    sentences = df_en['comment']

    try:
        tokenizer = AutoTokenizer.from_pretrained(model_path)
        model = AutoModelForSequenceClassification.from_pretrained(model_path)
    except Exception as e:
        print(f"Lỗi khi tải mô hình English: {e}")
        return

    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    model.to(device)

    batch_size = 16
    results = []

    for start_idx in range(0, len(sentences), batch_size):
        batch_bus_names = bus_names[start_idx:start_idx + batch_size].tolist()
        batch_sentences = sentences[start_idx:start_idx + batch_size].tolist()

        inputs = tokenizer(batch_sentences, padding=True, truncation=True, return_tensors="pt")
        inputs = {key: value.to(device) for key, value in inputs.items()}

        with torch.no_grad():
            outputs = model(**inputs)
            scores = outputs.logits.softmax(dim=-1).cpu().numpy()

        for i, (bus_name, sentence) in enumerate(zip(batch_bus_names, batch_sentences)):
            results.append({
                'Bus_Name': bus_name,
                'Comment': sentence,
                'POS': round(scores[i][model.config.label2id['POSITIVE']], 4),
                'NEG': round(scores[i][model.config.label2id['NEGATIVE']], 4)
            })

        torch.cuda.empty_cache()

    pd.DataFrame(results).to_json(output_json_temp, orient="records", force_ascii=False)
    merge_json_files(output_json, output_json_temp)
