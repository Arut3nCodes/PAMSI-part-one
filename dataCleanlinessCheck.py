import pandas as pd
import os

def analyze_large_dataframe(file_path, chunksize=100_000):
    analysis = {}

    for chunk in pd.read_csv(file_path, chunksize=chunksize, low_memory=True):
        for column in chunk.columns:
            col_data = chunk[column]

            if pd.api.types.is_numeric_dtype(col_data):
                if column not in analysis:
                    analysis[column] = {
                        "min": col_data.min(),
                        "max": col_data.max(),
                        "null_values": col_data.isnull().sum()
                    }
                else:
                    analysis[column]["min"] = min(analysis[column]["min"], col_data.min())
                    analysis[column]["max"] = max(analysis[column]["max"], col_data.max())
                    analysis[column]["null_values"] += col_data.isnull().sum()

            elif pd.api.types.is_string_dtype(col_data):
                if column not in analysis:
                    analysis[column] = {
                        "max_length": col_data.dropna().map(len).max(),
                        "null_values": col_data.isnull().sum()
                    }
                else:
                    analysis[column]["max_length"] = max(analysis[column]["max_length"], col_data.dropna().map(len).max())
                    analysis[column]["null_values"] += col_data.isnull().sum()

            else:
                if column not in analysis:
                    analysis[column] = {
                        "type": "Non-numeric/non-string column",
                        "null_values": col_data.isnull().sum()
                    }
                else:
                    analysis[column]["null_values"] += col_data.isnull().sum()

    return analysis

def analyze_files_in_directory(directory_path, output_csv="analysis_results.csv"):
    results = []
    for file_name in os.listdir(directory_path):
        file_path = os.path.join(directory_path, file_name)

        if os.path.isfile(file_path) and file_name.endswith('.csv'):
            try:
                print(f"Processing: {file_name}")
                file_analysis = analyze_large_dataframe(file_path)
                for column, stats in file_analysis.items():
                    results.append({
                        "File": file_name,
                        "Column": column,
                        "Min Value": stats.get("min", ""),
                        "Max Value": stats.get("max", ""),
                        "Max Length": stats.get("max_length", ""),
                        "Null Values": stats.get("null_values", ""),
                        "Notes": stats.get("type", "")
                    })
            except Exception as e:
                results.append({
                    "File": file_name,
                    "Column": "Error",
                    "Min Value": "",
                    "Max Value": "",
                    "Max Length": "",
                    "Null Values": "",
                    "Notes": f"Error processing file: {e}"
                })

    # Save results to CSV
    df = pd.DataFrame(results)
    df.to_csv(output_csv, index=False)
    print(f"Analysis results saved to {output_csv}")
    
if __name__ == "__main__":
    directory_path = "/home/arut3n/Documents/Studia/MagSem1/PAMSI/PAMSI-part-one/data/202301"
    analyze_files_in_directory(directory_path)    