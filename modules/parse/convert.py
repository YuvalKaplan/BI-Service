import io
import os
import pandas as pd
from modules.object.provider import Provider, Mapping, getMappingFromJson
from modules.object.provider_etf import EtfDownload

FILE_FOLDER = "./.downloads/"

def read_excel_from_buffer(file_buffer: io.BytesIO, mapping: Mapping) -> pd.DataFrame:
    dfs = pd.read_excel(file_buffer, engine="openpyxl", sheet_name=None, skiprows=mapping.skip_rows, header=mapping.header_row)
    if mapping.sheet:
        df = dfs[mapping.sheet]
    else:
        first_sheet_name = list(dfs.keys())[0]
        df = dfs[first_sheet_name]
    return df

def read_csv_from_buffer(file_buffer: io.StringIO, mapping: Mapping) -> pd.DataFrame:
    df = pd.read_csv(file_buffer, skiprows=mapping.skip_rows, header=mapping.header_row)
    return df

def read_file(file_name: str, format: str, mapping: Mapping) -> pd.DataFrame:
    if format == 'xlsx':
        with open(os.path.join(FILE_FOLDER, file_name), "rb") as f:
            data = f.read()
            file_buffer = io.BytesIO(data)
            return read_excel_from_buffer(file_buffer, mapping)
    else:
        with open(os.path.join(FILE_FOLDER, file_name), "r") as f:
            data = f.read()
            file_buffer = io.StringIO(data)
            return read_csv_from_buffer(file_buffer, mapping)

def clean_numeric_column(df: pd.DataFrame, column: str, as_type: str = "float") -> pd.Series:
    col = df[column].astype(str).str.strip()
    col_clean = col.str.replace(r"[$€£,%]", "", regex=True)
    col_numeric = pd.to_numeric(col_clean, errors="coerce")
        
    if as_type == "int":
        col_numeric = col_numeric.astype("Int64")  # nullable integer type
    
    return col_numeric

def convert_data(df, mapping: Mapping) -> pd.DataFrame:
    inverse_map = {v: k for k, v in mapping.columns.items()}
    df = df[inverse_map.keys()]
    df = df.rename(columns=inverse_map)
    df = df.dropna(how="all")
    required_cols = ["trade_date", "isin", "weight"]
    df = df.dropna(subset=required_cols)
    df["trade_date"] = pd.to_datetime(df["trade_date"], format=mapping.date_format, errors="coerce")
    df["market_value"] = clean_numeric_column(df, "market_value")
    df["weight"] = clean_numeric_column(df, "weight")

    # We are using a weighting that sums up to 1 (not 100%)
    total_weight = df["weight"].sum()
    if total_weight > 1:
        df["weight"] = df["weight"] / 100
        df["weight"] = df["weight"].round(6)

    df["ticker"] = df["ticker"].astype(str)
    df = df[
        ~df["ticker"].isin(mapping.remove_tickers) &
        df["ticker"].str.strip().ne("")
    ]
    return df    

def transform(download: EtfDownload, save: bool = False) -> pd.DataFrame:
    try: 
        file_format = download.etf.file_format or download.provider.file_format
        use_mapping  = download.etf.mapping or download.provider.mapping

        if file_format == None or use_mapping == None:
            raise Exception('Missing file type or mapping information in database for data trasformation.')

        mapping = getMappingFromJson(use_mapping)

        df = pd.DataFrame()
        if download.file_name and download.data:
            if file_format == 'xlsx' and isinstance(download.data, bytes):
                if save:
                    with open(os.path.join(FILE_FOLDER, download.file_name), "wb") as f:
                        f.write(download.data)
                file_buffer = io.BytesIO(download.data)
                df = read_excel_from_buffer(file_buffer, mapping)
            elif file_format == 'csv' and isinstance(download.data, str):
                if save:
                    with open(os.path.join(FILE_FOLDER, download.file_name), "w") as f:
                        f.write(download.data)
                file_buffer = io.StringIO(download.data)
                df = read_csv_from_buffer(file_buffer, mapping)

            df = convert_data(df=df, mapping=mapping)

        return df
        
    except Exception as e:
        raise Exception(f"Failed to convert downloaded ETF (${download.etf.name}) to Data Frame table data: {e}")

