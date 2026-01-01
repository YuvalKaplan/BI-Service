import io
import os
import csv
from datetime import datetime, timezone
import re
from openpyxl import load_workbook
import xlrd
import pandas as pd
from modules.object.provider import Mapping, getMappingFromJson
from modules.object.provider_etf import EtfDownload

FILE_FOLDER = "./.downloads/"
DECIMAL_PRECISION = 10

def read_xls_from_buffer(file_buffer: bytes, mapping: Mapping) -> list[list[str]]:
    book = xlrd.open_workbook(file_contents=file_buffer)
    sheet_name = mapping.sheet if mapping.sheet is not None else book.sheet_names()[0]
    sheet = book.sheet_by_name(sheet_name)
    full_rows = []
    for r in range(sheet.nrows):
        full_rows.append([sheet.cell_value(r, c) for c in range(sheet.ncols)])
    return full_rows
    
def read_xlsx_from_buffer(file_buffer: bytes, mapping: Mapping) -> list[list[str]]:
    wb = load_workbook(io.BytesIO(file_buffer), data_only=True)
    sheet = mapping.sheet if mapping.sheet is not None else wb.sheetnames[0]
    ws = wb[sheet]
    full_rows = []
    if not ws:
        raise Exception('Could not read raw data from Excel holdings file')
    
    for row in ws.iter_rows(values_only=True):
        full_rows.append(list(row))
    return full_rows
    
def read_csv_from_buffer(file_buffer: str, mapping: Mapping) -> list[list[str]]:
    reader = csv.reader(io.StringIO(file_buffer))
    return list(reader)

def read_file(file_name: str, format: str, mapping: Mapping) -> list[list[str]]:
    if format == 'xls':
        with open(os.path.join(FILE_FOLDER, file_name), "rb") as f:
            data = f.read()
            return read_xls_from_buffer(data, mapping)
    if format == 'xlsx':
        with open(os.path.join(FILE_FOLDER, file_name), "rb") as f:
            data = f.read()
            return read_xlsx_from_buffer(data, mapping)
    else:
        with open(os.path.join(FILE_FOLDER, file_name), "r") as f:
            data = f.read()
            return read_csv_from_buffer(data, mapping)

def clean_numeric_column(df: pd.DataFrame, column: str, as_type: str = "float") -> pd.Series:
    col = df[column].astype(str).str.strip()
    col_clean = col.str.replace(r"[$€£,%]", "", regex=True)
    col_numeric = pd.to_numeric(col_clean, errors="coerce")
        
    if as_type == "int":
        col_numeric = col_numeric.astype("Int64")  # nullable integer type
    
    return col_numeric

def clean_date(dirty: str, format: str) -> datetime:
    format_to_regex = {
        "%Y": r"\d{4}",
        "%y": r"\d{2}",
        "%m": r"\d{1,2}",
        "%d": r"\d{1,2}",
        "%b": r"\w{3}",
        "%B": r"\w+"
    }
    pattern = format
    for k, v in format_to_regex.items():
        pattern = pattern.replace(k, v)

    match = re.search(pattern, dirty)
    if match:
        return datetime.strptime(match.group(), format)
    
    raise Exception("Date could not be parsed")

def map_data(full_rows: list[list[str]], file_name:str, mapping: Mapping) -> pd.DataFrame:
    
    skip = mapping.skip_rows
    if mapping.multi_row_header == 2:
        # Two header rows
        header_1 = full_rows[skip]
        header_2 = full_rows[skip + 1]
        
        # Flatten into a single string
        header_1_fixed = []
        last = ""
        for h in header_1:
            if h not in [None, "", "nan"]:
                last = str(h).strip()
            header_1_fixed.append(last)

        no_prefix = set(getattr(mapping, "no_prefix_headers", []))

        # Now flatten properly using both levels
        header = []
        for top, sub in zip(header_1_fixed, header_2):
            sub = "" if sub is None else str(sub).strip()
            top = str(top).strip()

            if sub in no_prefix or top == "":
                # standalone second-layer header
                header.append(sub)
            else:
                header.append(f"{top} {sub}".strip())

        data_start = skip + mapping.header_data_gap + 2
    else:
        # Single header row
        header = [str(h).strip() if h is not None else "" for h in full_rows[skip]]
        data_start = skip + mapping.header_data_gap + 1

    data = full_rows[data_start:]
    df = pd.DataFrame(data, columns=header)

    df.columns = df.columns.str.replace('\ufeff', '', regex=True)

    good_date: datetime | None = None
    if mapping.date.none:
        good_date = datetime.now(timezone.utc)

    if mapping.date.in_file_name:
        good_date = clean_date(file_name, format=mapping.date.format)

    # If the date is not a part of the table or is only in the first row - grab it:
    if mapping.date.single:
        dirty = full_rows[mapping.date.single.row][mapping.date.single.col]
        good_date = clean_date(dirty, format=mapping.date.format) 

    # select only the rows of the product (if this is a multi-product sheet)
    if mapping.product_column:
        df = df[df[mapping.product_column] == mapping.product_symbol]

    # Select only columns that exist in the original dataframe
    inverse_map = {src.lower(): tgt for tgt, src in mapping.columns.items() if src is not None}
    df.columns = df.columns.str.lower()
    df = df[list(inverse_map.keys())]
    df.rename(columns=lambda c: inverse_map.get(c.lower(), c), inplace=True)

    # Ensure ALL target columns exist, including those mapped from None
    for tgt, src in mapping.columns.items():
        if tgt not in df.columns:
            df.loc[:, tgt] = None

    # Maintain the exact order defined in mapping
    df = df[list(mapping.columns.keys())]

    df = df.dropna(how="all")

    if good_date:
        df.loc[:, "trade_date"] = good_date
    else:
        df["trade_date"] = pd.to_datetime(df["trade_date"], format=mapping.date.format, errors="coerce")

    required_cols = ["trade_date", "ticker", "shares", "market_value", "weight"]
    df = df.dropna(subset=required_cols)

    df["market_value"] = clean_numeric_column(df, "market_value")
    df["weight"] = clean_numeric_column(df, "weight")
    df["shares"] = clean_numeric_column(df, "shares")
    df['ticker'] = df['ticker'].str.split(' ').str[0]

    
    # drop rows where the shares is empty or 0 (used for cash holdings)
    df = df[df["shares"].notna() & (df["shares"] > 0)]
    df = df[df["weight"].notna() & (df["weight"] > 0)]

    # We are using a weighting that sums up to 1 (not 100%)
    total_weight = df["weight"].sum()
    if total_weight > 1:
        df["weight"] = df["weight"] / 100
        df["weight"] = df["weight"].round(DECIMAL_PRECISION)

    df.loc[:,"ticker"] = df["ticker"].astype(str)
    df = df[
        ~df["ticker"].isin(mapping.remove_tickers) &
        df["ticker"].str.strip().ne("")
    ]

    # Sum up holdings that have the same ticker
    df = df.groupby('ticker', as_index=False).agg({
        'market_value': 'sum',
        'shares': 'sum',
        'weight': 'sum',
        'trade_date': 'first'
    })
    return df    

def transform(download: EtfDownload, save: bool = False) -> pd.DataFrame:
    try: 
        file_format = download.etf.file_format or download.provider.file_format
        use_mapping  = download.etf.mapping or download.provider.mapping

        if file_format == None or use_mapping == None:
            raise Exception('Missing file type or mapping information in database for data trasformation.')

        mapping = getMappingFromJson(use_mapping)

        full_rows: list[list[str]] = []
        df = pd.DataFrame()
        if download.file_name and download.data:
            if file_format == 'xls':
                if save:
                    with open(os.path.join(FILE_FOLDER, download.file_name), "wb") as f:
                        f.write(download.data)
                full_rows = read_xls_from_buffer(download.data, mapping)
            if file_format == 'xlsx':
                if save:
                    with open(os.path.join(FILE_FOLDER, download.file_name), "wb") as f:
                        f.write(download.data)
                full_rows = read_xlsx_from_buffer(download.data, mapping)
            elif file_format == 'csv':
                data = download.data.decode('utf-8-sig')
                if save:
                    with open(os.path.join(FILE_FOLDER, download.file_name), "w", encoding="utf-8") as f:
                        f.write(data)
                full_rows = read_csv_from_buffer(data, mapping)

            df = map_data(full_rows=full_rows, file_name=download.file_name, mapping=mapping)

        return df
        
    except Exception as e:
        raise Exception(f"Failed to convert downloaded ETF ({download.etf.name}) to Data Frame table data: {e}")

