import atexit
from modules.init.exit import cleanup
from modules.object import provider, provider_etf
from modules.parse.convert import read_file, convert_data

atexit.register(cleanup)

if __name__ == '__main__':
    try:
        provider_id = 1
        filename = 'Goldman Sachs ActiveBeta U.S. Large Cap Equity ETF.xlsx'
        p = provider.fetch_by_id(provider_id)
        if p and p.id:
            etf_list = provider_etf.fetch_by_provider_id(p.id)

            etf = etf_list[0]
            file_format = etf.file_format or p.file_format
            use_mapping  = etf.mapping or p.mapping

            if file_format == None or use_mapping == None:
                raise Exception('Missing file type or mapping information in database for data trasformation.')

            mapping = provider.getMappingFromJson(use_mapping)
            df = read_file(file_name=filename, format=file_format, mapping=mapping)
            df = convert_data(df=df, mapping=mapping)
        
            print(df.head())
            print("... ------------ ...")
            print(df.tail())

    except Exception as e:
        print(f"Error in converting single read: {e}")
