# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col,lit,when,min, max, avg
from datetime import datetime

QUERY_SERIE_TO_BE_PROCESSED="select dart,unit,product_type, product_subtype, consultant_name, version, country, market, hub,term, term_ref_year, normalized_scenario,GRANULARITY, f.EXPIRATION_DATE,f.RECEIPT_DATE,f.IDCONSULTANT,f.IDCURRENCY,f.IDDART, f.IDGEOGRAPHIC,f.IDGRANULARITY,f.IDORIGINATION,f.IDPRODUCT,f.IDPRODUCTTEMP,f.IDRECURRENTUPDATE,f.IDSCENARIO,f.IDTERM,f.IDTYPENODEORBUS,f.IDUNIT,f.IDURL_FEED,f.IDSERIE,f.INGESTION_DATE,f.IDVERSION,f.IDNORMALIZED_SCENARIO, count(*)  from fact_price f, dim_version v, dim_consultant c,  dim_geographic geo , dim_dart da , dim_granularity g, dim_term t, DIM_NORMALIZED_SCENARIO ns, DIM_PRODUCT pro, DIM_UNIT un where f.idversion=v.idversion and f.idconsultant=c.idconsultant and f.idgeographic= geo.idgeographic  and f.iddart=da.iddart and f.idgranularity =g.idgranularity  and f.idterm =t.idterm  and f.idnormalized_scenario= ns.idnormalized_scenario and f.idproduct=pro.idproduct and f.idunit=un.idunit and f.idorigination != '103' group by f.EXPIRATION_DATE,f.RECEIPT_DATE,f.IDCONSULTANT,f.IDCURRENCY,f.IDDART, f.IDGEOGRAPHIC,f.IDGRANULARITY,f.IDORIGINATION, f.IDPRODUCT,f.IDPRODUCTTEMP,f.IDRECURRENTUPDATE, f.IDSCENARIO,f.IDTERM,f.IDTYPENODEORBUS,f.IDUNIT,f.IDURL_FEED,f.IDSERIE,f.INGESTION_DATE,f.IDVERSION,f.IDNORMALIZED_SCENARIO,unit, consultant_name, version, country, market, hub,term, term_ref_year, product_type, product_subtype,  normalized_scenario,dart, GRANULARITY"
QUERY_PRICES="select year(LOCAL_DATA_DATE) data_year, f.EXPIRATION_DATE,f.RECEIPT_DATE,f.IDCONSULTANT,f.IDCURRENCY,f.IDDART, f.IDGEOGRAPHIC,f.IDGRANULARITY,f.IDORIGINATION,f.IDPRODUCT,f.IDPRODUCTTEMP,f.IDRECURRENTUPDATE,f.IDSCENARIO,f.IDSHAPE,f.IDTERM,f.IDTYPENODEORBUS,f.IDUNIT,f.IDURL_FEED,f.IDSERIE,f.INGESTION_DATE,f.IDVERSION,f.IDNORMALIZED_SCENARIO  , f.local_data_date, f.price from fact_price f where f.idorigination != '103'"
QUERY_UNIT = 'select UNIT, IDUNIT from DIM_UNIT'
QUERY_CURRENCY = 'select CURRENCY, IDCURRENCY from DIM_CURRENCY'
QUERY_TERM = 'select IDTERM, TERM, TERM_REF_YEAR from DIM_TERM'
QUERY_INFLATION = "select product_type, scenario, consultant_name, country, f.EXPIRATION_DATE, f.IDCONSULTANT, f.IDCURRENCY_EXCHANGE, f.IDCURRENCY_ORIGIN, f.IDGEOGRAPHIC, f.IDGRANULARITY, f.IDORIGINATION, f.IDPRODUCT, f.IDRECURRENTUPDATE, f.IDSCENARIO, f.IDUNIT, f.IDURL_FEED, f.INGESTION_DATE, f.RECEIPT_DATE, count(*)  from fact_index f, dim_consultant c,  dim_geographic geo, DIM_PRODUCT pro, DIM_SCENARIO sce, DIM_URL_FEED where f.idconsultant=c.idconsultant and f.idgeographic= geo.idgeographic and f.idproduct=pro.idproduct and f.idscenario=sce.idscenario and product_type = 'Inflation' and url_feed ilike 'xelio/%' group by f.EXPIRATION_DATE, f.IDCONSULTANT, f.IDCURRENCY_EXCHANGE, f.IDCURRENCY_ORIGIN, f.IDGEOGRAPHIC, f.IDGRANULARITY, f.IDORIGINATION, f.IDPRODUCT, f.IDRECURRENTUPDATE, f.IDSCENARIO, f.IDUNIT, f.IDURL_FEED, f.INGESTION_DATE, f.RECEIPT_DATE, product_type, scenario, consultant_name, country"
QUERY_FXR = "select product_type, scenario, consultant_name, country, f.EXPIRATION_DATE, f.IDCONSULTANT, f.IDCURRENCY_EXCHANGE, f.IDCURRENCY_ORIGIN, f.IDGEOGRAPHIC, f.IDGRANULARITY, f.IDORIGINATION, f.IDPRODUCT, f.IDRECURRENTUPDATE, f.IDSCENARIO, f.IDUNIT, f.IDURL_FEED, f.INGESTION_DATE, f.RECEIPT_DATE, count(*)  from fact_index f, dim_consultant c,  dim_geographic geo, DIM_PRODUCT pro, DIM_SCENARIO sce, DIM_URL_FEED where f.idconsultant=c.idconsultant and f.idgeographic= geo.idgeographic and f.idproduct=pro.idproduct and f.idscenario=sce.idscenario and product_type = 'FXR' and url_feed ilike 'xelio/%' group by f.EXPIRATION_DATE, f.IDCONSULTANT, f.IDCURRENCY_EXCHANGE, f.IDCURRENCY_ORIGIN, f.IDGEOGRAPHIC, f.IDGRANULARITY, f.IDORIGINATION, f.IDPRODUCT, f.IDRECURRENTUPDATE, f.IDSCENARIO, f.IDUNIT, f.IDURL_FEED, f.INGESTION_DATE, f.RECEIPT_DATE, product_type, scenario, consultant_name, country"
QUERY_INDEX = 'select idversion, year(LOCAL_DATA_DATE) data_year,f.EXPIRATION_DATE, f.IDCONSULTANT, f.IDCURRENCY_EXCHANGE, f.IDCURRENCY_ORIGIN, f.IDGEOGRAPHIC, f.IDGRANULARITY, f.IDORIGINATION, f.IDPRODUCT, f.IDPRODUCTTEMP, f.IDRECURRENTUPDATE, f.IDSCENARIO, f.IDSERIE, f.IDUNIT, f.IDURL_FEED, f.INGESTION_DATE, f.RECEIPT_DATE, f.local_data_date, f.index from fact_index f order by data_year'

def main(session: snowpark.Session, serie, inflation_serie_actual, actual_inflation_year,old_version_inflation, inflation_serie, new_inflation_year, new_version_inflation, new_unit_currency, serie_conversor,version_fxr, psinput, conditional): 
    ######## PARAMETERS
    #serie= 'Wholesale price::Ascend Analytics>2023-Q3>Central>USA-ERCOT-ERCOT HOUSTON->Granularity 05 MIN>Nominal>Day Ahead'
    #inflation_serie_actual= 'Nominal'
    #actual_inflation_year= 2022
    #old_version_inflation= '2024-Q2'
    #inflation_serie= 'FMI>USA'
    #new_inflation_year= 2022
    #new_version_inflation= '2024-Q2'
    #new_unit_currency=[
    #    "EUR",
    #    "EUR/MWh"
    #]
    #serie_conversor= 'X-ELIO>SPAIN'
    #version_fxr= '2024-Q2'
    #psinput= 'Wholesale price::Ascend Analytics>2023-Q3>Central>USA-ERCOT-ERCOT HOUSTON->Granularity 05 MIN>Nominal>Day Ahead: Conversion of term Nominal to Real-2022 2024-Q2'
    #psinput= 'Wholesale price::Aurora>2023-Q2>Central>USA-ERCOT-ERCOT HOUSTON->Granularity MONTHLY>Real-2022>Day Ahead: Conversion of currency USD to EUR 2024-Q2'
    #psinput= 'Wholesale price::Aurora>2023-Q2>Central>USA-ERCOT-ERCOT HOUSTON->Granularity MONTHLY>Real-2022>Day Ahead: Conversion of currency USD to EUR 2024-Q2 and term Real-2022 2024-Q2 to Real-2024 2024-Q2'
    #conditional=["term"]
    #conditional=["term","currency"]
    ###############
    cols_raw_table=['EXPIRATION_DATE','RECEIPT_DATE','LOCAL_DATA_DATE','IDCONSULTANT','IDCURRENCY','IDDART',
                    'IDGEOGRAPHIC','IDGRANULARITY','IDORIGINATION','IDPRODUCT','IDPRODUCTTEMP','IDRECURRENTUPDATE','IDSCENARIO',
                    'IDSHAPE','IDTERM','IDTYPENODEORBUS','IDUNIT','IDURL_FEED','PRICE','IDSERIE','INGESTION_DATE','IDVERSION','IDNORMALIZED_SCENARIO','PSINPUT' ]
    series_precios= get_series (session, serie)
    for price_row in series_precios.to_local_iterator():
        price_data= get_prices(session, price_row)
        price_data.show()
        if 'term' in conditional:
            price_data = get_term_convert(session,inflation_serie_actual,actual_inflation_year,old_version_inflation,inflation_serie,new_inflation_year,new_version_inflation,price_data)
        if 'currency' in conditional:
            price_data = get_currency_convert(session,new_unit_currency,serie_conversor,price_data)
        price_data = price_data.with_column("psinput", lit(psinput))
        price_data = price_data.drop("DATA_YEAR")
        price_data = price_data.withColumn("IDORIGINATION", lit("103"))
        price_data = price_data[cols_raw_table]
        price_data.write.save_as_table("dev_xelio.RAW.TRANSFORMATION_FACT_PRICE_ID", mode="append", table_type="")
        out_put=session.call("dev_xelio.raw.INSERT_TRANSFORMATION_FACT_PRICE_ID")
        return price_data
    return series_precios

def get_series (session,  v_captured_data_serie):
    product_type = v_captured_data_serie.split(':')[0]
    product_subtype = v_captured_data_serie.split(':')[1]
    filtros= v_captured_data_serie.split('>')
    geo= filtros[3]
    geo_coor= geo.split('-')
    term_=filtros[5] 
    term=term_.split('-')
    df = session.sql(QUERY_SERIE_TO_BE_PROCESSED)
    df = df.filter((col("product_type") == product_type))
    df = df.filter((col("product_subtype") == product_subtype))
    df = df.filter((col("consultant_name") == filtros[0].split(':')[-1] ) )    
    df = df.filter((col("version") == filtros[1] ) )
    df = df.filter((col("normalized_scenario") == filtros[2] ) )
    df = df.filter(col("country") ==geo_coor[0])
    df = df.filter(col("market") ==geo_coor[1])
    df = df.filter(col("hub") ==geo_coor[2])
    df = df.filter(col("Granularity") == filtros[4])
    df = df.filter(col("dart") == filtros[6])
    if len(term)==1:
        df = df.filter(col("term") ==term[0])
    else:
        df = df.filter(col("term") ==term[0])
        df = df.filter(col("term_ref_year") ==term[1])

    return df 

def get_prices (session,price_row):

    df = session.sql(QUERY_PRICES)     
    results = df.filter(
        (col('RECEIPT_DATE') == price_row['RECEIPT_DATE'] )&
        (col('IDCONSULTANT') == price_row['IDCONSULTANT'] )&
        (col('IDCURRENCY') == price_row['IDCURRENCY'] )& 
        (col('IDDART') == price_row['IDDART'] )&
        (col('IDGEOGRAPHIC') == price_row['IDGEOGRAPHIC'] )&
        (col('IDORIGINATION') == price_row['IDORIGINATION'] )&
        (col('IDPRODUCT') == price_row['IDPRODUCT'] )&
        (col('IDPRODUCTTEMP') == price_row['IDPRODUCTTEMP'] )&
        (col('IDRECURRENTUPDATE') == price_row['IDRECURRENTUPDATE'] )&
        (col('IDSCENARIO') == price_row['IDSCENARIO'] )&
        (col('IDTERM') == price_row['IDTERM'] ) &
        (col('IDTYPENODEORBUS') == price_row['IDTYPENODEORBUS'] )&
        (col('IDUNIT') == price_row['IDUNIT'] )&
        (col('IDURL_FEED') == price_row['IDURL_FEED'] )&
        (col('IDSERIE') == price_row['IDSERIE'] )&
        (col('IDVERSION') == price_row['IDVERSION'] )&
        (col('IDNORMALIZED_SCENARIO') == price_row['IDNORMALIZED_SCENARIO'] ) &
        (col('IDGRANULARITY') == price_row['IDGRANULARITY'] )
    )
    return results 

def get_series_index(session,inflation_data_serie):
    filtros= inflation_data_serie.split('>')
    df = session.sql(QUERY_INFLATION)
    df = df.filter((col("consultant_name") == filtros[0]) )
    df = df.filter(col("country") ==filtros[1])
    return df.first()

def get_series_fxr(session,fxr_data_serie):
    filtros= fxr_data_serie.split('>')
    df = session.sql(QUERY_FXR)
    df = df.filter((col("consultant_name") == filtros[0]) )
    df = df.filter(col("country") ==filtros[1])
    return df.first()

def get_fxr_variable(session,fxr,currency):
    df = session.sql(QUERY_CURRENCY)
    df = df.filter((col("CURRENCY") == currency) )
    df = df.collect()
    fxr = fxr.collect()
    if fxr[0]['IDCURRENCY_ORIGIN'] == df[0]['IDCURRENCY']:
        return 1
    if fxr[0]['IDCURRENCY_EXCHANGE'] == df[0]['IDCURRENCY']:
        return 0

def get_index (session,index_row):

    df = session.sql(QUERY_INDEX)
    results = df.filter(
        (col('IDPRODUCT') == index_row['IDPRODUCT'] )&
        (col('IDCONSULTANT') == index_row['IDCONSULTANT'] )& 
        (col('IDSCENARIO') == index_row['IDSCENARIO'] )&
        (col('IDGEOGRAPHIC') == index_row['IDGEOGRAPHIC'] )&
        (col('IDORIGINATION') == index_row['IDORIGINATION'] )&
        (col('IDGRANULARITY') == index_row['IDGRANULARITY'] )&
        (col('IDURL_FEED') == index_row['IDURL_FEED'] )
    )
    return results 

def get_index_inflation (session,index_row,year,inflation_version):

    df = session.sql(QUERY_INDEX)
    results = df.filter(
        (col('IDPRODUCT') == index_row['IDPRODUCT'] )&
        (col('IDCONSULTANT') == index_row['IDCONSULTANT'] )& 
        (col('IDSCENARIO') == index_row['IDSCENARIO'] )&
        (col('IDGEOGRAPHIC') == index_row['IDGEOGRAPHIC'] )&
        (col('IDORIGINATION') == index_row['IDORIGINATION'] )&
        (col('IDGRANULARITY') == index_row['IDGRANULARITY'] )&
        (col('IDURL_FEED') == index_row['IDURL_FEED'] )
    )
    versions = session.sql('select * from dim_version')
    versions = versions.filter(col("VERSION") == inflation_version)
    versions = versions.collect()
    results = results.filter((col('IDVERSION') == versions[0]['IDVERSION']) | (col('IDVERSION') == '3'))
    results_history = results.filter(col('IDVERSION') == '3')
    results_forecast = results.filter(col('IDVERSION') != '3')
    years_history = [row['DATA_YEAR'] for row in results_history.select("DATA_YEAR").distinct().collect()]
    filtered_results_forecast = results_forecast.filter(~col("DATA_YEAR").isin(years_history))
    results = results_history.unionAll(filtered_results_forecast)
    min_data_year = results.select(min(col("DATA_YEAR"))).collect()[0][0]
    max_data_year = results.select(max(col("DATA_YEAR"))).collect()[0][0]
    validate_years = [row['DATA_YEAR'] for row in results.select("DATA_YEAR").distinct().collect()]
    if len(validate_years) < max_data_year-min_data_year+1:
        print("Hay huecos en los datos de la inflacion")
    grouped_results = results.group_by("DATA_YEAR").agg(
        avg(col("INDEX")).alias("AVG_INDEX")
    )
    results = results.drop("INDEX")
    results = results.drop_duplicates(["DATA_YEAR"])
    results = results.join(
                grouped_results.select(col("DATA_YEAR").alias("DATA_YEAR_INDEX"), col("AVG_INDEX").alias(("INDEX"))),
                (results.data_year == col("DATA_YEAR_INDEX"))
            )
    all_rows = results.collect()
    row_index = next((index for index, row in enumerate(all_rows) if row['DATA_YEAR'] == year), None)
    acum = 1
    for row in all_rows[row_index+1:]:
        index_value = row['INDEX']
        acum = acum * ( 1 + index_value / 100 )
        results = results.with_column(
            'INDEX',
            when(col('DATA_YEAR') == row['DATA_YEAR'], acum).otherwise(col('INDEX'))
        )
    acum_neg = 1
    for row in reversed(all_rows[:row_index]):
        index_value = row['INDEX']
        acum_neg = acum * ( 1 + index_value / 100 )
        results = results.with_column(
            'INDEX',
            when(col('DATA_YEAR') == row['DATA_YEAR'], (1/acum_neg)).otherwise(col('INDEX'))
        )
    results = results.with_column(
        'INDEX',
        when(col('DATA_YEAR') == year, 1.0).otherwise(col('INDEX'))
    )
    return results 
    
def get_index_inflation_real (session,index_row,year_to,year_from,inflation_version):
    df = session.sql(QUERY_INDEX)
    results = df.filter(
        (col('IDPRODUCT') == index_row['IDPRODUCT'] )&
        (col('IDCONSULTANT') == index_row['IDCONSULTANT'] )& 
        (col('IDSCENARIO') == index_row['IDSCENARIO'] )&
        (col('IDGEOGRAPHIC') == index_row['IDGEOGRAPHIC'] )&
        (col('IDORIGINATION') == index_row['IDORIGINATION'] )&
        (col('IDGRANULARITY') == index_row['IDGRANULARITY'] )&
        (col('IDURL_FEED') == index_row['IDURL_FEED'] )
    )
    versions = session.sql('select * from dim_version')
    versions = versions.filter(col("VERSION") == inflation_version)
    versions = versions.collect()
    results = results.filter((col('IDVERSION') == versions[0]['IDVERSION']) | (col('IDVERSION') == '3'))
    results_history = results.filter(col('IDVERSION') == '3')
    results_forecast = results.filter(col('IDVERSION') != '3')
    years_history = [row['DATA_YEAR'] for row in results_history.select("DATA_YEAR").distinct().collect()]
    filtered_results_forecast = results_forecast.filter(~col("DATA_YEAR").isin(years_history))
    results = results_history.unionAll(filtered_results_forecast)
    min_data_year = results.select(min(col("DATA_YEAR"))).collect()[0][0]
    max_data_year = results.select(max(col("DATA_YEAR"))).collect()[0][0]
    validate_years = [row['DATA_YEAR'] for row in results.select("DATA_YEAR").distinct().collect()]
    if len(validate_years) < max_data_year-min_data_year+1:
        print("Hay huecos en los datos de la inflacion")
    grouped_results = results.group_by("DATA_YEAR").agg(
        avg(col("INDEX")).alias("AVG_INDEX")
    )
    results = results.drop("INDEX")
    results = results.drop_duplicates(["DATA_YEAR"])
    results = results.join(
                grouped_results.select(col("DATA_YEAR").alias("DATA_YEAR_INDEX"), col("AVG_INDEX").alias(("INDEX"))),
                (results.data_year == col("DATA_YEAR_INDEX"))
            )
    all_rows = results.collect()
    row_index_from = next((index for index, row in enumerate(all_rows) if row['DATA_YEAR'] == year_from), None)
    row_index_to = next((index for index, row in enumerate(all_rows) if row['DATA_YEAR'] == year_to), None)
    new_index = 1
    for i in range(row_index_from + 1, row_index_to + 1):
        new_index = new_index * ( 1 + all_rows[i]['INDEX'] / 100 )
    results = results.withColumn("INDEX", lit(new_index))
    results.show()
    return results

def get_term_convert(session,inflation_serie_actual,actual_inflation_year,old_version_inflation,inflation_serie,new_inflation_year,new_version_inflation,price_data):
    df = session.sql(QUERY_TERM)
    if inflation_serie_actual != 'Nominal' and inflation_serie == 'Nominal':
         serie_old_index= get_series_index(session,inflation_serie_actual)
         data_index= get_index_inflation(session,serie_old_index,actual_inflation_year,old_version_inflation)
         price_data = price_data.join(
                data_index.select(col("DATA_YEAR").alias("DATA_YEAR_FROM"), col("INDEX").alias("INDEX_FROM")),
                (price_data.data_year == col("DATA_YEAR_FROM"))
            )
         price_data = price_data.withColumn("PRICE_INDEX_FROM",col("PRICE") * col("INDEX_FROM"))
         price_data = price_data.withColumn("PRICE", col("PRICE_INDEX_FROM"))
         df = df.filter(col("TERM") == inflation_serie)
         df = df.collect()
         price_data = price_data.withColumn("IDTERM", lit(df[0]['IDTERM']))
    if inflation_serie != 'Nominal' and inflation_serie_actual == 'Nominal':
         serie_new_index= get_series_index(session,inflation_serie)
         data_index= get_index_inflation(session,serie_new_index,new_inflation_year,new_version_inflation)
         price_data = price_data.join(
                data_index.select(col("DATA_YEAR").alias("DATA_YEAR_TO"), col("INDEX").alias(("INDEX_TO"))),
                (price_data.data_year == col("DATA_YEAR_TO"))
            )
         price_data = price_data.withColumn("PRICE_INDEX_TO",col("PRICE") / col("INDEX_TO"))
         price_data = price_data.withColumn("PRICE", col("PRICE_INDEX_TO"))
         df = df.filter(col("TERM") == 'Real')
         df = df.filter(col("TERM_REF_YEAR") == new_inflation_year)
         df = df.collect()
         price_data = price_data.withColumn("IDTERM", lit(df[0]['IDTERM']))
    if inflation_serie != 'Nominal' and inflation_serie_actual != 'Nominal':
         serie_index= get_series_index(session,inflation_serie_actual)
         data_index= get_index_inflation_real(session,serie_index,new_inflation_year,actual_inflation_year,old_version_inflation)
         price_data = price_data.join(
                data_index.select(col("DATA_YEAR").alias("DATA_YEAR_TO"), col("INDEX").alias(("INDEX_TO"))),
                (price_data.data_year == col("DATA_YEAR_TO"))
            )
         price_data = price_data.withColumn("PRICE_INDEX_TO",col("PRICE") * col("INDEX_TO"))
         price_data = price_data.withColumn("PRICE", col("PRICE_INDEX_TO"))
         df = df.filter(col("TERM") == 'Real')
         df = df.filter(col("TERM_REF_YEAR") == new_inflation_year)
         df = df.collect()
         price_data = price_data.withColumn("IDTERM", lit(df[0]['IDTERM']))
    price_data= price_data.drop("ingestion_date")
    price_data = price_data.with_column("ingestion_date", lit(datetime.today().strftime('%Y-%m-%d %H:%M:%S')))
    return price_data

def get_currency_convert(session,new_unit_currency,conversor,price_data):
    df = session.sql(QUERY_UNIT)
    df_currency = session.sql(QUERY_CURRENCY)
    df = df.filter(col("UNIT") == new_unit_currency[1])
    df_currency = df_currency.filter(col("CURRENCY") == new_unit_currency[0])
    df = df.collect()
    df_currency = df_currency.collect()
    price_data = price_data.withColumn('IDUNIT', lit(df[0]['IDUNIT']))
    price_data = price_data.withColumn('IDCURRENCY', lit(df_currency[0]['IDCURRENCY']))
    serie_fxr= get_series_fxr(session,conversor)
    data_index= get_index(session,serie_fxr)
    price_data = price_data.join(
                data_index.select(col("DATA_YEAR").alias("DATA_YEAR_FXR"), col("INDEX").alias(("INDEX_FXR"))),
                (price_data.data_year == col("DATA_YEAR_FXR")),
                join_type="left"
            )
    variable = get_fxr_variable(session,data_index,new_unit_currency[0])
    if variable == 1:
        price_data = price_data.withColumn("PRICE_INDEX_FXR",col("PRICE") / col("INDEX_FXR"))
    if variable == 0:
        price_data = price_data.withColumn("PRICE_INDEX_FXR",col("PRICE") * col("INDEX_FXR"))
    price_data = price_data.withColumn("PRICE", col("PRICE_INDEX_FXR"))
    price_data = price_data.drop("ingestion_date","INDEX_FXR","DATA_YEAR_FXR","PRICE_INDEX_FXR")
    price_data = price_data.with_column("ingestion_date", lit(datetime.today().strftime('%Y-%m-%d %H:%M:%S')))
    return price_data
