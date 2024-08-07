
import snowflake.snowpark as snowpark  

from snowflake.snowpark.functions import col, lit, when, array_unique_agg, sum as sum_  , concat , concat_ws, avg, count, max, sum , min, row_number
from datetime import datetime
from snowflake.snowpark.window import Window

 
QUERY_SERIE_TO_BE_PROCESSED="select 	level, dart, product_subtype, consultant_name, version, country, market, hub,node, term, term_ref_year, normalized_scenario,GRANULARITY, f.EXPIRATION_DATE,f.RECEIPT_DATE,f.IDCONSULTANT,f.IDCURRENCY,f.IDDART, f.IDGEOGRAPHIC,f.IDGRANULARITY,f.IDORIGINATION,f.IDPRODUCT,f.IDPRODUCTTEMP,f.IDRECURRENTUPDATE,f.IDSCENARIO,f.IDSHAPE,f.IDTERM,f.IDTYPENODEORBUS,f.IDUNIT,f.IDURL_FEED,f.IDSERIE,f.INGESTION_DATE,f.IDVERSION,f.IDNORMALIZED_SCENARIO, count(*)  from fact_price f, dim_version v, dim_consultant c,  dim_geographic geo , dim_dart da , dim_granularity g, dim_term t, DIM_NORMALIZED_SCENARIO ns, DIM_PRODUCT pro where   PRODUCT_TYPE ='Wholesale price' and  f.idversion=v.idversion and f.idconsultant=c.idconsultant and f.idgeographic= geo.idgeographic  and f.iddart=da.iddart and f.idgranularity =g.idgranularity  and f.idterm =t.idterm  and f.idnormalized_scenario= ns.idnormalized_scenario and f.idproduct=pro.idproduct group by f.EXPIRATION_DATE,f.RECEIPT_DATE,f.IDCONSULTANT,f.IDCURRENCY,f.IDDART, f.IDGEOGRAPHIC,f.IDGRANULARITY,f.IDORIGINATION, f.IDPRODUCT,f.IDPRODUCTTEMP,f.IDRECURRENTUPDATE, f.IDSCENARIO,f.IDSHAPE,f.IDTERM,f.IDTYPENODEORBUS,f.IDUNIT,f.IDURL_FEED,f.IDSERIE,f.INGESTION_DATE,f.IDVERSION,f.IDNORMALIZED_SCENARIO, consultant_name, version, country, market, hub,term, term_ref_year, product_subtype,  normalized_scenario, GRANULARITY, DART, node, level"
QUERY_PRICES="select year(LOCAL_DATA_DATE) data_year,  month(LOCAL_DATA_DATE) data_month,truncate(local_Data_date, 'DAY')   data_day,hour(LOCAL_DATA_DATE) data_hour, f.EXPIRATION_DATE,f.RECEIPT_DATE,f.IDCONSULTANT,f.IDCURRENCY,f.IDDART, f.IDGEOGRAPHIC,f.IDGRANULARITY,f.IDORIGINATION,f.IDPRODUCT,f.IDPRODUCTTEMP,f.IDRECURRENTUPDATE,f.IDSCENARIO,f.IDSHAPE,f.IDTERM,f.IDTYPENODEORBUS,f.IDUNIT,f.IDURL_FEED,f.IDSERIE,f.INGESTION_DATE,f.IDVERSION,f.IDNORMALIZED_SCENARIO  , f.local_data_date, f.price from fact_price f"


Granularity_05_MIN = 9
Granularity_15_MIN = 1
Granularity_30_MIN = 10
Granularity_HOURLY = 8
Granularity_DAILY = 3 
Granularity_MONTHLY = 7
Granularity_YEARLY = 6


############################################
########## PARAMETROS SPREAD FIJAMOS A =0
N=0
TOP_from_1h=0
MIN_1h=0
TOP_from_2h = 0
MIN_2h= 0
TOP_from_3h= 0
MIN_3h= 0
TOP_from_4h= 0
MIN_4h= 0


##############REVISAR PARA SPREAD 
Price_spread_1H=19
Price_spread_2H= 20
Price_spread_3H=303
Price_spread_4H=304  

origination_calculated=101


cols_serie_precios=['EXPIRATION_DATE', 'RECEIPT_DATE', 'IDCONSULTANT', 'IDCURRENCY', 
              'IDDART', 'IDGEOGRAPHIC', 'IDORIGINATION', 
               'IDPRODUCTTEMP', 'IDRECURRENTUPDATE', 'IDSCENARIO', 
              'IDSHAPE', 'IDTERM', 'IDTYPENODEORBUS', 'IDUNIT', 'IDURL_FEED', 'IDSERIE', 'INGESTION_DATE', 'IDVERSION', 'IDNORMALIZED_SCENARIO']
cols_precio=['LOCAL_DATA_DATE', 'PRICE']
 
cols_fecha_spread= ['DATA_YEAR', 'DATA_MONTH', 'DATA_DAY']



cols_new_serie=['selected_price_serie' , 'ingestion_date']
   
def parametrice_spread_for_granularity(v_granularity):
    print(v_granularity)
    
    if v_granularity==Granularity_05_MIN :
        reg_hora=12
      
    elif v_granularity==Granularity_15_MIN:
        reg_hora=4
    elif v_granularity==Granularity_30_MIN:
        reg_hora=2
    elif v_granularity==Granularity_HOURLY:
        reg_hora=1
     
    max_orden=24*reg_hora
  
    N=1 ##SPREAD1H
    
    TOP_from_1h=  max_orden-reg_hora*N
    MIN_1h= reg_hora
    
    N=2 ##SPREAD2H
    TOP_from_2h = max_orden-reg_hora*N
    MIN_2h = reg_hora*N
    
    N=3 ##SPREAD3H
    TOP_from_3h= max_orden-reg_hora*N
    MIN_3h= reg_hora*N
    
    N=4 ##SPREAD4H
    TOP_from_4h= max_orden-reg_hora*N
    MIN_4h= reg_hora*N
    return TOP_from_1h,MIN_1h,TOP_from_2h,MIN_2h,TOP_from_3h,  MIN_3h,TOP_from_4h,  MIN_4h 
    

def get_series (session,  v_captured_data_serie):
    
    ## Ejemplo 'Ascend Analytics>2023-Q2>Central>USA-ERCOT-ERCOT HOUSTON->Day Ahead>Granularity 05 MIN>Nominal'

    
    
    filtros= v_captured_data_serie.split('>')
    f_consultant= filtros[0]
    f_version= filtros[1]
    f_scenario=filtros[2]
    geo= filtros[3]
    f_dart= filtros[4]
    f_gran=filtros[5]
    f_term= filtros[6]
    
    geo_coor= geo.split('-')
    term_=f_term
    term=term_.split('-')
    #['Aurora', '2023-Q4', 'Central', 'USA-CAISO-ZP26-', 'Granularity 05 MIN', 'Real-2022']
    # Consultant_name, version, normalized_scenario, geo, granularity, Term 
    #and  version='2023-Q4'and consultant_name = 'Aurora'and geo.country='USA' and geo.market='CAISO' and geo.hub='ZP26' and geo.node=''and  
    #dart='RT-RTD'and granularity='Granularity 05 MIN'and  term='Real' and 
    print('f_consultant', f_consultant)
    print('version', f_version)
    print('f_scenario', f_scenario)
    print('country', geo_coor[0])
    print('market', geo_coor[1])
    print('hub', geo_coor[2])
    print('node', geo_coor[3])
    print('f_term', f_term)
    print('f_gran', f_gran)
    
    df = session.sql(QUERY_SERIE_TO_BE_PROCESSED)
    df = df.filter((col("consultant_name") ==f_consultant ) )    
    df = df.filter((col("version") == f_version) )
    df = df.filter((col("normalized_scenario") == f_scenario ) )
    df = df.filter(col("country") ==geo_coor[0])
    df = df.filter(col("market") ==geo_coor[1])
    df = df.filter(col("hub") ==geo_coor[2])
    if (geo_coor[3] is None or geo_coor[3]=='-' or geo_coor[3]=='' ) :
        print('No hay node')
        df = df.filter(col("level") !='NODE')
    else: 
        print('node',geo_coor[3] )
        df = df.filter(col("node") ==geo_coor[3])
        df = df.filter(col("level") =='NODE')
        
    df = df.filter(col("Granularity") == f_gran)
    df = df.filter(col("dart") == f_dart)

    
    if len(term)==1:
        df = df.filter(col("term") ==term[0])
    else:
        df = df.filter(col("term") ==term[0])
        df = df.filter(col("term_ref_year") ==term[1])

    
    print(df.columns)
    print(df.count())
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
        (col('IDSHAPE') == price_row['IDSHAPE'] )&
        (col('IDTERM') == price_row['IDTERM'] ) &
        (col('IDTYPENODEORBUS') == price_row['IDTYPENODEORBUS'] )&
        (col('IDUNIT') == price_row['IDUNIT'] )&
        (col('IDURL_FEED') == price_row['IDURL_FEED'] )&
        (col('IDSERIE') == price_row['IDSERIE'] )&
        (col('IDVERSION') == price_row['IDVERSION'] )&
        (col('IDNORMALIZED_SCENARIO') == price_row['IDNORMALIZED_SCENARIO'] &
        (col('INGESTION_DATE') == price_row['INGESTION_DATE']  )
        )
    )
    
    
    print(results.columns)
    results= results.sort([col("DATA_DAY"), col("price").desc()])
  
    results= results.withColumn("row_number", row_number().over(Window.partition_by(col("DATA_DAY"),col("Data_year") ).order_by(col("DATA_DAY"), col("price"))) )
     
    return results 
def enrich_columns(price_row, spread_series):
    print("---------Asignar el resto de variables de la serie----------------")
    spread_series = spread_series.withColumn("EXPIRATION_DATE", lit(price_row["EXPIRATION_DATE"]))
    spread_series = spread_series.withColumn("RECEIPT_DATE", lit(price_row["RECEIPT_DATE"]))
    #LOCAL_DATA_DATE  --Viene en el DF
    
    spread_series = spread_series.withColumn("IDCONSULTANT", lit(price_row["IDCONSULTANT"]))
    spread_series = spread_series.withColumn("IDCURRENCY", lit(price_row["IDCURRENCY"]))
    spread_series = spread_series.withColumn("IDDART", lit(price_row["IDDART"]))
    spread_series = spread_series.withColumn("IDGEOGRAPHIC", lit(price_row["IDGEOGRAPHIC"]))
    # LA GRANULARIDAD SE ASIGNA EN LA GENERACION DE LOS DF FINALES 
    
    spread_series = spread_series.withColumn("IDORIGINATION", lit(origination_calculated))
    #  EL PRODUCT  SE ASIGNA EN LA GENERACION DE LOS DF FINALES 
    
    spread_series = spread_series.withColumn("IDPRODUCTTEMP", lit(price_row["IDPRODUCTTEMP"]))
    spread_series = spread_series.withColumn("IDRECURRENTUPDATE", lit(price_row["IDRECURRENTUPDATE"]))
    spread_series = spread_series.withColumn("IDSCENARIO", lit(price_row["IDSCENARIO"]))
    spread_series = spread_series.withColumn("IDSHAPE", lit(price_row["IDSHAPE"]))
    spread_series = spread_series.withColumn("IDTERM", lit(price_row["IDTERM"]))
    spread_series = spread_series.withColumn("IDTYPENODEORBUS", lit(price_row["IDTYPENODEORBUS"]))
    spread_series = spread_series.withColumn("IDUNIT", lit(price_row["IDUNIT"]))
    # Se asigna el ID_URLFEED SE ASIGNA EN EL PROCEDIMIENTO DE CONSOLIDACION 
    spread_series = spread_series.withColumn("IDURL_FEED", lit(price_row["IDURL_FEED"]))
    spread_series = spread_series.withColumn("IDSERIE", lit(price_row["IDSERIE"]))
    
    # spread_series= spread_series.withColumn("INGESTION_DATE",lit(price_row[""]))  Viene en el DF
    
    spread_series = spread_series.withColumn("IDVERSION", lit(price_row["IDVERSION"]))
    spread_series = spread_series.withColumn("IDNORMALIZED_SCENARIO", lit(price_row["IDNORMALIZED_SCENARIO"]))
    return spread_series



def main(session: snowpark.Session, selected_price_serie): 
     
    
    #selected_price_serie='Ascend Analytics>2023-Q2>Central>USA-ERCOT-ERCOT HOUSTON->Day Ahead>Granularity 05 MIN>Nominal'
    #selected_price_serie='Ascend Analytics>2023-Q2>Central>USA-ERCOT-ERCOT HOUSTON->Real Time>Granularity 05 MIN>Nominal'
    #selected_price_serie='Aurora>2023-Q4>Central>USA-ERCOT-ERCOT HOUSTON->Real Time>Granularity 15 MIN>Real-2022'
    #selected_price_serie= 'Aurora>2023-Q4>Messy Transition>AUSTRALIA-NEM-NEM SA->Day Ahead>Granularity 30 MIN>Real-2022'
    #selected_price_serie= 'Aurora>2023-Q4>Messy Transition>AUSTRALIA-NEM-NEM SA->Day Ahead>Granularity 30 MIN>Real-2022'
    #selected_price_serie= 'Aurora>2024-Q2>Central>USA-ERCOT-ERCOT HOUSTON->Real Time>Granularity 15 MIN>Real-2023'
    #selected_price_serie='Aurora>2024-Q2>Central>USA-ERCOT-ERCOT HOUSTON-AT_TR3>Real Time>Granularity 15 MIN>Real-2023'
    series_precios= get_series (session, selected_price_serie)
    # Print a sample of the dataframe to standard output.
    series_precios.show()
  
    
    for price_row in series_precios.to_local_iterator():
        price_data= get_prices(session, price_row)
        price_data.show()
        v_granularity= price_row["IDGRANULARITY"]
        spread_series= calculate_spread_serie(session, price_data, v_granularity )
        spread_series= spread_series.with_column('psinput',lit('Spread calculation:'+selected_price_serie )) 
        spread_series = spread_series.with_column("ingestion_date", lit(datetime.today().strftime('%Y-%m-%d %H:%M:%S'))) 
        spread_series = spread_series.with_column("selected_price_serie", lit(selected_price_serie)) 
        
        spread_series = enrich_columns(price_row, spread_series)        
        
        print(spread_series.columns)
        print("-------------------------")
         
        spread_series.write.save_as_table("dev_xelio.DWH.spread_prices_ID", mode="append", table_type="")
        new_series= calculate_new_series_spread(spread_series)
        new_series.write.save_as_table("dev_xelio.RAW.TRANSFORMATION_FACT_PRICE_ID", mode="append", table_type="")
        out_put=session.call("dev_xelio.raw.INSERT_TRANSFORMATION_FACT_PRICE_ID")
 
        return new_series
    return series_precios
    
def calculate_spread_serie(session, precios, v_granularity):
    """
    Esta función invoca al calculo del spread para cada una de los cáclulos que queremos realizar (1H, 2H,3H o 4H)

    Args:
    reg_min  : número de precios más bajos a considerar, depende de la granularidad 
    reg_max : número de precios más altos , depende de la granularidad  
    precios: DF de precios al que añade una columna, se filtran las fechas para las que no existe curtailment.
    spread_type : Si es un spread a 1H, 2H 3H o 4H 
    


    Returns:
    df: Data Frame en el que para cada dia, se calcula el 
    - promedio de precio entre los máximos del día -en funcion de la granularidad 
    - el promedio de precio entre los minimos  del día -en funcion de la granularidad,
    - el spread como la diferencia entre ambas y se cuenta cuantos registros de precios se han tenido en consideracion para ese promedio (en 5 minutales 12, en 15 minutales 4, en horarios 1...) .
    """
    
    print(precios.columns)

    ############# 1h
    TOP_from_1h,MIN_1h,TOP_from_2h,MIN_2h,TOP_from_3h,  MIN_3h,TOP_from_4h,  MIN_4h   = parametrice_spread_for_granularity(v_granularity)
    spread_1H = gen_spread_granularity(MIN_1h, TOP_from_1h,  precios, '1H')
    spread_2H = gen_spread_granularity(MIN_2h, TOP_from_2h,  precios, '2H')
    spread_3H = gen_spread_granularity(MIN_3h, TOP_from_3h,  precios, '3H')
    spread_4H = gen_spread_granularity(MIN_4h, TOP_from_4h,  precios, '4H')


    spread = spread_1H.union(spread_2H)
    spread = spread.union(spread_3H)
    spread = spread.union(spread_4H)
    spread=spread.rename(col("DATA_DAY"), "LOCAL_DATA_DATE")  
    return spread


def gen_spread_granularity(reg_min, reg_max,  precios, spread_type):
    """
    Esta función agrupa el DF de precios tomando el número de registros que corresponda (en funcion de la granularidad), 
    como registros maximos o como mínimos. El data frame viene ordenado por día por el importe del precio en las primeras filas los más bajos 
    en las últimas los más altos, así el método toma para considerar
     - los precios máximos los que aparezcan en las posiciones >= reg_max (si la granularidad es horaria, solo 1, si es 5minutal, 12...)  
     - los precios más bajos los que aparezcan en las n primeras posiciones < reg_min, si la granularidad es horaria el de la posicion 0 ...

    Args:
    reg_min  : número de precios más bajos a considerar, depende de la granularidad 
    reg_max : número de precios más altos , depende de la granularidad  
    precios: DF de precios al que añade una columna, se filtran las fechas para las que no existe curtailment.
    spread_type : Si es un spread a 1H, 2H 3H o 4H 



    Returns:
    df: Data Frame en el que para cada dia, se calcula el 
    - promedio de precio entre los máximos del día -en funcion de la granularidad 
    - el promedio de precio entre los minimos  del día -en funcion de la granularidad,
    - el spread como la diferencia entre ambas y se cuenta cuantos registros de precios se han tenido en consideracion para ese promedio (en 5 minutales 12, en 15 minutales 4, en horarios 1...) .
    """
    cols_group = cols_fecha_spread
    print('Columnas de la agrupacion para cada spread', cols_group)
    cols_spread = ['SPREAD_GRAN', 'DATA_YEAR', 'DATA_MONTH', 'DATA_DAY', 'SPREAD_PRICE', 'PRICE_MIN', 'COUNT_MIN',
                   'TOP_MIN', 'PRICE_MAX', 'COUNT_MAX', 'TOP_MAX']
    df_max_1h = precios.filter(col("row_number") > reg_max)
    df_min_1h = precios.filter(col("row_number") <= reg_min)
    max_agg_1h = df_max_1h.group_by(cols_group).agg(
        avg("price").alias("PRICE"),
        count("price").alias("COUNT"),
        max("price").alias("TOP")
    )
    max_agg_1h = max_agg_1h.withColumn("SPREAD_GRAN", lit(spread_type))
    min_agg_1h = df_min_1h.group_by(cols_group).agg(
        avg("price").alias("PRICE"),
        count("price").alias("COUNT"),
        min("price").alias("TOP"),

    )
    min_agg_1h = min_agg_1h.withColumn("SPREAD_GRAN", lit(spread_type))
    spread_1H = min_agg_1h.join(max_agg_1h, max_agg_1h.col("DATA_DAY") == min_agg_1h.col("DATA_DAY"), lsuffix="_min",
                                rsuffix="_max")
    spread_1H = spread_1H.with_column('SPREAD_PRICE', (spread_1H["PRICE_MAX"] - spread_1H["PRICE_MIN"]))
    spread_1H = spread_1H.rename(col("SPREAD_GRAN_MIN"), "SPREAD_GRAN")
    spread_1H = spread_1H.rename(col("DATA_DAY_MIN"), "DATA_DAY")
    spread_1H = spread_1H.rename(col("DATA_YEAR_MIN"), "DATA_YEAR")
    spread_1H = spread_1H.rename(col("DATA_MONTH_MIN"), "DATA_MONTH")

    return spread_1H[cols_spread]
    

def calculate_new_series_spread(captured_prices):
    cols_raw_table=['EXPIRATION_DATE','RECEIPT_DATE','LOCAL_DATA_DATE','IDCONSULTANT','IDCURRENCY','IDDART',
                    'IDGEOGRAPHIC','IDGRANULARITY','IDORIGINATION','IDPRODUCT','IDPRODUCTTEMP','IDRECURRENTUPDATE','IDSCENARIO',
                    'IDSHAPE','IDTERM','IDTYPENODEORBUS','IDUNIT','IDURL_FEED','PRICE','IDSERIE','INGESTION_DATE','IDVERSION','IDNORMALIZED_SCENARIO','PSINPUT' ]
                    
    
  
    #CAlcular Anual 
    cols_group=['DATA_YEAR','PSINPUT', 'SPREAD_GRAN']+cols_serie_precios +cols_new_serie
 
      
    df_year=captured_prices.group_by(cols_group).agg( avg("SPREAD_PRICE").alias("PRICE") )
    

     
     #Construir local_data_Date
    df_year = df_year.withColumn("local_data_date",concat_ws(lit('-') ,col("DATA_YEAR"),lit("01"),lit("01")).cast("date")) 
    #Asignar granularity 
    df_year=df_year.drop('IDGRANULARITY')
    df_year= df_year.with_column('IDGRANULARITY', lit(Granularity_YEARLY)) 
   
    
    #CAlcular Mensual  
    cols_group=['DATA_YEAR','DATA_MONTH','PSINPUT', 'SPREAD_GRAN']+cols_serie_precios +cols_new_serie
    df_month=captured_prices.group_by(cols_group).agg(avg("SPREAD_PRICE").alias("PRICE")  )
    

     #Construir local_data_Date
    df_month = df_month.withColumn("local_data_date",concat_ws(lit('-') ,col("DATA_YEAR"),col("DATA_MONTH"),lit("01")).cast("date")) 
    #Asignar granularity 
    df_month=df_month.drop('IDGRANULARITY')
    df_month= df_month.with_column('IDGRANULARITY', lit(Granularity_MONTHLY)) 
    print('-------------------------------------df_month----------------------')
    df_month.show()
    c1= cols_raw_table+['SPREAD_GRAN']
    c1.remove('IDPRODUCT') 
    df_month= df_month[c1]
    df_month.show()
    df_year= df_year[c1]
    df_year.show()
    df= df_month.union(df_year)

    df=df.drop('IDPRODUCT')
       #Asignar product 
    
    df= df.with_column('IDPRODUCT', when(col("SPREAD_GRAN") == '1H', lit(Price_spread_1H)).when(col("SPREAD_GRAN")  == '2H', lit(Price_spread_2H)).when(col("SPREAD_GRAN")  == '3H', lit(Price_spread_3H)).otherwise(lit(Price_spread_4H)))
    df.show()                                                                                                                                                                                                           
    
 
  
    

    return df[cols_raw_table]
 
 