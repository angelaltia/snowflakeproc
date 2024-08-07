# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark  

from snowflake.snowpark.functions import col, lit, when, array_unique_agg, sum as sum_  , concat , concat_ws, trim, upper
from datetime import datetime

 
QUERY_SERIE_TO_BE_PROCESSED="select  level, dart, product_subtype, consultant_name, version, country, market, hub, node, term, term_ref_year, normalized_scenario,GRANULARITY, f.EXPIRATION_DATE,f.RECEIPT_DATE,f.IDCONSULTANT,f.IDCURRENCY,f.IDDART, f.IDGEOGRAPHIC,f.IDGRANULARITY,f.IDORIGINATION,f.IDPRODUCT,f.IDPRODUCTTEMP,f.IDRECURRENTUPDATE,f.IDSCENARIO,f.IDSHAPE,f.IDTERM,f.IDTYPENODEORBUS,f.IDUNIT,f.IDURL_FEED,f.IDSERIE,f.INGESTION_DATE,f.IDVERSION,f.IDNORMALIZED_SCENARIO, count(*)  from fact_price f, dim_version v, dim_consultant c,  dim_geographic geo , dim_dart da , dim_granularity g, dim_term t, DIM_NORMALIZED_SCENARIO ns, DIM_PRODUCT pro where PRODUCT_TYPE ='Wholesale price'  and f.idversion=v.idversion and f.idconsultant=c.idconsultant and f.idgeographic= geo.idgeographic  and f.iddart=da.iddart and f.idgranularity =g.idgranularity  and f.idterm =t.idterm  and f.idnormalized_scenario= ns.idnormalized_scenario and f.idproduct=pro.idproduct group by f.EXPIRATION_DATE,f.RECEIPT_DATE,f.IDCONSULTANT,f.IDCURRENCY,f.IDDART, f.IDGEOGRAPHIC,f.IDGRANULARITY,f.IDORIGINATION, f.IDPRODUCT,f.IDPRODUCTTEMP,f.IDRECURRENTUPDATE, f.IDSCENARIO,f.IDSHAPE,f.IDTERM,f.IDTYPENODEORBUS,f.IDUNIT,f.IDURL_FEED,f.IDSERIE,f.INGESTION_DATE,f.IDVERSION,f.IDNORMALIZED_SCENARIO, consultant_name, version, country, market, hub,term, term_ref_year, product_subtype,  normalized_scenario, GRANULARITY, dart, node, level"
QUERY_PRICES='select year(LOCAL_DATA_DATE) data_year,  month(LOCAL_DATA_DATE) data_month,day(LOCAL_DATA_DATE) data_day,hour(LOCAL_DATA_DATE) data_hour, f.EXPIRATION_DATE,f.RECEIPT_DATE,f.IDCONSULTANT,f.IDCURRENCY,f.IDDART, f.IDGEOGRAPHIC,f.IDGRANULARITY,f.IDORIGINATION,f.IDPRODUCT,f.IDPRODUCTTEMP,f.IDRECURRENTUPDATE,f.IDSCENARIO,f.IDSHAPE,f.IDTERM,f.IDTYPENODEORBUS,f.IDUNIT,f.IDURL_FEED,f.IDSERIE,f.INGESTION_DATE,f.IDVERSION,f.IDNORMALIZED_SCENARIO  , f.local_data_date, f.price from fact_price f'
QUERY_SOLAR_VALUES= "select month(LOCAL_DATE) solar_data_month,day(LOCAL_DATE) solar_data_day,hour(LOCAL_DATE) solar_data_hour,	GENERATION ,	MARKET,	HUB,	node as solar_node  from DEV_XELIO.RAW.SOLAR_PROFILE_GRAL"
QUERY_SERIE_GREEN="select product_subtype, consultant_name, version, country, market, hub,term, term_ref_year, normalized_scenario,GRANULARITY,f.IDGRANULARITY as IDGRANULARITY_Green  , year(local_data_date) green_data_year,month(local_data_date)green_data_month,day(local_data_date)green_data_day, price as CURTAILMENT_TRIGGER  from fact_price f, dim_version v, dim_consultant c,  dim_geographic geo ,  dim_granularity g, dim_term t, DIM_NORMALIZED_SCENARIO ns, DIM_PRODUCT pro where f.idversion=v.idversion and f.idconsultant=c.idconsultant and f.idgeographic= geo.idgeographic  and f.idgranularity =g.idgranularity  and f.idterm =t.idterm  and f.idnormalized_scenario= ns.idnormalized_scenario and f.idproduct=pro.idproduct"


Granularity_05_MIN = 9
Granularity_15_MIN = 1
Granularity_30_MIN = 10
Granularity_HOURLY = 8
Granularity_DAILY = 3 
Granularity_MONTHLY = 7
Granularity_YEARLY = 6

Price_pre_curtailment=21
Price_post_curtailment_0= 26
Price_post_curtailment_at_minusGreen=42

Curtailment_perct_0=301
Curtailment_perct_green=302  

origination_calculated=101


cols_serie_precios=['EXPIRATION_DATE', 'RECEIPT_DATE', 'IDCONSULTANT', 'IDCURRENCY', 
              'IDDART', 'IDGEOGRAPHIC', 'IDGRANULARITY', 'IDORIGINATION', 
              'IDPRODUCT', 'IDPRODUCTTEMP', 'IDRECURRENTUPDATE', 'IDSCENARIO', 
              'IDSHAPE', 'IDTERM', 'IDTYPENODEORBUS', 'IDUNIT', 'IDURL_FEED', 'IDSERIE', 'INGESTION_DATE', 'IDVERSION', 'IDNORMALIZED_SCENARIO']
cols_precio=['LOCAL_DATA_DATE', 'PRICE']
 
cols_serie_captured= ['DATA_YEAR', 'DATA_MONTH', 'DATA_DAY', 'DATA_HOUR', 'CURTAILMENT_TRIGGER', 'GENERATION_GRAN', 'REVENUE', 'GEN_CURTAILED_0', 'GEN_CURTAILED_GREEN',
                      'REV_CURTAILED_0', 'REV_CURTAILED_GREEN']
cols_new_serie=['MODE', 'SOLAR_GEOGRAPHIC', 'selected_price_serie', 'selected_version_green', 'ingestion_date']
def main(session: snowpark.Session , selected_price_serie,selected_version_green,v_geographic,v_curt_fixed_trigger, v_curtailment_value, v_mode): 
    #session.call('DEV_XELIO.DWH.CALCULATE_SOLAR_CAPTURED_PRICES',
    #SELECTED_PRICE_SERIE,SELECTED_VERSION_GREEN ,V_GEOGRAPHIC ,V_CURT_FIXED_TRIGGER,V_CURTAILMENT_VALUE, V_MODE) 
    ###prueba 1 : granularidad 5 min 
    #v_curt_fixed_trigger="N"
    #v_geographic="CAISO-NP15--"
    #selected_version_green="REC:Aurora>2023-Q4>Central>USA-CAISO-->Granularity YEARLY>Real-2022"
    #selected_price_serie='Aurora>2023-Q4>Central>USA-CAISO-ZP26->RT-RTD>Granularity 05 MIN>Real-2022'
    #v_curtailment_value=0
    # v_mode='general'
    ###prueba 2 : granularidad HORARIA 
    #v_curt_fixed_trigger="N"
    #v_geographic="CAISO-NP15--"
    #selected_version_green="REC:Aurora>2023-Q4>Central>USA-CAISO-->Granularity YEARLY>Real-2022"
    #selected_price_serie='Aurora>2023-Q4>Central>USA-CAISO-ZP26->Day Ahead>Granularity HOURLY>Real-2022'
    #v_curtailment_value=0
    #v_mode='general'
    ###prueba 3 : NODAL
    #v_curt_fixed_trigger="N"
    #v_geographic="ERCOT-ERCOT HOUSTON--"
    #selected_version_green="REC:Aurora>2023-Q4>Central>USA-ERCOT-->Granularity YEARLY>Real-2022"
    #selected_price_serie='Aurora>2023-Q3>Central>USA-ERCOT-ERCOT HOUSTON->Real Time>Granularity 15 MIN>Real-2022'
    #v_curtailment_value=0
    #v_mode='general'
    ###prueba 4 : SOLAR PROFILE 
    #v_curt_fixed_trigger="N"
    #v_geographic="CAISO-ZP26-"
    #selected_version_green="REC:Aurora>2023-Q4>Central>USA-CAISO-->Granularity YEARLY>Real-2022"
    #selected_price_serie='Aurora>2023-Q3>Central>USA-CAISO-ZP26->Day Ahead>Granularity HOURLY>Real-2022'
    #v_curtailment_value=0
    #v_mode='general'
    ###prueba 5 : SOLAR PROFILE CORNWALL PROBLEMA DE SERIES CON un mes con un unico precio y sin generacion 
    
    #v_curt_fixed_trigger="N"
    #v_geographic="NEM-NEM SA--"
    #selected_version_green="LGC:Aurora>2024-Q1>Central>AUSTRALIA-NEM-->Granularity YEARLY>Real-2022"
    #selected_price_serie='CORNWALL>2023-Q4>Central>AUSTRALIA-NEM-NEM SA->Day Ahead>Granularity 05 MIN>Real-2023'
    #v_curtailment_value=0
    #v_mode='general'
    ###prueba 6 : SOLAR PROFILE subida a pro 
    
    #v_curt_fixed_trigger="N"
    #v_geographic="NEM-NEM QLD--"
    #selected_version_green="LGC:Aurora>2023-Q4>Central>AUSTRALIA-NEM-->Granularity YEARLY>Real-2022"
    #selected_price_serie='CORNWALL>2023-Q4>Central>AUSTRALIA-NEM-NEM QLD->Day Ahead>Granularity 30 MIN>Real-2023'
    #v_curtailment_value=0
    #v_mode='general'
    
    #v_curt_fixed_trigger="N"
    #v_geographic="ERCOT-ERCOT HOUSTON--"
    #selected_version_green="REC:X-ELIO>2022-Q2>Central>USA-ERCOT-->Granularity YEARLY>Nominal"
    #selected_price_serie= 'Aurora>2024-Q2>Central>USA-ERCOT-ERCOT HOUSTON->Real Time>Granularity 15 MIN>Real-2023'
    #selected_price_serie='Aurora>2024-Q2>Central>USA-ERCOT-ERCOT HOUSTON-AT_TR3>Real Time>Granularity 15 MIN>Real-2023'
    #v_curtailment_value=0
    #v_mode='general'
 
    series_precios= get_series (session, selected_price_serie)
    # Print a sample of the dataframe to standard output.
    series_precios.show()
    
    for price_row in series_precios.to_local_iterator():
        
        price_data= get_prices(session, price_row)
              
        v_granularity= price_row["IDGRANULARITY"]
        
        captured_prices= calculate_captured_serie(session, price_data,  v_geographic, v_granularity,  v_curt_fixed_trigger,v_curtailment_value, selected_version_green, selected_price_serie, v_mode ) 
        cols_serie_precios.remove('INGESTION_DATE') 
        columnas_persist_captured = cols_serie_precios+cols_precio+ cols_serie_captured+cols_new_serie
        captured_prices= captured_prices[columnas_persist_captured]  
        
        captured_prices.write.save_as_table("DEV_XELIO.DWH.solar_captured_prices_ID", mode="append", table_type="")
        new_series= calculate_new_series_solar(captured_prices)
        new_series.write.save_as_table("DEV_XELIO.RAW.TRANSFORMATION_FACT_PRICE_ID", mode="append", table_type="")
        out_put=session.call("DEV_XELIO.raw.INSERT_TRANSFORMATION_FACT_PRICE_ID")
 
        return new_series
        
        
        
    return new_series

 

def get_series (session,  v_captured_data_serie):
    #Aurora>2023-Q4>Central>USA-CAISO-ZP26->Granularity 05 MIN>Real-2022
    #Aurora>2023-Q4>Central>USA-CAISO-ZP26->RT-RTD>Granularity 05 MIN>Real-2022

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
        (col('IDNORMALIZED_SCENARIO') == price_row['IDNORMALIZED_SCENARIO'] )
    )
    
    
    
    return results 

def calculate_captured_serie(session, price_data, v_geographic, v_granularity,  v_curt_fixed_trigger,v_curtailment_value, selected_version_green, selected_price_serie,v_mode ):
    df_solar_profile=get_solar_general_profile (session, v_geographic)
    price_data=get_curtailment_trigger(session, v_curt_fixed_trigger, v_curtailment_value, selected_version_green, price_data)
    ##print('En calculate')
    ##print(price_data.columns)
   
    print('first', price_data.count())
  
    df= price_data.join(df_solar_profile, (
        (price_data.data_month == df_solar_profile.solar_data_month)&
        (price_data.data_day == df_solar_profile.solar_data_day)&
        (price_data.data_hour == df_solar_profile.solar_data_hour)  ),join_type="left" )
    print('joined', df.count())
   
    if v_granularity == Granularity_05_MIN:
        df = df.with_column("GENERATION_GRAN", (df["GENERATION"] / 12))
    elif v_granularity == Granularity_15_MIN:
        df = df.with_column("GENERATION_GRAN", (df["GENERATION"] / 4))
    elif v_granularity == Granularity_30_MIN:
        df = df.with_column("GENERATION_GRAN", (df["GENERATION"] / 2))
    elif v_granularity == Granularity_HOURLY:
        df = df.with_column("GENERATION_GRAN", (df["GENERATION"] ))
        
    df = df.with_column("REVENUE", (df["GENERATION_GRAN"] * df["PRICE"]))
    
    df= df.with_column("GEN_CURTAILED_0",when(df["PRICE"]> 0 , df['GENERATION_GRAN']).otherwise(0) )
    df= df.with_column("GEN_CURTAILED_GREEN",when((df["PRICE"] ) >= -df["CURTAILMENT_TRIGGER"],df['GENERATION_GRAN']).otherwise(0) )
    df= df.with_column("REV_CURTAILED_0",when(df["PRICE"]>= 0 , df['REVENUE']).otherwise(0) )
    df= df.with_column("REV_CURTAILED_GREEN",when((df["PRICE"] ) >= -df["CURTAILMENT_TRIGGER"],df['REVENUE']).otherwise(0) )
    
    df = df.with_column("MODE", lit(v_mode))
    df = df.with_column("SOLAR_GEOGRAPHIC", lit(v_geographic))
    if selected_version_green is None or selected_version_green==''or selected_version_green=='-'or selected_version_green=='--':
        selected_version_green='Curtailment Fixed at:'  +str(v_curtailment_value)
  
        
    df = df.with_column("selected_price_serie", lit(selected_price_serie))      
    df = df.with_column("selected_version_green", lit(selected_version_green))
    #Quitamos el ingestion_Date de la serie de precios y añadimos el nuevo ingestion_date 
    df= df.drop("ingestion_date")
   
    df = df.with_column("ingestion_date", lit(datetime.today().strftime('%Y-%m-%d %H:%M:%S')))
    print('saliendo', df.count())
    
    return df
    
    
def  get_solar_general_profile(session,  v_geographic ):
    v_geo= v_geographic.split('-')
    df_profile=session.sql(QUERY_SOLAR_VALUES)
    
    print(v_geo)
    print('market', v_geo[0])
    print('hub', v_geo[1])
    df_profile = df_profile.filter( 
        (col("market")  == v_geo[0]) &
        (col("hub") ==v_geo[1] ) 
    )
    if ((v_geo[2] !=' ')&(v_geo[2]!='')&(v_geo[2]!='-') & (v_geo[2] is not None)):
        v_node=v_geo[2]
        df_profile = df_profile.filter( (col("solar_node")  ==v_node) )
        print('filtro solar_node', v_node)
    else:
        print('NODE IS NULL')
        df_profile= df_profile.filter(col("solar_node").isNull())
        
    columna_solar=['solar_data_month','solar_data_day','solar_data_hour','GENERATION']
    print('df_profile.columns',df_profile.columns)
    print('df_profile.count()',df_profile.count())
    df_profile.show()
     
    return  df_profile[columna_solar ] 


def get_green_certificate_values(session,  selected_version_green):
    """
    Esta función devuelve get_green_certificate_values la lista de valores del green certificate selecionado por el usuario.

    Args:
    selected_version_green (char): valor seleccionado por el usurio 
    
    Returns:
    df: Valor del Green Certificate /LGC/Gdo o REC por fecha.
        
    """
    ##print("------------------------- get_green_certificate_values ------------------------")
    filtros_g= selected_version_green.split(':')
    ##print(filtros_g)
    product_subtype= filtros_g[0]
    filtros_serie= filtros_g[1]
    
    
    filtros= filtros_serie.split('>')
    geo= filtros[3]
    geo_coor= geo.split('-')
    term_=filtros[5] 
    term=term_.split('-')
    
    #['Aurora', '2023-Q4', 'Central', 'USA-CAISO-ZP26-', 'Granularity 05 MIN', 'Real-2022']
    # Consultant_name, version, normalized_scenario, geo, granularity, Term 
    #and  version='2023-Q4'and consultant_name = 'Aurora'and geo.country='USA' and geo.market='CAISO' and geo.hub='ZP26' and geo.node=''and  
    #dart='RT-RTD'and granularity='Granularity 05 MIN'and  term='Real' and 

    
    df = session.sql(QUERY_SERIE_GREEN)
  
    df = df.filter((col("product_subtype") == product_subtype) )
    df = df.filter((col("consultant_name") == filtros[0] ) )
   
    
    df = df.filter((col("version") == filtros[1] ) )
  
    
    df = df.filter((col("normalized_scenario") == filtros[2] ) )
    df = df.filter(col("country") ==geo_coor[0])
    df = df.filter(col("market") ==geo_coor[1])
    df = df.filter(col("hub") ==geo_coor[2])

    df = df.filter(col("Granularity") == filtros[4])

    if len(term)==1:
        df = df.filter(col("term") ==term[0])
    else:
        df = df.filter(col("term") ==term[0])
        df = df.filter(col("term_ref_year") ==term[1])

    return df 

     

     
    
 

     
def get_curtailment_trigger(session, v_curt_fixed_trigger, v_curtailment_value, selected_version_green, price_data ):
    """
    Esta función crea una nueva columna en el DF de precios con el valor del trigger del curtailment, 
    en funcion de lo que haya seleccionaddo el usuario un valor fijo o consulta el valor del REC desde la BBDD

    Args:
    v_curt_fixed_trigger (Y/N): indica el valor del trigger -Y Fijo (usa v_curtailment_value) , N- Serie Green 
    selected_version_green : Certificaod seleccionado por el usuario
    V_prices: DF de precios al que añade una columna, se filtran las fechas para las que no existe curtailment.
    
    
    
    Returns:
    df: Data Frame qactualizado con la columna CURTAILMENT_TRIGGER con el Green Certificate /LGC/Gdo o REC por fecha.
    """
    
    cols_serie_precios = price_data.columns 
   

    cols_serie_precios=cols_serie_precios+["CURTAILMENT_TRIGGER"]
    if v_curt_fixed_trigger=='Y':
        v_curtailment_value=v_curtailment_value
        price_data= price_data.with_column("CURTAILMENT_TRIGGER", lit(v_curtailment_value))
    else:
        df_green_certificate= get_green_certificate_values(session,  selected_version_green)

        V_gran=df_green_certificate.select(array_unique_agg("IDGRANULARITY_GREEN")).collect()
        gran= V_gran[0][0].split()
        if (len(gran) >3):
            raise Exception ('Más de un REC')
        v_gran= int(gran[1])
        
   
        
        if (v_gran==Granularity_DAILY):

             price_data= price_data.join(df_green_certificate, (
                (price_data.data_month == df_green_certificate.green_data_month)&
                (price_data.data_year == df_green_certificate.green_data_year) &
                (price_data.data_day == df_green_certificate.green_data_day) )
                                        )
        elif (v_gran==Granularity_MONTHLY):

            price_data= price_data.join(df_green_certificate, (
                (price_data.data_month == df_green_certificate.green_data_month)&
                (price_data.data_year == df_green_certificate.green_data_year) ) )
        elif (v_gran==Granularity_YEARLY) :

            price_data= price_data.join( df_green_certificate, price_data.data_year == df_green_certificate.green_data_year  ) 
        else:
            raise Exception('No se encuentra REC')

    return price_data
    
def get_price_pre_curtailment(captured_prices_grouped):
    #PRICE_PRE_CURTAILMENT: REVENUES/ GENERATION_MONTH --> conceptualmente precio medio considerando el apagado solar por horas
    df =captured_prices_grouped.with_column("PRICE_PRE_CURTAILMENT",  captured_prices_grouped["REVENUE"] / captured_prices_grouped["GENERATION_GRAN"])
    df=df.rename(col("PRICE_PRE_CURTAILMENT"), "Price")  
     
    df=df.drop('IDPRODUCT')
    df=df.with_column('IDPRODUCT', lit(Price_pre_curtailment) )
    df=df.with_column('PRODUCT_SUBTYPE', lit('PRICE_PRE_CURTAILMENT'))
    return df
def get_price_post_curtailment_0(captured_prices_grouped):
    #PRICE_POST_CURTAILMENT_0 : REV_CURTAILED_0/ GEN_CURTAILED_0 --> precio medio considerando en los revenues y en la generacion el apagado a precio=0
 
    df = captured_prices_grouped.with_column(
        "PRICE_POST_CURTAILMENT_0",
        when(col("GEN_CURTAILED_0") == 0, 0).otherwise(col("REV_CURTAILED_0") / col("GEN_CURTAILED_0"))
    ) 
    df=df.rename(col("PRICE_POST_CURTAILMENT_0"), "Price")  
    
    df=df.drop('IDPRODUCT')
    df=df.with_column('IDPRODUCT', lit(Price_post_curtailment_0) )
    df=df.with_column('PRODUCT_SUBTYPE', lit('PRICE_POST_CURTAILMENT_0'))
    return df
def get_price_post_curtailment_green(captured_prices_grouped):
    #PRICE_POST_CURTAILMENT_AT_MINUSGREEN: : REVENUES_MINUS_GREEN/ GEN_CURTAILED_GREEN --> precio medio considerando en los revenues y en la generación   el apagado a precio  -Green 
    df =captured_prices_grouped.with_column("PRICE_POST_CURTAILMENT_AT_MINUSGREEN", captured_prices_grouped["REV_CURTAILED_GREEN"] / captured_prices_grouped["GEN_CURTAILED_GREEN"])
    df=df.rename(col("PRICE_POST_CURTAILMENT_AT_MINUSGREEN"), "Price")  
    df=df.drop('IDPRODUCT')
    df=df.with_column('IDPRODUCT', lit(Price_post_curtailment_at_minusGreen) )
    df=df.with_column('PRODUCT_SUBTYPE', lit('PRICE_POST_CURTAILMENT_AT_MINUSGREEN'))
    return df
def get_curtailment_perct_0(captured_prices_grouped):
    #CURTAILMENT_PERCT_0 : 1- GEN_CURTAILED_0/GENERATION_MONTH --> % de generacion apagado por curtailment a 0..
    df =captured_prices_grouped.with_column("CURTAILMENT_PERCT_0", (1-captured_prices_grouped["GEN_CURTAILED_0"] / captured_prices_grouped["GENERATION_GRAN"]))
    df=df.rename(col("CURTAILMENT_PERCT_0"), "Price")  
    df=df.drop('IDPRODUCT')
    df=df.with_column('IDPRODUCT', lit(Curtailment_perct_0))
    df=df.with_column('PRODUCT_SUBTYPE', lit('% curtailment at 0'))
    return df
def get_curtailment_perct_green(captured_prices_grouped):
     #CURTAILMENT_PERCT_GREEN : 1- GEN_CURTAILED_GREEN/GENERATION_MONTH --> % de generacion apagado por curtailment a 0..
    df =captured_prices_grouped.with_column("CURTAILMENT_PERCT_GREEN", (1-captured_prices_grouped["GEN_CURTAILED_GREEN"] / captured_prices_grouped["GENERATION_GRAN"]))
    df=df.rename(col("CURTAILMENT_PERCT_GREEN"), "Price") 
    df=df.drop('IDPRODUCT')
    df=df.with_column('IDPRODUCT', lit(Curtailment_perct_green))
    df=df.with_column('PRODUCT_SUBTYPE', lit('% curtailment at minus greeen'))
    return df
    
def set_common_cols_captured_grouped_series(v_granularity, df ):
    
    cols_raw_table=['EXPIRATION_DATE','RECEIPT_DATE','LOCAL_DATA_DATE','IDCONSULTANT','IDCURRENCY','IDDART',
                    'IDGEOGRAPHIC','IDGRANULARITY','IDORIGINATION','IDPRODUCT','IDPRODUCTTEMP','IDRECURRENTUPDATE','IDSCENARIO',
                    'IDSHAPE','IDTERM','IDTYPENODEORBUS','IDUNIT','IDURL_FEED','PRICE','IDSERIE','INGESTION_DATE','IDVERSION','IDNORMALIZED_SCENARIO','PSINPUT' ]
                    #, 'PRODUCT_SUBTYPE' #TODO!!!!! Borrar cuando esté ok 
                       
    df= df.with_column('IDORIGINATION', lit(origination_calculated)) 
    df = df.with_column("ingestion_date", lit(datetime.today().strftime('%Y-%m-%d %H:%M:%S'))) 
    df=df.drop('IDGRANULARITY')
    df= df.with_column('IDGRANULARITY', lit(v_granularity)) 
    
    if v_granularity== Granularity_YEARLY:
        df = df.withColumn("local_data_date",concat_ws(lit('-') ,col("DATA_YEAR"),lit("01"),lit("01")).cast("date")) 
        df=df.drop('DATA_YEAR')
        df=df[cols_raw_table]
        
    elif v_granularity== Granularity_MONTHLY:         
        df = df.withColumn("local_data_date",concat_ws(lit('-') ,col("DATA_YEAR"),col("DATA_MONTH"),lit("01")).cast("date"))         
        df=df[cols_raw_table]
         
    return df
    
def calculate_new_series_solar(captured_prices):
    captured_prices=captured_prices.with_column('PSINPUT', concat(captured_prices.selected_price_serie, lit('|'),captured_prices.SELECTED_VERSION_GREEN, lit('|mode:'),captured_prices.mode,lit(':'), captured_prices.SOLAR_GEOGRAPHIC  ) )
  
    
    cols_group=['DATA_YEAR','PSINPUT']+cols_serie_precios +cols_new_serie
    
      
    captured_prices_grouped_year=captured_prices.group_by(cols_group).agg(
        sum_("REVENUE").alias("REVENUE"),
        sum_("GENERATION_GRAN").alias("GENERATION_GRAN"),
        sum_("GEN_CURTAILED_0").alias("GEN_CURTAILED_0"),
        sum_("GEN_CURTAILED_GREEN").alias("GEN_CURTAILED_GREEN"),
        sum_('REV_CURTAILED_0').alias('REV_CURTAILED_0'),
        sum_('REV_CURTAILED_GREEN').alias('REV_CURTAILED_GREEN'))
    
  
    

    captured_prices_grouped_year = captured_prices_grouped_year.filter((col("GENERATION_GRAN") > 0 ) )
    
   
    price_pre_curtailment_year=get_price_pre_curtailment(captured_prices_grouped_year )
    price_post_curtailment_0_year=get_price_post_curtailment_0(captured_prices_grouped_year )
    price_post_curtailment_minus_year=get_price_post_curtailment_green(captured_prices_grouped_year )
    curtailment_perct_0_year=get_curtailment_perct_0(captured_prices_grouped_year )
    curtailment_perct_green_year=get_curtailment_perct_green(captured_prices_grouped_year )
    
    df=price_pre_curtailment_year.union(price_post_curtailment_0_year)
    df=df.union(price_post_curtailment_minus_year)
    df=df.union(curtailment_perct_0_year)
    df=df.union(curtailment_perct_green_year)
    df_year=set_common_cols_captured_grouped_series(Granularity_YEARLY, df )
   
    #print('MENSUALES:::::::::::::::::::')     
    
    cols_group=['DATA_YEAR','DATA_MONTH','PSINPUT']+cols_serie_precios +cols_new_serie
    
    
    captured_prices_grouped_month=captured_prices.group_by(cols_group).agg(
        sum_("REVENUE").alias("REVENUE"),
        sum_("GENERATION_GRAN").alias("GENERATION_GRAN"),
        sum_("GEN_CURTAILED_0").alias("GEN_CURTAILED_0"),
        sum_("GEN_CURTAILED_GREEN").alias("GEN_CURTAILED_GREEN"),
        sum_('REV_CURTAILED_0').alias('REV_CURTAILED_0'),
        sum_('REV_CURTAILED_GREEN').alias('REV_CURTAILED_GREEN'))
    captured_prices_grouped_month = captured_prices_grouped_month.filter((col("GENERATION_GRAN") > 0 ) )
      
    price_pre_curtailment_month=get_price_pre_curtailment(captured_prices_grouped_month )
    price_post_curtailment_0_month=get_price_post_curtailment_0(captured_prices_grouped_month )
    price_post_curtailment_minus_month=get_price_post_curtailment_green(captured_prices_grouped_month )
    curtailment_perct_0_month=get_curtailment_perct_0(captured_prices_grouped_month )
    curtailment_perct_green_month=get_curtailment_perct_green(captured_prices_grouped_month )
  
    print('MENSUALES-count :::::::::::::::::::',price_pre_curtailment_month.count(),price_post_curtailment_0_month.count(),price_post_curtailment_minus_month.count(),curtailment_perct_0_month.count(),curtailment_perct_green_month.count())         
    p1=set_common_cols_captured_grouped_series(Granularity_MONTHLY, price_pre_curtailment_month )
    p2=set_common_cols_captured_grouped_series(Granularity_MONTHLY, price_post_curtailment_0_month )
    p3=set_common_cols_captured_grouped_series(Granularity_MONTHLY, price_post_curtailment_minus_month )
    p4=set_common_cols_captured_grouped_series(Granularity_MONTHLY, curtailment_perct_0_month )
    p5=set_common_cols_captured_grouped_series(Granularity_MONTHLY, curtailment_perct_green_month )
    df_month=p1.union(p2)
    df_month=df_month.union(p3)
    df_month=df_month.union(p4)
    df_month=df_month.union(p5) 
    
    
    df= df_month.union(df_year)
  
    print('all-count :::::::::::::::::::',df.count())       
   
     
    return df