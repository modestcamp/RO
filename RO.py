#                                                   xX_IsaI_Xx
#%% Extract
import pandas as pd
Dias=str(input("Rango de fechas (mmdd mmdd): "))
Clave=pd.read_csv("/Users/isaibb/Desktop/Clean/RO/Clave.csv")
Cupon=pd.read_csv("/Users/isaibb/Desktop/Clean/RO/Cupon.csv")
Cuarto=pd.read_csv("/Users/isaibb/Desktop/Clean/RO/Cuarto.csv")
Producto=pd.read_csv("/Users/isaibb/Desktop/Clean/RO/Producto.csv")
America=pd.read_csv("/Users/isaibb/Desktop/Clean/RO/America.csv")
Global=pd.read_csv("/Users/isaibb/Desktop/Clean/RO/Global.csv")
EMEA=pd.read_csv("/Users/isaibb/Desktop/Clean/RO/EMEA.csv")
#%% Clean
RO=America.append(Global, ignore_index=True)
del Global
del America
RO=RO.append(EMEA, ignore_index=True)
del EMEA
RO=RO[RO.Transaction_ID.notnull()]
RO=RO[["Timestamp", "Campaign", "CampaignType", "Media", "Placement", 
       "Banner/Adgroup", "Ad Interaction", "Paid Keywords", "Referrer Type",
       "Page", "City", "Country", "Order ID", "Sales", "Country.1", "Currency",
       "Number_of_rooms", "Guests", "Transaction_ID", "Prop_ID", "Page_Site",
       "Checkin_date", "Checkout_data", "Room_name", "Promotion", 
       "is_booking_center", "Tag"]]
#%%Joins
GO=pd.merge(Producto, Clave[["ID de transacción", "Palabra clave"]],
            on='ID de transacción', how='inner')
del Clave
del Producto
GO=GO.merge(Cuarto[["ID de transacción",
                    "Categoría de producto (comercio electrónico mejorado)"]],
                    on='ID de transacción', how='inner')
del Cuarto
GO=GO.merge(Cupon[["ID de transacción", "Código de cupón del producto"]],
                   on='ID de transacción', how='inner')
del Cupon
GO=GO.rename(columns={'ID de transacción': 'Transaction_ID',
                      'País': 'Country.1', 'CheckIn': 'Checkin_date',
                      'CheckOut': 'Checkout_data', 'Producto': 'Prop_ID',
                      'Fecha, hora y minuto': 'Timestamp', 'Cantidad': 'Number_of_rooms',
                      'Ingresos del producto': 'Sales',
                      'Categoría de producto (comercio electrónico mejorado)': 'Room_name'})
GO2=pd.merge(RO, GO, left_on='Transaction_ID',
            right_on='Transaction_ID', how='outer', indicator=True)
GO2=GO2.loc[GO2['_merge']=='right_only']
GO2=GO2[["Transaction_ID", "Country.1_y", "Checkin_date_y", "Checkout_data_y", "Fuente/Medio",
         "Timestamp_y", "Región", "Categoría de dispositivo", "Prop_ID_y", "Campaña",
         "Sales_y", "Number_of_rooms_y", "Palabra clave", "Room_name_y",
         "Código de cupón del producto"]]
GO2=GO2.rename(columns={"Country.1_y": "Country.1", "Checkin_date_y": "Checkin_date",
                        "Checkout_data_y": "Checkout_data", "Timestamp_y": "Timestamp",
                        "Prop_ID_y": "Prop_ID", "Sales_y": "Sales",
                        "Number_of_rooms_y":"Number_of_rooms", "Room_name_y": "Room_name",
                        "Fuente/Medio": "Media"})
GO2["Order ID"]=GO2["Transaction_ID"]
RO=RO.append(GO2[["Transaction_ID", "Country.1", "Checkin_date", "Checkout_data",
              "Timestamp", "Prop_ID", "Sales", "Number_of_rooms","Room_name",
              "Order ID", "Media"]], ignore_index=True)
RO=RO.merge(GO[["Transaction_ID", "Categoría de dispositivo", "Región", "Fuente/Medio",
                "Campaña", "Palabra clave", "Código de cupón del producto"]],
                on='Transaction_ID', how='outer')
#%% Limpieza
RO=RO[RO.Sales.notnull()]
RO=RO.drop_duplicates(subset=["Transaction_ID"])
RO.loc[RO['is_booking_center']==False, 'is_booking_center']="-"
RO.loc[RO['is_booking_center']==True, 'is_booking_center']="CallCenter"
RO.loc[RO['is_booking_center'].isnull(), 'is_booking_center']="GA"
RO.loc[RO['Guests'].isnull(), 'Guests']=RO["Number_of_rooms"]*2
RO.loc[RO['Page'].isnull(), 'Page']="A13:ROOM ONLY - Reservas"
RO.loc[RO['Currency'].isnull(), 'Currency']="USD"
RO.loc[RO['Page_Site'].isnull(), 'Page_Site']="Thank you page"
RO.loc[RO['Tag'].isnull(), 'Tag']="GTM-TEC"
RO.loc[RO['Prop_ID']=="Excellence Riviera Cancún", 'Prop_ID']="Excellence Riviera Cancun"
RO.loc[RO['Country.1']=="None", 'Country.1']=RO["Country"]
RO.loc[RO["Categoría de dispositivo"].isnull(), "Categoría de dispositivo"]="Desktop"
RO.loc[RO["Categoría de dispositivo"]=="desktop", "Categoría de dispositivo"]="Desktop"
RO.loc[RO["Categoría de dispositivo"]=="tablet", "Categoría de dispositivo"]="Tablet"
RO.loc[RO["Categoría de dispositivo"]=="mobile", "Categoría de dispositivo"]="Mobile"
#Paises
RO.loc[RO["Country.1"]=="United States", "Country.1"]="US"
RO.loc[RO["Country.1"]=="Canada", "Country.1"]="CA"
RO.loc[RO["Country.1"]=="Mexico", "Country.1"]="MX"
RO.loc[RO["Country.1"]=="Colombia", "Country.1"]="CO"
RO.loc[RO["Country.1"]=="Australia", "Country.1"]="AU"
RO.loc[RO["Country.1"]=="Belgium", "Country.1"]="BE"
RO.loc[RO["Country.1"]=="Bermuda", "Country.1"]="BM"
RO.loc[RO["Country.1"]=="Brazil", "Country.1"]="BR"
RO.loc[RO["Country.1"]=="Chile", "Country.1"]="CL"
RO.loc[RO["Country.1"]=="Cuba", "Country.1"]="CU"
RO.loc[RO["Country.1"]=="Denmark", "Country.1"]="DK"
RO.loc[RO["Country.1"]=="Dominican Republic", "Country.1"]="DO"
RO.loc[RO["Country.1"]=="Ecuador", "Country.1"]="EC"
RO.loc[RO["Country.1"]=="France", "Country.1"]="FR"
RO.loc[RO["Country.1"]=="El Salvador", "Country.1"]="SV"
RO.loc[RO["Country.1"]=="Germany", "Country.1"]="DE"
RO.loc[RO["Country.1"]=="Guatemala", "Country.1"]="GT"
RO.loc[RO["Country.1"]=="Hungary", "Country.1"]="HU"
RO.loc[RO["Country.1"]=="India", "Country.1"]="IN"
RO.loc[RO["Country.1"]=="Ireland", "Country.1"]="IE"
RO.loc[RO["Country.1"]=="Italy", "Country.1"]="IT"
RO.loc[RO["Country.1"]=="Jamaica", "Country.1"]="JM"
RO.loc[RO["Country.1"]=="Japan", "Country.1"]="JP"
RO.loc[RO["Country.1"]=="Liechtenstein", "Country.1"]="LI"
RO.loc[RO["Country.1"]=="Netherlands", "Country.1"]="NL"
RO.loc[RO["Country.1"]=="New Zealand", "Country.1"]="NZ"
RO.loc[RO["Country.1"]=="Norway", "Country.1"]="NO"
RO.loc[RO["Country.1"]=="Panama", "Country.1"]="PA"
RO.loc[RO["Country.1"]=="Paraguay", "Country.1"]="PY"
RO.loc[RO["Country.1"]=="Puerto Rico", "Country.1"]="PR"
RO.loc[RO["Country.1"]=="Russia", "Country.1"]="RU"
RO.loc[RO["Country.1"]=="South Korea", "Country.1"]="KR"
RO.loc[RO["Country.1"]=="Spain", "Country.1"]="ES"
RO.loc[RO["Country.1"]=="Switzerland", "Country.1"]="CH"
RO.loc[RO["Country.1"]=="Trinidad & Tobago", "Country.1"]="TT"
RO.loc[RO["Country.1"]=="United Kingdom", "Country.1"]="GB"
RO.loc[RO["Country.1"]=="Vietnam", "Country.1"]="VN"
RO.loc[RO["Country.1"]=="Argentina", "Country.1"]="AR"
RO.loc[RO["Country.1"]=="Bolivia", "Country.1"]="BO"
RO.loc[RO["Country.1"]=="Czechia", "Country.1"]="CZ"
RO.loc[RO["Country.1"]=="Venezuela", "Country.1"]="VE"
RO.loc[RO["Country.1"]=="Slovenia", "Country.1"]="SI"
RO.loc[RO["Country.1"]=="Turks & Caicos Islands", "Country.1"]="TC"
RO.loc[RO["Country.1"]=="Costa Rica", "Country.1"]="CR"
RO.loc[RO["Country.1"]=="Portugal", "Country.1"]="PT"
RO.loc[RO["Country.1"]=="Ukraine", "Country.1"]="UA"
RO.loc[RO["Country.1"]=="Cayman Islands", "Country.1"]="KY"
RO.loc[RO["Country.1"]=="Israel", "Country.1"]="IL"
#Ciudad
RO["City"]=RO["Región"]
RO.loc[RO["City"]=="Alabama", "City"]="AL"
RO.loc[RO["City"]=="Alaska", "City"]="AK"
RO.loc[RO["City"]=="American Samoa", "City"]="AS"
RO.loc[RO["City"]=="Arizona", "City"]="AZ"
RO.loc[RO["City"]=="Arkansas", "City"]="AR"
RO.loc[RO["City"]=="California", "City"]="CA"
RO.loc[RO["City"]=="Colorado", "City"]="CO"
RO.loc[RO["City"]=="Connecticut", "City"]="CT"
RO.loc[RO["City"]=="Delaware", "City"]="DE"
RO.loc[RO["City"]=="District of Columbia", "City"]="DC"
RO.loc[RO["City"]=="Florida", "City"]="FL"
RO.loc[RO["City"]=="Georgia", "City"]="GA"
RO.loc[RO["City"]=="Guam", "City"]="GU"
RO.loc[RO["City"]=="Hawaii", "City"]="HI"
RO.loc[RO["City"]=="Idaho", "City"]="ID"
RO.loc[RO["City"]=="Illinois", "City"]="IL"
RO.loc[RO["City"]=="Indiana", "City"]="IN"
RO.loc[RO["City"]=="Iowa", "City"]="IA"
RO.loc[RO["City"]=="Kansas", "City"]="KS"
RO.loc[RO["City"]=="Kentucky", "City"]="KY"
RO.loc[RO["City"]=="Louisiana", "City"]="LA"
RO.loc[RO["City"]=="Maine", "City"]="ME"
RO.loc[RO["City"]=="Maryland", "City"]="MD"
RO.loc[RO["City"]=="Massachusetts", "City"]="MA"
RO.loc[RO["City"]=="Michigan", "City"]="MI"
RO.loc[RO["City"]=="Minnesota", "City"]="MN"
RO.loc[RO["City"]=="Mississippi", "City"]="MS"
RO.loc[RO["City"]=="Missouri", "City"]="MO"
RO.loc[RO["City"]=="Montana", "City"]="MT"
RO.loc[RO["City"]=="Nebraska", "City"]="NE"
RO.loc[RO["City"]=="Nevada", "City"]="NV"
RO.loc[RO["City"]=="New Hampshire", "City"]="NH"
RO.loc[RO["City"]=="New Jersey", "City"]="NJ"
RO.loc[RO["City"]=="New Mexico", "City"]="NM"
RO.loc[RO["City"]=="New York", "City"]="NY"
RO.loc[RO["City"]=="North Carolina", "City"]="NC"
RO.loc[RO["City"]=="North Dakota", "City"]="ND"
RO.loc[RO["City"]=="Northern Mariana Island", "City"]="MP"
RO.loc[RO["City"]=="Ohio", "City"]="OH"
RO.loc[RO["City"]=="Oklahoma", "City"]="OK"
RO.loc[RO["City"]=="Oregon", "City"]="OR"
RO.loc[RO["City"]=="Pennsylvania", "City"]="PA"
RO.loc[RO["City"]=="Rhode Island", "City"]="RI"
RO.loc[RO["City"]=="South Carolina", "City"]="SC"
RO.loc[RO["City"]=="South Dakota", "City"]="SD"
RO.loc[RO["City"]=="Tennessee", "City"]="TN"
RO.loc[RO["City"]=="Texas", "City"]="TX"
RO.loc[RO["City"]=="Utah", "City"]="UT"
RO.loc[RO["City"]=="Vermont", "City"]="VT"
RO.loc[RO["City"]=="Virginia", "City"]="VA"
RO.loc[RO["City"]=="Virgin Islands", "City"]="VI"
RO.loc[RO["City"]=="Washington", "City"]="WA"
RO.loc[RO["City"]=="West Virginia", "City"]="WV"
RO.loc[RO["City"]=="Wisconsin", "City"]="WI"
RO.loc[RO["City"]=="(not set)", "City"]="-"
RO.loc[RO["City"].isnull(), "City"]="-"
RO.loc[(RO["Country.1"]=="US") & (~RO["City"].isin(["AL", "AK", "AS", "AZ", "AR", "CA", "CO",
                                                   "CT", "DE", "DC", "FL", "GA", "GU", "HI",
                                                   "ID", "IL", "IN", "IA", "KS", "KY", "LA",
                                                   "ME", "MD", "MA", "MI", "MN", "MS", "MO",
                                                   "MT", "NE", "NV", "NH", "NJ", "NM", "NY",
                                                   "NC","ND", "MP", "OH", "OK", "OR", "PA",
                                                   "RI", "SC", "SD", "TN", "TX", "UT", "VT",
                                                   "VA", "VI", "WA", "WV", "WI", "WY"])),
                                                   "City"]="-"
#Fechas
RO["Timestamp"]=RO["Timestamp"].str.replace(" ene ", "/01/", regex=False)
RO["Timestamp"]=RO["Timestamp"].str.replace(" feb ", "/02/", regex=False)
RO["Timestamp"]=RO["Timestamp"].str.replace(" mar ", "/03/", regex=False)
RO["Timestamp"]=RO["Timestamp"].str.replace(" abr ", "/04/", regex=False)
RO["Timestamp"]=RO["Timestamp"].str.replace(" may ", "/05/", regex=False)
RO["Timestamp"]=RO["Timestamp"].str.replace(" jun ", "/06/", regex=False)
RO["Timestamp"]=RO["Timestamp"].str.replace(" jul ", "/07/", regex=False)
RO["Timestamp"]=RO["Timestamp"].str.replace(" ago ", "/08/", regex=False)
RO["Timestamp"]=RO["Timestamp"].str.replace(" sep ", "/09/", regex=False)
RO["Timestamp"]=RO["Timestamp"].str.replace(" oct ", "/10/", regex=False)
RO["Timestamp"]=RO["Timestamp"].str.replace(" nov ", "/11/", regex=False)
RO["Timestamp"]=RO["Timestamp"].str.replace(" dic ", "/12/", regex=False)
RO["Timestamp"]=RO["Timestamp"].str.replace("2022,", "22", regex=False)
RO[["Day", "Month", "Year"]]=RO["Timestamp"].str.split(pat="/", expand=True)
RO.loc[RO["Day"].str.len()==1, "Timestamp"]="0"+RO["Timestamp"]
"""
Podria mejorar este algoritmo hacienod que solo limpie el timestamp de google, primero hacemos el split
usando pat=" " y ya despues limpiamos
"""
#%% Media
# Medios de Google
RO.loc[RO["Media"].isnull(), "Media"]=RO["Fuente/Medio"]
RO.loc[RO["Media"].isnull(), "Campaign"]=RO["Campaña"]
RO.loc[RO["Media"].isnull(), "Banner/Adgroup"]=RO["Palabra clave"]
RO.loc[RO["Promotion"].isnull(), "Promotion"]=RO["Código de cupón del producto"]
RO.loc[RO["Promotion"].isnull(), "Promotion"]="-"
RO.loc[RO["Promotion"]=="(not set)", "Promotion"]="-"
# Affiliered
RO.loc[RO["Media"].str.contains("Affilired", regex=False, na=False, case=False), "Placement"]=RO["Media"]
# Bride click
RO.loc[RO["Media"]=="Bride Click", "Placement"]=RO["Media"]
# DV360
RO.loc[RO["Media"]=="DV360", "Placement"]=RO["Media"]
# Martha Stewart
RO.loc[RO["Media"]=="Martha Stewart", "Placement"]=RO["Media"]
# Sojern
RO.loc[RO["Media"]=="Sojern", "Placement"]=RO["Media"]
RO.loc[RO["Media"]=="sojern / display", "Placement"]=RO["Media"]
RO.loc[RO["Media"].str.contains("sojern", regex=False, na=False, case=False), "Placement"]=RO["Media"]
# TravelZoo
RO.loc[RO["Media"]=="TravelZoo", "Placement"]=RO["Media"]
# Organic
RO.loc[RO["Media"].str.contains("organic", regex=False, na=False, case=False), "Campaign"]="-"
RO.loc[RO["Media"].str.contains("organic", regex=False, na=False, case=False), "CampaignType"]="Organic search"
RO.loc[RO["Media"].str.contains("organic", regex=False, na=False, case=False), "Placement"]="Organic"
RO.loc[RO["Media"].str.contains("organic", regex=False, na=False, case=False), "Banner/Adgroup"]="-"
RO.loc[RO["Media"].str.contains("organic", regex=False, na=False, case=False), "Ad Interaction"]="None"
RO.loc[RO["Media"].str.contains("organic", regex=False, na=False, case=False), "Paid Keywords"]="-"
RO.loc[RO["Media"].str.contains("organic", regex=False, na=False, case=False), "Referrer Type"]="Referring Site"
RO.loc[RO["Media"].str.contains("organic", regex=False, na=False, case=False), "Media"]="Organic"
# Direct
RO.loc[RO["Media"]=="(direct) / (none)", "Campaign"]="-"
RO.loc[RO["Media"]=="(direct) / (none)", "CampaignType"]="Direct access"
RO.loc[RO["Media"]=="(direct) / (none)", "Placement"]="Direct"
RO.loc[RO["Media"]=="(direct) / (none)", "Banner/Adgroup"]="-"
RO.loc[RO["Media"]=="(direct) / (none)", "Ad Interaction"]="None"
RO.loc[RO["Media"]=="(direct) / (none)", "Paid Keywords"]="-"
RO.loc[RO["Media"]=="(direct) / (none)", "Referrer Type"]="Direct Traffic"
RO.loc[RO["Media"]=="(direct) / (none)", "Media"]="Direct"
#External
RO.loc[RO["Media"].str.contains("referral", regex=False, na=False), "Campaign"]="-"
RO.loc[RO["Media"].str.contains("referral", regex=False, na=False), "CampaignType"]="External"
RO.loc[RO["Media"].str.contains("referral", regex=False, na=False), "Placement"]="External"
RO.loc[RO["Media"].str.contains("referral", regex=False, na=False), "Banner/Adgroup"]="-"
RO.loc[RO["Media"].str.contains("referral", regex=False, na=False), "Ad Interaction"]="None"
RO.loc[RO["Media"].str.contains("referral", regex=False, na=False), "Paid Keywords"]="-"
RO.loc[RO["Media"].str.contains("referral", regex=False, na=False), "Referrer Type"]="Referring Site"
RO.loc[RO["Media"].str.contains("referral", regex=False, na=False), "Media"]="External"
RO.loc[RO["Media"].str.contains("web", regex=False, na=False), "Campaign"]="-"
RO.loc[RO["Media"].str.contains("web", regex=False, na=False), "CampaignType"]="External"
RO.loc[RO["Media"].str.contains("web", regex=False, na=False), "Placement"]="External"
RO.loc[RO["Media"].str.contains("web", regex=False, na=False), "Banner/Adgroup"]="-"
RO.loc[RO["Media"].str.contains("web", regex=False, na=False), "Ad Interaction"]="None"
RO.loc[RO["Media"].str.contains("web", regex=False, na=False), "Paid Keywords"]="-"
RO.loc[RO["Media"].str.contains("web", regex=False, na=False), "Referrer Type"]="Referring Site"
RO.loc[RO["Media"].str.contains("web", regex=False, na=False), "Media"]="External"
# Steel House
RO.loc[RO["Media"].str.contains("steelhouse", regex=False, na=False, case=False), "Media"]="Steel House"
# -
RO.loc[RO["Media"].isnull(), "Campaign"]="-"
RO.loc[RO["Media"].isnull(), "CampaignType"]="-"
RO.loc[RO["Media"].isnull(), "Placement"]="-"
RO.loc[RO["Media"].isnull(), "Paid Keywords"]="-"
RO.loc[RO["Media"].isnull(), "Banner/Adgroup"]="-"
RO.loc[RO["Media"].isnull(), "Ad Interaction"]="-"
RO.loc[RO["Media"].isnull(), "Referrer Type"]="-"
RO.loc[RO["Media"].isnull(), "Media"]="-"
#%% Load
RO=RO[["Timestamp", "Campaign", "CampaignType", "Media", "Placement", "Banner/Adgroup", "Ad Interaction",
       "Paid Keywords", "Referrer Type", "Page", "City", "Order ID", "Sales", "Country.1", "Currency",
       "Number_of_rooms", "Guests", "Transaction_ID", "Prop_ID", "Page_Site", "Checkin_date",
       "Checkout_data", "Room_name", "Promotion", "is_booking_center", "Tag"]]
RO=RO.rename(columns={"Categoría de dispositivo": "Device", "Country.1": "Country"})
RO.to_csv("Adform_22"+Dias[:4]+"_22"+Dias[5:9]+".csv", index=False)
