#                                                   xX_IsaI_Xx
#%% Extract
import pandas as pd
Dias=str(input("Rango de fechas (mmdd mmdd): "))
Clave=pd.read_csv("/Users/isaibb/Desktop/Clean/RO/Clave.csv")
Cupon=pd.read_csv("/Users/isaibb/Desktop/Clean/RO/Cupon.csv")
Cuarto=pd.read_csv("/Users/isaibb/Desktop/Clean/RO/Cuarto.csv")
Producto=pd.read_csv("/Users/isaibb/Desktop/Clean/RO/Producto.csv")
America=pd.read_csv("/Users/isaibb/Desktop/Clean/RO/AMERICAS.csv", 
                    sep=',', lineterminator='\r')
Global=pd.read_csv("/Users/isaibb/Desktop/Clean/RO/Global.csv", 
                    sep=',', lineterminator='\r')
EMEA=pd.read_csv("/Users/isaibb/Desktop/Clean/RO/EMEA.csv", 
                    sep=',', lineterminator='\r')
#%% Functions
def one(col_1, col_2, val_1, val_2):
  RO.loc[RO[col_1]==val_1, col_2]=val_2  
#%% Clean
RO=pd.concat([America, Global, EMEA], ignore_index=True)
del Global
del America
del EMEA
RO=RO[RO.Transaction_ID.notnull()]
RO=RO[["Timestamp", "Campaign", "CampaignType", "Media", "Placement", 
       "Banner/Adgroup", "Ad Interaction", "Paid Keywords", "Referrer Type",
       "Page", "City", "Country", "Order ID", "Sales", "Country.1", "Currency",
       "Number_of_rooms", "Guests", "Transaction_ID", "Prop_ID", "Page_Site",
       "Checkin_date", "Checkout_data", "Room_name", "Promotion", 
       "is_booking_center", "Tag"]]
#Fechas RO
RO["Timestamp"]=RO["Timestamp"].str[1:]
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
Paises=["United States","Canada","Mexico","Colombia","Australia","Belgium","Bermuda","Brazil","Chile","Cuba","Denmark",
        "Dominican Republic","Ecuador","France","El Salvador","Germany","Guatemala","Hungary","India","Ireland","Italy",
        "Jamaica","Japan","Liechtenstein","Netherlands","New Zealand","Norway","Panama","Paraguay","Puerto Rico","Russia",
        "South Korea","Spain","Switzerland","Trinidad & Tobago","United Kingdom","Vietnam","Argentina","Bolivia","Czechia",
        "Venezuela","Slovenia","Turks & Caicos Islands","Costa Rica","Portugal","Ukraine","Cayman Islands","Israel","Finland",
        "Poland","Singapore", "Indonesia"]
Paises_ISO=["US","CA","MX","CO","AU","BE","BM","BR","CL","CU","DK","DO","EC","FR","SV","DE","GT","HU","IN","IE","IT","JM",
            "JP","LI","NL","NZ","NO","PA","PY","PR","RU","KR","ES","CH","TT","GB","VN","AR","BO","CZ","VE","SI","TC","CR",
            "PT","UA","KY","IL","FI","PL","SG", "ID"]
for i in range(len(Paises)):
    RO.loc[RO["Country.1"]==Paises[i], "Country.1"]=Paises_ISO[i]
    
#Ciudad
Ciudades=["Alabama", "Alaska","American Samoa","Arizona","Arkansas","California","Colorado","Connecticut","Delaware","District of Columbia","Florida","Georgia",
          "Guam","Hawaii","Idaho","Illinois","Indiana","Iowa","Kansas","Kentucky","Louisiana","Maine","Maryland","Massachusetts","Michigan","Minnesota","Mississippi",
          "Missouri","Montana","Nebraska","Nevada","New Hampshire","New Jersey","New Mexico","New York","North Carolina","North Dakota","Northern Mariana Island",
          "Ohio","Oklahoma","Oregon","Pennsylvania","Rhode Island","South Carolina","South Dakota","Tennessee","Texas","Utah","Vermont","Virginia","Virgin Islands",
          "Washington","West Virginia","Wisconsin","Wyoming", "(not set)"]
Ciudades_ISO=["AL","AK","AS","AZ","AR","CA","CO","CT","DE","DC","FL","GA","GU","HI","ID","IL","IN","IA","KS","KY","LA","ME","MD","MA","MI","MN","MS","MO","MT","NE",
              "NV","NH","NJ","NM","NY","NC","ND","MP","OH","OK","OR","PA","RI","SC","SD","TN","TX","UT","VT","VA","VI","WA","WV","WI","WY", "-"]
RO["City"]=RO["Región"]
for i in range(len(Ciudades)):
    RO.loc[RO["City"]==Ciudades[i], "City"]=Ciudades_ISO[i]
RO.loc[RO["City"].isnull(), "City"]="-"
RO.loc[(RO["Country.1"]=="US") & (~RO["City"].isin(Ciudades_ISO)),"City"]="-"
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
RO.loc[RO["Campaign"].isnull(), "Campaign"]=RO["Campaña"]
RO.loc[RO["Paid Keywords"].isnull(), "Paid Keywords"]=RO["Palabra clave"]
RO.loc[RO["Promotion"].isnull(), "Promotion"]=RO["Código de cupón del producto"]
RO.loc[RO["Promotion"].isnull(), "Promotion"]="-"
RO.loc[RO["Promotion"]=="(not set)", "Promotion"]="-"
RO.loc[RO["Campaign"]=="The Excellence Collection (Global Site-Tracking) - Tracking", "Campaign"]=RO["Campaña"]
#Bing
RO.loc[RO["Media"]=="bing / cpc", "CampaignType"]="Microsoft"
RO.loc[RO["Media"]=="bing / cpc", "Placement"]="Microsoft"
RO.loc[RO["Media"]=="bing / cpc", "Banner/Adgroup"]="Exacta"
RO.loc[RO["Media"]=="bing / cpc", "Ad Interaction"]="Click"
RO.loc[RO["Media"]=="bing / cpc", "Referrer Type"]="Search Engine"
RO.loc[RO["Media"]=="bing / cpc", "Media"]="Microsoft AdCenter"
#GMB
RO.loc[RO["Media"]=="google / gmb", "CampaignType"]="Display"
RO.loc[RO["Media"]=="google / gmb", "Placement"]="Business Listing"
RO.loc[RO["Media"]=="google / gmb", "Ad Interaction"]="Click"
RO.loc[RO["Media"]=="google / gmb", "Referrer Type"]="Campaign"
RO.loc[RO["Media"]=="google / gmb", "Banner/Adgroup"]=RO["Campaña"]
RO.loc[(RO["Media"]=="google / gmb") & (RO["Campaign"]).str.contains("xpc",
                        regex=True, case=False), "Campaign"]="AO EXCELLENCE RESORTS - Alkemy"
RO.loc[(RO["Media"]=="google / gmb") & (RO["Campaign"]).str.contains("xrc",
                        regex=True, case=False), "Campaign"]="AO EXCELLENCE RESORTS - Alkemy"
RO.loc[(RO["Media"]=="google / gmb") & (RO["Campaign"]).str.contains("xob",
                        regex=True, case=False), "Campaign"]="AO EXCELLENCE RESORTS - Alkemy"
RO.loc[(RO["Media"]=="google / gmb") & (RO["Campaign"]).str.contains("xpm",
                        regex=True, case=False), "Campaign"]="AO EXCELLENCE RESORTS - Alkemy"
RO.loc[(RO["Media"]=="google / gmb") & (RO["Campaign"]).str.contains("xec",
                        regex=True, case=False), "Campaign"]="AO EXCELLENCE RESORTS - Alkemy"
RO.loc[(RO["Media"]=="google / gmb") & (RO["Campaign"]).str.contains("bpm",
                        regex=True, case=False), "Campaign"]="AO - BELOVED HOTELS - Alkemy"
RO.loc[(RO["Media"]=="google / gmb") & (RO["Campaign"]).str.contains("fpm",
                        regex=True, case=False), "Campaign"]="AO - FINEST RESORTS - Alkemy"
RO.loc[(RO["Media"]=="google / gmb") & (RO["Campaign"]).str.contains("fpc",
                        regex=True, case=False), "Campaign"]="AO - FINEST RESORTS - Alkemy"

# RO.loc[(RO["Media"]=="google / gmb") & (RO["Banner/Adgroup"]).str.contains("xob",regex=True, case=True),
#        "Media"]=RO.loc[(RO["Media"]=="Google My Business") & (RO["Banner/Adgroup"]).str.contains("XOB", regex=True, case=True)]
# RO.loc[(RO["Media"]=="google / gmb") & (RO["Banner/Adgroup"]).str.contains("xpm",regex=True, case=True),
#        "Media"]=RO.loc[(RO["Media"]=="Google My Business") & (RO["Banner/Adgroup"]).str.contains("XPM", regex=True, case=True)]
# RO.loc[(RO["Media"]=="google / gmb") & (RO["Banner/Adgroup"]).str.contains("xrc",regex=True, case=True),
#        "Media"]=RO.loc[(RO["Media"]=="Google My Business") & (RO["Banner/Adgroup"]).str.contains("XRC", regex=True, case=True)]
# RO.loc[(RO["Media"]=="google / gmb") & (RO["Banner/Adgroup"]).str.contains("xec",regex=True, case=True),
#        "Media"]=RO.loc[(RO["Media"]=="Google My Business") & (RO["Banner/Adgroup"]).str.contains("XEC", regex=True, case=True)]
# RO.loc[(RO["Media"]=="google / gmb") & (RO["Banner/Adgroup"]).str.contains("xpc",regex=True, case=True),
#        "Media"]=RO.loc[(RO["Media"]=="Google My Business") & (RO["Banner/Adgroup"]).str.contains("XPC", regex=True, case=True)]
# RO.loc[(RO["Media"]=="google / gmb") & (RO["Banner/Adgroup"]).str.contains("fpm",regex=True, case=True),
#        "Media"]=RO.loc[(RO["Media"]=="Google My Business") & (RO["Banner/Adgroup"]).str.contains("FPM", regex=True, case=True)]
# RO.loc[(RO["Media"]=="google / gmb") & (RO["Banner/Adgroup"]).str.contains("bpm",regex=True, case=True),
#        "Media"]=RO.loc[(RO["Media"]=="Google My Business") & (RO["Banner/Adgroup"]).str.contains("BPM", regex=True, case=True)]
# RO.loc[(RO["Media"]=="google / gmb") & (RO["Banner/Adgroup"]).str.contains("fpc",regex=True, case=True),
#        "Media"]=RO.loc[(RO["Media"]=="Google My Business") & (RO["Banner/Adgroup"]).str.contains("FPC", regex=True, case=True)]

RO.loc[RO["Media"]=="google / gmb", "Media"]="Google My Business"
# HotelAds
RO.loc[RO["Media"]=="googlehotelads / metasearch", "Banner/Adgroup"]=RO["Campaña"]
RO.loc[RO["Media"]=="googlehotelads / metasearch", "CampaignType"]="Display"
RO.loc[RO["Media"]=="googlehotelads / metasearch", "Placement"]="Metabuscadores"
RO.loc[RO["Media"]=="googlehotelads / metasearch", "Ad Interaction"]="Click"
RO.loc[RO["Media"]=="googlehotelads / metasearch", "Paid Keywords"]="Metasearch_HotelAds_FPC"
RO.loc[RO["Media"]=="googlehotelads / metasearch", "Referrer Type"]="Campaign"
RO.loc[(RO["Media"]=="googlehotelads / metasearch") & (RO["Campaign"]).str.contains("xob",
                                        regex=True, case=False),
                                       "Banner/Adgroup"]="Metasearch_HotelAds_EUR_XOB"
RO.loc[(RO["Media"]=="googlehotelads / metasearch") & (RO["Campaign"]).str.contains("xrc",
                                        regex=True, case=False),
                                       "Banner/Adgroup"]="Metasearch_HotelAds_EUR_XRC"
RO.loc[(RO["Media"]=="googlehotelads / metasearch") & (RO["Campaign"]).str.contains("xpm",
                                        regex=True, case=False),
                                       "Banner/Adgroup"]="Metasearch_HotelAds_EUR_XPM"
RO.loc[(RO["Media"]=="googlehotelads / metasearch") & (RO["Campaign"]).str.contains("fpm",
                                        regex=True, case=False),
                                       "Banner/Adgroup"]="Metasearch_HotelAds_EUR_FPM"
RO.loc[(RO["Media"]=="googlehotelads / metasearch") & (RO["Campaign"]).str.contains("fpc",
                                        regex=True, case=False),
                                       "Banner/Adgroup"]="Metasearch_HotelAds_EUR_FPC"
RO.loc[(RO["Media"]=="googlehotelads / metasearch") & (RO["Campaign"]).str.contains("xob",
                                        regex=True, case=False),
                                       "Paid Keywords"]="Metasearch_HotelAds_EUR_XOB"
RO.loc[(RO["Media"]=="googlehotelads / metasearch") & (RO["Campaign"]).str.contains("xrc",
                                        regex=True, case=False),
                                       "Paid Keywords"]="Metasearch_HotelAds_EUR_XRC"
RO.loc[(RO["Media"]=="googlehotelads / metasearch") & (RO["Campaign"]).str.contains("xpm",
                                        regex=True, case=False),
                                       "Paid Keywords"]="Metasearch_HotelAds_EUR_XPM"
RO.loc[(RO["Media"]=="googlehotelads / metasearch") & (RO["Campaign"]).str.contains("fpm",
                                        regex=True, case=False),
                                       "Paid Keywords"]="Metasearch_HotelAds_EUR_FPM"
RO.loc[(RO["Media"]=="googlehotelads / metasearch") & (RO["Campaign"]).str.contains("fpc",
                                        regex=True, case=False),
                                       "Paid Keywords"]="Metasearch_HotelAds_EUR_FPC"
RO.loc[(RO["Media"]=="googlehotelads / metasearch") & (RO["Campaign"]).str.contains("xpc",
                                        regex=True, case=False),
                                       "Campaign"]="AO EXCELLENCE RESORTS - Alkemy"
RO.loc[(RO["Media"]=="googlehotelads / metasearch") & (RO["Campaign"]).str.contains("xrc",
                                        regex=True, case=False),
                                       "Campaign"]="AO EXCELLENCE RESORTS - Alkemy"
RO.loc[(RO["Media"]=="googlehotelads / metasearch") & (RO["Campaign"]).str.contains("xob",
                                        regex=True, case=False),
                                       "Campaign"]="AO EXCELLENCE RESORTS - Alkemy"
RO.loc[(RO["Media"]=="googlehotelads / metasearch") & (RO["Campaign"]).str.contains("xpm",
                                        regex=True, case=False),
                                       "Campaign"]="AO EXCELLENCE RESORTS - Alkemy"
RO.loc[(RO["Media"]=="googlehotelads / metasearch") & (RO["Campaign"]).str.contains("xec",
                                        regex=True, case=False), "Campaign"]="AO EXCELLENCE RESORTS - Alkemy"
RO.loc[(RO["Media"]=="googlehotelads / metasearch") & (RO["Campaign"]).str.contains("bpm",
                                        regex=True, case=False), "Campaign"]="AO - BELOVED HOTELS - Alkemy"
RO.loc[(RO["Media"]=="googlehotelads / metasearch") & (RO["Campaign"]).str.contains("fpm",
                                        regex=True, case=False), "Campaign"]="AO - FINEST RESORTS - Alkemy"
RO.loc[(RO["Media"]=="googlehotelads / metasearch") & (RO["Campaign"]).str.contains("fpc",
                                        regex=True, case=False), "Campaign"]="AO - FINEST RESORTS - Alkemy"
RO.loc[RO["Media"]=="googlehotelads / metasearch", "Media"]="HotelAds"
# Trivago
RO.loc[RO["Media"]=="trivago / metasearch", "CampaignType"]="Trivago"
RO.loc[RO["Media"]=="trivago / metasearch", "Placement"]="Metabuscadores"
RO.loc[RO["Media"]=="trivago / metasearch", "Ad Interaction"]="Click"
RO.loc[RO["Media"]=="trivago / metasearch", "Referrer Type"]="Campaign"
RO.loc[(RO["Media"]=="trivago / metasearch") & (RO["Campaign"]).str.contains("xob",
                                        regex=True, case=False),
                                       "Banner/Adgroup"]="Metasearch_Trivago_EUR_XOB"
RO.loc[(RO["Media"]=="trivago / metasearch") & (RO["Campaign"]).str.contains("xrc",
                                        regex=True, case=False),
                                       "Banner/Adgroup"]="Metasearch_Trivago_EUR_XRC"
RO.loc[(RO["Media"]=="trivago / metasearch") & (RO["Campaign"]).str.contains("xpm",
                                        regex=True, case=False),
                                       "Banner/Adgroup"]="Metasearch_Trivago_EUR_XPM"
RO.loc[(RO["Media"]=="trivago / metasearch") & (RO["Campaign"]).str.contains("fpm",
                                        regex=True, case=False),
                                       "Banner/Adgroup"]="Metasearch_Trivago_EUR_FPM"
RO.loc[(RO["Media"]=="trivago / metasearch") & (RO["Campaign"]).str.contains("fpc",
                                        regex=True, case=False),
                                       "Banner/Adgroup"]="Metasearch_Trivago_EUR_FPC"
RO.loc[(RO["Media"]=="trivago / metasearch") & (RO["Campaign"]).str.contains("xob",
                                        regex=True, case=False),
                                       "Paid Keywords"]="Metasearch_Trivago_EUR_XOB"
RO.loc[(RO["Media"]=="trivago / metasearch") & (RO["Campaign"]).str.contains("xrc",
                                        regex=True, case=False),
                                       "Paid Keywords"]="Metasearch_Trivago_EUR_XRC"
RO.loc[(RO["Media"]=="trivago / metasearch") & (RO["Campaign"]).str.contains("xpm",
                                        regex=True, case=False),
                                       "Paid Keywords"]="Metasearch_Trivago_EUR_XPM"
RO.loc[(RO["Media"]=="trivago / metasearch") & (RO["Campaign"]).str.contains("fpm",
                                        regex=True, case=False),
                                       "Paid Keywords"]="Metasearch_Trivago_EUR_FPM"
RO.loc[(RO["Media"]=="trivago / metasearch") & (RO["Campaign"]).str.contains("fpc",
                                        regex=True, case=False),
                                       "Paid Keywords"]="Metasearch_Trivago_EUR_FPC"
RO.loc[(RO["Media"]=="trivago / metasearch") & (RO["Campaign"]).str.contains("xec",
                                        regex=True, case=False), "Campaign"]="AO EXCELLENCE RESORTS - Alkemy"
RO.loc[(RO["Media"]=="trivago / metasearch") & (RO["Campaign"]).str.contains("bpm",
                                        regex=True, case=False), "Campaign"]="AO - BELOVED HOTELS - Alkemy"
RO.loc[(RO["Media"]=="trivago / metasearch") & (RO["Campaign"]).str.contains("fpm",
                                        regex=True, case=False), "Campaign"]="AO - FINEST RESORTS - Alkemy"
RO.loc[(RO["Media"]=="trivago / metasearch") & (RO["Campaign"]).str.contains("fpc",
                                        regex=True, case=False), "Campaign"]="AO - FINEST RESORTS - Alkemy"
RO.loc[RO["Media"]=="trivago / metasearch", "Media"]="Trivago"
# Kayak
RO.loc[RO["Media"]=="kayak / metasearch", "CampaignType"]="Display"
RO.loc[RO["Media"]=="kayak / metasearch", "Placement"]="Metabuscadores"
RO.loc[RO["Media"]=="kayak / metasearch", "Ad Interaction"]="Click"
RO.loc[RO["Media"]=="kayak / metasearch", "Referrer Type"]="Campaign"
RO.loc[(RO["Media"]=="kayak / metasearch") & (RO["Campaign"]).str.contains("xob",
                                        regex=True, case=False),
                                       "Banner/Adgroup"]="Metasearch_Kayak_EUR_XOB"
RO.loc[(RO["Media"]=="kayak / metasearch") & (RO["Campaign"]).str.contains("xrc",
                                        regex=True, case=False),
                                       "Banner/Adgroup"]="Metasearch_Kayak_EUR_XRC"
RO.loc[(RO["Media"]=="kayak / metasearch") & (RO["Campaign"]).str.contains("xpm",
                                        regex=True, case=False),
                                       "Banner/Adgroup"]="Metasearch_Kayak_EUR_XPM"
RO.loc[(RO["Media"]=="kayak / metasearch") & (RO["Campaign"]).str.contains("fpm",
                                        regex=True, case=False),
                                       "Banner/Adgroup"]="Metasearch_Kayak_EUR_FPM"
RO.loc[(RO["Media"]=="kayak / metasearch") & (RO["Campaign"]).str.contains("fpc",
                                        regex=True, case=False),
                                       "Banner/Adgroup"]="Metasearch_Kayak_EUR_FPC"
RO.loc[(RO["Media"]=="kayak / metasearch") & (RO["Campaign"]).str.contains("xrc",
                                        regex=True, case=False),
                                       "Paid Keywords"]="Metasearch_Kayak_EUR_XRC"
RO.loc[(RO["Media"]=="kayak / metasearch") & (RO["Campaign"]).str.contains("xpm",
                                        regex=True, case=False),
                                       "Paid Keywords"]="Metasearch_Kayak_EUR_XPM"
RO.loc[(RO["Media"]=="kayak / metasearch") & (RO["Campaign"]).str.contains("fpm",
                                        regex=True, case=False),
                                       "Paid Keywords"]="Metasearch_Kayak_EUR_FPM"
RO.loc[(RO["Media"]=="kayak / metasearch") & (RO["Campaign"]).str.contains("fpc",
                                        regex=True, case=False),
                                       "Paid Keywords"]="Metasearch_Kayak_EUR_FPC"
RO.loc[(RO["Media"]=="kayak / metasearch") & (RO["Campaign"]).str.contains("xec",
                                        regex=True, case=False), "Campaign"]="AO EXCELLENCE RESORTS - Alkemy"
RO.loc[(RO["Media"]=="kayak / metasearch") & (RO["Campaign"]).str.contains("bpm",
                                        regex=True, case=False), "Campaign"]="AO - BELOVED HOTELS - Alkemy"
RO.loc[(RO["Media"]=="kayak / metasearch") & (RO["Campaign"]).str.contains("fpm",
                                        regex=True, case=False), "Campaign"]="AO - FINEST RESORTS - Alkemy"
RO.loc[(RO["Media"]=="kayak / metasearch") & (RO["Campaign"]).str.contains("fpc",
                                        regex=True, case=False), "Campaign"]="AO - FINEST RESORTS - Alkemy"
RO.loc[RO["Media"]=="kayak / metasearch", "Campaign"]=RO["Campaña"]
RO.loc[RO["Media"]=="kayak / metasearch", "Media"]="Kayak"
# TripAdsvisor
RO.loc[RO["Media"].isin(["Trip Advisor Offers / Business Listing", "tripadvisor / business-listing"]),
                                       "CampaignType"]="Display"
RO.loc[RO["Media"].isin(["Trip Advisor Offers / Business Listing", "tripadvisor / business-listing"]),
                                       "Placement"]="Business Listing"
RO.loc[RO["Media"].isin(["Trip Advisor Offers / Business Listing", "tripadvisor / business-listing"]),
                                       "Ad Interaction"]="Click"
RO.loc[RO["Media"].isin(["Trip Advisor Offers / Business Listing", "tripadvisor / business-listing"]),
                                       "Referrer Type"]="Campaign"
RO.loc[RO["Media"]=="tripadvisor / metasearch", "CampaignType"]="Display"
RO.loc[RO["Media"]=="tripadvisor / metasearch", "Placement"]="Metabuscadores"
RO.loc[RO["Media"]=="tripadvisor / metasearch", "Ad Interaction"]="Click"
RO.loc[RO["Media"]=="tripadvisor / metasearch", "Referrer Type"]="Campaign"
RO.loc[(RO["Media"].isin(["Trip Advisor Offers / Business Listing", "tripadvisor / metasearch",
                          "tripadvisor / business-listing"])) & (RO["Campaign"]).str.contains("xec",
                          regex=True, case=False), "Campaign"]="AO EXCELLENCE RESORTS - Alkemy"
RO.loc[(RO["Media"].isin(["Trip Advisor Offers / Business Listing", "tripadvisor / metasearch",
                          "tripadvisor / business-listing"])) & (RO["Campaign"]).str.contains("bpm",
                          regex=True, case=False), "Campaign"]="AO - BELOVED HOTELS - Alkemy"
RO.loc[(RO["Media"].isin(["Trip Advisor Offers / Business Listing", "tripadvisor / metasearch",
                          "tripadvisor / business-listing"])) & (RO["Campaign"]).str.contains("fpm",
                          regex=True, case=False), "Campaign"]="AO - FINEST RESORTS - Alkemy"
RO.loc[(RO["Media"].isin(["Trip Advisor Offers / Business Listing", "tripadvisor / metasearch",
                          "tripadvisor / business-listing"])) & (RO["Campaign"]).str.contains("fpc",
                         regex=True, case=False), "Campaign"]="AO - FINEST RESORTS - Alkemy"
RO.loc[RO["Media"].isin(["Trip Advisor Offers / Business Listing", "tripadvisor / metasearch",
                         "tripadvisor / business-listing"]), "Media"]="TripAdvisor"
#Email
RO.loc[RO["Media"]=="newsletter / email", "CampaignType"]="Email"
RO.loc[RO["Media"]=="newsletter / email", "Banner/Adgroup"]=RO["Campaign"]
RO.loc[RO["Media"]=="newsletter / email", "Campaign"]="AO - TEC"
RO.loc[RO["Media"]=="newsletter / email", "Placement"]="Email"
RO.loc[RO["Media"]=="newsletter / email", "Ad Interaction"]="Click"
RO.loc[RO["Media"].isin(["newsletter / email", "Newsletter"]), "Paid Keywords"]="-"
RO.loc[RO["Media"]=="newsletter / email", "Referrer Type"]="Campaign"
RO.loc[RO["Media"]=="newsletter / email", "Media"]="Newsletter"
# Adwords
RO.loc[RO["Media"]=="google / cpc", "CampaignType"]="Google"
RO.loc[RO["Media"]=="google / cpc", "Placement"]="Google AdWords"
RO.loc[RO["Media"]=="google / cpc", "Banner/Adgroup"]="Exacta"
RO.loc[RO["Media"]=="google / cpc", "Ad Interaction"]="Click"
RO.loc[RO["Media"]=="google / cpc", "Referrer Type"]="Search Engine"
RO.loc[(RO["Media"]=="google / cpc") & (RO["Banner/Adgroup"]).str.contains("PerformanceMax", regex=True, case=False),
                                    "Media"]="GDN"
RO.loc[(RO["Media"]=="google / cpc") & (RO["Banner/Adgroup"]).str.contains("PERFORMANCE MAX",regex=True, case=False),
                                    "Media"]="GDN"
RO.loc[RO["Media"]=="google / cpc", "Media"]="Google AdWords"
#Facebook
RO.loc[RO["Media"]=="Facebook / Social", "CampaignType"]="Social Media"
RO.loc[RO["Media"].isin(["Facebook / Social", "FACEBOOK"]), "Placement"]="Facebook"
RO.loc[RO["Media"]=="Facebook / Social", "Ad Interaction"]="Click"
RO.loc[RO["Media"]=="Facebook / Social", "Referrer Type"]="Campaign"
RO.loc[RO["Media"].isin(["Facebook / Social", "FACEBOOK"]), "Media"]="Facebook"
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
RO.loc[RO["Media"]=="steelhouse", "Referrer Type"]="Campaign"
RO.loc[RO["Media"]=="steelhouse", "CampaignType"]="Display"
RO.loc[RO["Media"].str.contains("steelhouse", regex=False, na=False, case=False), "Media"]="Steel House"
RO.loc[RO["Media"]=="Steel House", "Placement"]=RO["Media"]

# -
RO.loc[RO["Media"].isnull(), "Campaign"]="-"
RO.loc[RO["Media"].isnull(), "CampaignType"]="-"
RO.loc[RO["Media"].isnull(), "Placement"]="-"
RO.loc[RO["Media"].isnull(), "Paid Keywords"]="-"
RO.loc[RO["Media"].isnull(), "Banner/Adgroup"]="-"
RO.loc[RO["Media"].isnull(), "Ad Interaction"]="-"
RO.loc[RO["Media"].isnull(), "Referrer Type"]="-"
RO.loc[RO["Media"].isnull(), "Media"]="-"
# GDN
RO.loc[RO["Media"]=="GDN", "Placement"]=RO["Media"]
RO.loc[RO["Media"]=="GDN", "CampaignType"]="Display"
# Display
RO.loc[(RO["CampaignType"]=="Display")&(RO["Placement"]=="Prospecting"), "Placement"]="Media"
#%% Campañas y claves
# EX
RO.loc[(RO["Campaign"]=="AO - TEC") & (RO["Banner/Adgroup"]).str.contains("XRC",
                                        regex=True, case=False), "Campaign"]="AO EXCELLENCE RESORTS - Alkemy"
RO.loc[(RO["Campaign"]=="AO - TEC") & (RO["Banner/Adgroup"]).str.contains("XEC",
                                        regex=True, case=False), "Campaign"]="AO EXCELLENCE RESORTS - Alkemy"
RO.loc[(RO["Campaign"]=="AO - TEC") & (RO["Banner/Adgroup"]).str.contains("XOB",
                                        regex=True, case=False), "Campaign"]="AO EXCELLENCE RESORTS - Alkemy"
RO.loc[(RO["Campaign"]=="AO - TEC") & (RO["Banner/Adgroup"]).str.contains("XPM",
                                        regex=True, case=False), "Campaign"]="AO EXCELLENCE RESORTS - Alkemy"
RO.loc[(RO["Campaign"]=="AO - TEC") & (RO["Banner/Adgroup"]).str.contains("XPC",
                                        regex=True, case=False), "Campaign"]="AO EXCELLENCE RESORTS - Alkemy"
RO.loc[(RO["Campaign"]=="AO - TEC") & (RO["Banner/Adgroup"]).str.contains("EX",
                                        regex=True, case=False), "Campaign"]="AO EXCELLENCE RESORTS - Alkemy"
# FR
RO.loc[(RO["Campaign"]=="AO - TEC") & (RO["Banner/Adgroup"]).str.contains("FPM",
                                        regex=True, case=False), "Campaign"]="AO - FINEST RESORTS - Alkemy"
RO.loc[(RO["Campaign"]=="AO - TEC") & (RO["Banner/Adgroup"]).str.contains("FPC",
                                        regex=True, case=False), "Campaign"]="AO - FINEST RESORTS - Alkemy"
RO.loc[(RO["Campaign"]=="AO - TEC") & (RO["Banner/Adgroup"]).str.contains("FR",
                                        regex=True, case=False), "Campaign"]="AO - FINEST RESORTS - Alkemy"
# BE
RO.loc[(RO["Campaign"]=="AO - TEC") & (RO["Banner/Adgroup"]).str.contains("BPM",
                                        regex=True, case=False), "Campaign"]="AO - BELOVED HOTELS - Alkemy"
# (not set)
RO.loc[RO["Campaign"]=="(not set)", "Campaign"]="-"
RO.loc[RO["Campaign"]=="(not set)", "CampaignType"]="-"
RO.loc[RO["Campaign"]=="(not set)", "Placement"]="-"
RO.loc[RO["Campaign"]=="(not set)", "Banner/Adgroup"]="-"
RO.loc[RO["Campaign"]=="(not set)", "Ad Interaction"]="-"
RO.loc[RO["Campaign"]=="(not set)", "Paid Keywords"]="-"
RO.loc[RO["Campaign"]=="(not set)", "Referrer Type"]="-"
# Rare
RO.loc[RO["Paid Keywords"].isin(["(not set)", "(not provided)", "Other..."]), "Paid Keywords"]="-"
RO.loc[RO["Paid Keywords"].isnull(), "Paid Keywords"]="-"
#%% Load
RO=RO[["Timestamp", "Campaign", "CampaignType", "Media", "Placement", "Banner/Adgroup", "Ad Interaction",
       "Paid Keywords", "Referrer Type", "Page", "City", "Order ID", "Sales", "Country.1", "Currency",
       "Number_of_rooms", "Guests", "Transaction_ID", "Prop_ID", "Page_Site", "Checkin_date",
       "Checkout_data", "Room_name", "Promotion", "is_booking_center", "Tag", "Categoría de dispositivo"]]
RO=RO.rename(columns={"Categoría de dispositivo": "Device", "Country.1": "Country"})
RO.to_csv("Adform_22"+Dias[:4]+"_22"+Dias[5:9]+".csv", index=False)
GO.to_csv("ROPY.csv", index=False)

print("Banner de GMB")
print("Banner de Tripadvisor")
print("Banner de Newsletter")
print("Banner de Facebook")
print("Banner de hotelads")
print("Banner de kayak")


"""
*hay un Finest Canc√∫n en banner
*VUELVE A CHECAR TRIVAGO
* En email hay mas medios de go, debo de agregarlos despues con un .isin()
*Esta vez no tuvbe problemas con el csv, al parecer al inicio fue un problema de MAC ahora solo hay que ver si
no tien eproblemas con el encoding
*Debo de cambiar L60 para quitar el append
*Algo hicwe que esta vez no jalo la campaña de GA éro espo es mejor, ahora rodas estan vacias y puedo buscar
la correcta
*payyd keywords todas la sraras deen de tener un "-"
en la linea 186 estoy usanod "&" como un and pero al parecer es un or, el and es "|"
En 14-20 encontre que los csv de Adform incluian "\n" en el Timestamp, lo cual causaba un error, por lo que
escogi un substring saltando estos primeros dos caracteres, lo raro es que esto acaba de pasar por lo que 
que he agregado un  modulo de fecha para RO, en el que si el primer caracter no es numerico entonces,
usar un substring del timestamp
En bing hay caracteres extraños en Exacta Ingles y Exacta Español, en CampaignType, Media y Placement
debe de decir Microsoft  en abnenr adgroup, Exacta para los vacios, en ad intercation, click para los avcios
y en paid keyword vacios, la clave solo falatrua ajustar las caampñas, desplegar un print con el recordatorio
hasta que areglemos esto
en Newsletter y email Campaign	CampaignType	Media	Placement deben de decir lo mismo, y en las banners
vacios jalamos la campaña, paid keyword "-", ad intercation "Click" en las vacias, refer tyoe "Campaign" todas
"""