#Este archivo contiene las líneas específicas que están en el archivo jupyeter notebook para llevar a cabo el ETL
#No tiene en cuenta los comentarios ni los print que se utilizaron para recabar información.
import numpy as np
import pandas as pd
import pyarrow 

semana0413 = pd.read_csv('DTS-04\Proyectos Individuales\PI01_DATA_ENGINEERING\Datasets\Datasets relevamiento precios\precios_semana_20200413.csv', encoding= 'utf_16_le')
semana0503 = pd.read_json('DTS-04\Proyectos Individuales\PI01_DATA_ENGINEERING\Datasets\Datasets relevamiento precios\precios_semana_20200503.json')
semana0518 = pd.read_csv('DTS-04\Proyectos Individuales\PI01_DATA_ENGINEERING\Datasets\Datasets relevamiento precios\precios_semana_20200518.txt', sep= '|')
semana0426= pd.read_excel('DTS-04\Proyectos Individuales\PI01_DATA_ENGINEERING\Datasets\Datasets relevamiento precios\precios_semanas_20200419_20200426.xlsx')
producto = pd.read_parquet('DTS-04\Proyectos Individuales\PI01_DATA_ENGINEERING\Datasets\Datasets relevamiento precios\producto.parquet')
sucursal = pd.read_csv ('DTS-04\Proyectos Individuales\PI01_DATA_ENGINEERING\Datasets\Datasets relevamiento precios\sucursal.csv')
semana0419= pd.read_excel('DTS-04\Proyectos Individuales\PI01_DATA_ENGINEERING\Datasets\Datasets relevamiento precios\precios_semanas_20200419_20200426.xlsx', sheet_name= 'precios_20200419_20200419') 

semana0413 = semana0413.rename(columns= {'sucursal_id': 'idSucursal', 'producto_id': 'idProducto'})
semana0518 = semana0518.rename(columns= {'sucursal_id': 'idSucursal', 'producto_id': 'idProducto'})
semana0503 = semana0503.rename(columns= {'sucursal_id': 'idSucursal', 'producto_id': 'idProducto'})
semana0426 = semana0426.rename(columns= {'sucursal_id': 'idSucursal', 'producto_id': 'idProducto'})
semana0419 = semana0419.rename(columns= {'sucursal_id': 'idSucursal', 'producto_id': 'idProducto'})
sucursal = sucursal.rename(columns = {'id' : 'idSucursal','comercioId': 'idComercio', 'banderaId': 'idBandera'})

producto = producto.rename(columns={'id' : 'idProducto','marca':'Marca','nombre':'Nombre','presentacion':'Presentacion'})
catConValor = producto[producto.categoria1.isin(['Almacén','Perfumería y Cuidado Personal', 'Alimentos Congelados'])]
producto = producto.drop(['categoria1', 'categoria2' , 'categoria3'], axis=1) 
producto =producto.drop([53619, 55798])
for i in range(0,4):
    producto.iloc[:,i] = producto.iloc[:,i].astype('string')

bandera = pd.concat([sucursal.idBandera, sucursal.banderaDescripcion], axis=1) 
bandera = bandera.rename(columns= {'banderaDescripcion': 'NombreBandera'})
comercio = pd.concat([sucursal.idComercio, sucursal.comercioRazonSocial], axis=1)
comercio = comercio.rename(columns= {'comercioRazonSocial': 'RazonSocial'})
lugarSucursal = sucursal.drop(['idComercio', 'idBandera','banderaDescripcion', 'comercioRazonSocial'], axis = 1)
lugarSucursal = lugarSucursal.rename(columns= {'provincia': 'CodProvincia','localidad':'Localidad', 'lat': 
                                    'Latitud', 'lng':'Longitud','sucursalNombre': 'SucursalNombre','sucursalTipo':'SucursalTipo' })
sucursal = sucursal.drop(['banderaDescripcion', 'comercioRazonSocial','provincia', 'localidad', 'direccion', 'lat', 'lng', 'sucursalNombre', 'sucursalTipo'], axis =1)
sucursal.idSucursal = sucursal.idSucursal.astype('string')
bandera.NombreBandera = bandera.NombreBandera.astype('string') 
comercio.RazonSocial = comercio.RazonSocial.astype('string')
for i in range(0,8):
    if i != 4 and i !=5:
        lugarSucursal.iloc[:,i] = lugarSucursal.iloc[:,i].astype('string')
    else:
        lugarSucursal.iloc[:,i] = lugarSucursal.iloc[:,i].astype('float')

semana0413.drop_duplicates(inplace= True)
sem0413SinValor = semana0413[semana0413.idProducto.isnull() == True]
sinValor0413 = list(sem0413SinValor.index)
semana0413 = semana0413.drop(sinValor0413)
for i in range(1,3):
    semana0413.iloc[:,i] = semana0413.iloc[:,i].astype('string')

semana0419 = semana0419[['precio', 'idProducto','idSucursal']]
for i in range (21663,22017):
    if semana0419.precio.iloc[i] == 4.399890e+16: 
        semana0419.precio.iloc[i]=semana0419.precio.iloc[i]/1e+14
for i in list([21663,21858,21876,21879,21987,22076]):
    semana0419.precio.iloc[i]=semana0419.precio.iloc[i]/1e+14
for i in list([22086,21988,21880]):
    semana0419.precio.iloc[i]=semana0419.precio.iloc[i]/1e+14
semana0419.precio[21670] = 429991.000/1000
import datetime as dt
for i in range (len(semana0419.precio)):
    if type(semana0419.idSucursal.iloc[i]) == dt.datetime:
        semana0419.idSucursal.iloc[i]= semana0419.idSucursal.iloc[i].strftime('%d-%m-%Y')
for i in range(1,3):
    semana0419.iloc[:,i] = semana0419.iloc[:,i].astype('string')
listasuc = []
for i in range (len(semana0419.precio)):
    if len(semana0419.idSucursal.iloc[i]) == 10:
        listasuc.append(semana0419.idSucursal.iloc[i])
sucMal = list(set(listasuc))
sucBien=['6-1-4','16-1-2702','10-1-6','20-1-1','22-01-2017','29-1-5','3-1-62','13-1-52','23-1-6216',
'6-1-9','16-1-2802','23-1-6294','18-1-5','25-1-1','9-2-39','10-1-26','9-3-5218',
'7-1-37','10-1-53','9-3-5201','11-5-3613','12-1-40','5-1-3','11-5-3608','10-1-54',
'9-3-5251','7-1-48','14-1-9','9-3-5202','29-1-7','23-1-6239','23-1-6269','6-2-21',
'10-1-29','9-3-5225','6-1-26','11-5-3604','23-1-6213','16-2-6004','11-5-2997',
'12-1-99','13-1-62','6-2-2','9-3-5227','10-1-18','10-1-46','9-3-5277','13-1-39',
'10-1-33','7-1-35','9-3-5206','10-1-55','10-1-48','23-1-6262','9-3-5216','11-5-3603','9-2-50','10-1-44','23-1-6221']
semana0419.idSucursal = semana0419.idSucursal.replace(sucMal,sucBien)
semana0419.idProducto= semana0419.idProducto.str.zfill(13)
semana0419.precio.fillna(0.0, inplace=True)

semana0426 = semana0426[['precio', 'idProducto','idSucursal']]
maximo0426 = semana0426[semana0426.precio > 1e+15]
semana0426 = semana0426.drop(semana0426[semana0426.precio > 1e+15].index)
semana0426.precio.fillna(0.0, inplace=True)
semana0426.idProducto.fillna(0.0, inplace=True)
for i in range(1,3):
    semana0426.iloc[:,i] = semana0426.iloc[:,i].astype('string')
semana0426.idProducto = semana0426.idProducto.str[:-2]
semana0426.idProducto= semana0426.idProducto.str.zfill(13)
for i in range (len(semana0426)):
    if semana0426.precio.iloc[i] % 10 == 0:
        semana0426.precio.iloc[i] = semana0426.precio.iloc[i]/10
    elif semana0426.precio.iloc[i] > 10000:
        semana0426.precio.iloc[i] = semana0426.precio.iloc[i]/100
    elif semana0426.precio.iloc[i] < 1000:
        semana0426.precio.iloc[i] = semana0426.precio.iloc[i]/10
    else:
        semana0426.precio.iloc[i] = semana0426.precio.iloc[i]/10

semana0503.precio = semana0503.precio.replace('',0).astype('float')
for i in range(1,3):
    semana0503.iloc[:,i] = semana0503.iloc[:,i].astype('string')

semana0518.drop_duplicates(inplace=True)
semana0518.drop(semana0518[semana0518.idSucursal.isnull() == True].index, inplace=True)
semana0518.precio.fillna(0.0, inplace=True)
for i in range(1,3):
    semana0518.iloc[:,i] = semana0518.iloc[:,i].astype('string')

maximo0426.idProducto= maximo0426.idProducto.fillna(0).astype('int').astype('string').str.zfill(13)
maximo0426.idSucursal=maximo0426.idSucursal.astype('string')
maximo0426.idSucursal=maximo0426.idSucursal.astype('string')
precioSemana = pd.concat([semana0413, semana0419, semana0426, semana0503], ignore_index=True)

from sqlalchemy import create_engine
engine = create_engine("mysql+pymysql://{user}:{pw}@localhost/{db}"
                       .format(user="root",
                               pw="MYSQL2022$",
                               db="ProyectoIndividual04"))
precioSemana.to_sql('PrecioSemana', con = engine, if_exists='append', index=False)
sucursal.to_sql('Sucursal', con = engine, if_exists='append', index=False)
bandera.to_sql('Bandera', con = engine, if_exists='append', index=False)
comercio.to_sql('Comercio', con = engine, if_exists='append', index=False)
lugarSucursal.to_sql('InfoSucursal', con = engine, if_exists='append', index=False)
maximo0426.to_sql('AuxPrecioSemana', con = engine, if_exists='append', index=False)

engine.dispose()