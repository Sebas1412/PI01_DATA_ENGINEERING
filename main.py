import pandas as pd
from sqlalchemy import create_engine
def incremental(rutaArchivo):
    if rutaArchivo[-1] == 't':
        precios_txt = pd.read_csv(f'{rutaArchivo}',  sep= '|')
        precios_txt = precios_txt.rename(columns= {'sucursal_id': 'idSucursal', 'producto_id': 'idProducto'})
        precios_txt.drop_duplicates(inplace=True)
        precios_txt.drop(precios_txt[precios_txt.idSucursal.isnull() == True].index, inplace=True)
        precios_txt.precio.fillna(0.0, inplace=True)
        for i in range(1,3):
            precios_txt.iloc[:,i] = precios_txt.iloc[:,i].astype('string')
        engine = create_engine("mysql+pymysql://{user}:{pw}@localhost/{db}"
                        .format(user="root",
                                pw="MYSQL2022$",
                                db="ProyectoIndividual04"))
        precios_txt.to_sql('precioSemana', con = engine, if_exists='append', index=False)
        engine.dispose()

    elif rutaArchivo[-1] == 'v':
        precios_csv = pd.read_csv(f'{rutaArchivo}',  encoding= 'utf_16_le')
        precios_csv = precios_csv.rename(columns= {'sucursal_id': 'idSucursal', 'producto_id': 'idProducto'})
        precios_csv.drop_duplicates(inplace= True)
        sem0413SinValor = precios_csv[precios_csv.idProducto.isnull() == True]
        sinValor0413 = list(sem0413SinValor.index)
        precios_csv = precios_csv.drop(sinValor0413)
        for i in range(1,3):
            precios_csv.iloc[:,i] = precios_csv.iloc[:,i].astype('string')
        engine = create_engine("mysql+pymysql://{user}:{pw}@localhost/{db}"
                        .format(user="root",
                                pw="MYSQL2022$",
                                db="ProyectoIndividual04"))
        precios_csv.to_sql('precioSemana', con = engine, if_exists='append', index=False)
        engine.dispose()   

    elif rutaArchivo[-1] == 'x':
        precios_excel = pd.read_csv(f'{rutaArchivo}')
        precios_excel = precios_excel.rename(columns= {'sucursal_id': 'idSucursal', 'producto_id': 'idProducto'})
        precios_excel = precios_excel[['precio', 'idProducto','idSucursal']]
        precios_excel = precios_excel.drop(precios_excel[precios_excel.precio > 1e+15].index)
        precios_excel.precio.fillna(0.0, inplace=True)
        precios_excel.idProducto.fillna(0.0, inplace=True)
        for i in range(1,3):
            precios_excel.iloc[:,i] = precios_excel.iloc[:,i].astype('string')
        precios_excel.idProducto = precios_excel.idProducto.str[:-2]
        precios_excel.idProducto= precios_excel.idProducto.str.zfill(13)
        engine = create_engine("mysql+pymysql://{user}:{pw}@localhost/{db}"
                        .format(user="root",
                                pw="MYSQL2022$",
                                db="ProyectoIndividual04"))
        precios_excel.to_sql('precioSemana', con = engine, if_exists='append', index=False)
        engine.dispose()   

    elif rutaArchivo[-1] == 'n':
        precios_json = pd.read_csv(f'{rutaArchivo}')   
        precios_json = precios_json.rename(columns= {'sucursal_id': 'idSucursal', 'producto_id': 'idProducto'})
        precios_json.precio = precios_json.precio.replace('',0).astype('float')
        for i in range(1,3):
            precios_json.iloc[:,i] = precios_json.iloc[:,i].astype('string')
        engine = create_engine("mysql+pymysql://{user}:{pw}@localhost/{db}"
                        .format(user="root",
                                pw="MYSQL2022$",
                                db="ProyectoIndividual04"))
        precios_json.to_sql('precioSemana', con = engine, if_exists='append', index=False)
        engine.dispose()    



incremental('DTS-04\Proyectos Individuales\PI01_DATA_ENGINEERING\Datasets\Datasets relevamiento precios\precios_semana_20200518.txt')

