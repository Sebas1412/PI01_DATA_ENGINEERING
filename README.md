# Proyecto Individual 1- Data 04- Soy Henry   
## Data Engineering
![image](https://querido-dinero.imgix.net/1397/C%C3%B3mo-transformar-una-idea-en-un-proyecto-real_Portada.png?w=1500&h=600&fit=crop&crop=faces&auto=format,compress&lossless=1)


## ¡ Explicacion del proyecto !
Diagrama de flujo

<img src="_src\assets\Diagrama de flujo.jpg" height="250">

En este primer proyecto lo que se propone es crear una base datos a partir de diferentes
datasets en distintos formatos. Una vez realizada la limpieza, se procede a crear una base de datos en la cual se pueda realizar querys y no se falle en el intento. Por último se pide que se realice una carga incremental de un archivo en formato .txt.

## Pasos seguidos
- Lo primer fue analizar cómo abrir los datasets, para ello se usaron diferentes librerías como pandas y pyarrow 
- Una vez abiertos y convertirdos en dataframes se procedió a cambiar algunos nombres de columnas para trabajar mejor
- Luego se profundizó en cada uno de los df y se procedió a su transformación, se modificaron algunos valores, otros se borraron y otros se guardaron en un dataframe diferente, cosa de que si es necesario, se pueda recuperar el dataframe original
- Ya teniendo los dfs transformados, aquellos que tenían la misma estructura, se decidió concatenarlos excepto el proveniente del .txt
- En el siguiente paso dejamos por un rato python y nos dirigimos a MySQL a realizar las tablas correspondientes para cargar los dataframes
- Ya de vuelta en python se importó la librería sqlalchemy para conectar python con MySQL y hacer la ingesta correspondiente.
- Para comprobar si la ingesta se hizo correctamente se procedió a contar el total de filas a traves de un query y otro en el cual se pide el precio promedio de una sucursal específica
- Luego se procedió a hacer una función en la cual, al poner solamente la ruta de acceso del nuevo datasets se haga la limpieza correspondiente y la ingesta de solamente los nuevos datos y no que se cargue el dataset de 0. La función en el archivo main.py reconoce la última letra de la ruta y entra a la condición correspondiente.
- Por último se volvió a relizar los mismos querys anteriores y comprobar de que sigue funcionando

## Aclaraciones
- El archivo PI_DataEngineer.py es lo mismo que el PI_DataEngineer.ipynb, nada más que solamente contienen los pasos necesarios para hacer las transformaciones a los datasets. No incluye los print que dan información ni los comentarios sobre el funcionamiento de cada código hecho.
- En primera instancia no se ha cargado el archivo .txt (aunque si se realizó en el archivo .ipynb su transformación), porque se entiende que es el que se debe usar para mostrar que se realiza una buena carga incremental.
- El archivo .db se crea una vez que ya se hizo la carga incremental del archivo .txt.
- Se da por sentado que los archivos van a venir en la misma forma que los archivos entregados hasta el momento.