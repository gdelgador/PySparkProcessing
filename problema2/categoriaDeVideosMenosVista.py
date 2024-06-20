from pyspark import SparkContext, SparkConf
from zipfile import ZipFile
import os
import sys
import shutil

# Crear un contexto de Spark
conf = SparkConf().setAppName("problema3")
sc = SparkContext(conf=conf)



def read_rdd_from_dataset(dataset_path:str):
    """Lectura de un rdd desde un archivo zip"""
    
    folder_path = os.path.dirname(dataset_path)
    filename = os.path.basename(dataset_path).split('.')[0]
    
    # 1. Eliminamos la carpeta data si existe
    shutil.rmtree(os.path.join(folder_path, 'data', filename))
    
    # descomprimiendo el archivo
    with ZipFile(dataset_path, 'r') as zip_ref:
        zip_ref.extractall(os.path.join(folder_path, 'data'))
    
    
    # lectura de rdd
    rdd = sc.wholeTextFiles(os.path.join(folder_path,'data', filename))
    rdd = rdd.filter(lambda x: "log.txt" not in x[0])
    
    rdd = rdd.map(lambda line: line[1].split("\t"))
    
    print(rdd.first())
    
    #.map(lambda fields: (fields[4], int(fields[6])))
    # retornando rdd
    
    
    pass



def main():
    
    # Validaciones internas
    if len(sys.argv) != 3:
        print("Usage: CategoriaDeVideosMenosVista.py <dataset_path> <output_path>")
        sys.exit(1)

    # Recuperando parametros
    dataset_path = sys.argv[1]
    output_path = sys.argv[2]
    
    # lectura dataset
    read_rdd_from_dataset(dataset_path)
    
    pass


if __name__ == '__main__':
    main()
    pass


# # Leer los datos
# rdd = sc.textFile("/ruta/a/la/carpeta/de/entrada/*.txt")

# # Dividir cada línea por tabulaciones y seleccionar solo las columnas de categoría y visitas
# rdd = rdd.map(lambda line: line.split("\t")).map(lambda fields: (fields[4], int(fields[6])))

# # Agrupar por categoría y sumar las visitas
# rdd_grouped = rdd.reduceByKey(lambda a, b: a + b)

# # Encontrar la categoría con menos visitas
# min_category = rdd_grouped.reduce(lambda a, b: a if a[1] < b[1] else b)

# # Guardar el resultado
# sc.parallelize([min_category]).saveAsTextFile("/ruta/a/la/carpeta/de/salida")

# # Detener el contexto de Spark
# sc.stop()