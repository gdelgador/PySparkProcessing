"""
spark-submit problema2/categoriaDeVideosMenosVista.py ./problema2/dataset/0301.zip ./problema2/output
"""
from pyspark import SparkContext, SparkConf
from zipfile import ZipFile
import os
import sys
import shutil

# Crear un contexto de Spark
conf = SparkConf().setAppName("problema2")
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
    
    
    # lectura de los archivos, eliminando el archivo log.txt
    rdd = sc.wholeTextFiles(os.path.join(folder_path,'data', filename))
    rdd = rdd.filter(lambda x: "log.txt" not in x[0])

    # separando por lineas
    rdd_lines = rdd.flatMap(lambda x: x[1].splitlines())
    
    return rdd_lines
    
    
def writeRddAsText(rrd ,file_path:str):
    """Escribe un rdd en un archivo de texto según una ruta data"""
    import shutil
    
    # Eliminamos directorio si existe
    shutil.rmtree(file_path, ignore_errors=True)
    
    # Generamos nuevo rdd con separador
    rdd_save = rrd.map(lambda x: f"{x[0]};{x[1]}")

    # almacenamos data en path
    rdd_save.saveAsTextFile(file_path)
    pass


def main():
    
    # 1. Validaciones internas
    if len(sys.argv) != 3:
        print("Usage: CategoriaDeVideosMenosVista.py <dataset_path> <output_path>")
        sys.exit(1)

    # 2. Recuperando parametros
    dataset_path = sys.argv[1]
    output_path = sys.argv[2]
    
    # 3. lectura dataset
    input_rdd = read_rdd_from_dataset(dataset_path)
    
    # 4. Lectura manteniendo columnas de interes: categoria y nro de visitas
    select_rdd = (
        input_rdd
        .map(lambda line: line.split("\t"))
        .filter(lambda fields: len(fields) > 5) # quito nulos
        .map(lambda fields: (fields[3], int(fields[5])))
    )
    
    # 5. Agrupo por categoria y sumo las visitas
    grouped_rdd = select_rdd.reduceByKey(lambda a, b: a + b)

    # 6. Encontrar la categoria con menos visitas -> retorna una lista con la categoria y el nro de visitas
    min_category = grouped_rdd.takeOrdered(1, key=lambda x: x[1])
    
    # 7. Escribir el resultado en un archivo de texto
    writeRddAsText(sc.parallelize(min_category) ,output_path)
    pass

if __name__ == '__main__':
    main()
    pass
