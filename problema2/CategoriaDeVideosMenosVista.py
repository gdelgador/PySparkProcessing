from pyspark import SparkContext, SparkConf

# Crear un contexto de Spark
conf = SparkConf().setAppName("problema3")
sc = SparkContext(conf=conf)

# Constantes
INPUT_DATASET = './problema2/dataset'
PATH_OUT = './problema2/output'


def input_dataset_path():
    
    pass



def main():
    
    # lectura dataset
    
    
    
    pass


if __name__ == '__main__':
    main()
    pass


# Leer los datos
rdd = sc.textFile("/ruta/a/la/carpeta/de/entrada/*.txt")

# Dividir cada línea por tabulaciones y seleccionar solo las columnas de categoría y visitas
rdd = rdd.map(lambda line: line.split("\t")).map(lambda fields: (fields[4], int(fields[6])))

# Agrupar por categoría y sumar las visitas
rdd_grouped = rdd.reduceByKey(lambda a, b: a + b)

# Encontrar la categoría con menos visitas
min_category = rdd_grouped.reduce(lambda a, b: a if a[1] < b[1] else b)

# Guardar el resultado
sc.parallelize([min_category]).saveAsTextFile("/ruta/a/la/carpeta/de/salida")

# Detener el contexto de Spark
sc.stop()