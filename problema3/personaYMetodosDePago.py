"""
Dado un dataset que contenga entradas con la forma “persona;método_pago;dinero_gastado”, 
crea un programa llamado personaYMetodosDePago que: 

a) Por cada persona indique en cuántas compras pagó más de 1500 euros con un medio de pago diferente a tarjeta de crédito. 
    La solución se tiene que guardar en un archivo llamado comprasSinTDCMayorDe1500. 
    
b) Por cada persona indique en cuántas compras pagó menos o igual a 1500 euros con un medio de pago 
    diferente a tarjeta de crédito. La solución se tiene que guardar en un archivo llamado comprasSinTDCMenoroIgualDe1500.
"""

from pyspark import SparkContext, SparkConf

# Crear un contexto de Spark
conf = SparkConf().setAppName("problema3")
sc = SparkContext(conf=conf)

# Constantes
PATH_INPUT = './problema3/casoDePrueba3.txt'
PATH_OUT = './problema3/output'


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


def processComprasSinTDCMayorDe1500(rdd):
    """Procesa las compras sin TDC mayores a 1500"""
    
    # aplico funcion de mapeo
    process_rdd = rdd.map(lambda x: (x[0], 1 if x[1]!='Tarjeta de crédito' and x[2]>1500 else 0))
    
    # agrupamiento
    group_rdd = process_rdd.reduceByKey(lambda x,y: x+y)
    return group_rdd

def processcomprasSinTDCMenoroIgualDe1500(rdd):
    """Procesa las compras sin TDC menores o iguales a 1500"""
    
    # aplico funcion de mapeo
    process_rdd = rdd.map(lambda x: (x[0], 1 if x[1]!='Tarjeta de crédito' and x[2]<=1500 else 0))
    
    # agrupamiento
    group_rdd = process_rdd.reduceByKey(lambda x,y: x+y)
    return group_rdd

def main():
    
    # 1. lectura de datos
    input_rdd = sc.textFile(PATH_INPUT)
    
    # 2. Procesamiento de datos
    
    # 2.1 Convirtiendo datos en tupla
    tuple_rdd = (input_rdd
        .map(lambda line: line.split(";"))
        .map(lambda x: (x[0], x[1], float(x[2])))
    )
    # 2.2 Procesando compras sin TDC mayores a 1500
    compras_rdd = processComprasSinTDCMayorDe1500(tuple_rdd)

    # 2.3 Procesando compras sin TDC menores o iguales a 1500
    reduce_rdd = processcomprasSinTDCMenoroIgualDe1500(tuple_rdd)
    
    # 3. Escritura de datos
    writeRddAsText(compras_rdd, f"{PATH_OUT}/comprasSinTDCMayorDe1500")
    writeRddAsText(reduce_rdd, f"{PATH_OUT}/comprasSinTDCMenoroIgualDe1500")


if __name__ == '__main__':
    main()