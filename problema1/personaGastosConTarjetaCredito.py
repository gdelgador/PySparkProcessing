"""
Dado un dataset que contenga entradas con la forma “persona;método_pago;dinero_gastado”, 
crea un programa llamado personaGastosConTarjetaCredito que para cada persona indique 
la suma del dinero gastado con tarjeta de crédito, con el formato persona;gastoconTDC.
"""
from pyspark import SparkContext, SparkConf

# Crear un contexto de Spark
conf = SparkConf().setAppName("problema1")
sc = SparkContext(conf=conf)

# Constantes -- cambiar según requerimiento
PATH_INPUT = './problema1/casoDePrueba1.txt'
PATH_OUT = './problema1/output/personaGastosConTarjetaCredito'


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
    
    # 1. lectura de datos
    input_rdd = sc.textFile(PATH_INPUT)
    
    # 2. Procesamiento de datos
    # 2.1 Convirtiendo datos en tupla
    tuple_rdd = (input_rdd
        .map(lambda line: line.split(";"))
        .map(lambda x: (x[0], x[1], float(x[2])))
    )
    # 2.2 Seleccionamos persona y gasto en caso este sea con TDC en otro caso se coloca 0
    process_rdd = tuple_rdd.map(lambda x: (x[0], x[2] if x[1] == 'Tarjeta de crédito' else 0))

    # 2.3 Sumarizamos los gastos por persona
    reduce_rdd = process_rdd.reduceByKey(lambda x, y: x + y)
    
    # 3. Escritura de datos
    writeRddAsText(reduce_rdd, PATH_OUT)
    pass

if __name__ == '__main__':
    main()