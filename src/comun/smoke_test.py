"""Prueba de humo del entorno, o sea la verificación de que todo lo instalado funciona junto.

Antes de construir un pipeline encima conviene comprobar que las piezas están bien colocadas, y esto lo hace en orden de menor a mayor dificultad para que cuando algo falle se sepa en qué punto pasó. Si la máquina virtual no levanta, el primer renglón ya dice que falló la versión de Java. 

Si el Parquet no se puede escribir, el quinto avisa que el problema es de permisos y no de la consulta. Ese orden existe porque el error más confuso de los que vimos en este laboratorio fue el de los permisos, que se hace pasar por un error de dirección mal escrita.

Cada prueba dice qué esperaba y qué obtuvo, porque un log que solo dice que pasó no sirve para saber después si el resultado era el correcto o simplemente un número cualquiera. Por eso el script compara contra un valor esperado en vez de solo imprimir.

Al final escribe un archivo Parquet de prueba en una carpeta temporal, lo lee de vuelta y lo borra, para que no ensucie la carpeta de datos con la verificación."""

import os
import shutil
import sys

# Agregamos la carpeta de los módulos compartidos al camino de búsqueda, porque este script

# vive en la misma carpeta que ellos y Python no la agrega solo cuando se ejecuta suelto
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import config
from evidencia import imprimir_header
from spark_session import get_spark

# La carpeta donde se escribe la prueba, y lleva un guion al final para que no se confunda

# con la foto cruda si alguien revisa la carpeta antes de que el script termine
CARPETA_PRUEBA = config.DATOS / "_prueba_fum"


def _comprobar(nombre, obtenido, esperado, pruebas):
    #Compara lo obtenido contra lo esperado, lo imprime y anota si pasó. 
    coincide = obtenido == esperado
    pruebas.append(coincide)
    print(f"{nombre}: obtenido {obtenido}, esperado {esperado}", flush=True)
    return coincide


def main():
    # Ejecuta todas las pruebas en orden y devuelve cero solo si todas pasaron. 
    imprimir_header("smoke_test.py")
    config.asegurar_carpetas()
    pruebas = []

    spark = get_spark("lab03-smoke-test")
    try:
        print("", flush=True)
        print(f"Spark {spark.version}", flush=True)
        print(f"Java {spark._jvm.java.lang.Runtime.getRuntime().version()}", flush=True)
        print(f"Maestro {spark.sparkContext.master}", flush=True)

        print("", flush=True)
        disponible = spark._jvm.org.apache.hadoop.io.nativeio.NativeIO.isAvailable()
        print(f"hadoop.home.dir {spark._jvm.System.getProperty('hadoop.home.dir')}", flush=True)
        _comprobar("Biblioteca nativa de Hadoop", disponible, True, pruebas)

        print("", flush=True)
        df = spark.range(100).selectExpr("id", "id * 3 AS triple")
        _comprobar("Filas generadas", df.count(), 100, pruebas)
        _comprobar("Suma de la columna", df.agg({"triple": "sum"}).collect()[0][0], 14850, pruebas)

        print("", flush=True)
        ruta_parquet = str(CARPETA_PRUEBA / "parquet").replace("\\", "/")
        df.write.mode("overwrite").parquet(ruta_parquet)
        leido = spark.read.parquet(ruta_parquet)
        _comprobar("Filas releyas de Parquet", leido.count(), 100, pruebas)
        _comprobar("Total releyo de Parquet", leido.agg({"triple": "sum"}).collect()[0][0], 14850, pruebas)

        tabla = leido.toPandas()
        _comprobar("Columnas de la tabla de pandas", len(tabla.columns), 2, pruebas)

        print("", flush=True)
        ruta_csv = str(CARPETA_PRUEBA / "csv").replace("\\", "/")
        df.write.mode("overwrite").option("header", True).csv(ruta_csv)
        partes = [f for f in os.listdir(CARPETA_PRUEBA / "csv") if f.startswith("part-") and f.endswith(".csv")]
        _comprobar("Archivos de CSV escritos", len(partes) > 0, True, pruebas)

        print("", flush=True)
        superadas = sum(1 for p in pruebas if p)
        print(f"Pruebas superadas {superadas} de {len(pruebas)}", flush=True)
        print("SMOKE TEST: OK" if all(pruebas) else "SMOKE TEST: FALLO", flush=True)
    finally:
        spark.stop()
        shutil.rmtree(CARPETA_PRUEBA, ignore_errors=True)
        print(f"Carpeta de prueba borrada {CARPETA_PRUEBA}", flush=True)

    return 0 if all(pruebas) else 1


if __name__ == "__main__":
    sys.exit(main())
