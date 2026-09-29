r"""Aplana el JSON arruinado y deja la zona bronze del lago en Parquet.

Bronze es la primera zona tabular del lago y su trabajo es uno solo, recibir el dato como
vino y dejarlo en filas. No limpia nada, no cambia ningún tipo y no tira ninguna fila, y
esa es la idea detrás de la zona. Si acá se limpiara, no tendríamos forma de comparar
cuánto se ganó después, porque el dato original ya no estaría. Por eso la zona bronze
guarda también los valores que no tienen sentido, y esos se van a contar pero no se van
a borrar, que es la diferencia entre limpiar y descartar.

El aplanado sí es trabajo de esta etapa, porque el JSON viene anidado. El servidor
devuelve un objeto por ciudad, y dentro un arreglo con el nombre del contaminante, y
dentro un arreglo con los valores, y adentro la hora. O sea que una lectura que en
concepto es una sola, en el archivo son cuatro niveles. Llevarlo a filas es lo que hace
que el resto del pipeline pueda trabajar con SQL en vez de tener que recorrer listas.

El nombre de la ciudad no viene en el archivo, viene la latitud y la longitud, así que
acá se cruzan contra el catálogo para poner el nombre. Es un detalle chico pero es el
tipo de cosa que hace que un informe se entienda, porque ver dieciocho grados latitud no
dice nada y ver Santiago sí.

El archivo de entrada sucio tiene 24 horas repetidas y eso no se saca acá, queda en bronze
como prueba. Duplicar es un problema de calidad de datos, no un error de formato, y el
paso siguiente lo detecta y lo resuelve.
"""

import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "src", "comun"))

import config
import fuentes
from evidencia import imprimir_header
from spark_session import get_spark

# El archivo sucio que dejó el paso anterior y la zona donde se escribe el resultado
ORIGEN = config.DATOS / "raw" / "aire_horario_sucio.json"
DESTINO = config.DATOS / "bronze" / "lecturas"


def _nombre_de_ciudad(latitude, longitude):
    """Cruza las coordenadas contra el catálogo para devolver el nombre de la ciudad.

    Se usa la distancia más cercana y no la igualdad exacta porque el servidor redondea las
    coordenadas a unos pocos decimales, o sea que el número que nos llega nunca es
    idéntico al que escribimos en el catálogo. Con la distancia nos alcanza, porque las
    ciudades del proyecto están a cientos de kilómetros unas de otras.
    """
    mejor = None
    mejor_distancia = None
    for clave, ciudad in fuentes.CIUDADES.items():
        distancia = abs(ciudad["latitude"] - latitude) + abs(ciudad["longitude"] - longitude)
        if mejor_distancia is None or distancia < mejor_distancia:
            mejor_distancia = distancia
            mejor = clave
    return mejor


def main():
    """Aplana el JSON a filas, agrega el nombre de la ciudad y escribe la zona bronze."""
    imprimir_header("02_ingesta.py")
    config.asegurar_carpetas()

    if not ORIGEN.exists():
        print(f"No existe {ORIGEN}, corre ensuciar.py primero")
        return 1

    print("")
    print(f"Origen  {ORIGEN.name}  {ORIGEN.stat().st_size} bytes")
    datos = json.loads(ORIGEN.read_text(encoding="utf-8"))

    # Aplanar a mano y no con la función de aplanado de Spark, porque Spark aplana
    # objetos pero no dos listas de distinta longitud que son las que tenemos acá, y
    # porque hacerlo a mano deja el código a la vista y se ve exactamente qué se le hizo
    filas = []
    for ciudad in datos:
        clave_ciudad = _nombre_de_ciudad(ciudad["latitude"], ciudad["longitude"])
        horario = ciudad["hourly"]
        horas = horario["time"]
        contaminantes = [c for c in horario if c != "time"]
        for indice, hora in enumerate(horas):
            fila = {
                "ciudad": clave_ciudad,
                "latitud": ciudad["latitude"],
                "longitud": ciudad["longitude"],
                "hora": hora,
                "utc_offset_seconds": ciudad.get("utc_offset_seconds"),
            }
            for contaminante in contaminantes:
                valores = horario[contaminante]
                fila[contaminante] = valores[indice] if indice < len(valores) else None
            filas.append(fila)

    print(f"Ciudades {len(datos)}")
    print(f"Filas     {len(filas)}")
    print("Columnas  " + ", ".join(sorted(filas[0].keys())))
    print("")

    spark = get_spark("lab03-02-ingesta")
    try:
        df = spark.createDataFrame(filas)
        print(f"Schema antes de guardar, tipos todos cadena porque asi vinieron")
        df.printSchema()
        print("")

        # Bronze se escribe sin particionar y en sobrescritura, porque todavia no sabemos
        # bien por que conviene partir. Particionar antes de limpiar, cuando la ciudad y
        # la fecha todavia pueden venir sucias, parte el archivo por cualquier lado
        df.write.mode("overwrite").parquet(str(DESTINO).replace("\\", "/"))

        leido = spark.read.parquet(str(DESTINO).replace("\\", "/"))
        print(f"Guardado  {DESTINO}")
        print(f"Releido  {leido.count()} filas")
        print("")

        # El recuento de defectos se deja aca porque es el dato que despues va al informe
        # para mostrar cuanto gano la limpieza, y para eso hay que medirlo antes
        from pyspark.sql import functions as f

        resumen = leido.select(
            f.count("*").alias("filas"),
            f.countDistinct("ciudad").alias("ciudades"),
            f.countDistinct("hora").alias("horas_distintas"),
            f.sum(f.when(f.col("pm2_5").isNull(), 1).otherwise(0)).alias("pm25_nulos"),
            f.sum(f.when(f.col("pm10").isNull(), 1).otherwise(0)).alias("pm10_nulos"),
            f.sum(f.when(f.col("nitrogen_dioxide").isNull(), 1).otherwise(0)).alias("no2_nulos"),
        ).collect()[0]
        print("Defectos que trae bronze")
        print(f"  filas                 {resumen['filas']}")
        print(f"  ciudades              {resumen['ciudades']}")
        print(f"  horas distintas       {resumen['horas_distintas']}")
        print(f"  pm2_5 nulos           {resumen['pm25_nulos']}")
        print(f"  pm10 nulos            {resumen['pm10_nulos']}")
        print(f"  nitrogen_dioxide nulos {resumen['no2_nulos']}")
        # Las horas repetidas se calculan contra el total esperado, que son las horas
        # distintas por cada ciudad. Restar solo lo que hay menos lo distinto daria un
        # numero que no significa nada, porque las horas se repiten por ciudad
        esperadas = resumen["horas_distintas"] * resumen["ciudades"]
        print(f"  horas repetidas       {resumen['filas'] - esperadas}")
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
