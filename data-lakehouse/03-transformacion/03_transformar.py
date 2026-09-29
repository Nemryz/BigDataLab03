r"""Limpia la zona bronze, la pasa a silver y calcula el indicador de episodio.

Esta es la etapa donde de verdad se limpia, y es donde se nota lo que vale tener un lago de
datos con capas. Bronze tiene todo lo que llegó, con los nulos, con los textos donde
debería haber números y con las horas repetidas. Silver es la misma información pero ya
ordenada, tipada y sin basura, y es la zona sobre la que después se consulta.

Se hacen seis cosas, y en este orden porque unas dependan de otras. Primero se sacan las
horas repetidas, porque si no, todo lo demás se calcula dos veces. Después se castea cada
columna de contaminante a número, y el truco está en que se castea con un valor de relleno
para los textos rotos, porque si se pide el número directo Spark falla con la primera fila
que no es número y no llegas a ver el resto. Tercero se borran los negativos, que no tienen
sentido físico. Cuarto se convierte la hora a una columna de fecha de verdad, y de paso se
corrige el desfase de zona horaria del servidor, que viene en otro campo. Quinto se separa
la fecha de la hora, porque partir por fecha y no por la hora completa hace que el lago
tenga carpetas manejables en vez de miles. Sexto, y esto es lo importante, se calcula la
columna que dice si esa hora fue mala.

Esa última columna es el corazón del proyecto. Una hora está contaminada cuando el PM2.5
iguala o pasa el umbral de 25 microgramos por metro cúbico, que es el valor que la propia
Organización Mundial de la Salud usa como límite diario. Con esa columna marcada, más
adelante se puede preguntar cuántas horas malas hubo por ciudad y agrupar las horas malas
consecutivas, que es un episodio. Esa agrupación es exactamente la ventana de sesión de
Kappa, así que aunque hagamos solo Lakehouse, el indicador que queda acá es el que después
se compara con las otras dos arquitecturas.

Silver se escribe partido por ciudad y por fecha, que es la forma en que un lago de datos
real se parte. Partir es dejar de guardar todo junto y separarlo en carpetas por un valor
que casi siempre se filtra, para que cuando se consulta una ciudad no haya que leer la de
al lado. Con cuatro ciudades y siete días son veintiocho carpetas, que es un tamaño sano.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "src", "comun"))

import config
import fuentes
from evidencia import imprimir_header
from spark_session import get_spark

ORIGEN = config.DATOS / "bronze" / "lecturas"
DESTINO = config.DATOS / "silver" / "lecturas_limpias"

# El relleno con el que se castea el texto a número. Se usa menos uno en vez de cero
# porque un PM2.5 negativo no tiene sentido, y con este valor la regla del rango de más
# abajo lo borra junto con los negativos que venían del sensor, así que queda limpio
RELLENO_PARA_TEXTO_ROTO = -1.0

# Los tres contaminantes que se limpian, y que están en el catálogo
CONTAMINANTES = tuple(fuentes.CONTAMINANTES.keys())


def main():
    """Limpia bronze, calcula el indicador de episodio y escribe silver particionado."""
    imprimir_header("03_transformar.py")
    config.asegurar_carpetas()

    if not ORIGEN.exists():
        print(f"No existe la zona bronze en {ORIGEN}, corre 02_ingesta.py primero")
        return 1

    spark = get_spark("lab03-03-transformar")
    try:
        from pyspark.sql import functions as f
        from pyspark.sql import types as t

        print("")
        print(f"Origen   {ORIGEN}")
        print(f"Destino  {DESTINO}")
        print(f"Umbral   {fuentes.UMBRAL_EPISODIO} ug/m3 para PM2.5")
        print("")

        df = spark.read.parquet(str(ORIGEN).replace("\\", "/"))
        antes = df.count()
        print(f"Filas en bronze  {antes}")
        print("")

        # Uno, quitar las horas repetidas. Se deja la primera aparición y se van las demás,
        # que es lo que haría un sistema real que recibe el mismo evento dos veces
        df = df.dropDuplicates(subset=["ciudad", "hora"])
        sin_repetidas = df.count()
        print(f"Despues de quitar horas repetidas  {sin_repetidas}, se fueron {antes - sin_repetidas}")

        # Dos, castear los textos a número. El valor de relleno evita que el cast reviente
        # con la primera fila mala, y los que quedan con relleno se anulan más abajo
        for columna in CONTAMINANTES:
            df = df.withColumn(columna, f.col(columna).cast(t.DoubleType()))

        # Tres, resolver lo que quedó sin sentido físico. Solo el PM2.5 puede tirar una
        # fila entera, porque es el que define el episodio y sin él la fila no sirve para
        # la pregunta que vamos a responder. Los otros dos se anulan si están malos pero
        # la fila se conserva, porque descartar toda la hora por un PM10 que falta tira
        # información que sí sirve, y con los nulos inyectados perdía casi un tercio
        # del dataset, que es una pérdida que no se puede justificar
        df = df.filter(f.col("pm2_5").isNotNull() & (f.col("pm2_5") >= 0))
        for columna in ("pm10", "nitrogen_dioxide"):
            df = df.withColumn(columna, f.when(f.col(columna) >= 0, f.col(columna)))
        tipado = df.count()
        nulos_pm25 = sin_repetidas - tipado
        print(f"Despues de tipar y resolver invalidos  {tipado}, filas con PM2.5 inservible {nulos_pm25}")

        # Cuatro, arreglar la hora. El servidor manda la hora local de cada ciudad y la
        # guarda aparte, así que se le resta ese desfase para dejar todo en utc, que es la
        # única forma de comparar horas entre ciudades que están en zonas distintas
        df = df.withColumn(
            "hora_utc",
            f.from_unixtime(
                f.unix_timestamp(f.col("hora"), "yyyy-MM-dd'T'HH:mm") - f.col("utc_offset_seconds")
            ).cast("timestamp"),
        )

        # Cinco, partir la fecha de la hora, porque por fecha son carpetas manejables
        df = df.withColumn("fecha", f.to_date(f.col("hora_utc")))
        df = df.withColumn("hora_del_dia", f.hour(f.col("hora_utc")))

        # Seis, el indicador de episodio. Se marca la hora como mala cuando el PM2.5 pasa
        # el umbral, y de paso se guarda la diferencia con el umbral porque sirve para
        # ordenar los episodios por gravedad sin tener que recalcular nada
        df = df.withColumn("es_episodio", (f.col("pm2_5") >= fuentes.UMBRAL_EPISODIO).cast("int"))
        df = df.withColumn("exceso", f.round(f.col("pm2_5") - fuentes.UMBRAL_EPISODIO, 2))

        # Se dejan solo las columnas que sirven, en un orden fijo, porque un lago de datos
        # con columnas que nadie usa se llena de basura que después nadie sabe borrar
        df = df.select(
            "ciudad", "fecha", "hora", "hora_utc", "hora_del_dia",
            "pm2_5", "pm10", "nitrogen_dioxide", "es_episodio", "exceso",
            "latitud", "longitud",
        )

        print("")
        print("Schema de silver")
        df.printSchema()
        print("")

        df.write.mode("overwrite").partitionBy("ciudad", "fecha").parquet(
            str(DESTINO).replace("\\", "/")
        )

        limpio = spark.read.parquet(str(DESTINO).replace("\\", "/"))
        print(f"Guardado {DESTINO}")
        print(f"Filas  {limpio.count()}")
        print("")

        # Lo que se anuló sin tirar la fila, que es distinto de lo que se tiró, y son los
        # dos números que hay que reportar por separado en el informe
        anulados = limpio.select(
            f.sum(f.when(f.col("pm10").isNull(), 1).otherwise(0)).alias("pm10"),
            f.sum(f.when(f.col("nitrogen_dioxide").isNull(), 1).otherwise(0)).alias("no2"),
        ).collect()[0]
        print(f"Valores anulados conservando la fila  pm10 {anulados['pm10']}  "
              f"nitrogen_dioxide {anulados['no2']}")
        print("")

        # El recuento final es el número que va al informe, el de cuanto se ganó limpiando
        por_ciudad = limpio.groupBy("ciudad").agg(
            f.count("*").alias("horas"),
            f.sum("es_episodio").alias("horas_sobre_umbral"),
            f.round(f.avg("pm2_5"), 2).alias("pm25_promedio"),
            f.round(f.max("pm2_5"), 2).alias("pm25_maximo"),
        ).orderBy("ciudad")
        por_ciudad.show(truncate=False)

        print(f"Horas sobre el umbral en total  {limpio.agg(f.sum('es_episodio').alias('n')).collect()[0]['n']}")
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
