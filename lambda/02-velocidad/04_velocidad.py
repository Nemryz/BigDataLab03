"""Capa de velocidad del Laboratorio 03, con Spark Structured Streaming.

Este es el brazo rápido de la arquitectura Lambda, el que responde apenas el dato aparece en el topic y sin esperar a que termine ningún lote.

Lee el topic desde el principio, le quita la capa JSON al mensaje y agrupa las horas contaminadas en episodios usando una ventana de sesión.

La ventana de sesión es el corazón del archivo y conviene entenderla antes de mirar el resto.

Una ventana de sesión agrupa los eventos que llegan seguidos y se cierra recién cuando pasa el tiempo de separación sin que llegue nada más, de modo que la agrupación no tiene un tamaño fijo sino que crece con el dato.

Eso encaja con este dominio, porque las horas malas de contaminación vienen en rachas y una racha es literalmente una sesión.

Con una ventana fija de una hora cada hora suelta sería un episodio, o sea la racha se cortaría en la mitad, en cambio con la de sesión el episodio se arma solo y termina justo donde el aire deja de estar sucio.

Se agrupa por ciudad, así que las rachas de Santiago no se mezclan con las de Mendoza.

Solo entran las lecturas que superan el umbral, porque una sesión armada con horas limpias no significaría nada y terminaría siendo una única sesión enorme por ciudad.

La marca de agua va cuatro horas por delante de la hora de lectura, que es lo que le permite a Spark olvidar la memoria de las sesiones que ya no van a crecer más.

Ese retardo es el doble del espacio entre sesiones a propósito, porque si la marca de agua quedara más cerca que el espacio la ventana podría cerrar una sesión que todavía estaba abierta.

El disparador availableNow hace que Spark se coma todo lo que haya en el topic en lotes sucesivos y después se detenga solo, que es lo que queremos para una corrida de laboratorio reproducible.

Cada lote le saca a Kafka como máximo cien mensajes, y ese tope está puesto para que la corrida se vea repartida en varios lotes y se pueda observar cómo las sesiones crecen de uno a otro en lugar de aparecer todas completas de una.

El resultado se escribe en Parquet dentro de la carpeta de salidas, y como el modo de salida es el completo, cada lote vuelve a mandar todas las sesiones que siguen vivas.

De esa manera una sesión aparece desde su primera hora y se reescribe cuantas veces haga falta con más lecturas encima, hasta que la marca de agua la da por terminada y la borra de la memoria.

El modo de actualización se probó primero y Spark lo rechaza, porque la ventana de sesión se apoya en la marca de agua y ese modo no sabe cuándo cerrar la fila.

El modo de anexar se descarta también, porque solo suelta una sesión cuando la marca de agua ya pasó de su fin y como esa marca va cuatro horas por detrás del dato, las sesiones de las últimas horas del topic no se escribirían nunca.

Por eso el paso siguiente, el script 05_resumen_velocidad, se encarga de quedarse con la versión más avanzada de cada episodio y de escribir el CSV que sí se versiona en el repositorio.

Uso:
  04_velocidad.py [--limpiar] [--topic T]
"""

import argparse
import os
import shutil
import sys

# Agregamos la carpeta de los módulos compartidos al camino de búsqueda, porque este script vive dos niveles más abajo de la raíz y Python no la encuentra sola.
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "src", "comun"))

import config
from evidencia import imprimir_header, iniciar_log
from fuentes import CONTAMINANTES
from spark_session import get_spark

from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, DoubleType, LongType, StringType, StructField, StructType

# Donde quedan las sesiones que va escribiendo cada lote.
SALIDA_PARQUET = config.SALIDAS / "velocidad" / "parquet"

# Donde Spark recuerda hasta qué offset leyó, y sin esto cada corrida volvería a leer el topic entero.
CHECKPOINT = config.CHECKPOINTS / "velocidad"

# El tiempo que puede pasar entre dos lecturas seguidas de una misma racha antes de dar la racha por terminada.
ESPACIO_SESION = "2 hours"

# Cuánto puede pasar después de la última lectura de una sesión antes de que Spark la dé por cerrada y la olvide.
MARCA_AGUA = "4 hours"


def _esquema_evento():
    """Devuelve el esquema con el que se desarma el JSON que manda el productor.

    Se declara campo por campo y no se deduce del primer lote, porque la deducción depende de lo que haya en el topic en ese instante y un lote temprano podría venir sin contaminantes.

    Los nombres de los contaminantes salen de la misma lista que arma el productor, de modo que el día que cambie el contrato del evento esta capa se entera sola."""
    contaminantes = [StructField(nombre, DoubleType()) for nombre in CONTAMINANTES]
    return StructType([
        StructField("ciudad", StringType()),
        StructField("ciudad_nombre", StringType()),
        StructField("hora_lectura", StringType()),
        *contaminantes,
        StructField("umbral", DoubleType()),
        StructField("supera_umbral", BooleanType()),
        StructField("evento_id", LongType()),
        StructField("emitido_utc", StringType()),
        StructField("broker", StringType()),
        StructField("topic", StringType()),
    ])


def _armar_flujo(spark, topic):
    """Devuelve el flujo de episodios que se lee del topic, armado pero todavía sin correr.

    La hora de lectura viene como texto porque ese es el formato en que la trae la foto, y se castea a timestamp ahí mismo porque la marca de agua y la ventana de sesión solo trabajan con fechas de verdad.

    El filtro de la hora nula es una red de seguridad, un mensaje con la hora rota no armaría una sesión sino que la envenenaría.

    El tope de mensajes por lote se pide en la lectura y no en la consulta, porque es ahí donde Kafka entrega de a trozos."""
    crudo = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", config.KAFKA_BROKER)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", config.MAX_OFFSETS_POR_LOTE)
        .load()
    )

    eventos = (
        crudo.select(F.from_json(F.col("value").cast("string"), _esquema_evento()).alias("evento"))
        .select("evento.*")
        .withColumn("hora_lectura", F.col("hora_lectura").cast("timestamp"))
        .filter(F.col("supera_umbral") & F.col("hora_lectura").isNotNull())
    )

    agrupado = (
        eventos.withWatermark("hora_lectura", MARCA_AGUA)
        .groupBy(F.col("ciudad"), F.session_window("hora_lectura", ESPACIO_SESION))
        .agg(
            F.count(F.lit(1)).alias("lecturas"),
            F.max("umbral").alias("umbral"),
            *[F.max(nombre).alias(f"{nombre}_max") for nombre in CONTAMINANTES],
            *[F.avg(nombre).alias(f"{nombre}_prom") for nombre in CONTAMINANTES],
        )
    )

    return (
        agrupado
        .withColumn("inicio", F.col("session_window").getField("start"))
        .withColumn("fin", F.col("session_window").getField("end"))
        .withColumn("duracion_horas", (F.unix_timestamp("fin") - F.unix_timestamp("inicio")) / F.lit(3600.0))
        .select(
            "ciudad",
            "inicio",
            "fin",
            "duracion_horas",
            "lecturas",
            "umbral",
            *[f"{nombre}_max" for nombre in CONTAMINANTES],
            *[f"{nombre}_prom" for nombre in CONTAMINANTES],
        )
    )


def _escribir_lote(lote, numero, estado):
    """Guarda las sesiones que siguen vivas al cierre de un lote y dice cuántas fueron.

    En el modo completo Spark le entrega todas las sesiones que todavía no expiraron, no solo las que cambiaron, así que el archivo de cada lote es una foto del estado y no un simple delta.

    Por eso se agrega encima de lo anterior y no se sobrescribe, de manera que los lotes previos sigan ahí y el resumen de la corrida pueda quedarse con la versión más avanzada de cada episodio."""
    lote.cache()
    filas = lote.count()
    estado["lote"] += 1
    estado["filas"] += filas

    print(f"Lote {estado['lote']:03d}  sesiones vivas {filas:4d}  acumulado {estado['filas']:5d}", flush=True)
    lote.write.mode("append").parquet(str(SALIDA_PARQUET))
    lote.unpersist()


def _borrar(ruta):
    """Borra una carpeta y devuelve si pudo, para que un fallo no pase inadvertido."""
    if not ruta.exists():
        print(f"  no existia  {ruta}", flush=True)
        return True

    try:
        shutil.rmtree(ruta)
    except OSError as error:
        print(f"  no se pudo borrar  {ruta}  {error}", flush=True)
        return False

    print(f"  borrada  {ruta}", flush=True)
    return True


def _limpiar():
    """Borra el checkpoint y el Parquet de la corrida anterior.

    Se borra el checkpoint también porque si no Spark recuerda que ya leyó el topic entero y en la corrida nueva no vuelve a procesar nada."""
    for ruta in (CHECKPOINT, SALIDA_PARQUET):
        _borrar(ruta)


def _recuento_final(spark):
    """Lee lo que quedó en disco y dice cuántos episodios distintos salieron en total.

    El Parquet guarda el mismo episodio una vez por cada lote en que creció, así que el total se cuenta sobre ciudad y hora de inicio y no sobre filas, que es el mismo criterio que usa el resumen."""
    if not SALIDA_PARQUET.exists():
        print("No quedo nada en disco, el topic estaba vacio", flush=True)
        return

    acumulado = spark.read.parquet(str(SALIDA_PARQUET))
    filas = acumulado.count()
    distintos = acumulado.select("ciudad", "inicio").distinct().count()

    print("", flush=True)
    print(f"Filas en disco  {filas}", flush=True)
    print(f"Episodios       {distintos}", flush=True)


def main():
    """Arranca el streaming, lo deja correr hasta que no quede nada por leer y cierra con el recuento."""
    parser = argparse.ArgumentParser(description="Capa de velocidad con Spark Structured Streaming")
    parser.add_argument("--limpiar", action="store_true", help="borra el checkpoint y el Parquet de la corrida anterior")
    parser.add_argument("--topic", default=config.TOPIC_EVENTOS, help="topic del que se lee")
    args = parser.parse_args()

    iniciar_log("04_velocidad")
    imprimir_header("04_velocidad.py")

    if args.limpiar:
        print("", flush=True)
        print("Corrida anterior", flush=True)
        _limpiar()
    SALIDA_PARQUET.parent.mkdir(parents=True, exist_ok=True)

    print("", flush=True)
    print(f"Topic      {args.topic}", flush=True)
    print(f"Broker     {config.KAFKA_BROKER}", flush=True)
    print(f"Espacio    {ESPACIO_SESION}", flush=True)
    print(f"Marca agua {MARCA_AGUA}", flush=True)
    print(f"Por lote   {config.MAX_OFFSETS_POR_LOTE} mensajes", flush=True)

    spark = get_spark("lab03-velocidad", con_kafka=True)
    episodios = _armar_flujo(spark, args.topic)
    estado = {"lote": 0, "filas": 0}

    def al_lote(lote, numero):
        """Llama al escritor de lotes con el estado de la corrida a mano."""
        _escribir_lote(lote, numero, estado)

    consulta = (
        episodios.writeStream
        .outputMode("complete")
        .option("checkpointLocation", str(CHECKPOINT))
        .trigger(availableNow=True)
        .foreachBatch(al_lote)
        .start()
    )

    print("", flush=True)
    print("Streaming en marcha", flush=True)
    consulta.awaitTermination()

    print("", flush=True)
    print(f"Lotes       {estado['lote']}", flush=True)
    print(f"Filas       {estado['filas']}  con episodios repetidos entre lotes", flush=True)
    _recuento_final(spark)

    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
