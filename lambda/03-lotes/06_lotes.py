"""Capa de lotes del Laboratorio 03, con Spark en modo batch.

Este es el brazo lento de la arquitectura Lambda, el que mira el topic entero de una sola vez y lo consolida en vistas que ya se pueden consultar sin volver a procesar nada.

Donde la capa de velocidad responde apenas aparece el mensaje, esta espera a que la cola esté completa, recorre el topic de punta a punta y devuelve el panorama de la semana entera.

La lectura se hace como una corrida batch, desde el primer offset hasta el último, así que no hay checkpoint ni estado que guardar entre ejecuciones.

Cada corrida vuelve a leer el topic, recalcula las vistas y las escribe de nuevo en modo sobrescribir, de manera que no hace falta ningún interruptor de limpieza para arrancar de cero.

A diferencia de la capa de velocidad acá no se prefiltra nada, entran las lecturas contaminadas y las limpias juntas, porque el porcentaje de cada grupo se calcula sobre el total.

Salen dos vistas.

La diaria cruza ciudad con día y trae las lecturas del día, cuántas superaron el umbral, el porcentaje que representan y los promedios y picos de cada contaminante.

La por ciudad aplana la semana en una fila por lugar, con los mismos promedios y picos calculados sobre los siete días juntos.

Los promedios se redondean a dos decimales dentro de la propia consulta y no al escribir, porque el número que entra al hash tiene que ser idéntico en cualquier máquina y una diferencia en el séptimo decimal basta para que dos corridas no cuadren.

El porcentaje sale del recuento de horas contaminadas sobre el total de lecturas del grupo, que es la pregunta que de verdad se le hace a cada día y a cada ciudad.

Cada vista se escribe dos veces, en Parquet para que Spark la relea sin parsear y en CSV para que se pueda abrir en cualquier planilla.

Además queda un gráfico con el promedio diario de partículas finas por ciudad, que es la curva que mejor cuenta la semana de un vistazo.

Al final se calcula un SHA-256 por vista con el mismo criterio de campos estables que usan las otras capas, de modo que dos corridas que procesaron el mismo topic dan los mismos hashes.

La suma de horas sobre umbral de la vista por ciudad se imprime junto con las alertas del topic, y las dos tienen que coincidir porque es el mismo conteo hecho por caminos distintos, uno desde los mensajes y otro desde las vistas consolidadas.

Uso:
  06_lotes.py [--topic T]
"""

import argparse
import hashlib
import json
import os
import sys
from datetime import datetime, timezone

# Agregamos la carpeta de los módulos compartidos al camino de búsqueda, porque este script vive dos niveles más abajo de la raíz y Python no la encuentra sola.
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "src", "comun"))

import config
import fuentes
from evidencia import imprimir_header, iniciar_log
from eventos import parsear
from spark_session import get_spark

from pyspark.sql import functions as F

# La carpeta donde quedan las vistas, con el Parquet de las dos y el CSV de cada una al lado.
SALIDA_LOTES = config.SALIDAS / "lotes"

# El Parquet, que es lo que Spark vuelve a leer sin tener que parsear nada.
SALIDA_PARQUET = SALIDA_LOTES / "parquet"

# El gráfico del promedio diario, que se guarda con las demás evidencias y no con los datos pesados.
GRAFICO = config.GRAFICOS / "lotes_diario.png"

# Las medidas que las dos vistas comparten, ya redondeadas dentro de la consulta.
CAMPOS_MEDIDOS = (
    "lecturas",
    "horas_sobre_umbral",
    "pct_sobre_umbral",
    *[f"{nombre}_prom" for nombre in fuentes.CONTAMINANTES],
    *[f"{nombre}_max" for nombre in fuentes.CONTAMINANTES],
)

# Los campos de cada vista que entran al hash, o sea la identidad de la fila y sus medidas.
CAMPOS_DIARIO = ("ciudad", "dia", *CAMPOS_MEDIDOS)
CAMPOS_CIUDADES = ("ciudad", *CAMPOS_MEDIDOS)


def _leer_topic(spark, topic):
    """Devuelve el DataFrame crudo del topic, leído de punta a punta como una corrida batch.

    El primer offset y el último se declaran en la lectura, que es la forma batch de decirle a Spark que se trague la cola entera de una.

    El modo tolerante a pérdidas queda encendido por las dudas, si alguien vació el topic mientras corríamos el dato que falta se ignora en lugar de romper la corrida.

    No hay marca de agua ni disparador porque no hay nada que seguir en vivo, el topic se corta en un instante y con eso alcanza."""
    return (
        spark.read.format("kafka")
        .option("kafka.bootstrap.servers", config.KAFKA_BROKER)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )


def _agregados():
    """Devuelve las columnas de agregación que las dos vistas comparten.

    Las lecturas se cuentan todas, contaminadas o no, porque el porcentaje de cada grupo se calcula sobre ese total.

    Las horas sobre umbral salen de la marca booleana que el productor puso en el evento, sumada como uno y cero, así que no hay que volver a comparar contra el umbral ni acordarse de con cuánto se armó.

    Los promedios se redondean acá mismo a dos decimales, que es donde empieza la reproducibilidad, el número ya llega listo al hash y no depende de cuántos decimales redondee quien escribe."""
    return [
        F.count(F.lit(1)).alias("lecturas"),
        F.sum(F.col("supera_umbral").cast("long")).alias("horas_sobre_umbral"),
        *[F.round(F.avg(nombre), 2).alias(f"{nombre}_prom") for nombre in fuentes.CONTAMINANTES],
        *[F.max(nombre).alias(f"{nombre}_max") for nombre in fuentes.CONTAMINANTES],
    ]


def _con_nombres(df):
    """Le agrega el nombre legible de cada ciudad, el mismo que usa el catálogo del productor.

    Se arma un mapa con Spark y no con pandas después, de manera que el Parquet que se escribe ya traiga la columna y el que lo relea no tenga que volver a traducir la clave.

    La clave que no esté en el catálogo se queda igual, así una ciudad desconocida aparece con su propio código en lugar de quedar vacía."""
    pares = [dato for clave, datos in fuentes.CIUDADES.items() for dato in (F.lit(clave), F.lit(datos["nombre"]))]
    mapa = F.create_map(*pares)
    return df.withColumn("ciudad_nombre", F.coalesce(mapa[F.col("ciudad")], F.col("ciudad")))


def _vista_diaria(eventos):
    """Devuelve una fila por ciudad y día, con el porcentaje del día ya calculado.

    La fecha se convierte a texto yyyy-MM-dd y no se deja como fecha, porque así el CSV, el hash y la consola muestran exactamente lo mismo sin que ninguna capa vuelva a interpretarla.

    El orden final es por ciudad y día, así cada ciudad aparece con su semana junta y dos corridas escriben el CSV byte por byte igual, que es lo que hace que el diff de git signifique algo."""
    agrupado = (
        eventos.withColumn("dia", F.date_format("hora_lectura", "yyyy-MM-dd"))
        .groupBy("ciudad", "dia")
        .agg(*_agregados())
        .withColumn("pct_sobre_umbral", F.round(F.col("horas_sobre_umbral") * 100.0 / F.col("lecturas"), 2))
    )

    return _con_nombres(agrupado).orderBy("ciudad", "dia").select(
        "ciudad",
        "ciudad_nombre",
        "dia",
        "lecturas",
        "horas_sobre_umbral",
        "pct_sobre_umbral",
        *[columna for nombre in fuentes.CONTAMINANTES for columna in (f"{nombre}_prom", f"{nombre}_max")],
    )


def _vista_ciudades(eventos):
    """Devuelve una fila por ciudad con la semana entera aplastada.

    Es la vista que responde cuánto empeora cada lugar mirándolo en conjunto, y con siete filas cabe en cualquier informe sin aplanar nada más.

    El orden es por ciudad para que dos corridas escriban el CSV idéntico."""
    agrupado = (
        eventos.groupBy("ciudad")
        .agg(*_agregados())
        .withColumn("pct_sobre_umbral", F.round(F.col("horas_sobre_umbral") * 100.0 / F.col("lecturas"), 2))
    )

    return _con_nombres(agrupado).orderBy("ciudad").select(
        "ciudad",
        "ciudad_nombre",
        "lecturas",
        "horas_sobre_umbral",
        "pct_sobre_umbral",
        *[columna for nombre in fuentes.CONTAMINANTES for columna in (f"{nombre}_prom", f"{nombre}_max")],
    )


def _escribir(vista, cuadro, nombre):
    """Escribe una vista en Parquet y en CSV, y dice dónde quedó cada uno.

    El Parquet se sobrescribe, que es lo que conviene en batch porque la vista nueva reemplaza a la vieja y no se acumula a lo largo de las corridas.

    El CSV sale de pandas y no de Spark porque el archivo es para abrirlo en una planilla, y así la codificación y el encabezado quedan bajo control."""
    destino_parquet = SALIDA_PARQUET / nombre
    destino_csv = SALIDA_LOTES / f"{nombre}.csv"

    vista.write.mode("overwrite").parquet(str(destino_parquet))
    cuadro.to_csv(str(destino_csv), index=False, encoding="utf-8")

    print(f"Parquet    {destino_parquet}", flush=True)
    print(f"CSV        {destino_csv}", flush=True)


def _hash_de(cuadro, campos, orden):
    """Devuelve el SHA-256 de una vista, sin lo que cambia entre corridas.

    Cada fila se reduce a los campos declarados y se ordena por las claves que se le pasan, que es el orden en que se leen las cosas y no el en que Spark decidió escribirlas.

    El texto se arma sin espacios de sobra para que dos maneras de escribir lo mismo no den dos hashes distintos."""
    recortados = [{campo: str(fila[campo]) for campo in campos} for _, fila in cuadro.iterrows()]
    recortados.sort(key=lambda fila: tuple(fila[campo] for campo in orden))

    texto = json.dumps(recortados, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(texto.encode("utf-8")).hexdigest()


def _resumen(total, alertas, horas, cuadro_diario, cuadro_ciudades):
    """Imprime el recuento de la corrida y el cruce que estas vistas hacen con la capa de velocidad."""
    print(f"Eventos        {total}", flush=True)
    print(f"Alertas        {alertas}", flush=True)
    print(f"Diario         {len(cuadro_diario)} filas", flush=True)
    print(f"Ciudades       {len(cuadro_ciudades)} filas", flush=True)

    cruce = "igual que las alertas" if horas == alertas else "NO coincide con las alertas"
    print(f"Horas umbral   {horas}  {cruce}", flush=True)


def _grafico(cuadro):
    """Dibuja el promedio diario de pm2.5 de cada ciudad y lo deja en la carpeta de evidencias.

    Una línea por ciudad sobre los siete días, que es la imagen con la que se cuenta la semana entera de un vistazo.

    La fecha del eje sale del texto yyyy-MM-dd de la vista y pandas la convierte solo para poder dibujar, el resto del cuadro no se toca."""
    if cuadro.empty:
        print("Sin gráfico porque no hay lecturas", flush=True)
        return

    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import pandas as pd

    ordenado = cuadro.assign(_fecha=pd.to_datetime(cuadro["dia"])).sort_values(["ciudad", "_fecha"])

    figura, eje = plt.subplots(figsize=(12, 5))
    for clave, filas in ordenado.groupby("ciudad"):
        etiqueta = fuentes.CIUDADES.get(clave, {}).get("nombre", clave)
        eje.plot(filas["_fecha"], filas["pm2_5_prom"], marker="o", label=etiqueta)

    eje.set_title("Promedio diario de pm2.5 por ciudad")
    eje.set_xlabel("Día")
    eje.set_ylabel("pm2.5 promedio ug/m3")
    eje.legend()
    eje.grid(axis="y", alpha=0.3)
    figura.autofmt_xdate()
    figura.tight_layout()
    figura.savefig(str(GRAFICO), dpi=140)
    plt.close(figura)

    print(f"Grafico    {GRAFICO}", flush=True)


def main():
    """Lee el topic entero, arma las dos vistas, las escribe y cierra con los hashes y el gráfico."""
    parser = argparse.ArgumentParser(description="Capa de lotes con Spark batch")
    parser.add_argument("--topic", default=config.TOPIC_EVENTOS, help="topic del que se lee")
    args = parser.parse_args()

    iniciar_log("06_lotes")
    imprimir_header("06_lotes.py")

    print("", flush=True)
    print(f"Topic      {args.topic}", flush=True)
    print(f"Broker     {config.KAFKA_BROKER}", flush=True)

    SALIDA_LOTES.mkdir(parents=True, exist_ok=True)

    spark = get_spark("lab03-lotes", con_kafka=True)

    eventos = parsear(_leer_topic(spark, args.topic)).filter(F.col("hora_lectura").isNotNull())
    eventos.cache()

    total = eventos.count()
    print("", flush=True)
    print(f"Topic leido  {total} eventos", flush=True)

    if total == 0:
        print("El topic esta vacio, corre primero 02_productor.py", flush=True)
        spark.stop()
        return 1

    alertas = eventos.filter(F.col("supera_umbral")).count()

    vista_diaria = _vista_diaria(eventos)
    vista_ciudades = _vista_ciudades(eventos)
    cuadro_diario = vista_diaria.toPandas()
    cuadro_ciudades = vista_ciudades.toPandas()
    horas = int(cuadro_ciudades["horas_sobre_umbral"].sum())

    print("", flush=True)
    _escribir(vista_diaria, cuadro_diario, "diario")
    print("", flush=True)
    _escribir(vista_ciudades, cuadro_ciudades, "ciudades")

    print("", flush=True)
    _resumen(total, alertas, horas, cuadro_diario, cuadro_ciudades)

    print("", flush=True)
    print(f"SHA-256 diario    {_hash_de(cuadro_diario, CAMPOS_DIARIO, ('dia', 'ciudad'))}", flush=True)
    print(f"SHA-256 ciudades  {_hash_de(cuadro_ciudades, CAMPOS_CIUDADES, ('ciudad',))}", flush=True)
    print(f"Campos diario     {', '.join(CAMPOS_DIARIO)}", flush=True)
    print(f"Campos ciudades   {', '.join(CAMPOS_CIUDADES)}", flush=True)
    print(f"Fecha corrida     {datetime.now(timezone.utc).isoformat(timespec='seconds')}", flush=True)

    _grafico(cuadro_diario)
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
