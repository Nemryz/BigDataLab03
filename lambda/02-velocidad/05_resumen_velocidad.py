"""Lee el Parquet de la capa de velocidad y deja un resumen que se puede versionar.

La capa de velocidad escribe en modo completo, así que cada lote manda todas las sesiones que siguen vivas y el mismo episodio aparece muchas veces a lo largo de la corrida.

Ese detalle es útil mientras corre, porque permite ver la sesión crecer de a poco en la consola, pero es un lío para el informe, que quiere una fila por episodio.

Este script es el que ordena el desorden, se queda con la versión más avanzada de cada episodio y escribe un CSV de una fila por episodio.

Para saber cuál es la más avanzada ordena por ciudad, hora de inicio y cantidad de lecturas, y se queda con la primera de cada grupo.

La columna de lecturas es la que marca el avance, porque el episodio que ya trajo cinco horas pesa más que el que solo alcanzó a traer dos.

Además del CSV imprime el resumen de la corrida y deja un gráfico con la semana completa en la carpeta de evidencias.

El hash sigue la misma idea que el verificador de la capa de ingesta, se calcula sobre la ciudad, el comienzo y el fin de la sesión, las horas que la forman y los picos de contaminantes.

Se deja afuera todo lo que depende de cuándo se corrió el script, de modo que dos corridas que procesaron el mismo topic dan el mismo hash aunque se hayan hecho días distintos.

Uso:
  05_resumen_velocidad.py
"""

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
from spark_session import get_spark

from pyspark.sql import Window
from pyspark.sql import functions as F

# El Parquet crudo que escribe 04_velocidad.py, con un episodio repetido por cada lote en que creció.
SALIDA_PARQUET = config.SALIDAS / "velocidad" / "parquet"

# El CSV final, que sí entra al repositorio porque pesa poco y es lo que se muestra en el informe.
SALIDA_CSV = config.SALIDAS / "velocidad" / "episodios.csv"

# El gráfico de la semana, que se guarda con las demás evidencias y no con los datos pesados.
GRAFICO = config.GRAFICOS / "velocidad_episodios.png"

# Los campos que entran al hash, que son los que dicen qué episodio pasó y cuánto duró.
CAMPOS_ESTABLES = (
    "ciudad",
    "inicio",
    "fin",
    "lecturas",
    "umbral",
    *[f"{nombre}_max" for nombre in fuentes.CONTAMINANTES],
)


def _leer_episodios(spark):
    """Devuelve el Parquet con una sola fila por episodio, o None si todavía no hay nada escrito.

    Se usa una ventana de número de fila y no una agregación con máximo, porque de esa manera se conservan todas las columnas del episodio y no solo la que se usó para elegir.

    El orden pone primero las sesiones con más lecturas, que son las versiones más avanzadas, así que la fila que sobrevive es la última que escribió la capa de velocidad."""
    if not SALIDA_PARQUET.exists():
        return None

    bruto = spark.read.parquet(str(SALIDA_PARQUET))
    ventana = Window.partitionBy("ciudad", "inicio").orderBy(F.col("lecturas").desc(), F.col("fin").desc())

    return (
        bruto.withColumn("orden", F.row_number().over(ventana))
        .filter(F.col("orden") == 1)
        .drop("orden")
        .orderBy(F.col("inicio").asc(), F.col("ciudad").asc())
    )


def _con_nombres(pandas):
    """Le agrega el nombre bonito de cada ciudad al cuadro, el que se lee en el informe."""
    nombres = {clave: datos["nombre"] for clave, datos in fuentes.CIUDADES.items()}
    pandas.insert(1, "ciudad_nombre", pandas["ciudad"].map(nombres).fillna(pandas["ciudad"]))
    return pandas


def _resumen(pandas):
    """Imprime el recuento de episodios: total, detalle por ciudad, el más largo y el más intenso."""
    print(f"Episodios  {len(pandas)}", flush=True)

    if pandas.empty:
        print("No hubo horas sobre el umbral, corre primero 04_velocidad.py", flush=True)
        return

    por_ciudad = pandas["ciudad"].value_counts().sort_index()
    detalle = ", ".join(f"{clave}={por_ciudad[clave]}" for clave in por_ciudad.index)
    print(f"Ciudades   {detalle}", flush=True)
    print(f"Primera    {pandas['inicio'].min()}", flush=True)
    print(f"Ultima     {pandas['fin'].max()}", flush=True)

    mas_largo = pandas.loc[pandas["lecturas"].idxmax()]
    mas_intenso = pandas.loc[pandas["pm2_5_max"].idxmax()]

    print("", flush=True)
    print(f"Mas horas   {mas_largo['ciudad_nombre']}  {mas_largo['inicio']}  {mas_largo['lecturas']} horas seguidas", flush=True)
    print(f"Mas picado  {mas_intenso['ciudad_nombre']}  {mas_intenso['pm2_5_max']:.1f} ug/m3 en {mas_intenso['inicio']}", flush=True)


def _hash_estable(pandas):
    """Devuelve el SHA-256 de los episodios, sin lo que cambia entre corridas.

    Cada fila se reduce a los campos de la tupla de arriba y se ordena por hora de inicio y ciudad, que es el orden en que pasaron las cosas.

    El texto se arma sin espacios de sobra para que dos maneras de escribir lo mismo no den dos hashes distintos."""
    recortados = [{campo: str(fila[campo]) for campo in CAMPOS_ESTABLES} for _, fila in pandas.iterrows()]
    recortados.sort(key=lambda f: (f["inicio"], f["ciudad"]))

    texto = json.dumps(recortados, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(texto.encode("utf-8")).hexdigest()


def _grafico(pandas):
    """Dibuja la semana con un punto por episodio y lo deja en la carpeta de evidencias.

    El eje horizontal es la hora en que arrancó la sesión y el vertical es la ciudad, de modo que la racha se ve como una línea de puntos y no como una tabla.

    El tamaño del punto crece con las horas que tuvo la sesión y el color con el pico de partículas, que son las dos cosas que importan para juzgar qué tan malo fue cada episodio."""
    if pandas.empty:
        print("Sin gráfico porque no hay episodios", flush=True)
        return

    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    ciudades = sorted(pandas["ciudad"].unique())
    alturas = {nombre: i for i, nombre in enumerate(ciudades)}

    figura, eje = plt.subplots(figsize=(12, 5))
    puntos = eje.scatter(
        pandas["inicio"],
        pandas["ciudad"].map(alturas),
        s=pandas["lecturas"] * 60 + 40,
        c=pandas["pm2_5_max"],
        cmap="YlOrRd",
        alpha=0.8,
        edgecolors="#333333",
        linewidths=0.6,
    )

    eje.set_yticks(list(alturas.values()))
    eje.set_yticklabels([fuentes.CIUDADES[c]["nombre"] for c in ciudades])
    eje.set_title("Episodios de contaminación por ciudad")
    eje.set_xlabel("Hora de inicio de la sesión")
    eje.set_ylim(-0.8, len(ciudades) - 0.2)
    figura.colorbar(puntos, ax=eje, label="pm2.5 máximo ug/m3")
    figura.autofmt_xdate()
    figura.tight_layout()
    figura.savefig(str(GRAFICO), dpi=140)
    plt.close(figura)

    print(f"Grafico    {GRAFICO}", flush=True)


def main():
    """Lee el Parquet, escribe el CSV, imprime el resumen y devuelve el hash de la corrida."""
    iniciar_log("05_resumen_velocidad")
    imprimir_header("05_resumen_velocidad.py")

    spark = get_spark("lab03-resumen-velocidad")

    episodios = _leer_episodios(spark)
    if episodios is None:
        print("", flush=True)
        print("No existe el Parquet, corre primero 04_velocidad.py", flush=True)
        spark.stop()
        return 1

    pandas = _con_nombres(episodios.toPandas())
    pandas.to_csv(str(SALIDA_CSV), index=False, encoding="utf-8")

    print("", flush=True)
    print(f"Parquet    {SALIDA_PARQUET}", flush=True)
    print(f"CSV        {SALIDA_CSV}", flush=True)
    print("", flush=True)
    _resumen(pandas)

    print("", flush=True)
    print(f"SHA-256 episodios  {_hash_estable(pandas)}", flush=True)
    print(f"Campos              {', '.join(CAMPOS_ESTABLES)}", flush=True)
    print(f"Fecha corrida       {datetime.now(timezone.utc).isoformat(timespec='seconds')}", flush=True)

    _grafico(pandas)
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
