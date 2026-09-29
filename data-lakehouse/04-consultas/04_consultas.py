r"""Consulta la zona silver con SQL y responde las tres preguntas del análisis.

Esta etapa es la que muestra para qué sirve todo lo anterior. Bronze y silver dejaron el
dato en filas tipadas y con la columna de episodio marcada, y acá se transforma en
respuestas. Se usa SQL y no la API de DataFrame a propósito, porque la pregunta que
nos pusimos es justamente si la zona silver es tan consultable como una base de datos, y
eso no se demuestra con funciones de Python, se demuestra escribiendo la consulta entera
en SQL y viendo que corre.

Son tres consultas y cada una ataca una pregunta distinta. La primera son las medias por
ciudad, que responde cuánto se contaminó en promedio cada lugar. La segunda son las horas
sobre el umbral, que dice cuántas veces se pasó el límite. Y la tercera, que es la más
interesante, agrupa las horas malas que fueron seguidas para convertirlas en episodios,
porque una hora mala sola no es lo mismo que cinco horas malas seguidas, que es cuando de
verdad el aire está feo y la gente se queda adentro.

Esa tercera consulta necesita un truco que vale la pena explicar, porque es lo que
distingue agrupar de contar. Para saber dónde termina un episodio hay que mirar la hora
anterior de la misma ciudad y ver si también estaba mala. Si lo estaba, la hora actual
sigue el mismo episodio y no arranca uno nuevo. Se hace con una función de ventana que
mira la fila de arriba, y la diferencia entre la hora actual y la anterior se convierte en
un número de grupo: cuando esa diferencia es uno, la hora pegó con la anterior y es el
mismo episodio, y cuando es más de uno, hubo un corte y arranca otro. Después se agrupa
por ciudad y por ese número, y cada grupo es un episodio. Es exactamente la lógica de
sesión de la arquitectura Kappa, y por eso el número de episodios que sale acá se puede
comparar directo con el que den las otras dos.

Cada resultado se escribe como Parquet en la zona gold, que es la zona de respuestas. Gold
no se vuelve a limpiar ni a reprocesar, es el resultado final y se puede leer con
cualquier herramienta, incluso sin Spark.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "src", "comun"))

import config
import fuentes
from consultas import CONSULTAS, EPISODIOS, EPISODIOS_RESUMEN, HORAS_SOBRE_UMBRAL, MEDIAS_POR_CIUDAD
from evidencia import imprimir_header
from spark_session import get_spark

ORIGEN = config.DATOS / "silver" / "lecturas_limpias"
DESTINO = config.DATOS / "gold"


def _guardar(df, nombre):
    """Escribe un resultado de consulta como Parquet en la zona gold."""
    ruta = DESTINO / nombre
    df.write.mode("overwrite").parquet(str(ruta).replace("\\", "/"))
    return ruta


def main():
    """Corre las tres consultas analíticas y guarda cada resultado en la zona gold."""
    imprimir_header("04_consultas.py")
    config.asegurar_carpetas()

    if not ORIGEN.exists():
        print(f"No existe la zona silver en {ORIGEN}, corre 03_transformar.py primero")
        return 1

    spark = get_spark("lab03-04-consultas")
    try:
        print("")
        print(f"Origen  {ORIGEN}")
        print(f"Destino {DESTINO}")
        print(f"Umbral  {fuentes.UMBRAL_EPISODIO} ug/m3")
        print("")

        df = spark.read.parquet(str(ORIGEN).replace("\\", "/"))
        df.createOrReplaceTempView("lecturas")
        print(f"Filas disponibles  {df.count()}")
        print("")

        # Una, medias por ciudad. El SQL está en el módulo compartido para que la misma
        # consulta sea la que se explica en el paso del EXPLAIN
        print("Consulta 1, medias por ciudad")
        medias = spark.sql(MEDIAS_POR_CIUDAD)
        medias.show(truncate=False)
        _guardar(medias, "medias_por_ciudad")
        print("")

        # Dos, horas sobre el umbral
        print("Consulta 2, horas sobre el umbral")
        sobre_umbral = spark.sql(HORAS_SOBRE_UMBRAL)
        sobre_umbral.show(truncate=False)
        _guardar(sobre_umbral, "horas_sobre_umbral")
        print("")

        # Tres, episodios, que es la consulta pesada del proyecto
        print("Consulta 3, episodios de contaminacion")
        episodios = spark.sql(EPISODIOS)
        episodios.show(50, truncate=False)
        # La tabla temporal se registra porque el resumen la consulta por nombre, y una
        # consulta que lee el resultado de otra anterior es lo que hace que esto sea SQL de
        # verdad y no una cadena de funciones de Python
        episodios.createOrReplaceTempView("episodios")
        _guardar(episodios, "episodios")
        print("")

        # Un cuarto que cierra el informe, leyendo la tabla temporal que registró el paso
        # anterior, que es el número comparable contra Kappa y contra la maquina de estados
        resumen = spark.sql(EPISODIOS_RESUMEN)
        print("Resumen de episodios por ciudad")
        resumen.show(truncate=False)
        _guardar(resumen, "episodios_resumen")
        print("")

        print(f"Guardado en {DESTINO}")
        for nombre, _, _ in CONSULTAS:
            print(f"  {nombre}")
        print("  episodios_resumen")
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
