r"""Cierra dos preguntas de la rúbrica con una evidencia escrita: qué tan grande es cada zona
y cómo piensa Spark ejecutar la consulta más pesada.

La primera parte mide el antes y el después. El JSON crudo que llega de la API pesa lo que
pesa, y las zonas Parquet del lago pesan lo que pesan después de pasar por ellas. La tabla
que sale de acá es la que se pega en el informe para mostrar que el lago no es solo una
carpeta con datos adentro sino una transformación medible.

Hay un resultado que conviene decir de frente porque contradice la intuición: en este
conjunto de datos Parquet pesa más que el JSON, no menos. No es un error de medición, es
que 586 filas son pocas, el JSON ya viene bastante comprimido de la API, y cada una de las
32 particiones por ciudad y fecha tiene que llevar su propia carpeta y su propio footer.
Parquet gana cuando hay millones de filas y columnas que no se leen completas, que es el
caso para el que está diseñado. Reportar el número aunque no quede bonito es lo que hace
que el resto de la evidencia se pueda creer.

La segunda parte guarda el plan de ejecución de la consulta de episodios, que es la más
pesada porque usa dos funciones de ventana y tres capas de CTE. Ese plan es la prueba de
que el trabajo usa Spark de verdad: muestra los intercambios de datos entre máquinas, las
agrupaciones por hash y los órdenes que la consulta necesita. Es la evidencia que no se
puede conseguir escribiendo texto a mano.

El SQL no se repite acá. Se importa del módulo compartido, así el plan que se guarda es el
plan de la consulta que corre en el paso 04 y no de una copia que pudo haberse desincronizado.
"""
import json
import os
import sys
import time
from typing import Optional, Union

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "src", "comun"))

import config
from consultas import CONSULTA_PESADA, CONSULTAS
from evidencia import imprimir_header
from spark_session import get_spark

EXPLAIN_DIR = config.EVIDENCIAS / "explain"
MANIFIESTO = config.DATOS / "raw" / "aire_horario_manifiesto.json"
SUCIO = config.DATOS / "raw" / "aire_horario_sucio.json"
BRONZE = config.DATOS / "bronze" / "lecturas"
SILVER = config.DATOS / "silver" / "lecturas_limpias"
GOLD = config.DATOS / "gold"

PLAN_SALIDA = EXPLAIN_DIR / "plan_consulta_pesada.txt"
TAMANO_SALIDA = EXPLAIN_DIR / "tamano_zonas.txt"

# El SQL crudo necesita esta opción para poder leerse. El archivo es un arreglo JSON que
# ocupa varias líneas, y sin multiLine Spark intenta interpretar cada línea como un
# registro independiente, no encuentra JSON válido en ninguna y termina devolviendo todo
# como registros corruptos en lugar de fallar con un error claro
OPCION_CRUDO = {"multiLine": "true"}

# Las zonas que se miden, con la pregunta que cada una contesta. Los nombres van acá y no
# se buscan con glob, porque un archivo olvidado en datos/gold arruinaría la tabla igual
# que arruinaría cualquier otra medición de este trabajo
ZONAS = (
    ("bronze", BRONZE, "datos crudos aplanados, sin tocar"),
    ("silver", SILVER, "limpios y particionados"),
    ("gold", GOLD, "tablas agregadas para responder"),
)


def ruta_snapshot() -> Optional[Union[os.PathLike, str]]:
    """Devuelve la ruta del snapshot bueno, leyéndola del manifiesto y no con un glob.

    El manifiesto es la única fuente de verdad de cuál archivo es el válido, igual que en
    los demás pasos, porque en datos/raw quedaron conviviendo varios snapshots y elegir el
    equivocado hace que toda la medición posterior mida otra semana distinta.
    """
    if not MANIFIESTO.exists():
        print(f"No existe el manifiesto {MANIFIESTO}, corre descargar.py primero")
        return None
    manifiesto = json.loads(MANIFIESTO.read_text(encoding="utf-8"))
    return MANIFIESTO.parent / manifiesto["archivo"]


def tamanio(ruta) -> int:
    """Suma los bytes de un archivo o de todo un árbol de carpetas."""
    if ruta is None or not ruta.exists():
        return 0
    if ruta.is_file():
        return ruta.stat().st_size
    return sum(f.stat().st_size for f in ruta.rglob("*") if f.is_file())


def archivos(ruta) -> int:
    """Cuenta los archivos de datos de un árbol, que es lo que hace que Parquet se lea en
    paralelo: cada archivo se lo puede dar a un hilo distinto sin que nadie se pise.

    Se ignoran los `_SUCCESS` y los `.crc` que escribe Hadoop, porque son marcas de control
    y no traen datos. Contarlos duplicaría el número y haría creer que una zona tiene el
    doble de paralelismo del que tiene.
    """
    if ruta is None or not ruta.exists():
        return 0
    if ruta.is_file():
        return 1
    return sum(
        1
        for f in ruta.rglob("*")
        if f.is_file() and not f.name.startswith("_") and not f.name.endswith(".crc")
    )


def lecturas_logicas(ruta) -> int:
    """Cuenta cuántas lecturas horarias trae un JSON crudo, contando de verdad y no
    multiplicando a mano.

    El JSON viene anidado: un arreglo con un objeto por ciudad y dentro de cada uno un
    arreglo con las 168 horas. Eso son 4 registros para Spark y 672 lecturas para quien
    las usa, y confundir las dos cifras es el error clásico al comparar contra Parquet.
    """
    if ruta is None or not ruta.exists():
        return 0
    datos = json.loads(ruta.read_text(encoding="utf-8"))
    total = 0
    for registro in datos:
        total += len(registro.get("hourly", {}).get("time", []))
    return total


def conteo_zonas(spark) -> tuple:
    """Cuenta las filas de cada zona leyéndolas con Spark, en lugar de escribir el número a
    mano.

    Si un día una transformación empieza a perder filas, el número de acá baja y la tabla
    queda desalineada contra los logs anteriores, que es justo lo que se quiere que pase.
    La zona gold se cuenta tabla por tabla porque sus cuatro salidas tienen columnas
    distintas y leerlas juntas en una sola llamada haría que Spark eligiera un esquema y
    fallara con los archivos que no lo cumplen.

    Devuelve dos cosas: los totales por zona y el detalle de la zona gold, que es el que
    hace que las 35 filas de la salida se puedan explicar en vez de solo reportarse.
    """
    conteos = {}
    conteos[BRONZE] = spark.read.parquet(str(BRONZE)).count()
    conteos[SILVER] = spark.read.parquet(str(SILVER)).count()

    detalle_gold = {}
    for tabla in sorted(GOLD.iterdir()):
        if tabla.is_dir():
            detalle_gold[tabla.name] = spark.read.parquet(str(tabla)).count()
    conteos[GOLD] = sum(detalle_gold.values())

    return conteos, detalle_gold


def tabla_tamanos(snap, conteos) -> str:
    """Arma la tabla de tamaños.

    Se imprime en la consola y se guarda en un archivo para que el log del día y el archivo
    de la entrega digan exactamente lo mismo, que es la regla que sigue todo este trabajo.
    """
    filas_crudas = lecturas_logicas(snap)
    lineas = []
    lineas.append("Zona          Lecturas   Archivos       Bytes   Bytes por lectura")
    lineas.append("-" * 74)

    def linea(nombre, ruta, lecturas):
        if ruta is None or not ruta.exists():
            return
        byte_total = tamanio(ruta)
        cantidad = archivos(ruta)
        por_lectura = f"{byte_total / lecturas:,.0f}" if lecturas else "no aplica"
        lineas.append(
            f"{nombre:<13} {lecturas:>8,} {cantidad:>10,} {byte_total:>10,} {por_lectura:>18}"
        )

    linea("raw snapshot", snap, filas_crudas)
    linea("raw sucio", SUCIO, filas_crudas)
    for nombre, ruta, _ in ZONAS:
        linea(nombre, ruta, conteos.get(ruta, 0))

    return "\n".join(lineas)


def plan_de_consulta(spark) -> str:
    """Pide a Spark el plan completo de la consulta pesada y lo devuelve como texto.

    Se usa la consulta importada del módulo compartido, no una copia escrita acá, porque
    si no el plan que se guarda dejaría de corresponder al SQL que corre en el paso 04 en
    cuanto alguien cambie una coma en cualquiera de los dos archivos.
    """
    sql = dict((nombre, consulta) for nombre, consulta, _ in CONSULTAS)[CONSULTA_PESADA]
    consulta = spark.sql(sql)
    try:
        # queryExecution().toString() trae las cuatro capas que Spark expone, que es lo
        # mismo que muestra el EXPLAIN extendido: el SQL que se entendió, el plan
        # analizado, el optimizado y el físico que se va a ejecutar de verdad
        texto = consulta._jdf.queryExecution().toString()
        if not texto or not texto.strip():
            texto = consulta._jdf.queryExecution().executedPlan().toString()
    except Exception as error:
        texto = f"No se pudo leer el plan interno de Spark: {error}\n"
    return texto.strip() + "\n"


def main() -> int:
    """Mide las zonas, guarda el plan de la consulta pesada y deja todo en evidencias."""
    imprimir_header("06_explain.py")

    snap = ruta_snapshot()
    if snap is None:
        return 1

    print(f"Snapshot crudo  {snap.name}")
    print(f"Consulta pesada {CONSULTA_PESADA}")
    print("")

    EXPLAIN_DIR.mkdir(parents=True, exist_ok=True)

    spark = get_spark("lab03-06-explain")
    try:
        # Primero los tamaños y los conteos, que es lo que da sentido a la comparación sin
        # la cual los bytes por sí solos no dicen si una zona creció porque tiene más datos
        # o porque se rompió algo
        print("Tamano de cada zona")
        conteos, detalle_gold = conteo_zonas(spark)
        tabla = tabla_tamanos(snap, conteos)
        print(tabla)
        print("")

        # El detalle de la zona gold, porque las 35 filas de la tabla anterior son la suma
        # de cuatro salidas y sin desagregarlas no se sabe si falta data o si una tabla
        # simple tiene menos filas que otra por la forma en que está agregada
        print("Detalle de la zona gold")
        print("Tabla                        Filas     Bytes")
        print("-" * 74)
        for nombre, filas in detalle_gold.items():
            ruta = GOLD / nombre
            print(f"{nombre:<28} {filas:>6,} {tamanio(ruta):>9,}")
        print("-" * 74)
        print(f"{'Total':<28} {sum(detalle_gold.values()):>6,} {tamanio(GOLD):>9,}")
        print("")

        # La lectura cruda contra la Parquet, cronometrada. Se mide igual aunque el
        # resultado no concluya, porque con 586 filas la diferencia está por debajo del
        # ruido de arranque y sacar una conclusión de velocidad de acá sería inventar
        print("Lectura cruda contra lectura Parquet")
        t0 = time.perf_counter()
        crudas = spark.read.options(**OPCION_CRUDO).json(str(snap)).count()
        t_crudo = time.perf_counter() - t0

        t0 = time.perf_counter()
        limpias = spark.read.parquet(str(SILVER)).count()
        t_parquet = time.perf_counter() - t0

        print(f"  JSON crudo con multiLine   {crudas:>6,} registros  {t_crudo:.3f} s")
        print(f"  Parquet de la zona silver  {limpias:>6,} registros  {t_parquet:.3f} s")
        print("  Con esta cantidad de filas la diferencia queda dentro del ruido de arranque")
        print("  de Spark, asi que de aqui no se saca ninguna conclusion de velocidad.")
        print("")

        # Y ahora el plan de la consulta más pesada, que es lo que prueba que Spark está
        # haciendo el trabajo y no una librería de Python haciéndose pasar por él. La vista
        # `lecturas` se registra acá igual que en el paso 04, porque el SQL consulta esa
        # vista por nombre y sin ella Spark no tiene nada que explicar, solo un error
        spark.read.parquet(str(SILVER)).createOrReplaceTempView("lecturas")
        print(f"Plan de ejecucion de la consulta {CONSULTA_PESADA}")
        print("-" * 74)
        plan = plan_de_consulta(spark)
        print(plan)
        PLAN_SALIDA.write_text(
            "# Plan de ejecucion de la consulta mas pesada del proyecto\n"
            f"# Consulta: {CONSULTA_PESADA}\n"
            f"# Generado: {time.strftime('%Y-%m-%d %H:%M:%S')}\n"
            f"# Spark: {spark.version}\n\n" + plan,
            encoding="utf-8",
        )
        print("-" * 74)
        print(f"Guardado en {PLAN_SALIDA}")
        print("")

        cabecera = (
            "# Tamano de cada zona del lago, bytes y lecturas\n"
            f"# Generado: {time.strftime('%Y-%m-%d %H:%M:%S')}\n"
            f"# Snapshot: {snap.name}\n"
            "# El detalle de gold se agrega porque las 35 filas del total son la suma de\n"
            "# cuatro salidas con propositos distintos y desagregadas se entienden.\n\n"
        )
        cuerpo = tabla + "\n\nDetalle de la zona gold\nTabla                        Filas     Bytes\n"
        cuerpo += "-" * 74 + "\n"
        for nombre, filas in detalle_gold.items():
            cuerpo += f"{nombre:<28} {filas:>6,} {tamanio(GOLD / nombre):>9,}\n"
        cuerpo += "-" * 74 + "\n"
        cuerpo += f"{'Total':<28} {sum(detalle_gold.values()):>6,} {tamanio(GOLD):>9,}\n"
        TAMANO_SALIDA.write_text(cabecera + cuerpo, encoding="utf-8")
        print(f"Guardado en {TAMANO_SALIDA}")
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
