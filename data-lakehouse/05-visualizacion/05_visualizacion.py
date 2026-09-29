r"""Gráficos de la zona silver, una serie temporal por ciudad y un mapa de calor.

El último paso del lago es mostrar el resultado, porque un número en una tabla no dice lo
mismo que una línea que se dispara. Estos dos gráficos existen por dos motivos distintos.
El primero es que la consigna lo pide, y el segundo, que es el que más me importa, es que
la figura es la mejor forma de explicar por qué se construyó esto, porque se ve de un
vistazo que Santiago y Valparaíso se comportan distinto que Mendoza y Puerto Montt, y eso
con una tabla hay que leérselo.

La primera figura es la serie temporal. Cada ciudad es una línea sobre el mismo eje, con
el umbral de 25 microgramos dibujado de forma punteada. La línea del umbral es lo que
convierte un gráfico cualquiera en una respuesta a una pregunta, porque sin ella solo se
ve una mancha de valores y con ella se ve en qué momento y en qué ciudad se cruzó el
límite. Las cuatro ciudades van juntas y no en cuatro figuras separadas, porque la
comparación es justamente lo interesante y separarlas obligaría a mirar cuatro imágenes.

La segunda es un mapa de calor con las ciudades en el renglón y la hora del día en la
columna, y cada celda tiene el promedio de PM2.5 de esa combinación. Sirve para encontrar
el patrón horario, que es la pregunta que la serie temporal no responde, porque en ella
las horas de un día y de otro quedan todas mezcladas en la misma línea. Con este mapa se
ve si la contaminación se junta en las horas de la mañana y de la tarde, que es lo que se
espera de una ciudad con tránsito y calefacción.

Los gráficos se guardan como PNG porque es el formato que se puede pegar en el informe sin
hacer falta ningún lector especial, y se guardan en las evidencias y no en las zonas de
datos porque no son datos, son la interpretación de los datos, y confundir las dos cosas
es como guardar un informe dentro de la base.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "src", "comun"))

import config
import fuentes
from evidencia import imprimir_header
from spark_session import get_spark

ORIGEN = config.DATOS / "silver" / "lecturas_limpias"
DESTINO = config.GRAFICOS

# Un color por ciudad, elegidos a mano para que se distingan también en escala de grises,
# que es como se ve cuando alguien imprime el informe en blanco y negro
COLORES = {
    "santiago": "#c0392b",
    "valparaiso": "#2980b9",
    "mendoza": "#27ae60",
    "puerto_montt": "#8e44ad",
}

# El orden en que se dibujan, con la ciudad más contaminada primero para que su línea
# quede abajo del todo y no tape a las otras
ORDEN_CIUDADES = ["santiago", "valparaiso", "mendoza", "puerto_montt"]


def serie_temporal(df, ruta):
    """Dibuja las cuatro ciudades sobre el mismo eje con el umbral marcado."""
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import pandas as pd

    fig, eje = plt.subplots(figsize=(14, 6))

    for ciudad in ORDEN_CIUDADES:
        serie = df[df["ciudad"] == ciudad].sort_values("hora_utc")
        if serie.empty:
            continue
        eje.plot(
            serie["hora_utc"], serie["pm2_5"],
            label=ciudad.replace("_", " ").title(),
            color=COLORES.get(ciudad, "#7f8c8d"),
            linewidth=1.6,
        )

    umbral = fuentes.UMBRAL_EPISODIO
    eje.axhline(umbral, color="#34495e", linestyle="--", linewidth=1.4,
                label=f"Umbral {umbral:g} ug/m3")

    # El rango se calcula de los datos y no se escribe a mano, porque la conversión a UTC
    # corre la última hora hacia el día siguiente y un título escrito de memoria termina
    # mintiendo un día, que es exactamente el tipo de error que nadie revisa
    inicio = pd.to_datetime(df["hora_utc"]).min()
    fin = pd.to_datetime(df["hora_utc"]).max()
    eje.set_title(
        f"PM2.5 horario por ciudad, del {inicio.strftime('%d-%m-%Y %H:%M')} "
        f"al {fin.strftime('%d-%m-%Y %H:%M')} en UTC",
        fontsize=14, pad=14,
    )
    eje.set_xlabel("Fecha")
    eje.set_ylabel("PM2.5 (ug/m3)")
    eje.legend(loc="upper right", framealpha=0.9)
    eje.grid(True, alpha=0.3)
    fig.autofmt_xdate(rotation=30)
    fig.tight_layout()
    fig.savefig(ruta, dpi=130)
    plt.close(fig)
    return ruta


def mapa_calor(df, ruta):
    """Cruza ciudad contra hora del día y colorea con el promedio de PM2.5."""
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    horas = sorted(df["hora_del_dia"].unique())
    filas = [c for c in ORDEN_CIUDADES if c in set(df["ciudad"])]

    # Se arma la matriz a mano con loc y no con pivot, porque así se ve explícitamente
    # qué hay en cada celda y no se depende de que pivot acomode los renglones por su
    # propio criterio, que a veces los devuelve en orden alfabético y queda distinto
    matriz = []
    for ciudad in filas:
        valores_fila = []
        for hora in horas:
            celda = df[(df["ciudad"] == ciudad) & (df["hora_del_dia"] == hora)]
            valores_fila.append(float(celda["pm2_5"].mean()) if len(celda) else 0.0)
        matriz.append(valores_fila)

    fig, eje = plt.subplots(figsize=(13, 4.2))
    imagen = eje.imshow(matriz, cmap="YlOrRd", aspect="auto")

    eje.set_xticks(range(len(horas)))
    eje.set_xticklabels([str(h) for h in horas])
    eje.set_yticks(range(len(filas)))
    eje.set_yticklabels([c.replace("_", " ").title() for c in filas])
    eje.set_xlabel("Hora del dia en UTC")
    eje.set_ylabel("Ciudad")
    eje.set_title("PM2.5 promedio por hora del dia, en microgramos por metro cubico",
                  fontsize=14, pad=14)

    # El número escrito en cada celda es lo que hace que el mapa se pueda leer sin
    # mirar la leyenda, y sin él hay que adivinar qué tan oscuro es qué
    for fila in range(len(filas)):
        for columna in range(len(horas)):
            valor = matriz[fila][columna]
            eje.text(columna, fila, f"{valor:.0f}", ha="center", va="center", fontsize=7,
                     color="black" if valor < 45 else "white")

    fig.colorbar(imagen, ax=eje, label="PM2.5 (ug/m3)", shrink=0.9)
    fig.tight_layout()
    fig.savefig(ruta, dpi=130)
    plt.close(fig)
    return ruta


def main():
    """Lee la zona silver y produce las dos figuras en la carpeta de evidencias."""
    imprimir_header("05_visualizacion.py")
    config.asegurar_carpetas()

    if not ORIGEN.exists():
        print(f"No existe la zona silver en {ORIGEN}, corre 03_transformar.py primero")
        return 1

    spark = get_spark("lab03-05-visualizacion")
    try:
        print("")
        print(f"Origen   {ORIGEN}")
        print(f"Destino  {DESTINO}")
        print("")

        df = spark.read.parquet(str(ORIGEN).replace("\\", "/")).toPandas()
        print(f"Filas leidas  {len(df)}")
        print(f"Ciudades      {sorted(df['ciudad'].unique())}")
        print(f"Rango         {df['fecha'].min()} a {df['fecha'].max()}")
        print("")

        figuras = [
            ("serie_temporal_ciudades.png", serie_temporal),
            ("mapa_calor_pm25.png", mapa_calor),
        ]

        for nombre, funcion in figuras:
            ruta = DESTINO / nombre
            funcion(df, ruta)
            print(f"Guardado  {ruta.name}  {ruta.stat().st_size} bytes")

        print("")
        print(f"Total {len(figuras)} figuras en {DESTINO}")
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
