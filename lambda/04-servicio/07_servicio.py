"""Capa de servicio del Laboratorio 03, con SQLite como punto único de consulta.

Hasta acá cada capa de la arquitectura Lambda respondió por su cuenta, el 05 con los episodios del streaming y el 06 con las vistas consolidadas del batch, y para ver el panorama completo hacía falta abrir los dos resultados por separado.

Este script es la capa que junta los dos caminos en un solo lugar, que es lo que el enunciado del laboratorio llama capa de servicio y lo que cierra el pipeline antes de la documentación y el informe.

La base se arma desde los CSV que dejaron las capas anteriores y no desde Spark, porque acá no hay nada que calcular a escala, solo tres archivos pequeños que hay que poner en tablas y consultar.

SQLite se eligió porque viene con Python de fábrica, no pide servidor ni instalación aparte y la base entera vive en un archivo que viaja junto con el código sin nada más que copiar.

La base se reconstruye entera en cada corrida, se borra el archivo anterior y se vuelven a cargar las tres tablas, de manera que el servicio siempre muestra el último estado de las capas y nunca una mezcla de corridas viejas con corridas nuevas.

Salen tres tablas.

La diaria y la de ciudades son las vistas del batch cargadas tal cual, con los mismos números que salieron del 06.

La de episodios trae el resumen del streaming, una fila por episodio con su inicio, su fin y su duración.

Con las tres tablas cargadas se corren cuatro consultas que se imprimen en la consola y quedan en el log.

La primera resume la semana por ciudad con las horas sobre umbral y el porcentaje que representan.

La segunda muestra los tres días más contaminados de toda la foto.

La tercera cuenta los episodios del streaming por ciudad con sus horas de ventana y sus lecturas contaminadas.

La cuarta es el cruce, que pone en la misma fila las horas contaminadas que cuenta el batch y las que suman los episodios del streaming, y las dos tienen que dar el mismo número porque es el mismo dato contado por caminos distintos.

Ese cruce es la comprobación final de la arquitectura, si las dos columnas cuadran las dos ramas procesaron exactamente lo mismo y la capa de servicio puede responder sin favoritos.

Uso:
  07_servicio.py
"""

import csv
import os
import sqlite3
import sys
from datetime import datetime, timezone

# Agregamos la carpeta de los módulos compartidos al camino de búsqueda, porque este script vive dos niveles más abajo de la raíz y Python no la encuentra sola.
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "src", "comun"))

import config
from evidencia import imprimir_header, iniciar_log

# La carpeta donde el servicio deja su base, junto a las salidas de las otras capas.
SALIDA_SERVICIO = config.SALIDAS / "servicio"

# La base de SQLite, un solo archivo que se borra y se rehace en cada corrida.
RUTA_BASE = SALIDA_SERVICIO / "aire.db"

# Las tres entradas, cada una con el CSV que produce la etapa anterior y con las columnas que ese CSV tiene que traer.

# Se declaran con su tipo de dato y no solo con el nombre, porque de esta tupla sale tanto el create table como la conversión al cargar, y así una columna que cambie de tipo se arregla en un solo lugar.

# El orden de las claves es el orden del encabezado del CSV, y se compara tal cual al cargar para que una edición manual del archivo se note enseguida y no se cuelen números viejos en la base.
ENTRADAS = {
    "diario": (
        config.SALIDAS / "lotes" / "diario.csv",
        {
            "ciudad": "TEXT",
            "ciudad_nombre": "TEXT",
            "dia": "TEXT",
            "lecturas": "INTEGER",
            "horas_sobre_umbral": "INTEGER",
            "pct_sobre_umbral": "REAL",
            "pm2_5_prom": "REAL",
            "pm2_5_max": "REAL",
            "pm10_prom": "REAL",
            "pm10_max": "REAL",
            "nitrogen_dioxide_prom": "REAL",
            "nitrogen_dioxide_max": "REAL",
        },
    ),
    "ciudades": (
        config.SALIDAS / "lotes" / "ciudades.csv",
        {
            "ciudad": "TEXT",
            "ciudad_nombre": "TEXT",
            "lecturas": "INTEGER",
            "horas_sobre_umbral": "INTEGER",
            "pct_sobre_umbral": "REAL",
            "pm2_5_prom": "REAL",
            "pm2_5_max": "REAL",
            "pm10_prom": "REAL",
            "pm10_max": "REAL",
            "nitrogen_dioxide_prom": "REAL",
            "nitrogen_dioxide_max": "REAL",
        },
    ),
    "episodios": (
        config.SALIDAS / "velocidad" / "episodios.csv",
        {
            "ciudad": "TEXT",
            "ciudad_nombre": "TEXT",
            "inicio": "TEXT",
            "fin": "TEXT",
            "duracion_horas": "REAL",
            "lecturas": "INTEGER",
            "umbral": "REAL",
            "pm2_5_max": "REAL",
            "pm10_max": "REAL",
            "nitrogen_dioxide_max": "REAL",
            "pm2_5_prom": "REAL",
            "pm10_prom": "REAL",
            "nitrogen_dioxide_prom": "REAL",
        },
    ),
}


def _convertir(valor, tipo):
    """Convierte el texto del CSV al tipo que su columna declaró en la tabla.

    Los enteros se pasan por float primero porque una columna como las lecturas puede llegar escrita como 168.0 según con qué redondeo la escribiera la capa que produjo el archivo, y int sobre ese texto se cae.

    Todo lo demás se queda como texto, que es lo que SQLite prefiere para las claves y para las fechas escritas."""
    if tipo == "INTEGER":
        return int(float(valor))
    if tipo == "REAL":
        return float(valor)
    return valor


def _crear_tabla(con, nombre, columnas):
    """Crea la tabla si todavía no existe, con los tipos que declaró la entrada.

    Si la tabla ya está de una corrida anterior no se toca, porque el vaciado lo hace el cargador y acá lo único que importa es que la estructura coincida con los encabezados del CSV."""
    campos = ", ".join(f"{columna} {tipo}" for columna, tipo in columnas.items())
    con.execute(f"CREATE TABLE IF NOT EXISTS {nombre} ({campos})")


def _cargar(con, nombre, ruta, columnas):
    """Vacía la tabla, carga el CSV entero y devuelve cuántas filas entraron.

    La tabla se vacía antes de cargar, que es como la base se reconstruye en cada corrida, así una fila vieja no puede sobrevivir a una lectura distinta de las capas.

    El encabezado del CSV se compara contra las columnas declaradas y si no coincide se corta antes de escribir nada, una columna de menos haría que la consulta más simple devuelva un número viejo en silencio.

    Devuelve None cuando el encabezado no cuadra, que es la señal para que main corte la corrida sin cerrar la base a medias."""
    with open(ruta, encoding="utf-8", newline="") as archivo:
        lector = csv.DictReader(archivo)
        if lector.fieldnames != list(columnas):
            print(f"El CSV {ruta} trae un encabezado distinto al esperado", flush=True)
            print(f"Encontrado   {', '.join(lector.fieldnames or [])}", flush=True)
            print(f"Esperado     {', '.join(columnas)}", flush=True)
            return None
        filas = list(lector)

    con.execute(f"DELETE FROM {nombre}")
    insertar = f"INSERT INTO {nombre} ({', '.join(columnas)}) VALUES ({', '.join('?' for _ in columnas)})"
    con.executemany(
        insertar,
        [tuple(_convertir(fila[columna], tipo) for columna, tipo in columnas.items()) for fila in filas],
    )
    return len(filas)


def _imprimir_tabla(titulo, encabezado, filas):
    """Imprime el resultado de una consulta con las columnas alineadas.

    Cada columna se ensancha hasta la celda más ancha, de manera que en el log la tabla se lee como una tabla y no como una nube de números pegados.

    El título va solo y sin decoración para que sirva también de separador entre una consulta y la siguiente cuando alguien pase el log por la mitad."""
    print(titulo, flush=True)

    anchos = []
    for posicion, columna in enumerate(encabezado):
        ancho = max([len(str(columna))] + [len(str(fila[posicion])) for fila in filas])
        anchos.append(ancho)

    print("  ".join(str(columna).ljust(ancho) for columna, ancho in zip(encabezado, anchos)), flush=True)
    for fila in filas:
        print("  ".join(str(valor).ljust(ancho) for valor, ancho in zip(fila, anchos)), flush=True)
    print("", flush=True)


def _consultas(con):
    """Corre las cuatro consultas del servicio, con el cruce entre capas como cierre.

    Las tres primeras son miradas simples, la semana por ciudad, los días más sucios y los episodios del streaming, y las tres se pueden responder con una sola tabla.

    La cuarta es la que junta las dos arquitecturas, un left join que trae las horas del batch y las lecturas de los episodios en la misma fila, y después se suman las dos columnas para comparar los totales.

    El veredicto final se calcula en Python y no en SQL, porque lo que se quiere es un renglón que se entienda solo en el log, con los dos totales y la palabra que diga si cuadran."""
    _imprimir_tabla(
        "Resumen por ciudad",
        ("ciudad", "lecturas", "horas umbral", "pct umbral", "pm2.5 prom"),
        con.execute(
            "SELECT ciudad_nombre, lecturas, horas_sobre_umbral, pct_sobre_umbral, pm2_5_prom "
            "FROM ciudades ORDER BY horas_sobre_umbral DESC"
        ).fetchall(),
    )

    _imprimir_tabla(
        "Dias mas contaminados",
        ("ciudad", "dia", "pm2.5 prom", "pm10 prom"),
        con.execute(
            "SELECT ciudad_nombre, dia, pm2_5_prom, pm10_prom "
            "FROM diario ORDER BY pm2_5_prom DESC LIMIT 3"
        ).fetchall(),
    )

    _imprimir_tabla(
        "Episodios del streaming por ciudad",
        ("ciudad", "episodios", "horas ventana", "lecturas contaminadas"),
        con.execute(
            "SELECT ciudad_nombre, COUNT(*), ROUND(SUM(duracion_horas), 2), SUM(lecturas) "
            "FROM episodios GROUP BY ciudad, ciudad_nombre ORDER BY SUM(lecturas) DESC"
        ).fetchall(),
    )

    cruces = con.execute(
        "SELECT c.ciudad_nombre, c.horas_sobre_umbral, COALESCE(SUM(e.lecturas), 0), COUNT(e.ciudad) "
        "FROM ciudades c LEFT JOIN episodios e ON e.ciudad = c.ciudad "
        "GROUP BY c.ciudad, c.ciudad_nombre, c.horas_sobre_umbral "
        "ORDER BY c.horas_sobre_umbral DESC"
    ).fetchall()
    _imprimir_tabla(
        "Cruce de las dos capas",
        ("ciudad", "horas batch", "lecturas episodios", "episodios"),
        cruces,
    )

    batch = sum(fila[1] for fila in cruces)
    streaming = sum(fila[2] for fila in cruces)
    veredicto = "coinciden" if batch == streaming else "NO coinciden"
    print(f"Horas batch        {batch}", flush=True)
    print(f"Lecturas episodios {streaming}", flush=True)
    print(f"El cruce           {veredicto}", flush=True)


def main():
    """Reconstruye la base desde los CSV de las dos capas y corre las consultas del servicio.

    Si falta alguno de los tres archivos la corrida se corta antes de tocar la base, avisando cuál falta y qué script la produce, así una base a medias no se confunde con una corrida completa."""
    iniciar_log("07_servicio")
    imprimir_header("07_servicio.py")

    faltantes = [ruta for ruta, _columnas in ENTRADAS.values() if not ruta.is_file()]
    if faltantes:
        print("", flush=True)
        for ruta in faltantes:
            print(f"Falta {ruta}", flush=True)
        print("Corre primero 05_resumen_velocidad.py y 06_lotes.py", flush=True)
        return 1

    SALIDA_SERVICIO.mkdir(parents=True, exist_ok=True)

    # Se borra la base anterior para que cada corrida arranque de cero, de manera que el servicio refleja siempre las últimas salidas de las capas y no una mezcla con corridas viejas.
    RUTA_BASE.unlink(missing_ok=True)

    con = sqlite3.connect(str(RUTA_BASE))

    print("", flush=True)
    for nombre, (ruta, columnas) in ENTRADAS.items():
        _crear_tabla(con, nombre, columnas)
        filas = _cargar(con, nombre, ruta, columnas)
        if filas is None:
            con.close()
            return 1
        print(f"{nombre:<10} {filas} filas desde {ruta.name}", flush=True)

    con.commit()

    print("", flush=True)
    _consultas(con)

    print("", flush=True)
    print(f"Base          {RUTA_BASE}", flush=True)
    print(f"Fecha corrida {datetime.now(timezone.utc).isoformat(timespec='seconds')}", flush=True)

    con.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
