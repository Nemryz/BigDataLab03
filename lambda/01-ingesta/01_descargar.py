r"""Descarga la foto del dataset de calidad del aire y la deja en el disco con su manifiesto.

Este es el único script del proyecto que toca la red, y eso es una decisión y no una
casualidad. La regla de todo el laboratorio es que la red se consulta una sola vez, antes de
correr nada, y que después el pipeline lee del disco. Si el productor llamara a la API en
cada corrida, cada vez traería datos distintos, el hash de reproducibilidad nunca cuadraría
y un corte de internet el día de la defensa nos dejaría sin demostración. Con la foto en
disco, la red pasa a ser un paso opcional y las tres corridas quedan siempre iguales.

La foto va en datos\raw\ y al lado queda un manifiesto en JSON con la dirección usada, la
fecha y el hash del contenido. El hash es lo que permite demostrar más adelante que dos
corridas partieron de los mismos datos.

Para correrlo es python 01_descargar.py y sin argumento muestra el catálogo de fuentes.
"""

import hashlib
import json
import os
import sys
import urllib.request
from datetime import date, datetime, timedelta, timezone

# Agregamos la carpeta de los módulos compartidos al camino de búsqueda, porque este script
# vive dos niveles más abajo de la raíz y Python no la encuentra sola
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "src", "comun"))

import config
import fuentes
from evidencia import imprimir_header, iniciar_log

# La carpeta donde queda la foto del dataset crudo
CARPETA_RAW = config.DATOS / "raw"

# La fecha de hoy menos una semana, para tener una semana de historia por omisión. La ventana
# es una que ya pasó completa a propósito, porque un día empezado ayer todavía está en
# revisión y el servidor lo puede corregir a medida que pasan las horas, y entonces dos
# descargas darían archivos distintos y el hash no cuadraría nunca
DIAS_DE_HISTORIA = 7


def _construir_url(clave_fuente, desde, hasta):
    """Arma la dirección de la fuente reemplazando los huecos con los valores que faltan.

    La plantilla de la URL viene con nombres entre llaves y acá se reemplazan con lo que
    corresponda según si la fuente pide una ciudad o varias. Lo del tiempo se escapa porque
    la barra de la zona horaria es un separador en la URL.
    """
    plantilla = fuentes.FUENTES[clave_fuente]["url"]
    latitudes = ",".join(str(v["latitude"]) for v in fuentes.CIUDADES.values())
    longitudes = ",".join(str(v["longitude"]) for v in fuentes.CIUDADES.values())
    contaminantes = ",".join(fuentes.CONTAMINANTES.keys())

    return (
        plantilla
        .replace("{latitudes}", latitudes)
        .replace("{longitudes}", longitudes)
        .replace("{latitude}", str(fuentes.CIUDADES["santiago"]["latitude"]))
        .replace("{longitude}", str(fuentes.CIUDADES["santiago"]["longitude"]))
        .replace("{contaminantes}", contaminantes)
        .replace("{desde}", desde)
        .replace("{hasta}", hasta)
        .replace("{dias}", "3")
    )


def _mostrar_catalogo():
    """Imprime las fuentes disponibles con su descripción y para qué sirve cada una."""
    print("", flush=True)
    print("Fuentes disponibles", flush=True)
    for clave, datos in fuentes.FUENTES.items():
        print(f"  {clave}", flush=True)
        print(f"    nombre   {datos['nombre']}", flush=True)
        print(f"    servidor  {datos['servidor']}", flush=True)
        print(f"    formato   {datos['formato']}", flush=True)
        print(f"    uso       {datos['uso']}", flush=True)
    print("", flush=True)
    print("Copia el nombre de la que quieras, por ejemplo", flush=True)
    print("  01_descargar.py aire_horario", flush=True)


def _limpiar(contenido):
    """Le saca a cada ciudad el campo con el tiempo que tardó el servidor en responder.

    Este detalle es el que hace que la foto sea realmente una foto. El servidor mete en cada
    ciudad un campo llamado generationtime_ms, que es lo que tardó en armar la respuesta, y
    ese número cambia en cada llamada aunque los datos sean exactamente los mismos. Lo
    comprobamos, porque dos descargas seguidas daban archivos distintos y el hash no cuadraba,
    y la causa era ese campo y no los datos. Sacándolo antes de guardar, el archivo queda
    idéntico byte por byte entre descargas y el hash significa algo.
    """
    datos = json.loads(contenido.decode("utf-8"))
    for ciudad in datos:
        ciudad.pop("generationtime_ms", None)
    return json.dumps(datos, indent=2, ensure_ascii=False).encode("utf-8")


def _hash_de(contenido):
    """Devuelve el hash SHA-256 del contenido en hexadecimal.

    Se usa el algoritmo de la biblioteca estándar en vez de uno hecho a mano, y se le pasa el
    contenido entero porque son unos pocos megas y así no hay que leerlo por trozos.
    """
    return hashlib.sha256(contenido).hexdigest()


def _hora_utc():
    """Devuelve la hora actual en UTC con formato de texto, para el manifiesto.

    Se usa el UTC y no la hora local porque los manifiestos de las tres ramas tienen que
    poder compararse entre sí, y si cada uno anotara su hora local mezclando horario de
    verano e invierno las diferencias no significarían nada.
    """
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def main():
    """Descarga la fuente pedida, la guarda con su manifiesto y muestra el resumen."""
    iniciar_log("01_descarga")
    imprimir_header("01_descargar.py")

    if len(sys.argv) < 2 or sys.argv[1] in ("-h", "--help", "ayuda"):
        _mostrar_catalogo()
        return 0

    clave = sys.argv[1]
    if clave not in fuentes.FUENTES:
        print("Esa fuente no existe, revisa el catálogo", flush=True)
        _mostrar_catalogo()
        return 1

    hoy = date.today()
    # El rango termina ayer y no hoy. El día de hoy está incompleto y el servidor lo va
    # corrigiendo a medida que pasan las horas, así que dos descargas del mismo rango dan
    # archivos distintos y el hash nunca cuadra. Con un rango que ya cerró completo, el dato
    # queda congelado y la foto sirve de verdad
    desde = (hoy - timedelta(days=DIAS_DE_HISTORIA)).isoformat()
    hasta = (hoy - timedelta(days=1)).isoformat()
    url = _construir_url(clave, desde, hasta)

    print("", flush=True)
    print(f"Fuente   {clave}", flush=True)
    print(f"Rango    {desde} a {hasta}", flush=True)
    print(f"URL      {url}", flush=True)
    print("", flush=True)
    print("Descargando", flush=True)

    peticion = urllib.request.Request(url, headers={"User-Agent": "BigDataLab03"})
    with urllib.request.urlopen(peticion, timeout=60) as respuesta:
        crudo = respuesta.read()

    # Guardamos el contenido ya sin el campo de tiempo del servidor, que es lo que hace que
    # dos descargas del mismo rango salgan byte por byte iguales
    contenido = _limpiar(crudo)

    CARPETA_RAW.mkdir(parents=True, exist_ok=True)
    archivo = CARPETA_RAW / f"{clave}_{desde}_{hasta}.json"
    archivo.write_bytes(contenido)
    digest = _hash_de(contenido)

    # El manifiesto es lo que hace que la foto sea evidencia y no solo un archivo suelto,
    # porque deja anotado de dónde salió, cuándo y cuánto pesaba
    manifiesto = {
        "fuente": clave,
        "nombre": fuentes.FUENTES[clave]["nombre"],
        "servidor": fuentes.FUENTES[clave]["servidor"],
        "url": url,
        "desde": desde,
        "hasta": hasta,
        "descargado_utc": _hora_utc(),
        "ciudades": list(fuentes.CIUDADES.keys()),
        "contaminantes": list(fuentes.CONTAMINANTES.keys()),
        "bytes": len(contenido),
        "sha256": digest,
        "archivo": archivo.name,
    }
    (CARPETA_RAW / f"{clave}_manifiesto.json").write_text(
        json.dumps(manifiesto, indent=2, ensure_ascii=False), encoding="utf-8"
    )

    print("", flush=True)
    print(f"Guardado  {archivo}", flush=True)
    print(f"Tamano    {len(contenido)} bytes", flush=True)
    print(f"SHA-256   {digest}", flush=True)
    print(f"Manifiesto {CARPETA_RAW / (clave + '_manifiesto.json')}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
