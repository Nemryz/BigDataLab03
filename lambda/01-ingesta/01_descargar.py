"""Descarga la foto del dataset de calidad del aire y la deja en el disco con su manifiesto.

Este es el único script del proyecto que toca la red, y eso es una decisión y no una casualidad.

La regla de todo el laboratorio es que la red se consulta una sola vez, antes de correr nada, y que después el pipeline lee del disco.

Si el productor llamara a la API en cada corrida, cada vez traería datos distintos, el hash de reproducibilidad nunca cuadraría y un corte de internet el día de la defensa nos dejaría sin demostración.

Con la foto en disco, la red pasa a ser un paso opcional y las tres corridas quedan siempre iguales.

La foto queda en la carpeta de datos crudos y al lado recibe un manifiesto en JSON con la dirección usada, la fecha y el hash del contenido.

El hash es lo que permite demostrar más adelante que dos corridas partieron de los mismos datos.

Uso:
  01_descargar.py                       muestra el catálogo de fuentes
  01_descargar.py aire_horario          descarga la foto de esa fuente
"""

import hashlib
import json
import os
import sys
import urllib.request
from datetime import date, datetime, timedelta, timezone

# Agregamos la carpeta de los módulos compartidos al camino de búsqueda, porque este script vive dos niveles más abajo de la raíz y Python no la encuentra sola.
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "src", "comun"))

import config
import fuentes
from evidencia import imprimir_header, iniciar_log

# La carpeta donde queda la foto del dataset crudo
CARPETA_RAW = config.DATOS / "raw"

# La fecha de hoy menos una semana, para tener una semana de historia por omisión.

# La ventana es una que ya pasó completa a propósito, porque un día empezado ayer todavía está en revisión y el servidor lo puede corregir a medida que pasan las horas, y entonces dos descargas darían archivos distintos y el hash no cuadraría nunca.
DIAS_DE_HISTORIA = 7

# Los días de pronóstico que se le piden a la fuente que no acepta fechas
DIAS_DE_PRONOSTICO = 3

# El nombre que se manda en la cabecera del pedido, para que el servidor sepa quién pregunta
AGENTE = "BigDataLab03"

# La ciudad de referencia para las fuentes que trabajan de a una sola ciudad
CIUDAD_REFERENCIA = "santiago"


def _parametros_de_url(desde, hasta):
    """Junta todos los valores que las plantillas pueden necesitar, sin mirar cuál se va a usar.

    Se arman de una vez y completas para que cualquier plantilla se pueda armar sin importar qué huecos traiga.

    Las que trabajan con varias ciudades piden latitudes y longitudes en lista, y las que trabajan con una sola piden el par suelto, así que las cuatro claves se ofrecen juntas."""
    ciudades = list(fuentes.CIUDADES.values())
    referencia = fuentes.CIUDADES[CIUDAD_REFERENCIA]
    return {
        "latitudes": ",".join(str(c["latitude"]) for c in ciudades),
        "longitudes": ",".join(str(c["longitude"]) for c in ciudades),
        "latitude": str(referencia["latitude"]),
        "longitude": str(referencia["longitude"]),
        "contaminantes": ",".join(fuentes.CONTAMINANTES),
        "desde": desde,
        "hasta": hasta,
        "dias": str(DIAS_DE_PRONOSTICO),
    }


def _construir_url(clave_fuente, desde, hasta):
    """Arma la dirección de la fuente llenando los huecos de su plantilla con los valores del rango pedido.

    La plantilla viene con nombres entre llaves y se completa con format, de modo que agregar una clave al catálogo no obliga a tocar esta función.

    Lo del tiempo se escapa porque la barra de la zona horaria es un separador en la dirección."""
    plantilla = fuentes.FUENTES[clave_fuente]["url"]
    return plantilla.format(**_parametros_de_url(desde, hasta))


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


def _verificar(datos):
    """Revisa que la respuesta tenga la forma que este proyecto espera antes de tocarla.

    Si el servidor cambia el arreglo por un objeto, o si algún contaminante deja de venir hora por hora, el fallo se entiende acá y no veinte líneas más abajo dentro de la escritura.

    Se recorren los contaminantes declarados en el catálogo y no una lista escrita a mano, así que el día que se agregue uno la verificación lo cubre sin acordarse de nada."""
    if not isinstance(datos, list) or not datos:
        raise ValueError("La respuesta no viene como lista de ciudades")

    for ciudad in datos:
        horario = ciudad.get("hourly")
        if not isinstance(horario, dict):
            raise ValueError("Una ciudad llegó sin el bloque hourly")

        horas = horario.get("time")
        if not isinstance(horas, list) or not horas:
            raise ValueError("Una ciudad llegó sin horas en hourly")

        for contaminante in fuentes.CONTAMINANTES:
            valores = horario.get(contaminante)
            if not isinstance(valores, list) or len(valores) != len(horas):
                raise ValueError(f"El contaminante {contaminante} no trae un valor por hora")


def _limpiar(contenido):
    """Le saca a cada ciudad el campo con el tiempo que tardó el servidor en responder y deja la foto lista para guardar.

    Este detalle es el que hace que la foto sea realmente una foto.

    El servidor mete en cada ciudad un campo llamado generationtime_ms, que es lo que tardó en armar la respuesta, y ese número cambia en cada llamada aunque los datos sean exactamente los mismos.

    Lo comprobamos, porque dos descargas seguidas daban archivos distintos y el hash no cuadraba, y la causa era ese campo y no los datos.

    Sacándolo antes de guardar, el archivo queda idéntico byte por byte entre descargas y el hash significa algo.

    Antes de limpiar se verifica la forma del contenido, de modo que una respuesta inesperada se rechaza en el umbral y no llega al disco."""
    datos = json.loads(contenido.decode("utf-8"))
    _verificar(datos)
    for ciudad in datos:
        ciudad.pop("generationtime_ms", None)
    return json.dumps(datos, indent=2, ensure_ascii=False).encode("utf-8")


def _hash_de(contenido):
    """Devuelve el hash SHA-256 del contenido en hexadecimal.

    Se usa el algoritmo de la biblioteca estándar en vez de uno hecho a mano, y se le pasa el contenido entero porque son unos pocos megas y así no hay que leerlo por trozos."""
    return hashlib.sha256(contenido).hexdigest()


def _hora_utc():
    """Devuelve la hora actual en UTC con formato de texto, para el manifiesto.

    Se usa el UTC y no la hora local porque los manifiestos de las tres ramas tienen que poder compararse entre sí, y si cada uno anotara su hora local mezclando horario de verano e invierno las diferencias no significarían nada."""
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _pedir(url):
    """Trae el contenido crudo de la dirección y devuelve los bytes que mandó el servidor.

    El pedido lleva un tiempo tope, porque una consulta que se queda colgada el día de la defensa es peor que una consulta que falla rápido y se puede reintentar."""
    peticion = urllib.request.Request(url, headers={"User-Agent": AGENTE})
    with urllib.request.urlopen(peticion, timeout=60) as respuesta:
        return respuesta.read()


def _rango_de_fechas():
    """Devuelve la semana que se le pide al servidor, cerrada de ayer para atrás.

    El rango termina ayer y no hoy.

    El día de hoy está incompleto y el servidor lo va corrigiendo a medida que pasan las horas, así que dos descargas del mismo rango dan archivos distintos y el hash nunca cuadra.

    Con un rango que ya cerró completo, el dato queda congelado y la foto sirve de verdad."""
    hoy = date.today()
    desde = (hoy - timedelta(days=DIAS_DE_HISTORIA)).isoformat()
    hasta = (hoy - timedelta(days=1)).isoformat()
    return desde, hasta


def _manifiesto(clave, url, desde, hasta, contenido, nombre_archivo):
    """Arma la ficha que hace que la foto sea evidencia y no solo un archivo suelto.

    Deja anotado de dónde salió, cuándo y cuánto pesaba, que son las tres preguntas que aparecen cuando alguien ajeno mira la carpeta por primera vez."""
    return {
        "fuente": clave,
        "nombre": fuentes.FUENTES[clave]["nombre"],
        "servidor": fuentes.FUENTES[clave]["servidor"],
        "url": url,
        "desde": desde,
        "hasta": hasta,
        "descargado_utc": _hora_utc(),
        "ciudades": list(fuentes.CIUDADES),
        "contaminantes": list(fuentes.CONTAMINANTES),
        "bytes": len(contenido),
        "sha256": _hash_de(contenido),
        "archivo": nombre_archivo,
    }


def _guardar(nombre, contenido):
    """Escribe los bytes en la carpeta cruda y devuelve la ruta donde quedaron.

    La carpeta se crea en el momento y no asume que exista, porque un repositorio recién clonado no trae directorios vacíos."""
    CARPETA_RAW.mkdir(parents=True, exist_ok=True)
    archivo = CARPETA_RAW / nombre
    archivo.write_bytes(contenido)
    return archivo


def _escribir_manifiesto(clave, ficha):
    """Deja la ficha en JSON al lado de la foto y devuelve su ruta."""
    destino = CARPETA_RAW / f"{clave}_manifiesto.json"
    destino.write_text(json.dumps(ficha, indent=2, ensure_ascii=False), encoding="utf-8")
    return destino


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

    desde, hasta = _rango_de_fechas()
    url = _construir_url(clave, desde, hasta)

    print("", flush=True)
    print(f"Fuente   {clave}", flush=True)
    print(f"Rango    {desde} a {hasta}", flush=True)
    print(f"URL      {url}", flush=True)
    print("", flush=True)
    print("Descargando", flush=True)

    contenido = _limpiar(_pedir(url))
    archivo = _guardar(f"{clave}_{desde}_{hasta}.json", contenido)
    ficha = _manifiesto(clave, url, desde, hasta, contenido, archivo.name)
    manifiesto = _escribir_manifiesto(clave, ficha)

    print("", flush=True)
    print(f"Guardado  {archivo}", flush=True)
    print(f"Tamano    {len(contenido)} bytes", flush=True)
    print(f"SHA-256   {ficha['sha256']}", flush=True)
    print(f"Manifiesto {manifiesto}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
