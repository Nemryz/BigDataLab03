"""Verifica el compose de Docker sin necesitar Docker instalado.

Este laboratorio se hace en Windows y en la máquina de referencia no hay Docker, así que la composición creada para levantar Kafka con contenedores no se pudo probar en vivo.

Antes de llevarla a una máquina con Docker conviene comprobar lo que sí se puede revisar sin el motor de contenedores, que el archivo esté bien formado y que traiga todo lo que Kafka necesita para arrancar en modo KRaft.

El script lee docker-compose.yml con PyYAML y pasa una lista de comprobaciones, cada una con su nombre y con lo que espera encontrar.

Si algo falta o está mal escrito, el script lo nombra y devuelve error, de manera que el fallo apunte a la pieza y no a una pila de excepciones.

La comprobación es estática.

No levanta contenedores ni consulta a Docker Hub, porque la red solo la usa el script de descarga y porque una imagen que no se pudo bajar no es lo mismo que un compose mal escrito.

Lo que queda para una máquina con Docker es la corrida real con docker compose up, y eso está documentado en el README y en la bitácora.

La evidencia de esta corrida queda en el log de la carpeta de evidencias con el encabezado de siempre.

Uso:
  verificar_compose.py
"""

import os
import sys

import yaml

# Agregamos la carpeta de los módulos compartidos al camino de búsqueda, porque este script vive en la carpeta de docker y Python no la encuentra sola.
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "src", "comun"))

import config
from evidencia import imprimir_header, iniciar_log

# El archivo que se verifica, junto al propio script
RUTA_COMPOSE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "docker-compose.yml")

# Las claves sin las cuales Kafka no arranca en modo KRaft, sacadas de la guía de la imagen oficial de Apache
CLAVES_KRAFT = (
    "KAFKA_LISTENERS",
    "KAFKA_ADVERTISED_LISTENERS",
    "KAFKA_CONTROLLER_LISTENER_NAMES",
    "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP",
    "KAFKA_CONTROLLER_QUORUM_VOTERS",
)

# Los factores de replicación que la guía manda fijar en uno para un solo nodo, porque el valor por omisión es tres y en un contenedor solo no hay réplicas que esperar
CLAVES_REPLICACION = (
    "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR",
    "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR",
    "KAFKA_SHARE_COORDINATOR_STATE_TOPIC_REPLICATION_FACTOR",
)

# La carpeta del volumen donde Kafka escribe sus datos dentro del contenedor
RUTA_VOLUMEN = "/var/lib/kafka/data"


def _cargar_compose():
    """Lee el compose y lo devuelve como diccionario, o devuelve None si el YAML está roto.

    Se abre con codificación explícita porque el archivo se escribió en UTF-8 y la consola de Windows asumiría otra cosa."""
    try:
        with open(RUTA_COMPOSE, encoding="utf-8") as archivo:
            return yaml.safe_load(archivo)
    except yaml.YAMLError as error:
        print(f"FALLO  el archivo no parsea como YAML, {error}", flush=True)
        return None


def _comprobaciones(datos):
    """Devuelve la lista de pares de nombre y resultado para cada pieza que se revisa.

    Están declaradas como datos y no como una corrida de impresiones para que agregar una comprobación sea agregar una línea y nada más.

    Se tolera que falte alguna clave entera con .get, porque si el archivo estuviera tan roto ya habría fallado el parseo de arriba."""
    servicios = (datos or {}).get("services") or {}
    servicio = servicios.get("kafka") or {}
    entorno = servicio.get("environment") or {}
    volumenes = (datos or {}).get("volumes") or {}
    healthcheck = servicio.get("healthcheck") or {}

    return [
        ("el archivo parsea como diccionario", isinstance(datos, dict)),
        ("el servicio kafka existe", "kafka" in servicios),
        ("la imagen es apache/kafka igual a la instalación local", servicio.get("image") == "apache/kafka:4.1.2"),
        ("el puerto 9092 del host queda publicado", "9092:9092" in (servicio.get("ports") or [])),
        ("el nodo hace de broker y de controlador a la vez", entorno.get("KAFKA_PROCESS_ROLES") == "broker,controller"),
        ("el nodo tiene identificador", entorno.get("KAFKA_NODE_ID") == 1),
        ("el cliente escucha en el mismo broker que usa el proyecto", config.KAFKA_BROKER in str(entorno.get("KAFKA_ADVERTISED_LISTENERS", ""))),
        ("las claves de KRaft están todas", all(clave in entorno for clave in CLAVES_KRAFT)),
        ("los factores de replicación están en uno", all(entorno.get(clave) == 1 for clave in CLAVES_REPLICACION)),
        ("el volumen de datos está declarado", "kafka-data" in volumenes),
        ("el contenedor monta ese volumen donde escribe Kafka", f"kafka-data:{RUTA_VOLUMEN}" in (servicio.get("volumes") or [])),
        ("las bitácoras del broker apuntan al volumen", entorno.get("KAFKA_LOG_DIRS") == RUTA_VOLUMEN),
        ("el healthcheck consulta al broker con kafka-topics", "kafka-topics.sh" in str(healthcheck.get("test", ""))),
    ]


def main():
    """Corre todas las comprobaciones, imprime cada resultado y devuelve cero si ninguna falló."""
    iniciar_log("08_verificacion_compose")
    imprimir_header("verificar_compose.py")

    print("", flush=True)
    print(f"Compose   {RUTA_COMPOSE}", flush=True)
    print("", flush=True)

    datos = _cargar_compose()
    if datos is None:
        return 1

    comprobaciones = _comprobaciones(datos)
    fallos = 0
    for nombre, salio in comprobaciones:
        marca = "OK" if salio else "FALLO"
        print(f"{marca:<5}  {nombre}", flush=True)
        if not salio:
            fallos += 1

    print("", flush=True)
    print(f"Total     {len(comprobaciones)} comprobaciones, {fallos} fallos", flush=True)

    if fallos:
        print("La composición no está lista hasta que las fallas desaparezcan", flush=True)
        return 1

    print("La composición está lista para una máquina con Docker", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
