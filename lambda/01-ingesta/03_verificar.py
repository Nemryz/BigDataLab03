"""Lee lo que quedó en el topic y devuelve el resumen de la corrida con su hash.

Después de que el productor se vació, hace falta una forma de comprobar que los mensajes llegaron de verdad y de comparar una corrida con otra.

Este script es esa comprobación, se conecta al topic, cuenta lo que hay, saca las alertas y calcula un hash sobre el contenido.

El hash es la parte importante.

Se calcula solo sobre los campos que son datos, o sea la ciudad, la hora de la lectura, los tres contaminantes, el umbral, la marca de alerta y el número de evento, y se dejan afuera los que son meramente técnicos, que son la hora en que se emitió el mensaje, que cambia siempre, y el broker y el topic, que son la dirección y no el contenido.

Con eso dos corridas que mandaron los mismos eventos dan el mismo hash aunque se hayan hecho con horas distintas, y ahí queda demostrado que el pipeline es reproducible.

Si el hash no cuadra, algo cambió entre una corrida y la otra y hay que averiguar qué.

Se lee todo el topic desde el principio y sin grupo de consumo, así que no se guarda ningún offset y cada ejecución vuelve a empezar de cero.

Por eso se puede correr cuantas veces se quiera sin que los resultados dependan de cuántas veces se corrió antes.

Uso:
  03_verificar.py
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

# Cuánto se espera sin recibir nada antes de dar por terminada la lectura.

# Es un tope y no una duración, si el topic ya está lleno se termina enseguida y si llegara algo tarde se espera igual.
SIN_MENSAJES_MS = 5000

# Los campos que entran al hash.

# Son los que dicen qué se midió y cuándo, que es el dato en sí.

# Los demás se dejan afuera a propósito porque varían entre corridas y harían que el hash nunca cuadrara aunque el contenido fuera idéntico.
CAMPOS_ESTABLES = (
    "evento_id",
    "ciudad",
    "hora_lectura",
    *fuentes.CONTAMINANTES,
    "umbral",
    "supera_umbral",
)


def _leer_topic(topic):
    """Devuelve la lista de eventos que hay en el topic, o None si no se pudo leer.

    Se pide el consumidor sin grupo de consumo, que es lo que desactiva el guardado de offsets.

    Así cada corrida parte del principio y no depende de lo que se consumió antes."""
    from kafka import KafkaConsumer
    from kafka.serializer import JsonSerializer

    try:
        consumidor = KafkaConsumer(
            topic,
            bootstrap_servers=config.KAFKA_BROKER,
            group_id=None,
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            consumer_timeout_ms=SIN_MENSAJES_MS,
            value_deserializer=JsonSerializer(),
        )
        registros = list(consumidor)
        consumidor.close()
    except Exception as error:
        print(f"No se pudo leer el topic {topic}: {error}", flush=True)
        print("Fijate que el broker este corriendo y que el productor haya mandado algo", flush=True)
        return None

    return [registro.value for registro in registros]


def _eventos_incompletos(eventos):
    """Devuelve los campos estables que faltan en al menos un evento.

    Un evento sin hora o sin contaminantes no rompe el hash de por sí, pero lo deja calculado sobre datos incompletos, que es peor porque el número sale igual y parece correcto.

    Se comparan contra los campos declarados en la tupla de arriba, así que el día que cambie el contrato del evento esta revisión cambia con él."""
    faltantes = set()
    for evento in eventos:
        for campo in CAMPOS_ESTABLES:
            if campo not in evento:
                faltantes.add(campo)
    return sorted(faltantes)


def _hash_estable(eventos):
    """Devuelve el SHA-256 del contenido de los eventos, sin lo que cambia entre corridas.

    Cada evento se reduce a sus campos estables y se ordena por número de evento, que es el orden en que salió.

    El texto se arma sin espacios de sobra para que dos maneras de escribir lo mismo no den dos hashes distintos."""
    recortados = [{campo: evento.get(campo) for campo in CAMPOS_ESTABLES} for evento in eventos]
    recortados.sort(key=lambda e: e.get("evento_id") or 0)

    texto = json.dumps(recortados, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(texto.encode("utf-8")).hexdigest()


def _recuento(eventos):
    """Separa los eventos en alertas, conteo por ciudad y rango de horas, sin imprimir nada."""
    alertas = [e for e in eventos if e.get("supera_umbral")]
    por_ciudad = {}
    horas = [e["hora_lectura"] for e in eventos if e.get("hora_lectura")]

    for evento in eventos:
        clave = evento.get("ciudad", "?")
        por_ciudad[clave] = por_ciudad.get(clave, 0) + 1

    return alertas, por_ciudad, horas


def _resumen(eventos):
    """Imprime el recuento de la corrida: mensajes, alertas, ciudades y rango de horas."""
    alertas, por_ciudad, horas = _recuento(eventos)

    print(f"Recibidos   {len(eventos)}", flush=True)
    print(f"Alertas     {len(alertas)}", flush=True)

    detalle = ", ".join(f"{clave}={por_ciudad[clave]}" for clave in sorted(por_ciudad))
    print(f"Ciudades    {detalle}", flush=True)

    if horas:
        print(f"Primer hora {min(horas)}", flush=True)
        print(f"Ultima hora {max(horas)}", flush=True)


def _cierre(hash_estable):
    """Imprime el hash de la corrida, los campos que lo formaron y cuándo se corrió."""
    print("", flush=True)
    print(f"SHA-256 estable  {hash_estable}", flush=True)
    print(f"Campos            {', '.join(CAMPOS_ESTABLES)}", flush=True)
    print(f"Fecha corrida     {datetime.now(timezone.utc).isoformat(timespec='seconds')}", flush=True)


def main():
    """Lee el topic, imprime el resumen y devuelve el hash de la corrida."""
    iniciar_log("03_verificacion")
    imprimir_header("03_verificar.py")

    print("", flush=True)
    print(f"Topic      {config.TOPIC_EVENTOS}", flush=True)
    print(f"Broker     {config.KAFKA_BROKER}", flush=True)

    eventos = _leer_topic(config.TOPIC_EVENTOS)
    if eventos is None:
        return 1

    if not eventos:
        print("El topic esta vacio, corre primero 02_productor.py", flush=True)
        return 1

    faltantes = _eventos_incompletos(eventos)
    if faltantes:
        print(f"Faltan campos en los eventos  {', '.join(faltantes)}", flush=True)
        return 1

    print("", flush=True)
    _resumen(eventos)
    _cierre(_hash_estable(eventos))
    return 0


if __name__ == "__main__":
    sys.exit(main())
