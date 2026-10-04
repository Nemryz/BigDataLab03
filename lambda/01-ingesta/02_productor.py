r"""Productor de Kafka que emite las lecturas de calidad del aire una por segundo.

Es la capa de ingesta de la arquitectura Lambda. Lee la foto que dejó 01_descargar.py, la
convierte en eventos de a uno y los manda al topic cada segundo, que es lo que pide el
enunciado cuando pide un productor que deje logs cada segundo.

El ritmo de un mensaje por segundo no es un capricho, es el punto de todo este diseño. Con
un productor que vaciara el dataset de un saque no existiría nada que observar correr: la
capa de velocidad tendría todo el trabajo en el primer milisegundo y la demo se limitaría a
imprimir un resultado ya calculado. A un mensaje por segundo el flujo es constante, se ve
avanzar la cola, y una alerta se dispara cuando le toca y no antes.

Un evento es una lectura horaria de una ciudad, o sea la hora y los tres contaminantes de
esa hora. Los eventos salen ordenados por hora y dentro de cada hora por ciudad, de modo que
cada cuatro mensajes representan la misma hora en las cuatro ciudades y se puede comparar
qué pasó en cada lugar al mismo tiempo.

De cada evento se anota además la hora real en que salió, aparte de la hora de la lectura.
Las dos son distintas y conviene no confundirlas: la de la lectura es cuando se midió el
aire, que es una fecha pasada que sale de la foto, y la de la emisión es cuando este
programa apretó el botón, que es ahora. La segunda es la que prueba que esto corrió en vivo.

Uso:
  02_productor.py                        60 mensajes a un por segundo
  02_productor.py --mensajes 20          corta antes
  02_productor.py --segundos 0.1         diez por segundo, para probar
  02_productor.py --limpiar              borra el topic antes de empezar
  02_productor.py --lista                imprime lo que enviaría y no manda nada
"""

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone

# Agregamos la carpeta de los módulos compartidos al camino de búsqueda, porque este script
# vive dos niveles más abajo de la raíz y Python no la encuentra sola
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "src", "comun"))

import config
import fuentes
from evidencia import imprimir_header, iniciar_log

# El nombre de la fuente que se descarga, repetido acá para no depender del nombre del archivo
CLAVE_FUENTE = "aire_horario"

# El campo que decide si una hora se considera contaminada, que es el mismo que usa la
# consulta de episodios del otro lado del proyecto
CAMPO_UMBRAL = "pm2_5"


def _manifiesto():
    """Lee el manifiesto de la foto y devuelve el nombre del archivo de datos.

    Se busca por manifiesto y no con un listado de la carpeta, y esa es una regla del
    proyecto entero. Un listado de la carpeta de datos crudos devuelve el archivo que haya, y si un día hay
    dos fotos por una corrida interrumpida se toma cualquiera de las dos a ciegas. El
    manifiesto dice cuál es la foto buena, y si no está el script se corta enseguida en vez
    de trabajar con datos al azar.
    """
    ruta = config.DATOS / "raw" / f"{CLAVE_FUENTE}_manifiesto.json"
    if not ruta.is_file():
        print(f"No se encontro el manifiesto {ruta}", flush=True)
        print("Corre primero 01_descargar.py " + CLAVE_FUENTE, flush=True)
        return None, None

    manifiesto = json.loads(ruta.read_text(encoding="utf-8"))
    archivo = config.DATOS / "raw" / manifiesto["archivo"]
    if not archivo.is_file():
        print(f"El manifiesto apunta a {archivo} y ese archivo no esta", flush=True)
        return None, None

    return manifiesto, archivo


def _cercana(latitude, longitude):
    """Cruza las coordenadas que mandó el servidor contra el catálogo y devuelve la ciudad.

    El nombre de la ciudad no viene en el archivo, solo vienen la latitud y la longitud, y
    acá se cruzan contra el catálogo para ponerlo. Se usa la distancia más cercana y no la
    igualdad exacta porque el servidor redondea las coordenadas a unos pocos decimales, o
    sea que el número que llega nunca es idéntico al que está escrito en el catálogo. Con
    la distancia alcanza, porque las cuatro ciudades del proyecto están a cientos de
    kilómetros unas de otras y ninguna se confunde con la vecina.

    Si la ciudad viene sin coordenadas devuelve None en vez de reventar, porque un registro
    malo no tendría que cortar el resto de la corrida.
    """
    if latitude is None or longitude is None:
        return None

    mejor = None
    mejor_distancia = None
    for clave, ciudad in fuentes.CIUDADES.items():
        distancia = abs(ciudad["latitude"] - latitude) + abs(ciudad["longitude"] - longitude)
        if mejor_distancia is None or distancia < mejor_distancia:
            mejor_distancia = distancia
            mejor = clave
    return mejor


def _eventos(archivo):
    """Convierte la foto en la lista de eventos que el productor va a mandar.

    Cada hora de cada ciudad se vuelve un evento independiente. Las horas malas vienen en
    rachas, así que el orden por hora hace que el episodio se vea correr de punta a punta en
    la consola en vez de aparecer de golpe.
    """
    datos = json.loads(archivo.read_text(encoding="utf-8"))

    eventos = []
    for ciudad in datos:
        clave_ciudad = _cercana(ciudad.get("latitude"), ciudad.get("longitude")) or "?"
        nombre_ciudad = fuentes.CIUDADES.get(clave_ciudad, {}).get("nombre", "desconocida")
        hora = ciudad.get("hourly", {})
        horas = hora.get("time", [])

        for i, marca in enumerate(horas):
            lectura = {
                "ciudad": clave_ciudad,
                "ciudad_nombre": nombre_ciudad,
                "hora_lectura": marca,
            }
            for contaminante in fuentes.CONTAMINANTES:
                valores = hora.get(contaminante, [])
                lectura[contaminante] = valores[i] if i < len(valores) else None

            # El umbral se evalúa acá y no en la capa de velocidad, porque el número sale de
            # la misma foto que el dato y así las dos capas ven el mismo criterio sin tener
            # que acordarse entre ellas. Sin dato no hay alerta, que es la respuesta que
            # conviene en un sistema que vigila algo: si no sabe, no avisa
            valor = lectura.get(CAMPO_UMBRAL)
            lectura["umbral"] = fuentes.UMBRAL_EPISODIO
            lectura["supera_umbral"] = (
                valor is not None and valor > fuentes.UMBRAL_EPISODIO
            )
            eventos.append(lectura)

    # Primero por hora y después por ciudad, que es lo que hace que cada cuatro mensajes
    # sean la misma hora en los cuatro lugares y se pueda comparar el mismo instante
    eventos.sort(key=lambda e: (e["hora_lectura"], e["ciudad"]))
    return eventos


def _asegurar_topic(bootstrap, topic):
    """Crea el topic si todavía no existe y devuelve si lo logró.

    Se hace explícito en vez de confiar en que el broker lo cree solo, porque el permiso de
    crear topics se puede apagar y en ese caso el productor fallaría con un error que no
    dice nada de topics. Si el topic ya está, la operación tira una excepción que acá se
    ignora a propósito: que exista es exactamente lo que queríamos.
    """
    try:
        from kafka.admin import KafkaAdminClient, NewTopic
    except ImportError:
        print("No se pudo importar el administrador de topics, se crea solo si el broker lo permite", flush=True)
        return True

    try:
        administrador = KafkaAdminClient(bootstrap_servers=bootstrap, client_id="productor-lab03")
        administrador.create_topics([NewTopic(name=topic, num_partitions=1, replication_factor=1)])
        administrador.close()
        print(f"Topic creado  {topic}", flush=True)
        return True
    except Exception as error:
        if "already exists" in str(error) or "TopicExists" in str(error):
            print(f"Topic ya existia  {topic}", flush=True)
            return True
        print(f"No se pudo crear el topic: {error}", flush=True)
        return False


def _borrar_topic(bootstrap, topic):
    """Borra el topic para que la corrida arranque en cero y devuelve si lo logró.

    Sin esto el topic acumula lo de todas las corridas, el verificador del paso siguiente
    cuenta sesenta mensajes en vez de treinta y la bitácora deja de cuadrar con lo que se
    ejecutó. El borrado es una operación que el broker confirma después, por eso se espera
    un momento antes de volver a crear el topic.

    Si el topic no existe no hay nada que borrar, que es exactamente el caso de la primera
    corrida, y ahí se sigue adelante sin más.
    """
    try:
        from kafka.admin import KafkaAdminClient
    except ImportError:
        print("Sin administrador de topics no se puede limpiar, se produce sobre lo que haya", flush=True)
        return True

    try:
        administrador = KafkaAdminClient(bootstrap_servers=bootstrap, client_id="productor-lab03")
    except Exception as error:
        print(f"No se pudo conectar al broker para limpiar: {error}", flush=True)
        return False

    try:
        administrador.delete_topics([topic], timeout_ms=15000, raise_errors=True)
        administrador.close()
        print(f"Topic borrado  {topic}", flush=True)
        # Se le da tiempo al broker a terminar el borrado. Si se recreara de inmediato el
        # topic nuevo podría chocar con la eliminación vieja que todavía se está procesando
        time.sleep(2)
        return True
    except Exception as error:
        administrador.close()
        if "unknown topic" in str(error).lower() or "does not exist" in str(error).lower():
            print(f"Topic no existia  {topic}, se arranca de cero", flush=True)
            return True
        print(f"No se pudo borrar el topic: {error}", flush=True)
        return False


def _argumentos():
    """Lee los parámetros de la línea de comandos y devuelve los valores ya revisados."""
    parseador = argparse.ArgumentParser(add_help=True)
    parseador.add_argument("--mensajes", type=int, default=60, help="cuantos eventos mandar")
    parseador.add_argument("--segundos", type=float, default=1.0, help="segundos entre mensajes")
    parseador.add_argument("--topic", default=config.TOPIC_EVENTOS, help="topic destino")
    parseador.add_argument("--lista", action="store_true", help="imprime los eventos y no manda nada")
    parseador.add_argument("--limpiar", action="store_true", help="borra el topic antes de producir")
    return parseador.parse_args()


def main():
    """Descarga la foto en memoria, arma los eventos y los emite al ritmo pedido."""
    iniciar_log("02_productor")
    imprimir_header("02_productor.py")
    args = _argumentos()

    manifiesto, archivo = _manifiesto()
    if manifiesto is None:
        return 1

    eventos = _eventos(archivo)
    print("", flush=True)
    print(f"Foto      {manifiesto['archivo']}", flush=True)
    print(f"Rango     {manifiesto['desde']} a {manifiesto['hasta']}", flush=True)
    print(f"SHA-256   {manifiesto['sha256']}", flush=True)
    print(f"Eventos   {len(eventos)} disponibles", flush=True)

    if not eventos:
        print("La foto no tiene horas, no hay nada que mandar", flush=True)
        return 1

    # Se corta la lista en los mensajes pedidos. Con menos de los que hay el productor no
    # agota la foto, que es lo que conviene en una demo, y con más de los que hay se manda
    # todo y después se corta
    a_mandar = eventos[: max(args.mensajes, 0)]

    print(f"Voy a mandar {len(a_mandar)} mensajes al topic {args.topic}", flush=True)
    print(f"Ritmo      {args.segundos} s por mensaje", flush=True)
    print("", flush=True)

    if args.lista:
        for i, evento in enumerate(a_mandar, start=1):
            print(f"{i:04d} {json.dumps(evento, ensure_ascii=False)}", flush=True)
        print("", flush=True)
        print(f"Fin de la lista, no se mando nada ({len(a_mandar)} eventos)", flush=True)
        return 0

    from kafka import KafkaProducer

    if args.limpiar and not _borrar_topic(config.KAFKA_BROKER, args.topic):
        return 1

    if not _asegurar_topic(config.KAFKA_BROKER, args.topic):
        return 1

    # Se confirma el envío con acks=1, o sea que basta que el broker lo escriba en su propio
    # log, que es el punto medio entre la velocidad y la garantía. Con acks=all el productor
    # esperaría a todos los réplicas, que en un broker solo es lo mismo pero más lento
    productor = KafkaProducer(
        bootstrap_servers=config.KAFKA_BROKER,
        acks=1,
        linger_ms=0,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        key_serializer=lambda v: v.encode("utf-8") if v else None,
    )

    enviados = 0
    fallidos = 0
    alertas = 0
    inicio = time.time()

    try:
        for numero, evento in enumerate(a_mandar, start=1):
            # La hora de emisión se anota justo antes de mandar, no al armar la lista, para
            # que refleje el momento real en que cada mensaje salió y no el del arranque
            evento = dict(evento)
            evento["evento_id"] = numero
            evento["emitido_utc"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
            evento["broker"] = config.KAFKA_BROKER
            evento["topic"] = args.topic

            futuro = productor.send(args.topic, key=evento["ciudad"], value=evento)
            futuro.get(timeout=30)

            enviados += 1
            if evento["supera_umbral"]:
                alertas += 1

            marca = "ALERTA" if evento["supera_umbral"] else "        "
            print(
                f"{numero:04d} {marca} {evento['ciudad_nombre']:<14} "
                f"{evento['hora_lectura']}  pm2.5={evento.get(CAMPO_UMBRAL)}",
                flush=True,
            )

            if args.segundos > 0 and numero < len(a_mandar):
                time.sleep(args.segundos)
    except KeyboardInterrupt:
        print("", flush=True)
        print("Cortado a mano", flush=True)
    except Exception as error:
        fallidos += 1
        print(f"Error mandando el mensaje: {error}", flush=True)
    finally:
        # El flush es lo que hace que los mensajes queden escritos de verdad antes de salir.
        # Sin él el productor se cierra y los que estaban en el búfer del cliente se pierden,
        # que es la falla más difícil de entender que hay, porque el programa terminó bien
        productor.flush(timeout=30)
        productor.close(timeout=10)

    duracion = time.time() - inicio
    print("", flush=True)
    print(f"Enviados   {enviados}", flush=True)
    print(f"Fallidos   {fallidos}", flush=True)
    print(f"Alertas    {alertas}", flush=True)
    print(f"Topic      {args.topic}", flush=True)
    print(f"Broker     {config.KAFKA_BROKER}", flush=True)
    print(f"Duracion   {duracion:.1f} s", flush=True)
    if envios_validos(enviados, duracion, args.segundos):
        print(f"Ritmo      {enviados / duracion:.2f} msg/s", flush=True)
    return 0 if fallidos == 0 else 1


def envios_validos(enviados, duracion, segundos):
    """Evita dividir por cero cuando la corrida fue instantánea.

    Se pone aparte porque con un solo mensaje y sin espera la duración puede quedar
    redondeada a cero, y una división por cero cortaría el resumen justo al final, después
    de que todo lo demás salió bien.
    """
    return enviados > 0 and duracion > 0 and segundos > 0


if __name__ == "__main__":
    sys.exit(main())
