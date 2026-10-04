"""Productor de Kafka que emite las lecturas de calidad del aire una por segundo.

Es la capa de ingesta de la arquitectura Lambda.

Lee la foto que dejó 01_descargar.py, la convierte en eventos de a uno y los manda al topic cada segundo, que es lo que pide el enunciado cuando pide un productor que deje logs cada segundo.

El ritmo de un mensaje por segundo no es un capricho, es el punto de todo este diseño.

Con un productor que vaciara el dataset de un saque no existiría nada que observar correr, la capa de velocidad tendría todo el trabajo en el primer milisegundo y la demo se limitaría a imprimir un resultado ya calculado.

A un mensaje por segundo el flujo es constante, se ve avanzar la cola, y una alerta se dispara cuando le toca y no antes.

Un evento es una lectura horaria de una ciudad, o sea la hora y los tres contaminantes de esa hora.

Los eventos salen ordenados por hora y dentro de cada hora por ciudad, de modo que cada cuatro mensajes representan la misma hora en las cuatro ciudades y se puede comparar qué pasó en cada lugar al mismo tiempo.

De cada evento se anota además la hora real en que salió, aparte de la hora de la lectura.

Las dos son distintas y conviene no confundirlas, la de la lectura es cuando se midió el aire, que es una fecha pasada que sale de la foto, y la de la emisión es cuando este programa apretó el botón, que es ahora.

La segunda es la que prueba que esto corrió en vivo.

Uso:
    02_productor.py                        60 mensajes a un por segundo
    02_productor.py --mensajes 20          corta antes
    02_productor.py --segundos 0.1         diez por segundo, para probar
    02_productor.py --limpiar              deja el broker en cero antes de empezar
    02_productor.py --lista                imprime lo que enviaría y no manda nada
"""
from evidencia import imprimir_header, iniciar_log
import fuentes
import config
import argparse
import json
import os
import shutil
import stat
import subprocess
import sys
import threading
import time
from datetime import datetime, timezone

# Agregamos la carpeta de los módulos compartidos al camino de búsqueda, porque este script vive dos niveles más abajo de la raíz y Python no la encuentra sola.
sys.path.insert(0, os.path.join(os.path.dirname(
    os.path.abspath(__file__)), "..", "..", "src", "comun"))


# El nombre de la fuente que se descarga, repetido acá para no depender del nombre del archivo
CLAVE_FUENTE = "aire_horario"

# El campo que decide si una hora se considera contaminada, que es el mismo que usa la consulta de episodios del otro lado del proyecto
CAMPO_UMBRAL = "pm2_5"

# El identificador que el cliente de administración presenta ante el broker, para que en los logs del servidor se distinga de los demás
CLIENTE_ADMIN = "productor-lab03"


def _manifiesto():
    """Lee el manifiesto de la foto y devuelve la ficha junto al archivo de datos.

    Se busca por manifiesto y no con un listado de la carpeta, y esa es una regla del proyecto entero.

    Un listado de la carpeta de datos crudos devuelve el archivo que haya, y si un día hay dos fotos por una corrida interrumpida se toma cualquiera de las dos a ciegas.

    El manifiesto dice cuál es la foto buena, y si no está el script se corta enseguida en vez de trabajar con datos al azar."""
    ruta = config.DATOS / "raw" / f"{CLAVE_FUENTE}_manifiesto.json"
    if not ruta.is_file():
        print(f"No se encontro el manifiesto {ruta}", flush=True)
        print("Corre primero 01_descargar.py " + CLAVE_FUENTE, flush=True)
        return None, None

    manifiesto = json.loads(ruta.read_text(encoding="utf-8"))
    archivo = config.DATOS / "raw" / manifiesto["archivo"]
    if not archivo.is_file():
        print(
            f"El manifiesto apunta a {archivo} y ese archivo no esta", flush=True)
        return None, None

    return manifiesto, archivo


def _cercana(latitude, longitude):
    """Cruza las coordenadas que mandó el servidor contra el catálogo y devuelve la ciudad.

    El nombre de la ciudad no viene en el archivo, solo vienen la latitud y la longitud, y acá se cruzan contra el catálogo para ponerlo.

    Se usa la distancia más cercana y no la igualdad exacta porque el servidor redondea las coordenadas a unos pocos decimales, o sea que el número que llega nunca es idéntico al que está escrito en el catálogo.

    Con la distancia alcanza, porque las cuatro ciudades del proyecto están a cientos de kilómetros unas de otras y ninguna se confunde con la vecina.

    Si la ciudad viene sin coordenadas devuelve None en vez de reventar, porque un registro malo no tendría que cortar el resto de la corrida."""
    if latitude is None or longitude is None:
        return None

    mejor = None
    mejor_distancia = None
    for clave, ciudad in fuentes.CIUDADES.items():
        distancia = abs(ciudad["latitude"] - latitude) + \
            abs(ciudad["longitude"] - longitude)
        if mejor_distancia is None or distancia < mejor_distancia:
            mejor_distancia = distancia
            mejor = clave
    return mejor


def _evento_de(ciudad, horas):
    # Devuelve los eventos de una sola ciudad, uno por cada hora de su registro.
    clave_ciudad = _cercana(ciudad.get("latitude"),
                            ciudad.get("longitude")) or "?"
    nombre_ciudad = fuentes.CIUDADES.get(
        clave_ciudad, {}).get("nombre", "desconocida")
    marcas = horas.get("time", [])

    nuevos = []
    for i, marca in enumerate(marcas):
        lectura = {"ciudad": clave_ciudad,
                   "ciudad_nombre": nombre_ciudad, "hora_lectura": marca}
        for contaminante in fuentes.CONTAMINANTES:
            valores = horas.get(contaminante, [])
            lectura[contaminante] = valores[i] if i < len(valores) else None

        # El umbral se evalúa acá y no en la capa de velocidad, porque el número sale de la misma foto que el dato y así las dos capas ven el mismo criterio sin tener que acordarse entre ellas.

        # Sin dato no hay alerta, que es la respuesta que conviene en un sistema que vigila algo, si no sabe, no avisa.
        valor = lectura.get(CAMPO_UMBRAL)
        lectura["umbral"] = fuentes.UMBRAL_EPISODIO
        lectura["supera_umbral"] = valor is not None and valor > fuentes.UMBRAL_EPISODIO
        nuevos.append(lectura)

    return nuevos


def _eventos(archivo):
    """Convierte la foto en la lista de eventos que el productor va a mandar.

    Cada hora de cada ciudad se vuelve un evento independiente.

    Las horas malas vienen en rachas, así que el orden por hora hace que el episodio se vea correr de punta a punta en la consola en vez de aparecer de golpe."""
    datos = json.loads(archivo.read_text(encoding="utf-8"))

    eventos = []
    for ciudad in datos:
        horas = ciudad.get("hourly", {})
        eventos.extend(_evento_de(ciudad, horas))

    # Primero por hora y después por ciudad, que es lo que hace que cada cuatro mensajes sean la misma hora en los cuatro lugares y se pueda comparar el mismo instante.
    eventos.sort(key=lambda e: (e["hora_lectura"], e["ciudad"]))
    return eventos


def _cliente_admin(bootstrap):
    # El import va adentro de la función y no arriba del archivo, porque el productor puede correr sin el paquete de administración cuando solo quiere mandar mensajes.
    from kafka.admin import KafkaAdminClient

    return KafkaAdminClient(bootstrap_servers=bootstrap, client_id=CLIENTE_ADMIN)


def _asegurar_topic(bootstrap, topic):
    """Crea el topic si todavía no existe y devuelve si lo logró.

    Se hace explícito en vez de confiar en que el broker lo cree solo, porque el permiso de crear topics se puede apagar y en ese caso el productor fallaría con un error que no dice nada de topics.

    Si el topic ya está, la operación tira una excepción que acá se ignora a propósito, que exista es exactamente lo que queríamos."""
    try:
        from kafka.admin import NewTopic

        administrador = _cliente_admin(bootstrap)
    except ImportError:
        print("No se pudo importar el administrador de topics, se crea solo si el broker lo permite", flush=True)
        return True
    except Exception as error:
        print(f"No se pudo crear el topic: {error}", flush=True)
        return False

    try:
        administrador.create_topics(
            [NewTopic(name=topic, num_partitions=1, replication_factor=1)])
        administrador.close()
        print(f"Topic creado  {topic}", flush=True)
        return True
    except Exception as error:
        administrador.close()
        if "already exists" in str(error) or "TopicExists" in str(error):
            print(f"Topic ya existia  {topic}", flush=True)
            return True
        print(f"No se pudo crear el topic: {error}", flush=True)
        return False


def _correr_script(nombre):
    """Corre uno de los scripts del proyecto, manda su salida a este log y devuelve si terminó bien.

    La consola se abre oculta para que no aparezca saltando en la pantalla del laboratorio.

    La salida se lee en un hilo aparte y no en el hilo principal, porque el arranque del broker deja un proceso largo vivo que hereda el manijón de la tubería y si se esperara el fin de la lectura nunca llegaría.

    El tiempo tope existe por lo mismo, un script colgado no debería poder trabar al productor entero."""
    script = config.RAIZ / "scripts" / nombre
    if not script.exists():
        print(f"No existe el script {script}, no se puede limpiar", flush=True)
        return False

    proceso = subprocess.Popen(
        ["powershell", "-NoProfile", "-ExecutionPolicy",
            "Bypass", "-File", str(script)],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        errors="replace",
        creationflags=subprocess.CREATE_NO_WINDOW,
    )

    def _reenviar():
        for linea in proceso.stdout:
            print(f"  {linea.rstrip()}", flush=True)

    lector = threading.Thread(target=_reenviar, daemon=True)
    lector.start()

    try:
        codigo = proceso.wait(timeout=180)
    except subprocess.TimeoutExpired:
        print("  el script se pasó del tiempo tope, se corta", flush=True)
        proceso.kill()
        proceso.wait()
        return False

    lector.join(timeout=5)
    return codigo == 0


def _quitar_solo_lectura(almacen):
    """Quita el atributo de solo lectura de todos los archivos de la carpeta de datos de Kafka.

    Kafka deja los archivos de checkpoint marcados como solo lectura, y Windows no permite borrar ninguno de ellos mientras ese atributo siga puesto, así que sin este paso el borrado se cae con un acceso denegado."""
    for _raiz, _carpetas, archivos in os.walk(almacen):
        for archivo in archivos:
            ruta = os.path.join(_raiz, archivo)
            try:
                os.chmod(ruta, stat.S_IWRITE)
            except OSError:
                pass


def _vaciar_almacenamiento(topic):
    """Deja el almacenamiento del broker en cero para que esta corrida arranque vacía y devuelve si lo logró.

    Sin esto el topic acumula lo de todas las corridas, el verificador del paso siguiente cuenta sesenta mensajes en vez de treinta y la bitácora deja de cuadrar con lo que se ejecutó.

    No se pide el borrado del topic porque en Windows esa operación renombra un directorio que el propio broker todavía tiene abierto, el renombrado sale con permiso denegado, el broker marca el volumen como inservible y se apaga solo.

    Se apaga y se prende con los scripts del proyecto, se borra la carpeta de datos, y el arranque vuelve a formatear el almacenamiento porque no encuentra el archivo de metadatos, que es el paso que el script ya hace por su cuenta.

    El topic vuelve a llamarse igual después, así que el resto del flujo no se entera del reinicio."""
    almacen = config.RAIZ / "kafka" / "kraft-logs"

    print("", flush=True)
    print("Limpiando el almacenamiento del broker", flush=True)

    if not _correr_script("stop_kafka.ps1"):
        print("No se pudo detener el broker, no se limpia nada", flush=True)
        return False

    # En Windows un archivo del directorio puede quedar abierto unos instantes después de que el broker deje de escuchar, así que se espera y se vuelve a intentar antes de rendirse.
    ultimo_error = None
    for _intento in range(2):
        if not almacen.exists():
            break
        _quitar_solo_lectura(almacen)
        try:
            shutil.rmtree(almacen)
            break
        except OSError as error:
            ultimo_error = error
            time.sleep(2)

    if almacen.exists():
        print(f"No se pudo borrar {almacen}: {ultimo_error}", flush=True)
        return False
    print(f"  almacenamiento borrado  {almacen}", flush=True)

    if not _correr_script("start_kafka.ps1"):
        print("No se pudo volver a arrancar el broker", flush=True)
        return False
    return True


def _argumentos():
    """Lee los parámetros de la línea de comandos y devuelve los valores ya revisados."""
    parseador = argparse.ArgumentParser(add_help=True)
    parseador.add_argument("--mensajes", type=int,
                           default=60, help="cuantos eventos mandar")
    parseador.add_argument("--segundos", type=float,
                           default=1.0, help="segundos entre mensajes")
    parseador.add_argument(
        "--topic", default=config.TOPIC_EVENTOS, help="topic destino")
    parseador.add_argument("--lista", action="store_true",
                           help="imprime los eventos y no manda nada")
    parseador.add_argument("--limpiar", action="store_true",
                           help="deja el almacenamiento del broker en cero antes de producir")
    return parseador.parse_args()


def _productor_de_kafka():
    """Abre el productor con la confirmación y el formato de mensaje que usa este laboratorio.

    Se confirma el envío con acks=1, o sea que basta que el broker lo escriba en su propio log, que es el punto medio entre la velocidad y la garantía.

    Con acks=all el productor esperaría a todos los réplicas, que en un broker solo es lo mismo pero más lento.

    Los serializadores son los que trae la librería y no dos funciones sueltas, porque con funciones la librería avisa en cada corrida que no reconoce el formato y mete ese aviso en medio del log de evidencia."""
    from kafka import KafkaProducer
    from kafka.serializer import DefaultSerializer, JsonSerializer

    return KafkaProducer(
        bootstrap_servers=config.KAFKA_BROKER,
        acks=1,
        linger_ms=0,
        value_serializer=JsonSerializer(),
        key_serializer=DefaultSerializer(),
    )


def _emitir(productor, a_mandar, args):
    """Manda los eventos al ritmo pedido y devuelve cuántos salieron, cuántos fallaron, cuántos alertaron y cuánto tardó.

    La hora de emisión se anota justo antes de mandar y no al armar la lista, para que refleje el momento real en que cada mensaje salió y no el del arranque."""
    enviados = 0
    fallidos = 0
    alertas = 0
    inicio = time.time()

    try:
        for numero, evento in enumerate(a_mandar, start=1):
            evento = dict(evento)
            evento["evento_id"] = numero
            evento["emitido_utc"] = datetime.now(
                timezone.utc).isoformat(timespec="seconds")
            evento["broker"] = config.KAFKA_BROKER
            evento["topic"] = args.topic

            futuro = productor.send(
                args.topic, key=evento["ciudad"], value=evento)
            futuro.get(timeout=30)

            enviados += 1
            if evento["supera_umbral"]:
                alertas += 1

            marca = "ALERTA" if evento["supera_umbral"] else "        "
            print(
                f"{numero:04d} {marca} {evento['ciudad_nombre']:<14} {evento['hora_lectura']}  pm2.5={evento.get(CAMPO_UMBRAL)}", flush=True)

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

        # Sin él el productor se cierra y los que estaban en el búfer del cliente se pierden, que es la falla más difícil de entender que hay, porque el programa terminó bien.
        productor.flush(timeout=30)
        productor.close(timeout=10)

    return enviados, fallidos, alertas, time.time() - inicio


def _velocidad_valida(enviados, duracion, segundos):
    """Evita dividir por cero cuando la corrida fue instantánea.

    Se pone aparte porque con un solo mensaje y sin espera la duración puede quedar redondeada a cero, y una división por cero cortaría el resumen justo al final, después de que todo lo demás salió bien."""
    return enviados > 0 and duracion > 0 and segundos > 0


def _resumen(enviados, fallidos, alertas, args, duracion):
    """Imprime el cierre de la corrida con los conteos y el ritmo real que se logró."""
    print("", flush=True)
    print(f"Enviados   {enviados}", flush=True)
    print(f"Fallidos   {fallidos}", flush=True)
    print(f"Alertas    {alertas}", flush=True)
    print(f"Topic      {args.topic}", flush=True)
    print(f"Broker     {config.KAFKA_BROKER}", flush=True)
    print(f"Duracion   {duracion:.1f} s", flush=True)
    if _velocidad_valida(enviados, duracion, args.segundos):
        print(f"Ritmo      {enviados / duracion:.2f} msg/s", flush=True)


def main():
    """Carga la foto en memoria, arma los eventos y los emite al ritmo pedido."""
    iniciar_log("02_productor")
    imprimir_header("02_productor.py")
    args = _argumentos()

    manifiesto, archivo = _manifiesto()
    if manifiesto is None:
        return 1

    eventos = _eventos(archivo)
    print("", flush=True)
    print(f"Foto      {manifiesto['archivo']}", flush=True)
    print(
        f"Rango     {manifiesto['desde']} a {manifiesto['hasta']}", flush=True)
    print(f"SHA-256   {manifiesto['sha256']}", flush=True)
    print(f"Eventos   {len(eventos)} disponibles", flush=True)

    if not eventos:
        print("La foto no tiene horas, no hay nada que mandar", flush=True)
        return 1

    # Se corta la lista en los mensajes pedidos.

    # Con menos de los que hay el productor no agota la foto, que es lo que conviene en una demo, y con más de los que hay se manda todo y después se corta.
    a_mandar = eventos[: max(args.mensajes, 0)]

    print(
        f"Voy a mandar {len(a_mandar)} mensajes al topic {args.topic}", flush=True)
    print(f"Ritmo      {args.segundos} s por mensaje", flush=True)
    print("", flush=True)

    if args.lista:
        for i, evento in enumerate(a_mandar, start=1):
            print(f"{i:04d} {json.dumps(evento, ensure_ascii=False)}", flush=True)
        print("", flush=True)
        print(
            f"Fin de la lista, no se mando nada ({len(a_mandar)} eventos)", flush=True)
        return 0

    if args.limpiar and not _vaciar_almacenamiento(args.topic):
        return 1

    if not _asegurar_topic(config.KAFKA_BROKER, args.topic):
        return 1

    enviados, fallidos, alertas, duracion = _emitir(
        _productor_de_kafka(), a_mandar, args)
    _resumen(enviados, fallidos, alertas, args, duracion)
    return 0 if fallidos == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
