"""Traza de cada ejecución para que la bitácora no se escriba a mano.

Cada log arranca con un encabezado que dice cuándo se corrió el script, con qué commit del código y con qué comando exacto, de manera que un log suelto en la carpeta de evidencias sirva como prueba sin que haga falta un párrafo al lado que lo explique. Es la diferencia entre guardar la consola y guardar un registro.

Lo único que hace este módulo es juntar cuatro datos que de otro modo cada script tendría que buscar por su cuenta. 

El módulo no importa PySpark al principio a propósito, porque muchos scripts lo necesitan
antes de tener una sesión y otros no lo necesitan nunca, como el que arma el entorno. Por
eso la versión de Spark se lee recién cuando se la pide, y si todavía no está instalada
simplemente se anota que no se pudo leer en vez de reventar.
"""

import atexit
import platform
import subprocess
import sys
from datetime import datetime, timezone

# Los espejos de la consola y de los errores que están encendidos ahora, si lo hay, y la
# ruta del archivo que escriben. Se guardan separados porque cerrar el log es volver a la
# salida original de las dos, y para eso hace falta tenerlas a mano
_espejo_salida = None
_espejo_error = None
_ruta_del_log = None


class _Espejo:
    """Manda a la vez a la consola y al archivo de log todo lo que se imprime.

    Esto reemplaza al guion de redirección de la shell ("> log 2>&1"). Con la redirección
    la consola se queda muda, así que quien ejecuta no ve si el script avanza o se cortó a
    mitad de camino, y si algo falla el error queda escondido en un archivo que primero hay
    que acordarse de abrir. Espejando la salida se ve todo en pantalla y al mismo tiempo
    queda el archivo para la bitácora, con el comando siempre igual.

    La consola de Windows trabaja con otra tabla de caracteres que el archivo, así que una
    letra rara puede romper el escrito y cortar la corrida. Antes de tirar la excepción se
    intenta otra vez con letras de reemplazo: se prefiere un signo de pregunta en la
    pantalla antes que perder la ejecución entera.
    """

    def __init__(self, original, archivo):
        self._original = original
        self._archivo = archivo

    def write(self, texto):
        try:
            self._original.write(texto)
        except UnicodeEncodeError:
            self._original.write(texto.encode("ascii", "replace").decode("ascii"))
        self._archivo.write(texto)
        return len(texto)

    def flush(self):
        self._original.flush()
        self._archivo.flush()

    def isatty(self):
        # Se responde lo mismo que la consola de verdad, porque hay bibliotecas que
        # imprimen distinto según creen que están frente a una terminal
        try:
            return self._original.isatty()
        except Exception:
            return False

    def fileno(self):
        return self._original.fileno()


def _commit_actual():
    """Devuelve el commit corto de git, o un texto de reemplazo si no hay repositorio.

    Se usa git porque el commit identifica la versión exacta del código que produjo el log, y con eso se puede volver a esa versión para reproducir el resultado. La carpeta del repositorio se le pasa explícitamente en vez de confiar en la carpeta de trabajo, porque si alguien corre el script desde el escritorio el comando se queda sin repositorio y el commit se pierde, que es justo el dato más importante del encabezado."""
    try:
        import config

        carpeta = str(config.RAIZ)
    except Exception:
        carpeta = None
    try:
        salida = subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"],
            stderr=subprocess.DEVNULL,
            text=True,
            cwd=carpeta,
        )
        return salida.strip()
    except Exception:
        return "sin-repositorio"


def _version_spark():
    """Devuelve la versión de PySpark instalada, o avisa que no pudo leerla.

    Se lee acá adentro y no arriba del archivo porque importar PySpark es caro y además obliga a que la máquina virtual de Java esté encendida, y para imprimir un encabezado no vale la pena pedirle eso al sistema.
    """
    try:
        import pyspark

        return pyspark.__version__
    except Exception:
        return "no-disponible"


def imprimir_header(script):
    """Imprime el encabezado de trazabilidad al comienzo de cada ejecución.

    Se llama una sola vez, apenas arranca el script, y conviene que sea lo primero que aparezca en el log. El flush del cierre está porque Python acumula la salida en un búfer cuando el log no va a una terminal, y sin eso el encabezado puede quedar al final del archivo o directamente no aparecer si el script se corta."""
    ahora = datetime.now(timezone.utc).isoformat(timespec="seconds")
    print(f"{script}  {ahora}", flush=True)
    print(f"commit  {_commit_actual()}", flush=True)
    print(f"comando  {' '.join(sys.argv)}", flush=True)
    print(f"python  {sys.version.split()[0]} en {sys.executable}", flush=True)
    print(f"pyspark  {_version_spark()}", flush=True)
    print(f"plataforma  {platform.platform()}", flush=True)


def iniciar_log(nombre):
    """Abre el log de la corrida y a partir de ahí espejea en él todo lo que se imprime.

    Se llama una sola vez, antes del encabezado, y se le pasa el nombre propio del script y
    no un número de orden. Con nombre propio el archivo no depende de en qué orden se
    corrieron los otros pasos ni de cuántos van, así que un log suelto se entiende sin
    tener que mirar la carpeta entera.

    Se espejean también los errores. Si el script se corta con una excepción, el aviso sale
    por el canal de errores y no por la salida normal, y sin esto el log terminaría diciendo
    que todo anduvo bien en una corrida que en realidad se cortó a mitad de camino.

    Si ya había un log encendido no hace nada, que es lo que conviene cuando un script se
    usa como módulo de otro: el que mandó sigue mandando y no se pisan dos archivos a la vez.
    """
    global _espejo_salida, _espejo_error, _ruta_del_log

    if _espejo_salida is not None:
        return _ruta_del_log

    import config

    config.LOGS.mkdir(parents=True, exist_ok=True)
    ruta = config.LOGS / f"{nombre}.log"
    archivo = open(ruta, "w", encoding="utf-8", newline="\n")

    _espejo_salida = _Espejo(sys.stdout, archivo)
    _espejo_error = _Espejo(sys.stderr, archivo)
    _ruta_del_log = ruta
    sys.stdout = _espejo_salida
    sys.stderr = _espejo_error
    # Se cierra al terminar el proceso y no al final de main, porque si el script se corta
    # con una excepción main nunca llega al final y el archivo quedaría sin cerrar
    atexit.register(cerrar_log)
    return ruta


def cerrar_log():
    """Anota adónde quedó el log, devuelve la consola y cierra el archivo."""
    global _espejo_salida, _espejo_error, _ruta_del_log

    if _espejo_salida is None:
        return

    espejo_salida, espejo_error, ruta = _espejo_salida, _espejo_error, _ruta_del_log
    _espejo_salida = None
    _espejo_error = None
    _ruta_del_log = None

    print("", flush=True)
    print(f"Log       {ruta}", flush=True)

    # Primero se descargan los búferes de las dos salidas mientras el archivo todavía está
    # abierto, y recién después se cierra
    espejo_salida.flush()
    espejo_error.flush()
    sys.stdout = espejo_salida._original
    sys.stderr = espejo_error._original
    espejo_salida._archivo.close()
