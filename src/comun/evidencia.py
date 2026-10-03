"""Traza de cada ejecución para que la bitácora no se escriba a mano.

Cada log arranca con un encabezado que dice cuándo se corrió el script, con qué commit del código y con qué comando exacto, de manera que un log suelto en la carpeta de evidencias sirva como prueba sin que haga falta un párrafo al lado que lo explique. Es la diferencia entre guardar la consola y guardar un registro.

Lo único que hace este módulo es juntar cuatro datos que de otro modo cada script tendría que buscar por su cuenta. 

El módulo no importa PySpark al principio a propósito, porque muchos scripts lo necesitan
antes de tener una sesión y otros no lo necesitan nunca, como el que arma el entorno. Por
eso la versión de Spark se lee recién cuando se la pide, y si todavía no está instalada
simplemente se anota que no se pudo leer en vez de reventar.
"""

import platform
import subprocess
import sys
from datetime import datetime, timezone


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
