"""Configuración central 

Este archivo reúne las direcciones y los valores que consulta el resto del código, y hace las veces de ficha de contacto del proyecto, porque repetir el domicilio en cien lugares distintos termina haciendo que dos copias se desincronicen sin que nadie se entere. Para adaptar el proyecto alcanza con editar el valor de acá, ya que ninguna dirección está escrita a mano en otro archivo.

Todo vive dentro de una sola carpeta, la del propio repositorio, y esa decisión explica casi todo lo demás. La raíz está en una ruta corta y sin espacios porque Spark en Windows se rompe cuando la ruta tiene espacios, y porque el sistema tiene un tope de 260 carácteres que se agota apenas metemos carpetas profundas. Y la carpeta pesada queda ignorada por git, porque pesa mucho y no le interesa a nadie el historial de un gigabyte de librerías.

Este archivo además deja el entorno preparado en el nivel del módulo, antes de que se importe PySpark, y el orden importa. Primero le aclara al intérprete cuál es la venv, después le avisa a Hadoop dónde está su carpeta y al final deja JAVA_HOME apuntando al JDK correcto. La máquina virtual de Java se lanza una sola vez, cuando se pide la sesión, y para ese momento las variables ya tienen que estar escritas, igual que hay que revisar los flujos del cerro antes de encenderlo.

De los valores en sí hay tres que conviene tener a mano. La memoria del driver está en 2 gigabytes porque esta cosa mía tiene 7,7 en total y Kafka se sirve una porción grande, así que si aparece un error de memoria conviene subir este número antes de sospechar de otra cosa. Y las carpetas de mezcla están en 4 y no en las 200 que Spark pone por omisión, dado que Spark reserva memoria para cada una y con doscientas se queda sin aire antes de empezar, de modo que si el dataset crece mucho este es el primer número que hay que subir, de uno en uno, mirando si sigue entrando todo.

El tercero es el que más costó encontrar, y merece su propio párrafo. Al escribir cualquier archivo, Spark le pone permisos, y en Windows esa operación la hace un binario de Hadoop llamado winutils que no viene con nada. Sin él la lectura de datos funciona sin problemas, pero la escritura revienta con un error que no dice nada de permisos, sino que se hace pasar por una ruta mal escrita, y por eso cuesta tanto darse cuenta. La versión que instalamos es la 3.4 porque es la más cercana a la 3.5 que trae PySpark, y el proyecto la declara por su cuenta para que en otra máquina no haya que configurarla a mano."""

import os
from pathlib import Path

# La carpeta donde winget deja el JDK de Eclipse, y dentro están las versiones con su número en el nombre
CARPETA_TEMURIN = Path(r"C:\Program Files\Eclipse Adoptium")


def resolver_java() -> Path | None:
    """Busca el JDK 17 de Temurin y lo devuelve si lo encuentra.

    Existe porque la variable JAVA_HOME se escribe en el registro de Windows y solo la leen las consolas que se abren después. Un proceso que ya venía corriendo se queda con el valor viejo, y si en esa máquina el PATH trae el Java 20 de Oracle, Spark arranca con una versión que no aguanta. El nombre va en mayúsculas porque así exactamente lo lee PySpark cuando lanza la máquina virtual. Si la carpeta no existe devolvemos nada en lugar de fallar, porque puede que en otra máquina el JDK se haya instalado en otro lado y en ese caso conviene que sea el propio sistema el que decida."""
    for candidato in sorted(CARPETA_TEMURIN.glob("jdk-17*")):
        if (candidato / "bin" / "java.exe").is_file():
            return candidato
    return None

# La raíz del proyecto, deducida desde la ubicación de este archivo para que ninguna ruta escrita a mano se quede vieja
RAIZ = Path(__file__).resolve().parents[2]

# El intérprete de la venv, que es el único de toda la máquina que tiene PySpark instalado
VENV_PYTHON = RAIZ / ".venv" / "Scripts" / "python.exe"

# Aclaramos que PySpark use la venv, y esto va antes de importar PySpark para que los procesos hijos abran el Python correcto
os.environ.setdefault("PYSPARK_PYTHON", str(VENV_PYTHON))
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", str(VENV_PYTHON))

# Apuntamos JAVA_HOME al JDK 17 antes de que arranque la máquina virtual, y si no lo encontramos dejamos la variable como estaba
_java_encontrado = resolver_java()
if _java_encontrado is not None:
    os.environ["JAVA_HOME"] = str(_java_encontrado)

# La carpeta con los binarios de Hadoop que Windows necesita para poder poner permisos a los archivos que Spark escribe
HADOOP_HOME = RAIZ / "winutils"

# La ruta que ve la JVM tiene que llevar barras normales, porque con contrabarras la JVM se los come al leer la propiedad y cree que la dirección no es absoluta
HADOOP_HOME_JVM = str(HADOOP_HOME).replace("\\", "/")

# Le avisamos a Hadoop dónde está esa carpeta, porque sin este dato la escritura de Parquet falla con un error que no parece del mismo problema
os.environ.setdefault("HADOOP_HOME", HADOOP_HOME_JVM)

# Los datos crudos, y las cuatro zonas separadas para que un error apunte siempre a un lugar conocido
DATOS = RAIZ / "datos"

# El estado del streaming, que se borra y se rehace cada vez que cambiamos la lógica de una ventana
CHECKPOINTS = RAIZ / "checkpoints"

# La memoria donde quedan los jars del conector de Kafka, que es lo que nos deja trabajar sin internet
IVY_DIR = RAIZ / ".ivy2"

# Las evidencias van dentro del repositorio porque son parte de la entrega y el profesor tiene que verlas
EVIDENCIAS = RAIZ / "evidencias"

# Los logs van a su propia carpeta para distinguirlos del código de un vistazo
LOGS = EVIDENCIAS / "logs"

# Los gráficos también van en las evidencias, porque son parte de lo que se entrega y un
# PNG suelto en la raíz se pierde entre el código
GRAFICOS = EVIDENCIAS / "graficos"

# La dirección del broker de Kafka, y el nombre es corto a propósito para no alargar más la ruta
KAFKA_HOME = RAIZ / "kafka"

# El puerto por omisión de Kafka en la máquina local, y el topic donde el productor deja y el consumidor saca
KAFKA_BROKER = "localhost:9092"
TOPIC_EVENTOS = "lab03.eventos"

# Dos hilos, porque con doce procesadores se podría más, pero Kafka también pide memoria y no conviene apretar la máquina
MASTER = "local[2]"

# El escritorio donde Spark ordena todo antes de escribir, súbelo a 4g si pasas a una máquina con 16 GB de RAM
DRIVER_MEMORY = "2g"

# Las carpetas donde Spark deposita los datos que debe mezclar, en 4 y no en las 200 del valor por omisión para no quedarnos sin memoria
SHUFFLE_PARTITIONS = "4"

# Fijamos la hora para que las ventanas del streaming agrupen siempre igual, porque si flotara dos corridas darían horas distintas
ZONA_HORARIA = "America/Santiago"

# Las fechas se leen en modo tolerante porque el dataset viene sucio a propósito, y estricto se caería en el primer dato raro
POLITICA_FECHA = "CORRECTED"

# El conector oficial de Spark para Kafka, ojo que es spark-sql-kafka y no spark-kafka, y la 2.13 es la versión de Scala
SPARK_PACKAGES = "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.9"

# La semilla del generador, que es lo que hace que dos corridas produzcan los mismos datos y así las bitácoras se comparen
SEED = 20260928

# Las cuatro zonas del lago, que se nombran juntas para que todos los scripts apunten al mismo orden
ZONAS_DATOS = ("raw", "bronze", "silver", "gold")

# La carpeta de resultados de cada arquitectura, que se llama igual en las tres ramas
CARPETA_SALIDAS = "salidas"


def preparar_winutils() -> None:
    # Deja la biblioteca nativa de Windows con el nombre que Hadoop espera encontrar.
    carpeta_bin = HADOOP_HOME / "bin"
    if not carpeta_bin.is_dir():
        return
    origen = carpeta_bin / "hadoop.dll"
    destino = carpeta_bin / "winutils.dll"
    if origen.is_file() and not destino.is_file():
        destino.write_bytes(origen.read_bytes())


def asegurar_carpetas() -> None:
    """Crea las carpetas del proyecto cuando todavía no existen. 
        
        Existe por un motivo concreto, cuando alguien clona el repositorio en otra máquina las carpetas vacías no viajan con él porque git no guarda directorios sin archivos, así que sin esta función el primer script se caería al intentar escribir en una carpeta fantasma."""
    for zona in ZONAS_DATOS:
        (DATOS / zona).mkdir(parents=True, exist_ok=True)
    CHECKPOINTS.mkdir(parents=True, exist_ok=True)
    IVY_DIR.mkdir(parents=True, exist_ok=True)
    LOGS.mkdir(parents=True, exist_ok=True)
    (EVIDENCIAS / "pantallazos").mkdir(parents=True, exist_ok=True)
    (EVIDENCIAS / "entorno").mkdir(parents=True, exist_ok=True)
    GRAFICOS.mkdir(parents=True, exist_ok=True)
    (EVIDENCIAS / "explain").mkdir(parents=True, exist_ok=True)
