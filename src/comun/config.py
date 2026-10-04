"""Configuración central del Laboratorio 03.

Este archivo reúne las direcciones y los valores que consulta el resto del código, y hace las veces de ficha de contacto del proyecto.

Repetir el domicilio en cien lugares distintos termina haciendo que dos copias se desincronicen sin que nadie se entere, así que adaptar el proyecto alcanza con editar el valor de acá.

Ninguna dirección está escrita a mano en otro archivo.

Todo vive dentro de una sola carpeta, la del propio repositorio, y esa decisión explica casi todo lo demás.

La raíz está en una ruta corta y sin espacios porque Spark en Windows se rompe cuando la ruta tiene espacios, y porque el sistema tiene un tope de 260 caracteres que se agota apenas metemos carpetas profundas.

La carpeta pesada queda ignorada por git, porque pesa mucho y no le interesa a nadie el historial de un gigabyte de librerías.

Este archivo además deja el entorno preparado en el nivel del módulo, antes de que se importe PySpark, y el orden importa.

Primero le aclara al intérprete cuál es la venv, después le avisa a Hadoop dónde está su carpeta y al final deja JAVA_HOME apuntando al JDK correcto.

La máquina virtual de Java se lanza una sola vez, cuando se pide la sesión, y para ese momento las variables ya tienen que estar escritas, igual que hay que revisar los flujos del cerro antes de encenderlo.

De los valores en sí hay tres que conviene tener a mano.

La memoria del driver está en 2 gigabytes porque esta máquina tiene 7,7 en total y Kafka se sirve una porción grande, así que si aparece un error de memoria conviene subir este número antes de sospechar de otra cosa.

Las carpetas de mezcla están en 4 y no en las 200 que Spark pone por omisión, dado que Spark reserva memoria para cada una y con doscientas se queda sin aire antes de empezar, de modo que si el dataset crece mucho este es el primer número que hay que subir, de uno en uno, mirando si sigue entrando todo.

El tercero es el que más costó encontrar, y merece su propio párrafo.

Al escribir cualquier archivo, Spark le pone permisos, y en Windows esa operación la hace un binario de Hadoop llamado winutils que no viene con nada.

Sin él la lectura de datos funciona sin problemas, pero la escritura revienta con un error que no dice nada de permisos, sino que se hace pasar por una ruta mal escrita, y por eso cuesta tanto darse cuenta.

La versión que instalamos es la 3.4 porque es la más cercana a la 3.5 que trae PySpark, y el proyecto la declara por su cuenta para que en otra máquina no haya que configurarla a mano.
"""

import os
from pathlib import Path

# La carpeta donde winget deja el JDK de Eclipse, y dentro están las versiones con su número en el nombre
CARPETA_TEMURIN = Path(r"C:\Program Files\Eclipse Adoptium")


def resolver_java() -> Path | None:
    """Busca el JDK 17 de Temurin y lo devuelve si lo encuentra.

    Existe porque la variable JAVA_HOME se escribe en el registro de Windows y solo la leen las consolas que se abren después.

    Un proceso que ya venía corriendo se queda con el valor viejo, y si en esa máquina el PATH trae el Java 20 de Oracle, Spark arranca con una versión que no aguanta.

    El nombre va en mayúsculas porque así exactamente lo lee PySpark cuando lanza la máquina virtual.

    Si la carpeta no existe devolvemos nada en lugar de fallar, porque puede que en otra máquina el JDK se haya instalado en otro lado y en ese caso conviene que sea el propio sistema el que decida."""
    for candidato in sorted(CARPETA_TEMURIN.glob("jdk-17*")):
        if (candidato / "bin" / "java.exe").is_file():
            return candidato
    return None


# La raíz del proyecto, deducida desde la ubicación de este archivo para que ninguna ruta escrita a mano se quede vieja
RAIZ = Path(__file__).resolve().parents[2]

# El intérprete de la venv, que es el único de toda la máquina que tiene PySpark instalado
VENV_PYTHON = RAIZ / ".venv" / "Scripts" / "python.exe"

# La carpeta con los binarios de Hadoop que Windows necesita para poner permisos a los archivos que Spark escribe
HADOOP_HOME = RAIZ / "winutils"

# La ruta que ve la JVM tiene que llevar barras normales, porque con contrabarras la JVM se los come al leer la propiedad y cree que la dirección no es absoluta
HADOOP_HOME_JVM = str(HADOOP_HOME).replace("\\", "/")


def _fijar_entorno() -> None:
    """Escribe las tres variables que PySpark y Hadoop leen cuando arrancan.

    Corre una sola vez, al importar este archivo, y por eso va antes de cualquier import de PySpark.

    El orden de las asignaciones decide si la máquina virtual encuentra el intérprete correcto, la carpeta de Hadoop y el JDK que esperaba.

    Dos de las tres se ponen con setdefault para respetar lo que la máquina ya traía puesta, salvo JAVA_HOME, que se sobreescribe siempre porque una versión equivocada de Java es justamente el fallo que buscamos evitar."""
    os.environ.setdefault("PYSPARK_PYTHON", str(VENV_PYTHON))
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", str(VENV_PYTHON))
    java = resolver_java()
    if java is not None:
        os.environ["JAVA_HOME"] = str(java)
    os.environ.setdefault("HADOOP_HOME", HADOOP_HOME_JVM)


_fijar_entorno()

# Los datos crudos, y la carpeta de cada zona para que un error apunte siempre a un lugar conocido
DATOS = RAIZ / "datos"

# El estado del streaming, que se borra y se rehace cada vez que cambiamos la lógica de una ventana
CHECKPOINTS = RAIZ / "checkpoints"

# La memoria donde quedan los jars del conector de Kafka, que es lo que nos deja trabajar sin internet
IVY_DIR = RAIZ / ".ivy2"

# Las evidencias van dentro del repositorio porque son parte de la entrega y el profesor tiene que verlas
EVIDENCIAS = RAIZ / "evidencias"

# Los logs van a su propia carpeta para distinguirlos del código de un vistazo
LOGS = EVIDENCIAS / "logs"

# Los gráficos también van en las evidencias, porque son parte de lo que se entrega y un PNG suelto en la raíz se pierde entre el código
GRAFICOS = EVIDENCIAS / "graficos"

# La dirección del broker de Kafka, y el nombre es corto a propósito para no alargar más la ruta
KAFKA_HOME = RAIZ / "kafka"
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

# El conector oficial de Spark para Kafka, ojo que es spark-sql-kafka y no spark-kafka.

# El número que sigue al guión es la versión de Scala, y tiene que ser la de la distribución que instaló pip, no la última que exista.

# PySpark 3.5.9 viene compilado con Scala 2.12, como se ve en el nombre del jar spark-sql_2.12-3.5.9.jar que trae dentro de su carpeta de jars, así que el conector también tiene que ser 2.12.

# Con 2.13 el Ivy resuelve una dependencia que no existe en esa distribución y la sesión revienta al leer el primer topic.
SPARK_PACKAGES = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.9"


def jars_kafka_locales() -> list[str]:
    """Devuelve las rutas de los jars del conector de Kafka que ya están en el proyecto, o una lista vacía.

    Ivy deja una copia de cada jar en dos lugares distintos dentro de .ivy2, uno en la cache con la estructura de nombres de Maven y otro en la carpeta jars donde junta todos con el nombre simplificado.

    El segundo es el que sirve para este propósito, porque es el que se le puede pasar a la JVM como una lista de archivos sueltos sin tener que entender de dónde vino cada uno.

    Se busca el conector de Kafka nada más y no todos los jars del proyecto, porque si algún día aparece otro conector hay que pedirlo por su cuenta.

    Si el conector está pero le falta una de sus dependencias, la lista se devuelve vacía igual, porque una lista incompleta hace que la JVM arranque y reviente después al leer el primer topic, que es un error mucho más caro de entender que no encontrar nada y bajar todo de nuevo.

    Todos los patrones llevan un asterisco adelante, y eso no es un detalle menor.

    Ivy no guarda el archivo como spark-sql-kafka-0-10_2.12-3.5.9.jar sino con el grupo de Maven pegado al frente, o sea org.apache.spark_spark-sql-kafka-0-10_2.12-3.5.9.jar, así que un patrón que empiece por el nombre del paquete no matchea nada y la función devuelve la lista vacía en silencio.

    Ese fallo no avisa, simplemente se va por el otro camino y baja todo de nuevo."""
    carpeta = IVY_DIR / "jars"
    if not carpeta.is_dir():
        return []

    conector = sorted(carpeta.glob("*spark-sql-kafka-0-10_*.jar"))
    if not conector:
        return []

    # Las dependencias que la resolución de Ivy trajo junto con el conector, sacadas de la lista que imprime el propio Ivy en su informe de resolución.

    # Se declaran por nombre y no por versión para que una actualización de paquete no deje la lista desactualizada.
    dependencias = (
        "*spark-token-provider-kafka-0-10_*.jar",
        "*kafka-clients-*.jar",
        "*commons-pool2-*.jar",
        "*slf4j-api-*.jar",
        "*lz4-java-*.jar",
        "*snappy-java-*.jar",
        "*jsr305-*.jar",
        "*commons-logging-*.jar",
        "*hadoop-client-api-*.jar",
        "*hadoop-client-runtime-*.jar",
    )

    encontrados: list[str] = []
    for patron in [conector[0].name, *dependencias]:
        coincidencia = sorted(carpeta.glob(patron))
        if not coincidencia:
            return []
        encontrados.append(str(coincidencia[0]))

    return encontrados


# La semilla del generador, que es lo que hace que dos corridas produzcan los mismos datos y así las bitácoras se comparen
SEED = 20260928

# Las carpetas de datos que tiene que existir.

# En la arquitectura Lambda solo hay una, la foto cruda que baja 01_descargar.py.

# No hay zonas intermedias porque acá no hay lago que transformar por capas, el dato entra por Kafka y de ahí sale por los dos caminos que son el batch y el streaming.

# Se deja la lista igual de todas formas para que los scripts no tengan que cambiar si alguna vez se agrega una carpeta.
ZONAS_DATOS = ("raw",)

# La carpeta de resultados de cada fase, y se llama igual en todas para que el informe pueda compararlas sin traducir nombres
CARPETA_SALIDAS = "salidas"

# Las subcarpetas dentro de las evidencias.

# Se juntan en una sola tupla para que sumar una sala nueva signifique agregar un nombre y nada más.
SUBCARPETAS_EVIDENCIAS = ("pantallazos", "entorno", "graficos", "explain")


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

    Existe porque cuando alguien clona el repositorio en otra máquina las carpetas vacías no viajan con él, ya que git no guarda directorios sin archivos.

    Sin esta función el primer script se caería al intentar escribir en una carpeta fantasma.

    Recorre las listas declaradas más arriba y le pide a cada carpeta que se cree si hace falta, de modo que el día que aparezca una nueva alcanza con agregarla a la tupla correspondiente."""
    requeridas = [*(DATOS / zona for zona in ZONAS_DATOS), CHECKPOINTS, IVY_DIR, LOGS]
    requeridas += [EVIDENCIAS / nombre for nombre in SUBCARPETAS_EVIDENCIAS]
    for carpeta in requeridas:
        carpeta.mkdir(parents=True, exist_ok=True)
