"""Arma la sesión de Spark que usan todos los scripts del laboratorio.

Los valores viven en config.py, y acá solo se traducen a la forma en que el constructor de PySpark los recibe.

El archivo importa config en la primera línea a propósito, porque config deja escrito el entorno de Java y de la venv al importarse y ese trabajo tiene que existir antes de que se cargue PySpark.
"""

import config

# Las opciones que comparten todas las sesiones, guardadas como pares de clave y valor para que sumar una signifique agregar un renglón y nada más.

# El orden no importa, Spark las aplica todas antes de pedir la sesión.
OPCIONES_BASE = (
    ("spark.driver.memory", config.DRIVER_MEMORY),
    ("spark.sql.shuffle.partitions", config.SHUFFLE_PARTITIONS),
    ("spark.sql.session.timeZone", config.ZONA_HORARIA),
    ("spark.sql.legacy.timeParserPolicy", config.POLITICA_FECHA),
    ("spark.ui.showConsoleProgress", "false"),
    ("spark.jars.ivy", str(config.IVY_DIR)),
)

# Las dos claves entre las que se decide el camino del conector de Kafka
CLAVE_JARS_LOCALES = "spark.jars"
CLAVE_JARS_IVY = "spark.jars.packages"


def _conector_de_kafka() -> tuple[str, str]:
    """Devuelve la clave y el valor con los que el conector de Kafka entra a la sesión.

    Hay dos caminos y el que se elige depende de si los jars ya están bajados en la máquina.

    El primero manda a Ivy a buscarlos a Maven, que es como se consiguen la primera vez, pero para eso hace falta internet y además Ivy vuelve a resolver la dependencia en cada arranque, con un cache que caduca a las 24 horas.

    Justo eso es lo que no queremos el día de la defensa, así que si los jars ya están en la carpeta del proyecto se pasan a la JVM por su ruta directa y la sesión ni se entera de que existe una red.

    Si algún día cambia el nombre de alguna de las dos claves, alcanza con tocar el par que devuelve esta función y el resto del módulo sigue igual."""
    locales = config.jars_kafka_locales()
    if locales:
        return CLAVE_JARS_LOCALES, ",".join(locales)
    return CLAVE_JARS_IVY, config.SPARK_PACKAGES


def _opciones_de_java() -> str:
    # Le pasamos a la JVM dónde está la carpeta de Hadoop y dónde buscar bibliotecas, y sin esto cualquier escritura falla.

    # La dirección tiene que llevar barras normales, porque con contrabarras la JVM se los come al leer la propiedad y cree que la ruta no es absoluta.
    carpeta = config.HADOOP_HOME_JVM
    return f"-Dhadoop.home.dir={carpeta} -Djava.library.path={carpeta}/bin"


def get_spark(nombre_app: str, con_kafka: bool = False):
    """Devuelve una sesión de Spark configurada con los valores de config.py.

    El modo local con dos hilos significa que todo corre en esta máquina, sin clúster de verdad, que es lo que corresponde en una demo de casa.

    La conexión con Kafka se pide con un interruptor y no con un parámetro obligatorio, porque hay scripts que solo leen Parquet y para esos el conector es peso muerto.

    El método reutiliza la sesión anterior si ya existía, en vez de abrir un segundo motor que pelee por la memoria con el primero."""
    # Antes de ararmos dejamos la biblioteca nativa de Windows con el nombre que Hadoop busca
    config.preparar_winutils()

    from pyspark.sql import SparkSession

    constructor = SparkSession.builder.appName(nombre_app)
    constructor = constructor.master(config.MASTER)
    for clave, valor in OPCIONES_BASE:
        constructor = constructor.config(clave, valor)

    if con_kafka:
        clave, valor = _conector_de_kafka()
        constructor = constructor.config(clave, valor)

    constructor = constructor.config("spark.driver.extraJavaOptions", _opciones_de_java())

    # Pedimos la sesión armada
    spark = constructor.getOrCreate()

    # Subimos el nivel de los mensajes a WARNING, para que la consola se lea limpia y los logs sirvan como evidencia legible
    spark.sparkContext.setLogLevel("WARN")

    return spark
