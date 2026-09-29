"""Fábrica de sesiones de Spark para el Laboratorio 03 de Big Data.

Este archivo tiene una sola misión, que es el molde con el que se fabrica una sesión de
Spark ya configurada, para que ningún script tenga que acordarse de los valores. Es como
una receta donde todos los cocineros usan la misma bandeja, y no como una libreta donde
cada uno anota las suyas a su modo. La única diferencia entre un script y otro es si
necesitan Kafka, y para eso existe el interruptor que aparece más abajo, de manera que un
script que no usa Kafka no arrastra el conector ni espera su descarga.
"""
import config


def get_spark(nombre_app, con_kafka=False):
    """Devuelve una sesión de Spark configurada según lo que define config.py.

    El nombre de la aplicación es lo que aparece arriba en la interfaz web de Spark, que
    es la pantalla que usamos de evidencia, así que conviene que diga algo reconocible en
    lugar de un nombre genérico. El interruptor de Kafka se deja apagado por omisión porque
    pedir el conector tiene un costo la primera vez y solo lo pagarían los scripts que en
    verdad leen o escriben en el topic.
    """
    from pyspark.sql import SparkSession

    # Antes de armar nada dejamos la biblioteca nativa de Windows con el nombre que Hadoop busca
    config.preparar_winutils()

    # Arrancamos el constructor con el nombre de la aplicación, y de ahí en adelante le vamos sumando opciones
    constructor = SparkSession.builder.appName(nombre_app)

    # El modo local con dos hilos, que en una máquina de la casa significa que todo corre acá sin clúster de verdad
    constructor = constructor.master(config.MASTER)

    # Le damos 2 GB al proceso principal, que es el que sostiene a los demás, y es el ajuste que evita que la demo se ahogue
    constructor = constructor.config("spark.driver.memory", config.DRIVER_MEMORY)

    # Bajamos las carpetas de mezcla de 200 a 4, que es el cambio que más se nota en una máquina chica con 7,7 GB
    constructor = constructor.config("spark.sql.shuffle.partitions", config.SHUFFLE_PARTITIONS)

    # Fijamos la hora porque las ventanas del streaming y las marcas de agua la usan, y si flotara dos corridas no coincidirían
    constructor = constructor.config("spark.sql.session.timeZone", config.ZONA_HORARIA)

    # Ponemos las fechas en modo tolerante, porque el dataset viene sucio y en modo estricto la sesión se caería de entrada
    constructor = constructor.config("spark.sql.legacy.timeParserPolicy", config.POLITICA_FECHA)

    # Apagamos la barra de progreso, porque llena los logs de caracteres de control y los deja impossibles de copiar para el informe
    constructor = constructor.config("spark.ui.showConsoleProgress", "false")

    # Le indicamos dónde dejar los jars que baja de internet, y esta línea es la que nos salva si el día de la defensa no hay red
    constructor = constructor.config("spark.jars.ivy", str(config.IVY_DIR))

    # El conector de Kafka entra solo si el interruptor está encendido, así la rama Lakehouse nunca espera la descarga de esos megas
    if con_kafka:
        constructor = constructor.config("spark.jars.packages", config.SPARK_PACKAGES)

    # Le pasamos a la JVM dónde está la carpeta de Hadoop y dónde buscar bibliotecas, y sin esto cualquier escritura falla
    # Ojo que la dirección tiene que llevar barras normales, porque con contrabarras la JVM se los come al leer la propiedad
    opciones_java = "-Dhadoop.home.dir=" + config.HADOOP_HOME_JVM
    opciones_java += " -Djava.library.path=" + config.HADOOP_HOME_JVM + "/bin"
    constructor = constructor.config("spark.driver.extraJavaOptions", opciones_java)

    # Pedimos la sesión armada, y el método reutiliza la anterior si ya existía en vez de abrir un segundo motor que pelee por la memoria
    spark = constructor.getOrCreate()

    # Subimos el nivel de los mensajes a WARNING, para que la consola se lea limpia y los logs sirvan como evidencia legible
    spark.sparkContext.setLogLevel("WARN")

    return spark
