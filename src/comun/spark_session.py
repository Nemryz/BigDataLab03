import config


def get_spark(nombre_app, con_kafka=False):
    #Devuelve una sesión de Spark configurada según lo que define config.py.
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

    # El conector de Kafka entra solo si el interruptor está encendido.
    # Hay dos caminos, y el que se elige depende de si los jars ya están bajados en la máquina.
    # El primero manda a Ivy a buscarlos a Maven, que es como se consiguen la primera vez, pero
    # para eso hace falta internet y además Ivy vuelve a resolver la dependencia en cada
    # arranque, con un cache que caduca a las 24 horas. Justo eso es lo que no queremos el día
    # de la defensa, así que si los jars ya están en la carpeta del proyecto se pasan a la JVM
    # por su ruta directa y la sesión ni se entera de que existe una red.
    if con_kafka:
        jars_locales = config.jars_kafka_locales()
        if jars_locales:
            constructor = constructor.config("spark.jars", ",".join(jars_locales))
        else:
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
