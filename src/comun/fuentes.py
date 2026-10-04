"""Catálogo de fuentes de datos reales para el Laboratorio 03.

Aquí van las fuentes que se usaron, las tres verificadas contra el servidor antes de anotarlas, y no hay más.

La idea de tenerlas en un diccionario al principio del archivo es que mañana sea fácil agregar una cuarta, porque alcanza con escribir una entrada más y nada más tiene que cambiar en el resto del código.

Las tres vienen del mismo dominio, que es la calidad del aire en Chile, y eso es a propósito.

Las tres ramas del laboratorio procesan el mismo problema, y por eso la tabla comparativa tiene sentido, porque estamos midiendo arquitecturas sobre la misma pregunta y no dos preguntas distintas.

Si cada rama usara su propio dominio, cualquier diferencia en los resultados podría deberse a los datos y no a la arquitectura.

Por qué estas y no otras.

La primera necesidad es que no pidan clave de API, porque una clave es un dato que hay que proteger, que se puede filtrar y que nadie del equipo va a tener en un trabajo de cátedra.

La segunda es que el dato de una fecha dada sea estable, y esto es lo importante.

Si el servidor entregara valores que cambian con cada llamada, como hace uno de precios de bolsa, el hash de reproducibilidad del proyecto nunca va a cuadrar y la bitácora no se puede comparar entre corridas.

Las tres son pronósticos archivados por hora, no sensores vivos, así que lo que se descarga hoy para el martes da el mismo número que se hubiera descargado el lunes.

La tercera necesidad es que el dato tenga forma de evento, o sea una marca de tiempo y algo que se pueda medir.

Una lectura horaria de material particulado cumple las dos, y además tiene una propiedad que nos viene perfecta para la ventana de sesión de Kappa, que es que las horas malas de contaminación vienen en rachas.

Una racha es literalmente una sesión.
"""

# Las ciudades que se usan en todo el laboratorio, con la latitud y la longitud de su centro urbano.

# El formato de las coordenadas lo exige el servidor y va al revés, o sea primero la latitud y después la longitud, y es el orden en que se mandan en la consulta.
CIUDADES = {
    "santiago": {"nombre": "Santiago", "latitude": -33.45, "longitude": -70.67},
    "mendoza": {"nombre": "Mendoza", "latitude": -32.99, "longitude": -68.85},
    "valparaiso": {"nombre": "Valparaíso", "latitude": -33.05, "longitude": -71.62},
    "puerto_montt": {"nombre": "Puerto Montt", "latitude": -41.47, "longitude": -72.94},
}

# Los tres contaminantes que se piden, con la clave que usa el servidor y la unidad en que viene.

# La unidad la anotamos aparte porque el servidor no la manda en ningún lado, y sin ella un valor de 29 no se sabe si es miligramos, microgramos u otra cosa.
CONTAMINANTES = {
    "pm2_5": {"nombre": "PM2.5", "unidad": "ug/m3", "descripcion": "partículas finas, las que más afectan"},
    "pm10": {"nombre": "PM10", "unidad": "ug/m3", "descripcion": "partículas gruesas"},
    "nitrogen_dioxide": {"nombre": "NO2", "unidad": "ug/m3", "descripcion": "dióxido de nitrógeno"},
}

# El umbral por sobre del cual una hora se considera contaminada.

# Con 25 se usa el valor que la propia Organización Mundial de la Salud sugiere como límite diario, y con eso el cálculo de episodios tiene un respaldo y no es un número que sacamos de la manga.
UMBRAL_EPISODIO = 25.0

# Las tres fuentes verificadas.

# Cada una anota para qué se eligió, qué clase de archivo produce y qué forma tiene la dirección, porque los tres servidores no se llaman igual ni devuelven lo mismo.
FUENTES = {
    "aire_horario": {
        "nombre": "Calidad del aire horaria, Open-Meteo",
        "servidor": "air-quality-api.open-meteo.com",
        "uso": "Las tres ramas, es la fuente principal",
        "formato": "json",
        "descripcion": "Lecturas horarias de contaminante por ciudad, con histórico y pronóstico",
        "url": (
            "https://air-quality-api.open-meteo.com/v1/air-quality"
            "?latitude={latitudes}"
            "&longitude={longitudes}"
            "&hourly={contaminantes}"
            "&timezone=America%2FSantiago"
            "&start_date={desde}"
            "&end_date={hasta}"
        ),
        "notas": "Devuelve un objeto por ciudad, cada uno con un arreglo de horas en hourly",
    },
    "aire_forecast": {
        "nombre": "Calidad del aire del futuro, Open-Meteo",
        "servidor": "air-quality-api.open-meteo.com",
        "uso": "Para tener horas siguientes a la exposición si queremos en vivo",
        "formato": "json",
        "descripcion": "Lo mismo que la anterior pero sin fechas, se le pide el futuro",
        "url": (
            "https://air-quality-api.open-meteo.com/v1/air-quality"
            "?latitude={latitudes}"
            "&longitude={longitudes}"
            "&hourly={contaminantes}"
            "&timezone=America%2FSantiago"
            "&forecast_days={dias}"
        ),
        "notas": "Sirve para la versión en vivo, pero el dato cambia al pasar las horas",
    },
    "aire_archivo": {
        "nombre": "Archivo histórico diario, Open-Meteo",
        "servidor": "archive-api.open-meteo.com",
        "uso": "Descarga diaria en CSV, alternativa para la comparación de ramas",
        "formato": "csv",
        "descripcion": "Resumen diario por ciudad, y viene con una fila de metadatos arriba",
        "url": (
            "https://archive-api.open-meteo.com/v1/archive"
            "?latitude={latitude}"
            "&longitude={longitude}"
            "&start_date={desde}"
            "&end_date={hasta}"
            "&daily=temperature_2m_mean,precipitation_sum"
            "&timezone=America%2FSantiago"
            "&format=csv"
        ),
        "notas": "Devuelve CSV literal, con una línea de metadatos antes de los datos y las unidades en el nombre de la columna",
    },
}
