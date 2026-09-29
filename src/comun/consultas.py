r"""Las consultas analíticas del proyecto, escritas una sola vez y usadas por dos scripts.

El motivo de existir de este módulo es evitar una trampa que no se ve hasta que aparece.
'04_consultas.py' corre las consultas y guarda los resultados, y '06_explain.py' tiene que
tomar la consulta más pesada y pedirle a Spark que explique cómo piensa ejecutarla. Si cada
uno escribiera su propia copia del SQL, el plan que se explica dejaría de ser el plan de la
consulta que realmente corre en cuanto alguien cambie una sola cosa en un archivo, y la
evidencia terminaría explicando un texto que ya nadie ejecuta.

Por eso el SQL vive acá, que es un módulo sin Spark, sin rutas y sin nada que pueda
fallar, y los dos scripts lo importan. Cambiar una consulta en un solo lugar las cambia en
las dos cosas que dependen de ella, que es lo que hace que la evidencia siga siendo
verdadera.

Las consultas son cuatro y las tres primeras contestan las preguntas del trabajo. La
cuarta es un resumen que se calcula encima de la tercera, porque contar episodios por
ciudad no sirve mucho sin la duración promedio y sin el pico, y esos dos números son los
que después se comparan contra las otras dos arquitecturas.

La consulta de episodios es la pesada, y lo es por dos razones concretas. La primera es
que usa funciones de ventana, que obligan a Spark a ordenar los datos por ciudad y por
hora, y ordenar siempre cuesta más que leer. La segunda es que tiene tres capas de CTE
una adentro de otra, y cada una de esas capas puede significar un intercambio extra entre
máquinas. Las otras tres son agrupaciones simples que se resuelven con una sola pasada.
"""

# Medias por ciudad separando las horas que pasaron el umbral de las que no. Se agrupa en
# dos niveles porque la diferencia entre la media general y la media de los días malos es
# justamente lo que separa una ciudad con aire malo de una que tiene ratos malos
MEDIAS_POR_CIUDAD = """
    SELECT
        ciudad,
        CASE WHEN es_episodio = 1 THEN 'sobre_umbral' ELSE 'bajo_umbral' END AS tramo,
        COUNT(*) AS horas,
        ROUND(AVG(pm2_5), 2) AS pm25_promedio,
        ROUND(MAX(pm2_5), 2) AS pm25_maximo,
        ROUND(AVG(pm10), 2) AS pm10_promedio,
        ROUND(AVG(nitrogen_dioxide), 2) AS no2_promedio
    FROM lecturas
    GROUP BY ciudad, tramo
    ORDER BY ciudad, tramo
"""

# Horas por encima del umbral, con el porcentaje para poder comparar ciudades que no
# tienen la misma cantidad de datos disponibles
HORAS_SOBRE_UMBRAL = """
    SELECT
        ciudad,
        COUNT(*) AS horas_totales,
        SUM(es_episodio) AS horas_sobre_umbral,
        ROUND(100.0 * SUM(es_episodio) / COUNT(*), 2) AS porcentaje,
        ROUND(AVG(CASE WHEN es_episodio = 1 THEN pm2_5 END), 2) AS promedio_cuando_pasa,
        ROUND(MAX(exceso), 2) AS peor_exceso
    FROM lecturas
    GROUP BY ciudad
    ORDER BY horas_sobre_umbral DESC
"""

# La consulta pesada. Agrupa las horas malas que fueron seguidas en episodios usando dos
# funciones de ventana. La primera mira la hora anterior de la misma ciudad, y la segunda
# acumula el conteo de cortes para que cada episodio tenga su propio número. Con eso cada
# episodio queda como un grupo al que se le puede calcular duración, inicio y pico
EPISODIOS = """
    WITH horas_malas AS (
        SELECT
            ciudad,
            hora_utc,
            pm2_5,
            ROW_NUMBER() OVER (PARTITION BY ciudad ORDER BY hora_utc) AS posicion,
            LAG(hora_utc) OVER (PARTITION BY ciudad ORDER BY hora_utc) AS hora_anterior
        FROM lecturas
        WHERE es_episodio = 1
    ),
    cortadas AS (
        SELECT
            ciudad,
            hora_utc,
            pm2_5,
            posicion,
            CASE
                WHEN hora_anterior IS NULL THEN 1
                WHEN UNIX_TIMESTAMP(hora_utc) - UNIX_TIMESTAMP(hora_anterior) = 3600 THEN 0
                ELSE 1
            END AS arranca_episodio
        FROM horas_malas
    ),
    numerados AS (
        SELECT
            ciudad, hora_utc, pm2_5, posicion,
            SUM(arranca_episodio) OVER (
                PARTITION BY ciudad ORDER BY posicion
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS numero_episodio
        FROM cortadas
    )
    SELECT
        ciudad,
        numero_episodio,
        COUNT(*) AS duracion_horas,
        MIN(hora_utc) AS inicio,
        MAX(hora_utc) AS fin,
        ROUND(AVG(pm2_5), 2) AS pm25_promedio,
        ROUND(MAX(pm2_5), 2) AS pm25_pico
    FROM numerados
    GROUP BY ciudad, numero_episodio
    ORDER BY ciudad, inicio
"""

# El resumen que se calcula encima de la tabla de episodios, o sea que lee el resultado de
# la consulta anterior. Eso solo se puede si la anterior quedó registrada como tabla
EPISODIOS_RESUMEN = """
    SELECT
        ciudad,
        COUNT(*) AS episodios,
        SUM(duracion_horas) AS horas_en_episodios,
        ROUND(AVG(duracion_horas), 2) AS duracion_promedio,
        MAX(duracion_horas) AS episodio_mas_largo,
        ROUND(MAX(pm25_pico), 2) AS pm25_pico_del_peor
    FROM episodios
    GROUP BY ciudad
    ORDER BY episodios DESC
"""

# Las cuatro en el orden en que se corren, con el nombre de la tabla que cada una deja en
# la zona gold y la pregunta que contesta
CONSULTAS = (
    ("medias_por_ciudad", MEDIAS_POR_CIUDAD, "Cuanto se contamino en promedio cada ciudad"),
    ("horas_sobre_umbral", HORAS_SOBRE_UMBRAL, "Cuantas horas se paso el limite permitido"),
    ("episodios", EPISODIOS, "Cuantas rachas de horas malas hubo y cuanto duro cada una"),
)

# La consulta más pesada, identificada por su nombre, para que el script del EXPLAIN no
# tenga que adivinar cuál es ni mantener su propia copia del criterio
CONSULTA_PESADA = "episodios"
