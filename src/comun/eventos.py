"""El esquema del evento que circula por el topic, declarado una sola vez para las dos capas.

La capa de velocidad y la de lotes leen el mismo mensaje y las dos tienen que interpretarlo igual, así que el esquema vive en un solo archivo y no uno por script.

Si el productor agrega un campo mañana, se agrega acá y las dos capas lo empiezan a ver al mismo tiempo, sin que nadie tenga que recordar dónde estaba el otro esquema.

Se declara campo por campo y no se deduce del primer lote, porque la deducción depende de lo que haya en el topic en ese instante y un lote temprano podría venir sin contaminantes.

Los nombres de los contaminantes salen del mismo catálogo que usa el productor, de modo que la clave del servidor, el mensaje y la vista consolidada hablen siempre el mismo idioma.

La hora viene como texto porque ese es el formato en que la trae la foto, y se convierte a timestamp en el mismo paso porque la marca de agua, la ventana de sesión y la agrupación por día solo trabajan con fechas de verdad.
"""

from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, DoubleType, LongType, StringType, StructField, StructType

import fuentes

# Los campos que trae el mensaje, en el mismo orden en que los arma el productor.

# Un campo que no venga queda en nulo y uno que sobren se ignora, así que el esquema tolera que el contrato crezca sin romperle a nadie.
ESQUEMA_EVENTO = StructType([
    StructField("ciudad", StringType()),
    StructField("ciudad_nombre", StringType()),
    StructField("hora_lectura", StringType()),
    *[StructField(nombre, DoubleType()) for nombre in fuentes.CONTAMINANTES],
    StructField("umbral", DoubleType()),
    StructField("supera_umbral", BooleanType()),
    StructField("evento_id", LongType()),
    StructField("emitido_utc", StringType()),
    StructField("broker", StringType()),
    StructField("topic", StringType()),
])


def parsear(crudo):
    """Devuelve el DataFrame de eventos leído del topic, ya convertido en columnas de verdad.

    El crudo es lo que devuelve Kafka, un par de columnas binarias llamadas key y value donde el value trae el JSON adentro, y acá se le saca esa capa.

    Solo se convierte el formato, no se filtra ni se ordena, porque cada capa decide después qué le interesa, la de velocidad se queda con las alertas y la de lotes se queda con todo.

    El casteo de la hora se hace acá y no en cada script, así el que llegue después no tiene que acordarse de hacerlo para que la marca de agua le funcione."""
    return (
        crudo.select(F.from_json(F.col("value").cast(
            "string"), ESQUEMA_EVENTO).alias("evento"))
        .select("evento.*")
        .withColumn("hora_lectura", F.col("hora_lectura").cast("timestamp"))
    )
