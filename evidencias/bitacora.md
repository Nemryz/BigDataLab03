# Bitácora de laboratorio

Paso a paso para abrir, ejecutar y probar todo el proyecto en esta máquina.

Cada sección dice qué se corre, qué se debe ver y qué no se debe tocar alrededor, porque casi todos los problemas de este laboratorio salieron de un comando parecido al correcto y no del correcto.

Las rutas están completas para que se puedan copiar sin adivinar nada.

## Abrir la terminal en el lugar correcto

Se abre PowerShell, se entra a la raíz del proyecto y se comprueba dónde se paró antes de escribir nada más.

```powershell
Set-Location -LiteralPath C:\BigDataLab03
Get-Location
```

La salida debe terminar en BigDataLab03.

Todos los scripts encuentran sus archivos solos porque resuelven las rutas contra su propia ubicación, pero cualquier redirección escrita a mano con el símbolo de mayor que cae en la carpeta donde esté la terminal y no donde toca.

La barra invertida no hace falta escaparla si se usa -LiteralPath, y es la forma más segura de entrar porque evita que PowerShell interprete corchetes o signos de ruta como comodines.

## El entorno de Python

Todo se corre con el intérprete de la venv del proyecto y nunca con el python del PATH.

```powershell
& .venv\Scripts\python.exe --version
```

Debe decir Python 3.12.10.

En esta máquina conviven un 3.12 y un 3.13, el pip del PATH pertenece al 3.13 y el import de pyspark falla después sin explicación si se instala ahí.

Para instalar o quitar algo se usa siempre la forma con m pip y la ruta de la venv arriba.

```powershell
& .venv\Scripts\python.exe -m pip install paquete
```

Nunca pip install paquete suelto.

## Java

Spark solo acepta Java 8, 11 o 17 y en el PATH hay un 20 que Spark arranca igual y después revienta.

La configuración del proyecto escribe JAVA_HOME antes de importar PySpark, así que en el día a día no hay que setear nada a mano.

Si se quiere comprobar de qué Java se está hablando:

```powershell
& "C:\Program Files\Eclipse Adoptium\jdk-17.0.20.101-hotspot\bin\java.exe" -version
```

No se debe sobreescribir JAVA_HOME con el Java del PATH para probar nada, porque el error aparece mucho después del lugar donde se causó.

## Prender Kafka

```powershell
scripts\start_kafka.ps1
```

El script formatea el almacenamiento si hace falta, arranca el broker en segundo plano y espera la línea exacta que dice que el servidor terminó de arrancar.

No adivina, espera, y si la línea no aparece dentro del tiempo dado devuelve error.

Para comprobar que el broker quedó escuchando:

```powershell
Test-NetConnection -ComputerName localhost -Port 9092 -InformationLevel Quiet
```

Debe devolver True.

Si PowerShell se queja por la política de ejecución, la misma llamada se hace con el parámetro explícito.

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\start_kafka.ps1
```

No se debe correr el arranque dos veces seguidas, porque el segundo intento toma el primer broker ya vivo y devuelve un error de puerto ocupado que no es un fallo real.

## Apagar Kafka

```powershell
scripts\stop_kafka.ps1
```

El script busca el proceso cuyo nombre es kafka.Kafka, lo corta, espera a que el puerto 9092 quede libre y escribe la evidencia del cierre en evidencias/logs/04_kafka_cierre.log.

Si el puerto no se libera dentro del tiempo dado devuelve error, que es la señal de que algo quedó colgado.

No se debe usar el kafka-server-stop.bat que trae la distribución.

Ese script se apoya en wmic y las versiones nuevas de Windows 11 ya no lo traen, así que no hace nada y encima devuelve el código cero como si hubiera salido bien.

No se debe cerrar la ventana de la terminal mientras el productor está mandando mensajes, porque el broker corre en segundo plano pero los avisos de la corrida se pierden con ella.

## Levantar Kafka con Docker

Docker es opcional en el enunciado y en esta máquina no está instalado, así que esta vía quedó escrita pero no probada.

La composición está en `docker/docker-compose.yml` y levanta un solo servicio, el broker de Kafka 4.1.2 en modo KRaft dentro de un contenedor.

```powershell
cd docker
docker compose up -d
```

El servicio publica el puerto 9092 en el host, así que el resto del flujo no cambia y el productor sigue apuntando a `localhost:9092`.

Para ver si ya quedó sano:

```powershell
docker compose ps
docker compose logs kafka
```

La columna STATUS del primer comando debe terminar en `running` con health `healthy`, y en los logs aparece la línea del servidor terminado de arrancar.

Para bajarlo:

```powershell
docker compose down
```

No se debe levantar el contenedor y el broker de Windows al mismo tiempo, porque los dos quieren el puerto 9092 y el segundo falla con un error de puerto ocupado que parece un fallo del script y no lo es.

No se debe usar `docker compose down -v` sin querer borrar los datos, porque esa variante elimina el volumen nombrado donde el contenedor guarda los topics.

Si docker no está instalado, el error de que el comando no se reconoce es la señal de que esta vía no aplica en esa máquina, y el arranque sigue siendo `scripts\start_kafka.ps1`.

Si la imagen `apache/kafka:4.1.2` no se puede bajar, conviene revisar la conexión antes que el archivo, porque la etiqueta existe en Docker Hub y el compose la pide tal cual.

## Verificar el compose sin Docker

La composición se puede revisar en esta máquina aunque no haya Docker, porque en una primera pasada lo que casi siempre falla es el archivo y no el motor de contenedores.

El verificador vive en `docker/verificar_compose.py` y se corre con la venv desde la raíz, igual que el resto de los scripts.

```powershell
& .venv\Scripts\python.exe docker\verificar_compose.py
```

Lee el compose con PyYAML y pasa trece comprobaciones, que el YAML parsea, que el servicio kafka existe, que la imagen es `apache/kafka:4.1.2`, que el puerto 9092 queda publicado, que el nodo hace de broker y de controlador a la vez, que el cliente anuncia el mismo broker que usa el proyecto, que las claves de KRaft están todas, que los factores de replicación están en uno, que el volumen está declarado y montado donde Kafka escribe, y que el healthcheck consulta al broker.

Cada comprobación sale con su nombre y con OK o FALLO, así que el fallo apunta a la pieza y no deja adivinando.

La salida completa queda como evidencia en `evidencias/logs/08_verificacion_compose.log` con el encabezado de siempre.

Para confirmar que la imagen existe antes de viajar a una máquina con Docker:

```powershell
$r = Invoke-WebRequest -Uri "https://hub.docker.com/v2/repositories/apache/kafka/tags/4.1.2" -UseBasicParsing
$r.StatusCode
```

Debe devolver 200, que es la respuesta de que la etiqueta está publicada.

No se debe tomar esta verificación como prueba de que el contenedor levanta, porque no levanta nada, solo revisa el archivo, y la corrida real es `docker compose up` en una máquina con Docker.

No se debe correr el verificador con el python del PATH, porque ese intérprete no tiene el proyecto ni sus paquetes y el error que devuelve no explica nada.

Si el script se cae con `ModuleNotFoundError: No module named yaml`, es que la venv no tiene PyYAML todavía, y se arregla reinstalando las versiones pineadas con `& .venv\Scripts\python.exe -m pip install -r requirements.txt`.

## Comprobar que el entorno sigue sano

La verificación rápida tiene dos pasos y se corren desde la raíz.

```powershell
& .venv\Scripts\python.exe -W error -m py_compile lambda\01-ingesta\01_descargar.py
& .venv\Scripts\python.exe src\comun\smoke_test.py
```

El primer comando compila con los avisos convertidos en error, así que cualquier deprecation que aparezca obliga a arreglarla en lugar de pasar desapercibida.

La prueba de humo levanta la máquina virtual, escribe un Parquet, lo lee de vuelta y compara contra valores esperados, y debe cerrar con código cero.

No se debe mirar solo que el script termine, hay que leer las líneas que dicen esperado y obtenido porque un log que solo dice que pasó no sirve para saber después si el resultado era el correcto.

## Correr la fase de ingesta completa

Se corren en este orden y sin saltos, desde la raíz.

```powershell
scripts\start_kafka.ps1
& .venv\Scripts\python.exe lambda\01-ingesta\01_descargar.py aire_horario
& .venv\Scripts\python.exe lambda\01-ingesta\02_productor.py --mensajes 30 --limpiar
& .venv\Scripts\python.exe lambda\01-ingesta\03_verificar.py
scripts\stop_kafka.ps1
```

La descarga es el único paso que toca la red y se hace una sola vez, porque después el pipeline lee del disco y una segunda llamada a la API podría traer datos distintos.

El productor con --limpiar deja el broker en cero antes de empezar, que es lo que hace que una corrida sea comparable con la anterior.

Antes de mandar nada conviene ver la lista sin tocar el broker.

```powershell
& .venv\Scripts\python.exe lambda\01-ingesta\02_productor.py --lista --mensajes 8
```

Debe imprimir los 672 eventos disponibles y el orden en que saldrían, con las cuatro ciudades alternándose hora por hora.

El verificador se puede correr las veces que quiera porque lee el topic desde el principio y sin grupo de consumo, así que no guarda offsets ni depende de cuántas veces se corrió antes.

Si el hash estable sale igual en dos corridas, la ingesta es reproducible.

No se debe regenerar la foto de datos para probar cosas, el rango es fijo del 2026-09-26 al 2026-10-02 y cambiarlo hace que ningún hash vuelva a cuadrar.

No se debe correr --limpiar cuando haya datos que se quieran conservar, porque vacía toda la carpeta de almacenamiento del broker y no solo el topic.

## Correr la capa de velocidad

Se corren los dos scripts de la fase después del productor y sin apagar el broker en el medio.

```powershell
& .venv\Scripts\python.exe lambda\01-ingesta\02_productor.py --mensajes 672 --segundos 0.02 --limpiar
& .venv\Scripts\python.exe lambda\02-velocidad\04_velocidad.py --limpiar
& .venv\Scripts\python.exe lambda\02-velocidad\05_resumen_velocidad.py
```

El productor manda la semana entera a ritmo acelerado para que la cola esté llena cuando arranca el streaming, y con --limpiar el topic vuelve a cero antes de empezar.

04_velocidad.py es el que lee, agrupa y escribe, y debe imprimir siete líneas de lote, una por cada cien mensajes que le saca a Kafka más el resto.

El recuento que cierra el script debe dar `Lotes 7`, `Filas en disco 37` y `Episodios 7`.

Las 37 filas son las siete sesiones contadas una vez por cada lote en que siguió viva, y por eso son más que los episodios de verdad.

05_resumen_velocidad.py es el que ordena el desorden y debe decir `Episodios 7` y `Ciudades santiago=5, valparaiso=2`.

También señala el episodio más largo, que en la corrida de referencia es el de Santiago desde 2026-09-29 12:00 con 74 horas seguidas, y el más intenso, con un pico de 93.8 ug/m3.

Al final imprime el SHA-256 de los episodios, que en la corrida de referencia es 66654610b6057dd0dcbe296270963eabc20f1138649526928058a22cea8328c2 y debe salir igual todas las veces que se repita la secuencia completa.

El CSV queda en lambda/salidas/velocidad/episodios.csv y el gráfico en evidencias/graficos/velocidad_episodios.png.

No se debe correr 04_velocidad.py sin el --limpiar cuando se volvió a mandar el topic desde cero, porque el checkpoint viejo le dice a Spark que ya leyó todo y en la corrida nueva no procesa nada.

No se debe correr 05_resumen_velocidad.py con los avisos convertidos en error, PySpark usa un método de pandas que ya está marcado como obsoleto y el script se cae en la conversión sin que nada esté roto.

No se debe borrar la carpeta checkpoints/velocidad a mano mientras la corrida sigue corriendo, la limpieza se hace con el --limpiar apagado el streaming.

## Correr la capa de lotes

Se corre después de la capa de velocidad y con el broker arriba, porque lee el topic de Kafka completo de una sola vez.

```powershell
& .venv\Scripts\python.exe lambda\03-lotes\06_lotes.py
```

No necesita ningún interruptor de limpieza, es una corrida batch desde el primer offset hasta el último que se sobrescribe sobre la anterior, y al terminar mata sus propios procesos de Spark.

Debe imprimir `Topic leido 672 eventos`, después las rutas de Parquet y CSV de las dos vistas y después `Eventos 672`, `Diario 28 filas` y `Ciudades 4 filas`.

El renglón de cruce debe decir `Horas umbral 128  igual que las alertas`, porque las 128 horas contaminadas que suman las vistas son las mismas 128 alertas que contó la capa de velocidad, contadas por caminos distintos, una desde los mensajes y otra desde las vistas.

Los SHA-256 de la corrida de referencia son c3b1ba75a99b57feede7a328b45fc4150202e3ad6f150a32295d9f55f3e1fd1f para la vista diaria y bae6d8817f0e68b6e5866ce5cb993bb69aefd7694eb6c5a61e8ffac54617e870 para la de ciudades, y deben salir iguales todas las veces que se lea el mismo topic.

Los archivos quedan en lambda/salidas/lotes con diario.csv, ciudades.csv y la carpeta parquet, y el gráfico en evidencias/graficos/lotes_diario.png.

## Correr la capa de servicio

Se corre al final, después de que las dos capas anteriores ya dejaron sus CSV, y no necesita broker ni Spark porque solo abre tres archivos y los carga en SQLite.

```powershell
& .venv\Scripts\python.exe lambda\04-servicio\07_servicio.py
```

Debe imprimir tres líneas de carga, `diario 28 filas`, `ciudades 4 filas` y `episodios 7 filas`, y después las cuatro consultas del servicio.

La primera consulta resume la semana por ciudad, la segunda muestra los tres días más contaminados de la foto, la tercera cuenta los episodios del streaming por ciudad y la cuarta es el cruce entre las dos capas.

La de cruce es la que cierra la arquitectura, muestra en la misma fila las horas batch y las lecturas de los episodios de cada ciudad, y termina con los renglones `Horas batch 128`, `Lecturas episodios 128` y `El cruce coincide`.

Esas 128 horas son las mismas que ya cuadraron en las otras capas, o sea las 128 alertas del topic contadas por tercera vez, ahora con las dos ramas de la arquitectura lado a lado en una sola tabla.

La base queda en lambda/salidas/servicio/aire.db y no se versiona porque se rehace en cada corrida, en cambio las consultas quedan impresas en evidencias/logs/07_servicio.log.

La corrida tarda segundos, porque no levanta ninguna JVM y solo pasa tres archivos chicos por el sqlite3 que trae Python.

## Los logs

Cada comando escribe su propia salida en evidencias/logs y el nombre del archivo indica qué lo produjo.

```powershell
Get-Content evidencias\logs\02_productor.log -Encoding UTF8
```

El -Encoding UTF8 hace falta, porque sin él los acentos salen corruptos en las terminales que usan la página de códigos 850.

Para mirar las últimas líneas del broker:

```powershell
Get-Content evidencias\logs\03_kafka_broker.log -Tail 30
```

No se deben editar los logs a mano.

El archivo 03_kafka_broker.log.err no se puede borrar mientras el broker siga corriendo, y eso no es un error del sistema sino el aviso de que alguien lo tiene abierto.

## Qué no hacer, resumido

No se debe usar el python ni el pip del PATH, se usa siempre la venv con la ruta absoluta.

No se debe correr Spark con el Java del PATH, se deja que la configuración escriba JAVA_HOME.

No se debe borrar un topic de Kafka en Windows, el intento de mover la carpeta del log revienta el broker entero y deja el directorio de datos en un estado que Kafka toma por un fallo de disco.

No se debe apagar el broker con el kafka-server-stop.bat de la distribución, usa wmic y devuelve cero aunque no haga nada.

No se debe deshabilitar ni tocar Windows Defender para que deje pasar los binarios, los archivos del proyecto se generan localmente y no hay motivo para eso.

No se debe tocar la carpeta datos/raw a mano ni borrarla sin antes anotar el hash del manifiesto, porque es la fuente de verdad de la que depende la reproducibilidad.

No se debe mirar la carpeta datos/raw con un patrón comodín desde un script, se lee siempre por el manifiesto, que es quien sabe qué archivos hay.

No se debe convertir todos los avisos en error en los scripts que pasan un DataFrame a pandas, PySpark avisa por un método que pandas ya marcó como obsoleto y el script se cae sin que haya nada malo.

No se debe dejar el Parquet de la capa de velocidad sin su CSV al lado, el Parquet guarda episodios repetidos a propósito y el CSV es el que tiene una fila por episodio.

No se debe correr 06_lotes.py antes de que el productor termine de mandar la semana, las vistas se calculan sobre lo que haya en el topic en ese instante y a mitad de producción sale una semana incompleta con todos los hashes cambiados.

No se debe correr 06_lotes.py con los avisos convertidos en error, como el 05 pasa un DataFrame a pandas para escribir el CSV y se cae con el aviso de desuso de pandas sin que haya nada malo.

No se debe correr 07_servicio.py antes del 05 y el 06, sin sus tres CSV la corrida se corta avisando cuál falta y no deja ninguna base a medias.

No se debe editar la base aire.db a mano para cambiar un número, la base se borra y se reconstruye completa en cada corrida y cualquier cambio manual desaparece sin aviso.

## Errores típicos y qué hacer

Si el productor dice que el broker no está corriendo, se enciende con start_kafka.ps1 y se vuelve a probar.

Si la limpieza falla con acceso denegado sobre la carpeta de datos, es porque Kafka dejó archivos de checkpoint en solo lectura y Windows no deja borrarlos mientras el atributo siga puesto.

Si el arranque del productor se queda colgado sin imprimir nada, era el proceso largo heredando el manijón de la salida, y eso ya está resuelto con la lectura en hilo aparte con tiempo tope.

Si el conector de Kafka no encuentra las clases, es porque se pidió con sufijo 2.13 y Spark trae 2.12.

Si el arranque dice que no encuentra la configuración de un bloque, es el aviso inofensivo de la reconfiguración dinámica y no impide que el almacenamiento se formatee.

Si PySpark no aparece en el import, el pip que corrió pertenecía al otro intérprete.

Si la escritura de Parquet falla con un error que parece una ruta mal escrita, en realidad faltan permisos y hay que revisar winutils.

Si algo falla y no se sabe dónde, se empieza por la prueba de humo, porque ordena las comprobaciones de menor a mayor dificultad y marca en qué punto se rompió.

Si 04_velocidad.py revienta con Update output mode not supported for session window, no es la máquina, es que alguien cambió el modo de salida, la capa va en modo completo.

Si al terminar Spark aparece ERROR ShutdownHookManager sobre una carpeta temporal, es Windows que no puede borrar la carpeta mientras la JVM todavía tiene los jars abierta, el resumen ya se escribió antes.

Si 05_resumen_velocidad.py dice que no existe el Parquet, es porque 04_velocidad.py no corrió o porque un --limpiar posterior lo borró.

Si el CSV sale con menos episodios de los esperados, se compara contra el recuento de episodios del 04, porque los dos cuentan por ciudad y hora de inicio y deberían dar el mismo número.

Si 06_lotes.py dice `El topic esta vacio, corre primero 02_productor.py`, el productor no corrió o alguien limpió el topic después de él, se repite la secuencia completa desde el productor con --limpiar.

Si la sesión de Spark no arranca o se queda colgado en el arranque, la máquina quedó sin memoria libre después de varias corridas seguidas y la JVM queda a medias, se espera a que se libere memoria y se reintenta sin cambiar nada, la corrida sale igual.

Si el renglón de cruce no da 128, el topic se leyó a mitad de la producción o se modificó entre corridas, se vuelve a la secuencia completa desde el productor con --limpiar antes de desconfiar de las vistas.

Si 07_servicio.py dice `Falta` seguido de la ruta de un CSV, esa capa no corrió o alguien limpió las salidas, se corren el 05 y el 06 en orden y se vuelve a intentar el servicio.

Si el servicio dice que el encabezado de un CSV es distinto al esperado, alguien editó el archivo a mano, se regenera la capa que lo produce en lugar de tocar el CSV.

Si el cruce del servicio dice `NO coinciden`, una de las dos capas se corrió contra un topic distinto, se repite la secuencia completa desde el productor con --limpiar y las dos vuelven a dar 128.
