# BigDataLab03

Laboratorio 03 de Big Data, construido sobre arquitectura Lambda con Kafka, Spark y SQLite.

El mismo problema de fondo que el resto del curso, calidad del aire en cuatro ciudades chilenas, se procesa de tres maneras distintas para poder comparar arquitecturas sobre una pregunta idéntica.

Este documento describe lo que ya está construido y lo que falta, con los comandos exactos para reproducirlo en otra máquina.

## Estado del proyecto

| Fase | Qué cubre | Estado |
| ---- | --------- | ------ |
| 0 | Limpieza del repositorio y scripts de arranque | hecha |
| 1 | Biblioteca compartida, configuración y prueba de humo | hecha |
| 2.1 | Descarga de la foto, productor de Kafka y verificación | hecha |
| 2.2 | Capa de velocidad con Spark Structured Streaming | hecha |
| 2.3 | Capa de lotes con Spark batch | hecha |
| 2.4 | Servicio de consulta sobre SQLite | hecha |
| 2.5 | Docker | escrita sin ejecutar |
| 2.6 | Verificación Docker | pendiente |
| 2.7 | Documentación final | pendiente |
| 3 | Informe, tabla comparativa y presentación | pendiente |

## Arquitectura

La arquitectura Lambda separa el mismo dato en dos caminos que se alimentan del mismo origen.

El dato entra una sola vez, se publica en un topic de Kafka y desde ahí se bifurca.

| Camino | Velocidad | Destino |
| ------ | --------- | ------- |
| Capa de velocidad | casi en vivo, ventana de sesión | resultados inmediatos |
| Capa de lotes | diferido, todo el histórico | resultados consolidados |

El topic actúa como la frontera entre lo que ya llegó y lo que todavía se está procesando.

Los eventos llevan la hora de la lectura y la hora de la emisión por separado, porque la primera dice cuándo se midió el aire y la segunda prueba que el mensaje salió en vivo.

## Estructura del repositorio

El orden de carpetas está pensado para lo que se va a crear después.

| Ruta | Contenido | Fase |
| ---- | --------- | ---- |
| scripts | Arranque del entorno, broker de Kafka y captura del entorno | 0 |
| src/comun | Configuración, sesión de Spark, evidencia, catálogo de fuentes | 1 |
| lambda/01-ingesta | Descarga, productor y verificador | 2.1 |
| lambda/02-velocidad | Streaming de Spark | 2.2 |
| lambda/03-lotes | Lectura batch del topic y vistas consolidadas | 2.3 |
| lambda/04-servicio | Capa de servicio sobre SQLite | 2.4 |
| docker | Composición de Docker con el broker de Kafka en contenedor | 2.5 |
| lambda/salidas | Resultados pesados de cada fase | 2.x |
| evidencias | Logs, capturas de entorno, gráficos y pantallazos | todas |
| docs | Informe técnico, tabla comparativa y guion | 3 |
| datos | Foto cruda descargada, ignorada por git | 2.1 |
| checkpoints | Estado del streaming, ignorado por git | 2.2 |

Las carpetas pesadas quedan fuera del historial porque pesan megabytes y nadie necesita ver su evolución.

Las carpetas vacías no viajan con un clon, así que la función asegurar_carpetas las vuelve a crear en cada arranque.

## Requisitos

| Componente | Versión | Por qué esa |
| ---------- | ------- | ----------- |
| Python | 3.12.10 | venv propia dentro del repo |
| Temurin JDK | 17.0.20.1 | Spark 3.5 exige Java 8, 11 o 17 |
| PySpark | 3.5.9 | empaqueta Hadoop 3.3.4, que tiene winutils publicado |
| pandas | 2.3.3 | PySpark 4.x no soporta del todo pandas 3 en adelante |
| winutils | 3.3.5 | el binario nativo que Windows necesita para escribir |
| Apache Kafka | 4.1.2 | broker en modo KRaft, sin ZooKeeper |
| kafka-python | 3.0.11 | productor y consumidor ligeros para la ingesta |
| Docker | 20.10.4 o posterior | opcional en el enunciado, solo para la composición de la fase 2.5 |

La máquina de referencia tiene 7,7 GB de memoria, y por eso el driver de Spark arranca con 2 GB y las carpetas de mezcla bajan de 200 a 4.

Ojo con los Pythons.

En la máquina de referencia conviven un 3.12 y un 3.13, el pip del PATH pertenece al 3.13 mientras que python abre el 3.12.

Todos los comandos de este documento usan la ruta absoluta a la venv y la forma con m pip, porque usar pip suelto instala en el intérprete equivocado y después el import falla sin explicación.

## Instalación paso a paso

### Paso 1, Java 17

Spark 3.5 solo acepta Java 8, 11 o 17, y la máquina trae un Java 20 de Oracle en el PATH que Spark arranca igual y después revienta.

El instalador de Temurin deja el JDK en una carpeta que lleva la versión en el nombre, y la configuración del proyecto lo busca ahí y escribe JAVA_HOME antes de que se importe PySpark.

### Paso 2, la venv y PySpark

```powershell
scripts\bootstrap.ps1
```

El script crea la venv, instala las versiones pineadas de requirements.txt, prepara winutils, formatea el almacenamiento de Kafka, baja los jars del conector y corre la prueba de humo.

Es idempotente, se puede lanzar las veces que haga falta porque antes de cada paso pregunta si la cosa ya está hecha.

### Paso 3, winutils

Windows no trae el binario que Hadoop usa para poner permisos, y sin él la lectura funciona pero la escritura falla con un error que se hace pasar por una ruta mal escrita.

La copia se hace dentro de la carpeta del proyecto, de modo que la configuración no depende de nada instalado fuera del repositorio.

### Paso 4, Apache Kafka

```powershell
scripts\start_kafka.ps1
```

El broker arranca en segundo plano, formatea el almacenamiento si hace falta y espera a leer la línea exacta que dice que el servidor terminó de arrancar.

El script no adivina, espera, y si la línea no aparece dentro del tiempo dado se declara fallido.

Para detenerlo con evidencia del cierre:

```powershell
scripts\stop_kafka.ps1
```

### Paso 5, prueba de humo

```powershell
& .venv\Scripts\python.exe src\comun\smoke_test.py
```

Verifica en orden creciente de dificultad que la máquina virtual levanta, que la biblioteca nativa de Hadoop responde, que las cuentas sobre un DataFrame dan lo esperado y que Parquet, CSV y pandas funcionan.

Cada prueba compara contra un valor esperado, porque un log que solo dice que pasó no sirve para saber después si el resultado era correcto.

## El flujo completo

Se corren en orden desde la raíz del repositorio.

```powershell
scripts\start_kafka.ps1
& .venv\Scripts\python.exe lambda\01-ingesta\01_descargar.py aire_horario
& .venv\Scripts\python.exe lambda\01-ingesta\02_productor.py --mensajes 672 --segundos 0.02 --limpiar
& .venv\Scripts\python.exe lambda\01-ingesta\03_verificar.py
& .venv\Scripts\python.exe lambda\02-velocidad\04_velocidad.py --limpiar
& .venv\Scripts\python.exe lambda\02-velocidad\05_resumen_velocidad.py
& .venv\Scripts\python.exe lambda\03-lotes\06_lotes.py
& .venv\Scripts\python.exe lambda\04-servicio\07_servicio.py
scripts\stop_kafka.ps1
```

La descarga se hace una sola vez y después el pipeline lee del disco.

Con --segundos 0.02 la semana entera tarda unos dieciséis segundos en salir, que es lo justo para que la cola se vea llena sin hacer esperar a nadie.

Para una corrida más corta que sirva de prueba basta con --mensajes 30 y se omite el --segundos.

Si el productor llamara a la API en cada corrida, cada vez traería datos distintos, el hash nunca cuadraría y un corte de internet el día de la defensa dejaría la demo sin sostén.

El productor acepta varios interruptores.

| Interruptor | Qué hace |
| ----------- | -------- |
| --mensajes N | manda N eventos y corta |
| --segundos S | espera S segundos entre mensajes |
| --topic T | escribe en otro topic |
| --lista | imprime lo que enviaría y no manda nada |
| --limpiar | deja el almacenamiento del broker en cero antes de empezar |

Para revisar la lista sin tocar el broker:

```powershell
& .venv\Scripts\python.exe lambda\01-ingesta\02_productor.py --lista --mensajes 8
```

## Datos reales, descargados una vez

El script 01_descargar es el único que toca la red.

Trae una semana cerrada de lecturas horarias de cuatro ciudades y tres contaminantes desde Open-Meteo, un servidor que no pide clave de API y que devuelve pronósticos archivados, o sea valores que no cambian entre llamadas.

Antes de guardar le quita a cada ciudad el campo generationtime_ms, que es lo que tardó el servidor en armar la respuesta.

Ese número cambia en cada llamada aunque los datos sean idénticos, y por eso dos descargas seguidas daban archivos distintos.

Quitándolo antes de escribir, el archivo queda idéntico byte por byte entre corridas y el hash significa algo.

Al lado de la foto queda un manifiesto en JSON con la dirección usada, el rango, el peso y el SHA-256 del contenido.

El productor busca la foto por manifiesto y no con un listado de la carpeta, porque un listado devuelve el archivo que haya y una corrida interrumpida podría dejar dos.

## El productor

Cada evento es una lectura horaria de una ciudad, con su hora y sus tres contaminantes.

Los eventos salen ordenados por hora y después por ciudad, de modo que cada cuatro mensajes representan la misma hora en los cuatro lugares y se puede comparar el mismo instante en todas partes.

El umbral de 25 microgramos se evalúa al armar el evento y no en la capa de velocidad, porque el número sale de la misma foto que el dato y así las dos capas ven el mismo criterio sin acordarse entre ellas.

Sin dato no hay alerta, que es la respuesta que conviene en un sistema que vigila algo.

El mensaje se confirma con acks igual a 1, el punto medio entre la velocidad y la garantía.

La clave del mensaje es la ciudad, así que las lecturas de un mismo lugar quedan juntas en la partición.

## Verificación y reproducibilidad

El verificador lee todo el topic desde el principio y sin grupo de consumo, así que no guarda offset y cada ejecución vuelve a empezar de cero.

Imprime el total de mensajes, las alertas, el detalle por ciudad y el rango de horas, y después calcula un SHA-256 sobre los campos que son datos.

A ese hash llegan la ciudad, la hora de la lectura, los tres contaminantes, el umbral, la marca de alerta y el número de evento.

Se dejan afuera la hora de emisión, el broker y el topic, porque son la dirección y el momento y no el contenido.

Con eso dos corridas que mandaron los mismos eventos dan el mismo hash aunque se hayan hecho con horas distintas.

Si el hash no cuadra, algo cambió entre una corrida y la otra y hay que averiguar qué.

Antes de calcular nada, el verificador revisa que cada evento traiga los campos estables completos.

Un evento incompleto corta la corrida con aviso, porque el hash igual se calcula sobre datos faltantes y sale un número que parece correcto.

La semilla fija de 20260928 hace que dos corridas del mismo proceso generen los mismos datos, y el resto depende de no introducir ningún valor que dependa de la fecha de hoy.

## La capa de velocidad

La capa de velocidad es el camino rápido de la arquitectura Lambda, el que toma el topic y devuelve episodios de contaminación sin esperar a que termine nada.

Se apoya en una ventana de sesión, que es la pieza que conviene entender antes de mirar los comandos.

Una ventana de sesión agrupa los eventos que llegan seguidos y se cierra recién cuando pasa el tiempo de separación sin que llegue nada más, de modo que el agrupamiento no tiene un tamaño fijo sino que crece con el dato.

Con una ventana fija de una hora cada hora suelta sería un episodio y las rachas se cortarían en la mitad, en cambio con la de sesión el episodio se arma solo y termina justo donde el aire deja de estar sucio.

La separación elegida es de dos horas, o sea si pasan tres horas sin ninguna lectura contaminada la racha se da por cerrada y empieza otra.

Se agrupa por ciudad, así que las rachas de Santiago no se mezclan con las de Mendoza, y solo entran las lecturas que superan el umbral.

La marca de agua va cuatro horas por delante de la hora de lectura y su trabajo es otro, decide cuándo Spark puede olvidar una sesión que ya no va a crecer más.

Ese retardo es el doble de la separación a propósito, porque si quedara más cerca que la separación la ventana podría cerrar una sesión que todavía estaba abierta.

Cada lote le saca a Kafka como máximo cien mensajes, así que la semana entra en siete lotes y en la consola se ve el número de sesiones vivas subir de tres a siete a medida que avanza la cola.

El modo de salida es el completo y no el de actualización, que fue el primero que se probó.

Spark lo rechaza con un error claro, la ventana de sesión se apoya en la marca de agua y el modo de actualización no sabe cuándo cerrar la fila.

El modo de anexar tampoco serviría, porque solo suelta una sesión cuando la marca de agua ya pasó de su fin y como esa marca va cuatro horas por detrás del dato, las sesiones de las últimas horas del topic no se escribirían nunca.

En el modo completo cada lote vuelve a mandar todas las sesiones que siguen vivas, así que el mismo episodio aparece muchas veces en el Parquet con más lecturas encima cada vez.

Por eso el segundo script de la fase se llama resumen y no consulta.

05_resumen_velocidad.py se queda con la versión más avanzada de cada episodio, escribe un CSV de una fila por episodio dentro de lambda/salidas/velocidad, imprime el recuento por ciudad, señala el episodio más largo y el más intenso y deja un gráfico de la semana en evidencias/graficos.

También calcula un SHA-256 sobre la ciudad, el comienzo, el fin, las lecturas y los picos de contaminantes, con el mismo criterio que el verificador de la ingesta.

Dos corridas que procesaron el mismo topic dan el mismo hash, que es la manera de probar que la capa de velocidad es reproducible y no una casualidad de la corrida.

Para correrla, con el broker arriba y el productor ya ejecutado:

```powershell
& .venv\Scripts\python.exe lambda\02-velocidad\04_velocidad.py --limpiar
& .venv\Scripts\python.exe lambda\02-velocidad\05_resumen_velocidad.py
```

El interruptor limpiar borra el checkpoint y el Parquet de la corrida anterior, y hace falta apretarlo cada vez que se vuelve a mandar el topic desde cero, porque si no Spark recuerda haber leído todo y en la corrida nueva no procesa nada.

La semana completa deja 128 horas sobre el umbral agrupadas en siete episodios, cinco en Santiago y dos en Valparaíso.

Mendoza y Puerto Montt no pasaron el umbral ninguna hora de esa semana, así que no aparecen en el gráfico, y eso también es un resultado y no un fallo del proceso.

## La capa de lotes

La capa de lotes es el camino lento de la arquitectura Lambda, el que lee el topic entero de una sola vez y lo deja consolidado en dos vistas.

Mientras la capa de velocidad responde apenas aparece el mensaje, esta espera a que la cola esté completa, recorre el topic de punta a punta y devuelve el panorama de la semana entera.

La lectura se hace como una corrida batch, desde el primer offset hasta el último, así que no hay checkpoint ni estado que conservar y cada ejecución se pisa a la anterior con el modo sobrescribir, sin ningún interruptor de limpieza.

A diferencia de la capa de velocidad acá no se prefiltra nada, entran las lecturas contaminadas y las limpias juntas, porque el porcentaje de cada grupo se calcula sobre el total.

Salen dos vistas escritas en Parquet y en CSV dentro de lambda/salidas/lotes.

La diaria cruza ciudad con día y trae las lecturas del día, las horas que superaron el umbral, el porcentaje que representan y los promedios y picos de cada contaminante.

La por ciudad aplana la semana en una sola fila por lugar, que es la vista con la que se compara qué tan distinto le fue a cada ciudad.

Los promedios se redondean a dos decimales dentro de la propia consulta y no al escribir, porque el número que entra al hash tiene que ser idéntico en cualquier máquina.

Cada vista se cierra con su propio SHA-256 sobre los campos estables, con el mismo criterio que las otras capas, y la suma de horas sobre umbral de la vista por ciudad se imprime junto con las alertas del topic.

En la corrida de referencia las dos cuentas dan 128, o sea las mismas 128 horas contaminadas contadas por caminos distintos, uno desde los mensajes y otro desde las vistas, y esa coincidencia es la que prueba que las dos ramas de la arquitectura ven el mismo dato.

El gráfico de evidencias/graficos/lotes_diario.png muestra el promedio diario de pm2.5 de cada ciudad, una línea por lugar sobre los siete días.

Para correrla, con el broker arriba y el productor ya ejecutado:

```powershell
& .venv\Scripts\python.exe lambda\03-lotes\06_lotes.py
```

La corrida de referencia deja 28 filas en la vista diaria, cuatro ciudades por siete días, y cuatro filas en la por ciudad.

Dos corridas que leyeron el mismo topic dan los mismos dos hashes, que es la manera de probar que la capa de lotes es reproducible.

## La capa de servicio

La capa de servicio es donde se juntan los dos caminos de la arquitectura Lambda, porque hasta acá cada capa respondió por su cuenta y la pregunta completa necesitaba los dos resultados juntos.

El enunciado del laboratorio pide una capa de servicio sobre SQLite o una carpeta CSV, y elegimos SQLite porque viene con Python de fábrica, no necesita servidor ni instalación aparte y la base entera cabe en un archivo que viaja con el código.

07_servicio.py toma los tres CSV que dejaron las capas anteriores, el diario y el de ciudades del batch y el de episodios del streaming, y los carga en una base que se reconstruye entera en cada corrida.

La base se borra y se rehace de cero en cada ejecución, así que el servicio siempre muestra el último estado de las capas y nunca una mezcla de corridas viejas con corridas nuevas.

Salen tres tablas, la diaria y la de ciudades con las vistas del batch tal cual salieron del 06 y la de episodios con una fila por episodio del 05, con su inicio, su fin y su duración.

El encabezado de cada CSV se compara contra las columnas declaradas antes de escribir, y si alguien tocó un archivo a mano la corrida se corta en lugar de cargar números viejos en silencio.

Con las tablas cargadas se corren cuatro consultas.

La primera resume la semana por ciudad, la segunda muestra los tres días más contaminados de la foto, la tercera cuenta los episodios del streaming por ciudad y la cuarta es el cruce entre las dos capas.

El cruce pone en la misma fila las horas contaminadas que cuenta el batch y las lecturas contaminadas que suman los episodios del streaming, y en la corrida de referencia las dos suman 128, o sea las mismas 128 horas que ya validaron el 05 y el 06 contra las alertas del topic.

Ese número repetido por tercera vez es el que cierra la arquitectura, las dos ramas procesaron exactamente el mismo dato y la capa de servicio lo puede mostrar sin elegir favoritos.

Para correrla, después de las dos capas anteriores:

```powershell
& .venv\Scripts\python.exe lambda\04-servicio\07_servicio.py
```

La base queda en lambda/salidas/servicio/aire.db, ignorada por git porque se regenera con cada corrida, y las cuatro consultas quedan impresas en la consola y en evidencias/logs/07_servicio.log.

SQLite no agrega ningún requisito a la instalación, porque el módulo sqlite3 viene incluido en Python.

## Cómo se registran las corridas

Cada script abre su propio log en evidencias/logs apenas arranca y a partir de ahí espeja en él todo lo que se imprime.

Se espejan también los errores, de modo que una corrida que se corta con excepción deja constancia en el archivo y no solo en la consola.

El encabezado de cada log anota el momento en UTC, el commit del código, el comando exacto, la versión de Python, la versión de PySpark y la plataforma.

Con eso un log suelto sirve como prueba sin que haga falta un párrafo al lado que lo explique.

## Windows te bloquea un archivo

Si al abrir scripts\lab03.bat Windows te dice que está bloqueado o te sale el aviso del Escudo de Windows, no es un virus.

Es la marca que Windows le pone a los archivos que vienen de afuera, que se llama Mark of the Web, y lo hace porque no conoce el archivo.

Un archivo que viene de git clone no debería tener esa marca, pero si lo bajaste con el navegador o lo copiaste de un pen drive la puede tener.

Se quita con esta línea, que solo le saca la marca y no toca nada más:

```powershell
Get-ChildItem -Recurse -Include *.bat, *.ps1 | Unblock-File
```

Lo que no vamos a hacer, y conviene decirlo porque es tentador, es desactivar Windows Defender o agregar excepciones.

No hace falta y no corresponde.

El scripts\lab03.bat está escrito justamente para que no haya nada que señalar, no descarga nada, no tiene código empaquetado, no usa llamadas reflejadas y nunca baja un archivo y lo ejecuta en el mismo paso.

Solo mira si las piezas están y, si están, corre las que ya están escritas.

## Reproducir el entorno desde cero

```powershell
scripts\bootstrap.ps1
```

Recrea la venv e instala las versiones pineadas de requirements.txt.

## Docker

Docker es opcional en el enunciado, y en la máquina de referencia no está instalado, así que la composición se escribió y se validó como YAML pero no se llegó a levantar.

Lo que hay es `docker/docker-compose.yml`, una composición mínima con un solo servicio, el broker de Kafka 4.1.2 en modo KRaft dentro de un contenedor, con el puerto 9092 publicado y un volumen nombrado para los datos.

La versión de la imagen coincide con la instalación local que usa el resto del flujo, así que el bootstrap server sigue siendo `localhost:9092` y ningún script cambia.

En una máquina con Docker 20.10.4 o posterior se levanta desde la raíz:

```powershell
cd docker
docker compose up -d
docker compose ps
```

Cuando la columna STATUS del ps dice `healthy` el broker ya acepta consultas, y desde ahí el flujo completo corre igual que con `scripts\start_kafka.ps1`.

Para bajarlo:

```powershell
docker compose down
```

La variante `docker compose down -v` borra también el volumen de datos, y eso solo se hace cuando se quiere empezar de cero.

No debe levantarse la composición y el broker de Windows al mismo tiempo, porque los dos pelean por el puerto 9092 y el segundo arranque falla con un error de puerto ocupado que no dice de quién es la culpa.

Mientras tanto, en esta máquina el arranque real sigue siendo `scripts\start_kafka.ps1`, y la corrida de Docker queda para la fase 2.6.

## Documentos

| Documento | Rúbrica |
| --------- | ------- |
| docs/informe_tecnico.md | Documentación, 15 puntos |
| docs/tabla_comparativa.md | Análisis comparativo, 10 puntos |
| docs/veredicto_arquitecturas.md | Decisión del equipo |
| docs/guion_presentacion.md | Presentación y tiempo, 20 puntos |
| evidencias/bitacora.md | Evidencia del proceso |
| evidencias/entorno/entorno.txt | Huella del entorno, la genera scripts\00_capturar_entorno.ps1 |
| evidencias/logs | Bitácora automática de cada corrida |

## Problemas que encontramos y cómo se resolvieron

Esto se documenta a propósito, porque es lo que vale el criterio de resolución de problemas.

| Problema | Causa real | Solución |
| -------- | ---------- | -------- |
| ModuleNotFoundError de pyspark | el pip del PATH es el de Python 3.13 y python abre el 3.12 | usar siempre la venv y la forma con m pip |
| Unsupported class file major version | JAVA_HOME vacío, así que Spark tomaba el Java 20 del PATH | apuntar JAVA_HOME al Temurin 17 |
| HADOOP_HOME y hadoop.home.dir sin definir al escribir Parquet | falta el binario de permisos de Windows | instalar winutils |
| UnsatisfiedLinkError en NativeIO Windows access | Hadoop busca winutils.dll y solo existe hadoop.dll | copiar el DLL con el otro nombre |
| Hadoop home directory no es una ruta absoluta | la JVM se come los contrabarras de las propiedades | pasar la dirección con barras normales |
| ModuleNotFoundError de distutils en toPandas | Python 3.12 quitó distutils de la estándar | instalar setuptools |
| PySpark no soporta pandas 3 o superior | pip resolvió pandas 3 sin consultarlo | pinear pandas 2.3.3 |
| La sesión revienta al leer el primer topic | el conector se pidió compilado con Scala 2.13 y PySpark trae 2.12 | declarar el conector con sufijo 2.12 |
| El conector se baja de internet en cada arranque | Ivy rehace la resolución con un cache que vence a las 24 horas | pasar los jars locales de .ivy2 por spark.jars |
| El formateo de Kafka falla sin escribir nada | faltaba el flag standalone en un nodo que es broker y controlador a la vez | pasar el flag standalone junto con el identificador del clúster |
| El conector carga pero el productor no puede leer | el topic quedó con mensajes de corridas anteriores | dejar el broker en cero con el interruptor limpiar antes de producir |
| Borrar un topic rompe el broker | en Windows Kafka no puede mover la carpeta del log mientras la tiene abierta | no se borran topics, se vacía la carpeta de datos con el interruptor limpiar |
| La parada del broker no hace nada y devuelve cero igual | el script oficial de parada usa wmic, que las versiones nuevas de Windows 11 ya no traen | detener el proceso desde PowerShell buscándolo por el nombre kafka.Kafka |
| La limpieza se cae con acceso denegado | Kafka deja los archivos de checkpoint marcados como solo lectura y Windows no deja borrarlos | quitar el atributo de solo lectura antes de borrar la carpeta |
| El arranque del broker deja el productor colgado | el proceso largo hereda el manijón de la salida y la lectura nunca llega a su fin | leer la salida en un hilo aparte y con un tiempo tope |
| Aviso de desuso por value_deserializer | se le pasó una función en vez de una deserializadora de la librería | usar JsonSerializer del paquete kafka.serializer |
| Update output mode not supported for session window | la ventana de sesión depende de la marca de agua y ese modo no sabe cuándo cerrar la fila | usar el modo completo y deduplicar en el resumen |
| ERROR ShutdownHookManager al cerrar Spark | Windows no puede borrar la carpeta temporal mientras la JVM todavía tiene los jars abiertos | es ruido del cierre, el resumen ya se escribió antes de que aparezca |
| Aviso de desuso por is_datetime64tz_dtype | PySpark llama al método que pandas avisó que va a quitar al pasar un DataFrame a pandas | no convertir los avisos en error en el script que usa toPandas |
| La sesión de Spark no arranca o se queda colgado en el arranque | la máquina quedó sin memoria libre después de varias corridas seguidas y la JVM queda a medias | esperar a que se libere memoria, cerrar lo que esté pesando y reintentar, la corrida sale igual |
| El servicio dice que falta un CSV | el 05 o el 06 no corrieron o alguien limpió las salidas | correr las dos capas anteriores en orden, la base se reconstruye sola en la corrida siguiente |
| docker compose no corre en la máquina de referencia | no hay Docker instalado y el enunciado lo marca como opcional | validar la sintaxis del compose con un parser de YAML y dejar la corrida para la fase 2.6 en una máquina con Docker |
