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
| 2.2 | Capa de velocidad con Spark Structured Streaming | pendiente |
| 2.3 | Capa de lotes con Spark batch | pendiente |
| 2.4 | Servicio de consulta sobre SQLite | pendiente |
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
| lambda/03-lotes | Consultas batch de Spark | 2.3 |
| lambda/04-servicio | Servicio de consulta | 2.4 |
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
& .venv\Scripts\python.exe lambda\01-ingesta\02_productor.py --mensajes 30 --limpiar
& .venv\Scripts\python.exe lambda\01-ingesta\03_verificar.py
scripts\stop_kafka.ps1
```

La descarga se hace una sola vez y después el pipeline lee del disco.

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
