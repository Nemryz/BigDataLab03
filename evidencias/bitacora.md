# PASOS DE LABORATORIO 03 – BIG DATA

El presente documento registra, de forma ordenada y verificable, los pasos realizados para construir y correr el pipeline de calidad del aire sobre arquitectura Lambda con Apache Kafka, Apache Spark y SQLite.

Cada sección describe una parte del trabajo, muestra el comando ejecutado y presenta la salida real obtenida, tal como quedó impresa en la consola y guardada en evidencias/logs.

Todos los comandos se corren desde la raíz del repositorio y usan la venv del proyecto, cuya ruta absoluta se repite en cada orden para que ninguna ejecución dependa del intérprete del PATH.

---

## DATASET UTILIZADO

La foto de datos baja de Open-Meteo, un servidor que no pide clave de API y devuelve pronósticos archivados de calidad del aire por hora.

La semana no está escrita a mano, el script de descarga la recalcula en cada corrida y siempre termina ayer, porque el día de hoy está incompleto y el servidor lo va corrigiendo a medida que pasan las horas.

En la corrida de referencia el rango fue del 2026-09-27 al 2026-10-03, o sea siete días completos por cuatro ciudades por veinticuatro horas, que suman las 672 lecturas horarias que procesa el pipeline.

| Ciudad | País | Lecturas en la semana |
| ------ | ---- | --------------------- |
| Santiago | Chile | 168 |
| Valparaíso | Chile | 168 |
| Puerto Montt | Chile | 168 |
| Mendoza | Argentina | 168 |

Cada lectura trae pm2_5, pm10 y nitrogen_dioxide contra un umbral de 25 microgramos por metro cúbico que se evalúa al armar el evento, así que las dos capas ven el mismo criterio sin acordarse entre ellas.

Al lado del archivo queda el manifiesto en JSON con la dirección usada, el rango, el peso y el SHA-256 del contenido, y ese manifiesto es lo que congela la corrida porque el productor busca la foto por manifiesto y no por listado de la carpeta.

Antes de guardar se le quita a cada ciudad el campo generationtime_ms, que cambia en cada llamada aunque los datos sean idénticos, y así el archivo queda byte por byte igual entre descargas del mismo rango.

El SHA-256 de la foto de referencia es `e117f7b8a3170ea119ba4367ec4aa6a295d943fc7e1c0e0e5f598857d82639f3`.

---

## PREPARACIÓN DEL ENTORNO

Se abre la terminal en la raíz del repositorio.

```powershell
cd C:\BigDataLab03
```

Los scripts de la carpeta scripts pueden quedar bloqueados por la política de ejecución de PowerShell, y se levanta el permiso solo para la sesión con una orden que muere con la terminal y no toca la máquina.

```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass -Force
```

De ahí en adelante los scripts se llaman con el operador de llamada y corren sin vueltas.

```powershell
& scripts\start_kafka.ps1
```

El proyecto usa su propia venv dentro del repositorio, y todos los comandos de este documento apuntan a ella con la ruta absoluta.

```powershell
& .venv\Scripts\python.exe --version
```

**RESULTADO OBTENIDO:**

```text
Python 3.12.10
```

En la máquina de referencia conviven un 3.12 y un 3.13, el pip del PATH pertenece al 3.13 mientras que python abre el 3.12, y por eso los paquetes se instalan siempre con la forma `& .venv\Scripts\python.exe -m pip install -r requirements.txt`.

Spark 3.5 solo acepta Java 8, 11 o 17 y la máquina trae un Java 20 de Oracle en el PATH que Spark arranca igual y después revienta.

La configuración del proyecto busca el Temurin 17 en su carpeta con la versión en el nombre y escribe JAVA_HOME antes de que se importe PySpark, de modo que la prueba de humo confirma al final qué Java se usó.

El arranque desde cero se hace con un solo script idempotente, que pregunta antes de cada paso y se puede lanzar las veces que haga falta.

```powershell
& scripts\bootstrap.ps1
```

**RESULTADO OBTENIDO:**

```text
Bootstrap de BigDataLab03
Repositorio  C:\BigDataLab03

Paso 1 de 5, Java 17
  ya estaba instalado en C:\Program Files\Eclipse Adoptium\jdk-17.0.20.101-hotspot
  OJO, esto no aplica a la consola que lo esta corriendo, abre una nueva al final

Paso 2 de 5, la venv
  creando con Python 3.12
  creada

Paso 3 de 5, PySpark y librerias
  instalando desde requisitos.txt, con las versiones exactas
  instalado

Paso 4 de 5, winutils
  descargando
  descargados, y la copia con el otro nombre tambien

Paso 5 de 5, Apache Kafka, solo si falta
  descargando, son 127 MB
  instalado en la carpeta kafka

Verificacion
```

El script crea la venv, instala las versiones pineadas de requirements.txt, prepara winutils, formatea el almacenamiento de Kafka si hace falta, baja los jars del conector y termina con la prueba de humo.

---

## COMPROBAR QUE EL ENTORNO SIGUE SANO

La verificación rápida tiene dos pasos y se corren desde la raíz.

```powershell
& .venv\Scripts\python.exe -W error -m py_compile lambda\01-ingesta\01_descargar.py
& .venv\Scripts\python.exe src\comun\smoke_test.py
```

**RESULTADO OBTENIDO:**

```text
Spark 3.5.9
Java 17.0.20.1+1
Maestro local[2]

hadoop.home.dir C:/BigDataLab03/winutils
Biblioteca nativa de Hadoop: obtenido True, esperado True

Filas generadas: obtenido 100, esperado 100
Suma de la columna: obtenido 14850, esperado 14850

Filas releyas de Parquet: obtenido 100, esperado 100
Total releyo de Parquet: obtenido 14850, esperado 14850
Columnas de la tabla de pandas: obtenido 2, esperado 2

Archivos de CSV escritos: obtenido True, esperado True

Pruebas superadas 7 de 7
SMOKE TEST: OK
```

El primer comando compila con los avisos convertidos en error, así que cualquier deprecation que aparezca obliga a arreglarla en lugar de pasar desapercibida, y no imprime nada ni devuelve cero, que es la señal correcta.

La prueba de humo levanta la máquina virtual, escribe un Parquet, lo lee de vuelta y compara contra valores esperados, y debe cerrar con código cero.

No se debe mirar solo que el script termine, hay que leer las líneas que dicen esperado y obtenido porque un log que solo dice que pasó no sirve para saber después si el resultado era el correcto.

---

## APACHE KAFKA

El broker corre en local en modo KRaft, sin ZooKeeper, con un heap de 256 megabytes y el puerto 9092, que es el mismo que después usan el productor y el conector de Spark.

```powershell
& scripts\start_kafka.ps1
```

**RESULTADO OBTENIDO:**

```text
Arrancando el broker de Kafka
  java      C:\Program Files\Eclipse Adoptium\jdk-17.0.20.101-hotspot
  memoria   -Xmx256M -Xms128M
  broker    localhost:9092
  log       C:\BigDataLab03\evidencias\logs\03_kafka_broker.log
  proceso   31412
  esperando a que el servidor levante

El broker arranco bien
  puerto 9092 responde  True
```

El script no adivina, espera la línea exacta que dice que el servidor terminó de arrancar, y si la línea no aparece dentro del tiempo dado se declara fallido.

Para detenerlo con evidencia del cierre:

```powershell
& scripts\stop_kafka.ps1
```

**RESULTADO OBTENIDO:**

```text
Deteniendo el broker de Kafka
El broker se detuvo, el puerto 9092 quedo libre
  evidencia en C:\BigDataLab03\evidencias\logs\04_kafka_cierre.log
```

La parada escribe su propio log, así que un cierre limpio queda probado igual que un arranque.

---

## DESCARGA DE LA FOTO

El script 01 es el único que toca la red y se corre una sola vez por corrida, porque después el pipeline lee del disco.

```powershell
& .venv\Scripts\python.exe lambda\01-ingesta\01_descargar.py aire_horario
```

**RESULTADO OBTENIDO:**

```text
01_descargar.py  2026-10-04T04:03:32+00:00
commit  fd206d7
comando  lambda\01-ingesta\01_descargar.py aire_horario
python  3.12.10 en C:\BigDataLab03\.venv\Scripts\python.exe
pyspark  3.5.9
plataforma  Windows-11-10.0.26200-SP0

Fuente   aire_horario
Rango    2026-09-27 a 2026-10-03
URL      https://air-quality-api.open-meteo.com/v1/air-quality?latitude=-33.45,-32.99,-33.05,-41.47&longitude=-70.67,-68.85,-71.62,-72.94&hourly=pm2_5,pm10,nitrogen_dioxide&timezone=America%2FSantiago&start_date=2026-09-27&end_date=2026-10-03

Descargando

Guardado  C:\BigDataLab03\datos\raw\aire_horario_2026-09-27_2026-10-03.json
Tamano    47822 bytes
SHA-256   e117f7b8a3170ea119ba4367ec4aa6a295d943fc7e1c0e0e5f598857d82639f3
Manifiesto C:\BigDataLab03\datos\raw\aire_horario_manifiesto.json

Log       C:\BigDataLab03\evidencias\logs\01_descarga.log
```

Cada corrida deja el encabezado con el momento en UTC, el commit, el comando y las versiones, así que un log suelto sirve como prueba sin un párrafo al lado que lo explique.

Si el productor llamara a la API en cada corrida, cada vez traería datos distintos, el hash nunca cuadraría y un corte de internet el día de la defensa dejaría la demo sin sostén, y por eso la foto se baja una vez y el productor la lee del disco.

No se debe regenerar la foto a la ligera, porque una semana distinta cambia todos los valores del pipeline y ningún hash vuelve a cuadrar con la corrida anterior.

---

## PRODUCTOR DE KAFKA

Antes de mandar conviene ver la lista sin tocar el broker.

```powershell
& .venv\Scripts\python.exe lambda\01-ingesta\02_productor.py --lista --mensajes 8
```

**RESULTADO OBTENIDO:**

```text
02_productor.py  2026-10-04T04:04:30+00:00
commit  fd206d7
comando  lambda\01-ingesta\02_productor.py --lista --mensajes 8
python  3.12.10 en C:\BigDataLab03\.venv\Scripts\python.exe
pyspark  3.5.9
plataforma  Windows-11-10.0.26200-SP0

Foto      aire_horario_2026-09-27_2026-10-03.json
Rango     2026-09-27 a 2026-10-03
SHA-256   e117f7b8a3170ea119ba4367ec4aa6a295d943fc7e1c0e0e5f598857d82639f3
Eventos   672 disponibles
Voy a mandar 8 mensajes al topic lab03.eventos
Ritmo      1.0 s por mensaje

0001 {"ciudad": "mendoza", "ciudad_nombre": "Mendoza", "hora_lectura": "2026-09-27T00:00", "pm2_5": 3.6, "pm10": 3.7, "nitrogen_dioxide": 7.0, "umbral": 25.0, "supera_umbral": false}
0002 {"ciudad": "puerto_montt", "ciudad_nombre": "Puerto Montt", "hora_lectura": "2026-09-27T00:00", "pm2_5": 2.5, "pm10": 3.3, "nitrogen_dioxide": 1.8, "umbral": 25.0, "supera_umbral": false}
0003 {"ciudad": "santiago", "ciudad_nombre": "Santiago", "hora_lectura": "2026-09-27T00:00", "pm2_5": 42.6, "pm10": 44.5, "nitrogen_dioxide": 38.0, "umbral": 25.0, "supera_umbral": true}
0004 {"ciudad": "valparaiso", "ciudad_nombre": "Valparaíso", "hora_lectura": "2026-09-27T00:00", "pm2_5": 9.1, "pm10": 10.4, "nitrogen_dioxide": 9.8, "umbral": 25.0, "supera_umbral": false}
0005 {"ciudad": "mendoza", "ciudad_nombre": "Mendoza", "hora_lectura": "2026-09-27T01:00", "pm2_5": 5.0, "pm10": 5.2, "nitrogen_dioxide": 5.8, "umbral": 25.0, "supera_umbral": false}
0006 {"ciudad": "puerto_montt", "ciudad_nombre": "Puerto Montt", "hora_lectura": "2026-09-27T01:00", "pm2_5": 2.2, "pm10": 2.7, "nitrogen_dioxide": 1.5, "umbral": 25.0, "supera_umbral": false}
0007 {"ciudad": "santiago", "ciudad_nombre": "Santiago", "hora_lectura": "2026-09-27T01:00", "pm2_5": 43.3, "pm10": 45.2, "nitrogen_dioxide": 36.9, "umbral": 25.0, "supera_umbral": true}
0008 {"ciudad": "valparaiso", "ciudad_nombre": "Valparaíso", "hora_lectura": "2026-09-27T01:00", "pm2_5": 9.3, "pm10": 10.6, "nitrogen_dioxide": 8.1, "umbral": 25.0, "supera_umbral": false}

Fin de la lista, no se mando nada (8 eventos)

Log       C:\BigDataLab03\evidencias\logs\02_productor.log
```

El modo lista imprime lo que enviaría y no manda nada, así que sirve para revisar el orden, que es hora por hora y después ciudad, con las cuatro ciudades alternándose.

Después viene la corrida de verdad, con el broker arriba y el almacenamiento en cero antes de empezar.

```powershell
& .venv\Scripts\python.exe lambda\01-ingesta\02_productor.py --mensajes 672 --segundos 0.02 --limpiar
```

**RESULTADO OBTENIDO:**

```text
02_productor.py  2026-10-04T04:04:40+00:00
commit  fd206d7
comando  lambda\01-ingesta\02_productor.py --mensajes 672 --segundos 0.02 --limpiar
python  3.12.10 en C:\BigDataLab03\.venv\Scripts\python.exe
pyspark  3.5.9
plataforma  Windows-11-10.0.26200-SP0

Foto      aire_horario_2026-09-27_2026-10-03.json
Rango     2026-09-27 a 2026-10-03
SHA-256   e117f7b8a3170ea119ba4367ec4aa6a295d943fc7e1c0e0e5f598857d82639f3
Eventos   672 disponibles
Voy a mandar 672 mensajes al topic lab03.eventos
Ritmo      0.02 s por mensaje


Limpiando el almacenamiento del broker
  Deteniendo el broker de Kafka
  El broker se detuvo, el puerto 9092 quedo libre
    evidencia en C:\BigDataLab03\evidencias\logs\04_kafka_cierre.log
  almacenamiento borrado  C:\BigDataLab03\kafka\kraft-logs
  Formateando el almacenamiento de Kafka
    Formatting dynamic metadata voter directory C:/BigDataLab03/kafka/kraft-logs with metadata.version 4.1-IV1.
    storage formateado con el cluster 0oc5gu4IS0a_zEfpdtnW1Q
  Arrancando el broker de Kafka
    java      C:\Program Files\Eclipse Adoptium\jdk-17.0.20.101-hotspot
    memoria   -Xmx256M -Xms128M
    broker    localhost:9092
    log       C:\BigDataLab03\evidencias\logs\03_kafka_broker.log
    proceso   6872
    esperando a que el servidor levante

  El broker arranco bien
    puerto 9092 responde  True
Topic creado  lab03.eventos
0001          Mendoza        2026-09-27T00:00  pm2.5=3.6
0002          Puerto Montt   2026-09-27T00:00  pm2.5=2.5
0003 ALERTA Santiago       2026-09-27T00:00  pm2.5=42.6
0004          Valparaíso     2026-09-27T00:00  pm2.5=9.1
0005          Mendoza        2026-09-27T01:00  pm2.5=5.0
0006          Puerto Montt   2026-09-27T01:00  pm2.5=2.5
0007 ALERTA Santiago       2026-09-27T01:00  pm2.5=43.3
0008          Valparaíso     2026-09-27T01:00  pm2.5=9.3
...
se omiten las lineas 0009 a 0671, todas del mismo formato

0672          Valparaíso     2026-10-03T23:00  pm2.5=12.2

Enviados   672
Fallidos   0
Alertas    111
Topic      lab03.eventos
Broker     localhost:9092
Duracion   16.3 s
Ritmo      41.22 msg/s

Log       C:\BigDataLab03\evidencias\logs\02_productor.log
```

El interruptor limpiar detiene el broker, borra la carpeta de datos, formatea el almacenamiento con un identificador de clúster nuevo y vuelve a arrancar, y eso es lo que hace que una corrida sea comparable con la anterior.

Con --segundos 0.02 la semana entera tarda dieciséis segundos en salir, que es lo justo para que la cola se vea llena sin hacer esperar a nadie.

Los eventos salen ordenados por hora y después por ciudad, de modo que cada cuatro mensajes representan la misma hora en los cuatro lugares.

El umbral se evalúa al armar el evento y no después, así que la marca de alerta viaja dentro del mensaje y las dos capas leen el mismo criterio.

El mensaje se confirma con acks igual a 1, el punto medio entre la velocidad y la garantía, y la clave del mensaje es la ciudad, de modo que las lecturas de un mismo lugar quedan juntas en la partición.

---

## VERIFICACIÓN DE LA INGESTA

El verificador se puede correr las veces que quiera porque lee el topic desde el principio y sin grupo de consumo, así que no guarda offsets ni depende de cuántas veces se corrió antes.

```powershell
& .venv\Scripts\python.exe lambda\01-ingesta\03_verificar.py
```

**RESULTADO OBTENIDO:**

```text
03_verificar.py  2026-10-04T04:05:37+00:00
commit  fd206d7
comando  lambda\01-ingesta\03_verificar.py
python  3.12.10 en C:\BigDataLab03\.venv\Scripts\python.exe
pyspark  3.5.9
plataforma  Windows-11-10.0.26200-SP0

Topic      lab03.eventos
Broker     localhost:9092

Recibidos   672
Alertas     111
Ciudades    mendoza=168, puerto_montt=168, santiago=168, valparaiso=168
Primer hora 2026-09-27T00:00
Ultima hora 2026-10-03T23:00

SHA-256 estable  6fa4f1f40f34cf26e0b1cda55b06c8f4d2521087bc9e9bf52c3d6395fb07dd9b
Campos            evento_id, ciudad, hora_lectura, pm2_5, pm10, nitrogen_dioxide, umbral, supera_umbral
Fecha corrida     2026-10-04T04:05:43+00:00

Log       C:\BigDataLab03\evidencias\logs\03_verificacion.log
```

Al hash llegan la ciudad, la hora de la lectura, los tres contaminantes, el umbral, la marca de alerta y el número de evento, y se dejan afuera la hora de emisión, el broker y el topic, porque son la dirección y el momento y no el contenido.

Antes de calcular nada el verificador revisa que cada evento traiga los campos estables completos, porque un evento incompleto corta la corrida con aviso, ya que el hash igual se calcula sobre datos faltantes y sale un número que parece correcto.

Si el hash estable sale igual en dos corridas, la ingesta es reproducible.

---

## CAPA DE VELOCIDAD

La capa de velocidad es el camino rápido de la arquitectura Lambda, el que toma el topic y devuelve episodios de contaminación sin esperar a que termine nada.

Se apoya en una ventana de sesión, que agrupa los eventos que llegan seguidos y se cierra recién cuando pasa el tiempo de separación sin que llegue nada más.

La separación elegida es de dos horas, o sea si pasan tres horas sin ninguna lectura contaminada la racha se da por cerrada y empieza otra, y se agrupa por ciudad, así que las rachas de Santiago no se mezclan con las de Mendoza.

La marca de agua va cuatro horas por delante de la hora de lectura y decide cuándo Spark puede olvidar una sesión que ya no va a crecer más, y ese retardo es el doble de la separación a propósito, porque si quedara más cerca la ventana podría cerrar una sesión que todavía estaba abierta.

Primero se manda la semana completa al topic y después se corre el streaming con la limpieza de la corrida anterior.

```powershell
& .venv\Scripts\python.exe lambda\01-ingesta\02_productor.py --mensajes 672 --segundos 0.02 --limpiar
& .venv\Scripts\python.exe lambda\02-velocidad\04_velocidad.py --limpiar
```

**RESULTADO OBTENIDO:**

```text
04_velocidad.py  2026-10-04T04:05:51+00:00
commit  fd206d7
comando  lambda\02-velocidad\04_velocidad.py --limpiar
python  3.12.10 en C:\BigDataLab03\.venv\Scripts\python.exe
pyspark  3.5.9
plataforma  Windows-11-10.0.26200-SP0

Corrida anterior
  borrada  C:\BigDataLab03\checkpoints\velocidad
  borrada  C:\BigDataLab03\lambda\salidas\velocidad\parquet

Topic      lab03.eventos
Broker     localhost:9092
Espacio    2 hours
Marca agua 4 hours
Por lote   100 mensajes

Streaming en marcha
Lote 001  sesiones vivas    2  acumulado     2
Lote 002  sesiones vivas    2  acumulado     4
Lote 003  sesiones vivas    4  acumulado     8
Lote 004  sesiones vivas    4  acumulado    12
Lote 005  sesiones vivas    5  acumulado    17
Lote 006  sesiones vivas    5  acumulado    22
Lote 007  sesiones vivas    6  acumulado    28

Lotes       7
Filas       28  con episodios repetidos entre lotes

Filas en disco  28
Episodios       6

Log       C:\BigDataLab03\evidencias\logs\04_velocidad.log
```

Cada lote le saca a Kafka como máximo cien mensajes, así que la semana entra en siete lotes, y en la consola se ve el número de sesiones vivas subir a medida que avanza la cola.

El modo de salida es el completo, porque la ventana de sesión se apoya en la marca de agua y el modo de actualización no sabe cuándo cerrar la fila, y el modo de anexar tampoco serviría porque las sesiones de las últimas horas del topic no se escribirían nunca.

En el modo completo cada lote vuelve a mandar todas las sesiones que siguen vivas, así que el mismo episodio aparece repetido en el Parquet con más lecturas encima cada vez, y por eso el recuento de filas es mayor que el de episodios.

Al terminar aparece un ERROR ShutdownHookManager sobre una carpeta temporal, que es Windows que no puede borrar la carpeta mientras la JVM todavía tiene los jars abierta, y no es un fallo porque el resumen ya se escribió antes de que aparezca.

Después viene el resumen, que es el que ordena el desorden y se queda con la versión más avanzada de cada episodio.

```powershell
& .venv\Scripts\python.exe lambda\02-velocidad\05_resumen_velocidad.py
```

**RESULTADO OBTENIDO:**

```text
05_resumen_velocidad.py  2026-10-04T04:06:38+00:00
commit  fd206d7
comando  lambda\02-velocidad\05_resumen_velocidad.py
python  3.12.10 en C:\BigDataLab03\.venv\Scripts\python.exe
pyspark  3.5.9
plataforma  Windows-11-10.0.26200-SP0

Parquet    C:\BigDataLab03\lambda\salidas\velocidad\parquet
CSV        C:\BigDataLab03\lambda\salidas\velocidad\episodios.csv

Episodios  6
Ciudades   santiago=5, valparaiso=1
Primera    2026-09-27 00:00:00
Ultima     2026-10-03 18:00:00

Mas horas   Santiago  2026-09-29 12:00:00  74 horas seguidas
Mas picado  Santiago  68.7 ug/m3 en 2026-09-29 12:00:00

SHA-256 episodios  befb30473fcb54e617c392857566ecbc6e506c3d5663c90fc36df577c53e1bf6
Campos              ciudad, inicio, fin, lecturas, umbral, pm2_5_max, pm10_max, nitrogen_dioxide_max
Fecha corrida       2026-10-04T04:06:59+00:00
Grafico    C:\BigDataLab03\evidencias\graficos\velocidad_episodios.png

Log       C:\BigDataLab03\evidencias\logs\05_resumen_velocidad.log
```

El CSV queda con una fila por episodio dentro de lambda/salidas/velocidad y el gráfico de la semana queda en evidencias/graficos, y los episodios repetidos del Parquet quedan resueltos porque el resumen se queda con la versión más avanzada de cada uno.

El SHA-256 se calcula sobre la ciudad, el comienzo, el fin, las lecturas y los picos de contaminantes, con el mismo criterio que el verificador de la ingesta, y dos corridas que procesaron el mismo topic dan el mismo hash.

No se debe correr el 04 sin el --limpiar cuando se volvió a mandar el topic desde cero, porque el checkpoint viejo le dice a Spark que ya leyó todo y en la corrida nueva no procesa nada.

No se debe correr el 05 con los avisos convertidos en error, PySpark usa un método de pandas que ya está marcado como obsoleto y el script se cae en la conversión sin que nada esté roto.

---

## CAPA DE LOTES

La capa de lotes es el camino lento de la arquitectura Lambda, el que lee el topic entero de una sola vez y lo deja consolidado en dos vistas.

No necesita ningún interruptor de limpieza, es una corrida batch desde el primer offset hasta el último que se sobrescribe sobre la anterior, y al terminar mata sus propios procesos de Spark.

Se corre con el broker arriba y el productor ya ejecutado.

```powershell
& .venv\Scripts\python.exe lambda\03-lotes\06_lotes.py
```

**RESULTADO OBTENIDO:**

```text
06_lotes.py  2026-10-04T04:07:11+00:00
commit  fd206d7
comando  lambda\03-lotes\06_lotes.py
python  3.12.10 en C:\BigDataLab03\.venv\Scripts\python.exe
pyspark  3.5.9
plataforma  Windows-11-10.0.26200-SP0

Topic      lab03.eventos
Broker     localhost:9092

Topic leido  672 eventos

Parquet    C:\BigDataLab03\lambda\salidas\lotes\parquet\diario
CSV        C:\BigDataLab03\lambda\salidas\lotes\diario.csv

Parquet    C:\BigDataLab03\lambda\salidas\lotes\parquet\ciudades
CSV        C:\BigDataLab03\lambda\salidas\lotes\ciudades.csv

Eventos        672
Alertas        111
Diario         28 filas
Ciudades       4 filas
Horas umbral   111  igual que las alertas

SHA-256 diario    af4ae41968a401c4c493d96e9654c170526cf2d85b37a18642435e8dcc2a9063
SHA-256 ciudades  a9d4aad45505871435811536de5fe7bef23a54b7ab0f11cefe8988e2a6662f65
Campos diario     ciudad, dia, lecturas, horas_sobre_umbral, pct_sobre_umbral, pm2_5_prom, pm10_prom, nitrogen_dioxide_prom, pm2_5_max, pm10_max, nitrogen_dioxide_max
Campos ciudades   ciudad, lecturas, horas_sobre_umbral, pct_sobre_umbral, pm2_5_prom, pm10_prom, nitrogen_dioxide_prom, pm2_5_max, pm10_max, nitrogen_dioxide_max
Fecha corrida     2026-10-04T04:07:29+00:00
Grafico    C:\BigDataLab03\evidencias\graficos\lotes_diario.png

Log       C:\BigDataLab03\evidencias\logs\06_lotes.log
```

El renglón de cruce dice `Horas umbral 111 igual que las alertas`, porque las 111 horas contaminadas que suman las vistas son las mismas 111 alertas que contó la ingesta, contadas por caminos distintos, una desde los mensajes y otra desde las vistas.

La vista diaria trae cuatro ciudades por siete días, o sea 28 filas, y la de ciudades deja una fila por lugar para comparar la semana entera de un vistazo.

Los promedios se redondean a dos decimales dentro de la propia consulta y no al escribir, porque el número que entra al hash tiene que ser idéntico en cualquier máquina.

Los SHA-256 de la corrida de referencia son `af4ae41968a401c4c493d96e9654c170526cf2d85b37a18642435e8dcc2a9063` para la vista diaria y `a9d4aad45505871435811536de5fe7bef23a54b7ab0f11cefe8988e2a6662f65` para la de ciudades, y deben salir iguales todas las veces que se lea el mismo topic.

No se debe correr el 06 antes de que el productor termine de mandar la semana, las vistas se calculan sobre lo que haya en el topic en ese instante y a mitad de producción sale una semana incompleta con todos los hashes cambiados.

---

## CAPA DE SERVICIO

La capa de servicio es donde se juntan los dos caminos de la arquitectura Lambda.

Se corre al final, después de que las dos capas anteriores ya dejaron sus CSV, y no necesita broker ni Spark porque solo abre tres archivos y los carga en SQLite.

```powershell
& .venv\Scripts\python.exe lambda\04-servicio\07_servicio.py
```

**RESULTADO OBTENIDO:**

```text
07_servicio.py  2026-10-04T04:07:43+00:00
commit  fd206d7
comando  lambda\04-servicio\07_servicio.py
python  3.12.10 en C:\BigDataLab03\.venv\Scripts\python.exe
pyspark  3.5.9
plataforma  Windows-11-10.0.26200-SP0

diario     28 filas desde diario.csv
ciudades   4 filas desde ciudades.csv
episodios  6 filas desde episodios.csv

Resumen por ciudad
ciudad        lecturas  horas umbral  pct umbral  pm2.5 prom
Santiago      168       107           63.69       32.02
Valparaíso    168       4             2.38        10.88
Mendoza       168       0             0.0         6.35
Puerto Montt  168       0             0.0         3.16

Dias mas contaminados
ciudad    dia         pm2.5 prom  pm10 prom
Santiago  2026-10-01  44.7        47.04
Santiago  2026-09-30  40.87       45.14
Santiago  2026-10-02  37.55       38.79

Episodios del streaming por ciudad
ciudad      episodios  horas ventana  lecturas contaminadas
Santiago    5          113.0          107
Valparaíso  1          5.0            4

Cruce de las dos capas
ciudad        horas batch  lecturas episodios  episodios
Santiago      107          107                 5
Valparaíso    4            4                   1
Mendoza       0            0                   0
Puerto Montt  0            0                   0

Horas batch        111
Lecturas episodios 111
El cruce           coinciden

Base          C:\BigDataLab03\lambda\salidas\servicio\aire.db
Fecha corrida 2026-10-04T04:07:44+00:00

Log       C:\BigDataLab03\evidencias\logs\07_servicio.log
```

La base se borra y se rehace de cero en cada ejecución, así que el servicio siempre muestra el último estado de las capas y nunca una mezcla de corridas viejas con corridas nuevas.

El encabezado de cada CSV se compara contra las columnas declaradas antes de escribir, y si alguien tocó un archivo a mano la corrida se corta en lugar de cargar números viejos en silencio.

El renglón de cruce es el que cierra la arquitectura, muestra en la misma fila las horas batch y las lecturas de los episodios de cada ciudad, y termina con los renglones `Horas batch 111`, `Lecturas episodios 111` y `El cruce coinciden`.

Esas 111 horas son las mismas que ya cuadraron en las otras capas, o sea las 111 alertas del topic contadas por tercera vez, ahora con las dos ramas de la arquitectura lado a lado en una sola tabla.

La corrida tarda segundos, porque no levanta ninguna JVM y solo pasa tres archivos chicos por el sqlite3 que trae Python, y la base queda en lambda/salidas/servicio/aire.db sin versionarse porque se rehace en cada corrida.

---

## CORRIDA COMPLETA DE PUNTA A PUNTA

La secuencia entera, desde la descarga hasta la capa de servicio, se corrió el 2026-10-04 y cerró todas las comprobaciones.

| Comprobación | Valor obtenido |
| ------------ | -------------- |
| Lecturas descargadas y publicadas | 672 |
| Fallidos del productor | 0 |
| Alertas en el topic | 111 |
| Episodios del streaming | 6 |
| Filas de la vista diaria | 28 |
| Horas batch contra lecturas de episodios | 111 igual que 111 |
| Cruce de la capa de servicio | coincide |
| Saneamiento del broker al terminar | puerto 9092 libre |

El orden sin saltos es descarga, productor con limpiar, verificador, streaming con limpiar, resumen, lotes, servicio y parada del broker.

No se debe apagar el broker en el medio de la secuencia, las capas de velocidad y de lotes lo necesitan arriba para leer el topic.

---

## DOCKER

Docker es opcional en el enunciado y en la máquina de referencia no está instalado, así que la composición se creó y se validó como YAML pero no se llegó a levantar en vivo.

Lo que hay es `docker/docker-compose.yml`, una composición mínima con un solo servicio, el broker de Kafka 4.1.2 en modo KRaft dentro de un contenedor, con el puerto 9092 publicado y un volumen nombrado para los datos.

En una máquina con Docker 20.10.4 o posterior se levanta desde la raíz:

```powershell
cd docker
docker compose up -d
docker compose ps
docker compose logs kafka
docker compose down
```

La columna STATUS del ps debe terminar en `running` con health `healthy`, y en los logs aparece la línea del servidor terminado de arrancar.

No se debe levantar el contenedor y el broker de Windows al mismo tiempo, porque los dos quieren el puerto 9092 y el segundo falla con un error de puerto ocupado que parece un fallo del script y no lo es.

No se debe usar `docker compose down -v` sin querer borrar los datos, porque esa variante elimina el volumen nombrado donde el contenedor guarda los topics.

Si docker no está instalado, el error de que el comando no se reconoce es la señal de que esta vía no aplica en esa máquina, y el arranque sigue siendo `scripts\start_kafka.ps1`.

Antes de viajar a una máquina con Docker conviene confirmar que la imagen existe, porque la etiqueta se pide tal cual:

```powershell
$r = Invoke-WebRequest -Uri "https://hub.docker.com/v2/repositories/apache/kafka/tags/4.1.2" -UseBasicParsing
$r.StatusCode
```

**RESULTADO OBTENIDO:**

```text
200
```

El código 200 es la respuesta de que la etiqueta está publicada en Docker Hub.

---

## VERIFICACIÓN DEL COMPOSE SIN DOCKER

La composición se puede revisar en esta máquina aunque no haya Docker, porque en una primera pasada lo que casi siempre falla es el archivo y no el motor de contenedores.

El verificador vive en `docker/verificar_compose.py` y se corre con la venv desde la raíz, igual que el resto de los scripts.

```powershell
& .venv\Scripts\python.exe docker\verificar_compose.py
```

**RESULTADO OBTENIDO:**

```text
verificar_compose.py  2026-10-04T03:51:01+00:00
commit  96d0d22
comando  C:\BigDataLab03\docker\verificar_compose.py
python  3.12.10 en C:\BigDataLab03\.venv\Scripts\python.exe
pyspark  3.5.9
plataforma  Windows-11-10.0.26200-SP0

Compose   C:\BigDataLab03\docker\docker-compose.yml

OK     el archivo parsea como diccionario
OK     el servicio kafka existe
OK     la imagen es apache/kafka igual a la instalación local
OK     el puerto 9092 del host queda publicado
OK     el nodo hace de broker y de controlador a la vez
OK     el nodo tiene identificador
OK     el cliente escucha en el mismo broker que usa el proyecto
OK     las claves de KRaft están todas
OK     los factores de replicación están en uno
OK     el volumen de datos está declarado
OK     el contenedor monta ese volumen donde escribe Kafka
OK     las bitácoras del broker apuntan al volumen
OK     el healthcheck consulta al broker con kafka-topics

Total     13 comprobaciones, 0 fallos
La composición está lista para una máquina con Docker

Log       C:\BigDataLab03\evidencias\logs\08_verificacion_compose.log
```

Lee el compose con PyYAML y pasa trece comprobaciones, cada una con su nombre y con lo que espera encontrar, así que un fallo apunta directo a la pieza rota en vez de dejar adivinando.

No se debe tomar esta verificación como prueba de que el contenedor levanta, porque no levanta nada, solo revisa el archivo, y la corrida real es `docker compose up` en una máquina con Docker.

Si el script se cae con `ModuleNotFoundError: No module named yaml`, es que la venv no tiene PyYAML todavía, y se arregla reinstalando las versiones pineadas con `& .venv\Scripts\python.exe -m pip install -r requirements.txt`.

---

## LOS LOGS

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

---

## QUÉ NO HACER, RESUMIDO

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

---

## ERRORES TÍPICOS Y QUÉ HACER

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

Si el renglón de cruce no da 111, el topic se leyó a mitad de la producción o se modificó entre corridas, se vuelve a la secuencia completa desde el productor con --limpiar antes de desconfiar de las vistas.

Si 07_servicio.py dice `Falta` seguido de la ruta de un CSV, esa capa no corrió o alguien limpió las salidas, se corren el 05 y el 06 en orden y se vuelve a intentar el servicio.

Si el servicio dice que el encabezado de un CSV es distinto al esperado, alguien editó el archivo a mano, se regenera la capa que lo produce en lugar de tocar el CSV.

Si el cruce del servicio dice `NO coinciden`, una de las dos capas se corrió contra un topic distinto, se repite la secuencia completa desde el productor con --limpiar y las dos vuelven a dar el mismo número en los dos lados.

Si la política de ejecución de PowerShell bloquea un script de la carpeta scripts, se levanta el permiso solo para la sesión con `Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass` y se vuelve a llamar al script.
