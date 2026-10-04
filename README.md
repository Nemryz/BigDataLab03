# BigDataLab03

Este repositorio es dónde se ubicará el Laboratorio N°3 para la asignatura de
Big Data.

Implementación de la arquitectura Data Lakehouse sobre un entorno local en
Windows, con pipeline funcionando sobre datos reales, evidencia reproducible y
análisis comparativo. Las tres arquitecturas de referencia, Lambda, Kappa y Data
Lakehouse, se comparan en 'docs/tabla_comparativa.md' sobre el mismo problema, y
la que se construye de punta a punta es la del lago de datos.

## Arquitectura del repositorio

Todo vive dentro de una sola carpeta, la del propio repositorio, y esa decisión
explica casi todo lo demás. La raíz está en una ruta corta y sin espacios porque
Spark en Windows se rompe cuando la ruta tiene espacios, y porque el sistema
tiene un tope de 260 caracteres que se agota apenas metemos carpetas profundas.
La carpeta pesada queda ignorada por git, porque pesa mucho y no le interesa a
nadie el historial de un gigabyte de librerías.

```
C:\BigDataLab03\
  .venv\                       Python 3.12 con PySpark 3.5.9
  winutils\                    binarios de Hadoop que Windows necesita para escribir
  datos\                       zonas raw, bronze, silver y gold
  checkpoints\                 estado del Structured Streaming
  .ivy2\                       memoria del conector Spark con Kafka
  kafka\                       Apache Kafka 4.1.2
  src\comun\                   módulos compartidos, incluido el SQL de las consultas
  data-lakehouse\              rama Data Lakehouse
  lambda\                      rama Lambda
  kappa\                       rama Kappa
  evidencias\                  logs, pantallazos, gráficos, planes, huella del entorno
  docs\                        informe, tabla comparativa, veredicto
```

Las carpetas de cada rama llevan un número al principio, y ese número es el
orden en que se van a construir. Mirar el árbol ya dice en qué vamos.

| Carpeta | Ramas | Contenido | Estado |
| --------- | ------- | ----------- | ------ |
| '01-generacion' | lakehouse | descarga el snapshot y arruina una copia con semilla fija | hecho |
| '01-ingesta' | lakehouse, lambda, kappa | el generador o el productor que alimenta el sistema | pendiente |
| '02-ingesta' | lakehouse | aplana el JSON y escribe la zona bronze | hecho |
| '02-velocidad' | lambda | la capa de baja latencia | pendiente |
| '03-lotes' | lambda | la capa por lotes | pendiente |
| '03-transformacion' | lakehouse | limpieza y Parquet particionado por ciudad y fecha | hecho |
| '04-consultas' | lakehouse | SQL analítico sobre la zona silver | hecho |
| '05-visualizacion' | lakehouse | gráficos con matplotlib | hecho |
| '06-explain' | lakehouse | plan de ejecución de Spark y tamaños de las zonas | hecho |
| '04-servicio' | lambda | la capa SQLite que une las dos vías | pendiente |
| '02-procesamiento' | kappa | ventanas de tumbling, sliding y sesión | pendiente |
| '03-reprocesamiento' | kappa | replay del historial con lógica cambiada | pendiente |
| 'salidas\' | todas | resultados de la rama | pendiente |

## Requisitos

| Componente | Versión | Por qué esa |
| Python | 3.12.10 | venv propia dentro del repo |
| Temurin JDK | 17.0.20.1 | Spark 3.5 exige Java 8, 11 o 17 |
| PySpark | 3.5.9 | empaqueta Hadoop 3.3.4, que tiene winutils publicado |
| pandas | 2.3.3 | PySpark 4.x no soporta del todo pandas 3 en adelante |
| winutils | 3.3.5 | el binario nativo que Windows necesita para escribir |
| Apache Kafka | 4.1.2 | solo para Lambda y Kappa, en modo KRaft |

> Ojo con los Pythons. En mi pc hay un 3.12 y un 3.13 instalados, y el pip del
> PATH pertenece al 3.13 mientras que 'python' es el 3.12. Por eso todos los
> comandos de este documento usan la ruta absoluta a la venv y la forma '-m
> pip'. Si usás 'pip' suelto, instalás en el intérprete equivocado y después el
> import falla sin explicación.

# Instalación paso a paso

Cada bloque se ejecuta en PowerShell. Abre una ventana nueva al final, porque la
variable 'JAVA_HOME' recién se lee en las sesiones que se abren después de
configurarla.

## Paso 1: Java 17

Spark 3.5 corre con Java 8, 11 o 17, y nada más. Como dije antes, mi pc traía
Java 18 y Java 20, que no sirven para el funcionamiento de todo el laboratorio,
entonces tocará instalarlo.

```powershell
winget install EclipseAdoptium.Temurin.17.JDK --accept-source-agreements --accept-package-agreements
```

Apuntamos 'JAVA_HOME' al JDK nuevo y lo ponemos delante en el PATH, porque el
'java' del PATH apuntaba al shim de Oracle con Java 20 y Spark usa la variable
primero.

```powershell
$jdk = (Get-ChildItem "C:\Program Files\Eclipse Adoptium" -Directory)[0].FullName
[Environment]::SetEnvironmentVariable("JAVA_HOME", $jdk, "User")

$rutaUsuario = [Environment]::GetEnvironmentVariable("Path", "User")
[Environment]::SetEnvironmentVariable("Path", "%JAVA_HOME%\bin;" + $rutaUsuario, "User")
```

Tienes que colocar donde esta [rutaUsuario] obviamente tu ruta o tu path.

Para verificar, abre una ventana nueva de PowerShell y corré esto de acá:

```powershell
java -version
```

Tiene que decir '17.0.x'. Si dice 18 o 20, la variable no tomó y hay que repetir
el paso.

## Paso 2: La venv y PySpark

```powershell
$repo = "C:\BigDataLab03"
$py   = "$repo\.venv\Scripts\python.exe"

& "C:\Users\[TU_USUARIO]\AppData\Local\Programs\Python\Python312\python.exe" -m venv "$repo\.venv"
```

Instalamos en tres pasos y en este orden, porque cada uno depende del anterior.

```powershell
& $py -m pip install --upgrade pip
& $py -m pip install "pyspark[sql]==3.5.9" matplotlib
& $py -m pip install "pandas==2.3.3" "numpy==2.5.3" setuptools
```

¿Por qué los tres por separado?

El primero deja el PySpark. El segundo es el que importa, con pandas y numpy en
las versiones que calzan con PySpark 3.5.9 y con matplotlib, porque si se
instalan sueltos pip resuelve versiones que se pisan entre ellas.

Y el tercero es el que más confunde. 'setuptools' parece innecesario pero no lo
es, ya que Python 3.12 quitó 'distutils' de la biblioteca estándar y PySpark
todavía lo usa, así que sin este paquete la función 'toPandas()' revienta con un
error de módulo inexistente.

Para verificar todo:

```powershell
& $py -c "import pyspark, pandas, numpy, pyarrow, setuptools; print(pyspark.__version__, pandas.__version__, numpy.__version__, pyarrow.__version__)"
& $py -m pip check
```

Lo primero tiene que imprimir '3.5.9 2.3.3 2.5.3 25.0.1' y lo segundo tiene que
decir 'No broken requirements found'. Si 'pip check' se queja de numpy, es que
se coló una versión vieja y hay que reinstalar el segundo paso.

## Paso 3: winutils

Este es el que más tiempo costó y el que más conviene explicar, porque su error
miente demasiado. Leer Parquet y contar filas funciona sin winutils, escribir
no, y cuando falla el mensaje dice que falta 'HADOOP_HOME', como si la ruta
estuviera mal, cuando en realidad lo que falta es un archivo.

La razón es que al escribir un archivo Spark le pone permisos, y en Windows esa
operación la hace un binario de Hadoop que no viene con nada. Lo descargamos y
lo dejamos en su carpeta.

```powershell
$bin = "$repo\winutils\bin"
New-Item -ItemType Directory -Force -Path $bin | Out-Null
$base = "https://github.com/cdarlint/winutils/raw/master/hadoop-3.3.5/bin"

curl.exe -sL -o "$bin\winutils.exe" "$base/winutils.exe"
curl.exe -sL -o "$bin\hadoop.dll"  "$base/hadoop.dll"
Copy-Item "$bin\hadoop.dll" "$bin\winutils.dll" -Force
```

Las tres líneas importan demasiado, dado que los dos archivos que se descargan
son el ejecutable y la biblioteca. El 'Copy-Item' es el que casi nadie
encuentra, y hace falta porque Hadoop busca una biblioteca llamada 'winutils'
mientras que lo que se descarga se llama 'hadoop.dll', así que sin la copia
todas las lecturas funcionan y ninguna escritura.

Para verificar, la copia también la puede hacer el propio código.

```powershell
& $py -c "import sys; sys.path.insert(0, '$repo\src\comun'); import config; config.preparar_winutils(); print('winutils.dll ->', (config.HADOOP_HOME / 'bin' / 'winutils.dll').exists())"
```

## Paso 4: Apache Kafka

Kafka solo hace falta para las ramas Lambda y Kappa, y no lo necesita el
Lakehouse.

```powershell
curl.exe -L -o "C:\BigDataLab03\kafka.tgz" "https://archive.apache.org/dist/kafka/4.1.2/kafka_2.13-4.1.2.tgz"
tar -xzf "C:\BigDataLab03\kafka.tgz" -C "C:\BigDataLab03"
Rename-Item "C:\BigDataLab03\kafka_2.13-4.1.2" "kafka"
```

Kafka 4.x trabaja en modo KRaft y ya no usa ZooKeeper, así que cualquier
tutorial anterior que mencione 'zookeeper-server-start' está obsoleto. Después
se formatea el almacenamiento y se arranca, y eso lo hace 'start_kafka.ps1'.

Para que funcione, tiene que responder el puerto 9092.

```powershell
Test-NetConnection -ComputerName localhost -Port 9092
```

## Paso 5: Smoke test

Prueba de humo, o sea que verifica que las cuatro piezas anteriores se puedan
usar juntas antes de construir nada encima.

```powershell
cmd /c "`"$py`" `"$repo\src\comun\smoke_test.py`" > `"$repo\evidencias\logs\00_smoke_test.log`" 2>&1"
Get-Content "$repo\evidencias\logs\00_smoke_test.log"
```

Tiene que terminar con 'SMOKE TEST: OK' y siete de siete pruebas superadas.

Ojo con este comando, porque la forma intuitiva de escribirlo no sirve. Si usás
el 2>&1 con Tee-Object de PowerShell, el log se guarda con cuatro líneas de
decoración de error metidas en medio, del tipo 'CategoryInfo' y
'FullyQualifiedErrorId', que no las escribe nuestro script sino PowerShell que
se queja de que el proceso escribió en la salida de errores. Como el log es
evidencia, ahí no puede quedar basura, por eso el comando pasa por cmd, que no
decora nada.

Después el Get-Content es solo para ver el resultado en pantalla, porque cmd
escribe directo al archivo y no a la consola.

Al final del log vas a ver unas líneas que dicen 'CORRECTO: el proceso con
PID...' y esas no son nuestras, las pone Windows cuando cierra los procesos
hijos de la máquina virtual de Java. Se dejan como están a propósito, porque un
log de evidencia tiene que ser crudo y filtrarlo sería deshonesto.

## Sobre cómo registrar los pasos

Cada script del proyecto imprime al comienzo un encabezado con la fecha, el
commit de git, el comando exacto y las versiones, y eso lo hace el módulo
'evidencia.py'. La idea es que un log suelto en 'evidencias\logs' sirva como
prueba sin que haga falta un párrafo al lado que lo explique. Para guardar la
salida de cualquier script, se usa siempre el mismo comando del paso 5,
cambiando la ruta del script y la del log.

## El flujo completo, en tres pasos

Estos son los tres comandos, en este orden. Nada más hay que hacer.

```powershell
git clone https://github.com/Nemryz/BigDataLab03.git C:\BigDataLab03
cd C:\BigDataLab03
.\bootstrap.ps1
.\lab03.bat
```

El primero baja el código, el segundo instala las herramientas y el tercero
corre la prueba de humo. El paso dos se hace una sola vez por máquina. El paso
tres se puede repetir cuantas veces quieras, y también se puede abrir con doble
clic.

## Datos reales, descargados una vez

Para las tres ramas usamos datos reales de calidad del aire en cuatro ciudades,
Santiago, Mendoza, Valparaíso y Puerto Montt. La descarga la hace un solo
script, y después de eso el pipeline nunca más toca la red.

```powershell
& $py ".\data-lakehouse\01-generacion\descargar.py" aire_horario
```

Sin argumentos te muestra el catálogo de las tres fuentes disponibles. El script
deja el archivo en 'datos\raw\' y al lado un manifiesto con la dirección usada,
la fecha y el hash.

La regla de por qué la red se toca una sola vez es la más importante del
proyecto. Si el pipeline llamara a la API en cada corrida, cada una traería
datos distintos, el hash de reproducibilidad no cuadraría nunca y un corte de
internet el día de la defensa nos dejaría sin demostración. Con la foto en
disco, la red pasa a ser un paso opcional y el pipeline queda siempre igual. Eso
además nos permite comparar las tres arquitecturas sobre el mismo problema, que
es lo que le da sentido a la tabla comparativa.

## El pipeline del Data Lakehouse, paso a paso

Son seis scripts que se corren en orden, y cada uno deja su salida en una zona
distinta del lago. Las zonas no son decorativas, cada una guarda el dato en un
estado distinto y ninguna se toca salvo la que produce ese paso.

| Paso | Script | Zona que escribe | Qué le pasa al dato |
| ---- | ------ | ---------------- | ------------------- |
| 1 | '01-generacion\ensuciar.py' | 'datos\raw' | le inyecta nulos, texto donde debería haber números, negativos y horas repetidas con semilla fija |
| 2 | '02-ingesta\02_ingesta.py' | 'datos\bronze' | aplana el JSON anidado a filas y guarda los tipos como llegaron |
| 3 | '03-transformacion\03_transformar.py' | 'datos\silver' | limpia, castea, saca repetidas y calcula la columna de episodio |
| 4 | '04-consultas\04_consultas.py' | 'datos\gold' | cuatro consultas SQL analíticas |
| 5 | '05-visualizacion\05_visualizacion.py' | 'evidencias\graficos' | dos gráficos con matplotlib |
| 6 | '06-explain\06_explain.py' | 'evidencias\explain' | el plan de Spark y el tamaño de cada zona |

Los seis están escritos y probados. Los cinco primeros producen el lago y sus
gráficos, y el sexto es el que deja la evidencia de que Spark hace el trabajo y
no una librería haciéndose pasar por él.

Hay una pieza de soporte que no aparece en la tabla porque no escribe nada: el
archivo 'src\comun\consultas.py' guarda el SQL de las tres consultas analíticas
y de su resumen. El paso 4 lo importa para correrlas y el paso 6 lo importa para
explicar la más pesada. Estar en un solo lugar es lo que garantiza que el plan
que se explica sea el plan que realmente se ejecuta, y no una copia que se
desincronizó.

Los comandos, en este orden, con el mismo patrón del Paso 5 de la instalación
para que la salida quede guardada como evidencia.

```powershell
cmd /c "`"$py`" `"$repo\data-lakehouse\01-generacion\ensuciar.py`" > `"$repo\evidencias\logs\10_ensuciar.log`" 2>&1"
cmd /c "`"$py`" `"$repo\data-lakehouse\02-ingesta\02_ingesta.py`" > `"$repo\evidencias\logs\11_ingesta.log`" 2>&1"
cmd /c "`"$py`" `"$repo\data-lakehouse\03-transformacion\03_transformar.py`" > `"$repo\evidencias\logs\12_transformar.log`" 2>&1"
cmd /c "`"$py`" `"$repo\data-lakehouse\04-consultas\04_consultas.py`" > `"$repo\evidencias\logs\13_consultas.log`" 2>&1"
cmd /c "`"$py`" `"$repo\data-lakehouse\05-visualizacion\05_visualizacion.py`" > `"$repo\evidencias\logs\14_visualizacion.log`" 2>&1"
cmd /c "`"$py`" `"$repo\data-lakehouse\06-explain\06_explain.py`" > `"$repo\evidencias\logs\15_explain.log`" 2>&1"
```

### Por qué se arruinan los datos a propósito

Este es el punto que conviene entender antes de mirar cualquier número. Si el
pipeline recibiera datos perfectos, la limpieza no se podría demostrar, porque
no habría nada que limpiar. Entonces el paso 1 toma el snapshot real y lo rompe
con una semilla fija, que es 'config.SEED', de modo que dos corridas arruinan
exactamente lo mismo y los resultados se pueden comparar. El archivo bueno no se
modifica, la destrucción va a una copia.

### Qué se gana limpiando

Los números salen de la corrida real sobre el snapshot del 22 al 28 de
septiembre.

| Medida | Valor |
| ------ | ----- |
| Filas en bronze | 696 |
| Horas repetidas detectadas y quitadas | 24 |
| Filas en silver | 586 |
| Valores de PM10 anulados sin tirar la fila | 72 |
| Valores de NO2 anulados sin tirar la fila | 67 |
| Particiones silver por ciudad y fecha | 32 |

La decisión que más cambió el resultado fue no tirar filas por un contaminante
secundario. La primera versión filtraba cada columna por separado y se llevaron
puestas 216 de 672 filas, o sea un tercio del dataset, porque una fila cae si le
falta cualquiera de los tres. Solo el PM2.5 puede tirar una fila entera, porque
es el que define el episodio. Los otros dos se anulan y la fila se conserva, y
así se pasó a perder 86 en vez de 216.

### El indicador de episodio

La columna 'es_episodio' vale uno cuando el PM2.5 alcanza o pasa los 25 ug/m3,
que es el límite diario que usa la Organización Mundial de la Salud, y la
columna 'exceso' guarda cuánto se pasó para poder ordenar los episodios sin
recalcular nada.

Sobre 586 filas limpias hay 145 horas por encima del umbral, y esas 145 horas se
agrupan en 23 episodios, o sea rachas de horas malas seguidas. Agrupar es
distinto de contar, y para saber dónde termina un episodio hay que mirar la hora
anterior de la misma ciudad y ver si también estaba mala. Esa diferencia entre
horas consecutivas es la que arranca un episodio nuevo, y con eso la consulta
SQL entrega duración, inicio y pico de cada uno.

| Ciudad | Horas totales | Sobre umbral | Porcentaje | Episodios | Episodio más largo | PM2.5 pico |
| ------ | ------------- | ------------ | ---------- | --------- | ------------------ | ---------- |
| santiago | 149 | 122 | 81.88 | 19 | 21 horas | 108.5 |
| valparaiso | 142 | 23 | 16.20 | 4 | 10 horas | 37.6 |
| mendoza | 145 | 0 | 0.00 | 0 | 0 | 23.8 |
| puerto_montt | 150 | 0 | 0.00 | 0 | 0 | 12.5 |

Ese número de episodios es justamente el que se puede comparar contra la ventana
de sesión de Kappa y contra la máquina de estados, que es lo que hace que la
tabla comparativa tenga sentido en vez de ser una lista de opiniones.

### Qué se ve en los gráficos

El paso 5 deja dos figuras en 'evidencias\graficos', y ambas se generan de la
zona silver, o sea de los datos ya limpios, no de los crudos.

La primera es la serie temporal de PM2.5 por ciudad con la línea del umbral de
25 ug/m3 dibujada encima. Se lee de un vistazo que Santiago se sostiene por
encima del límite casi toda la semana y que las tres ciudades restantes viven
por debajo, con Valparaíso cruzándolo solo en tramos. El título no está escrito
a mano: se calcula del propio conjunto, porque la conversión a UTC corre la
última hora local hacia el día siguiente y un título fijo termina mintiendo un
día entero.

La segunda es un mapa de calor de 4 por 24, ciudades contra hora del día, con el
valor promedio anotado en cada celda. Esa es la figura que muestra el patrón
horario: en Santiago las horas UTC de la madrugada son las más cargadas, que es
la noche local, y en Puerto Montt el color ni se mueve.

Los archivos son:

| Archivo | Figura |
| ------- | ------ |
| 'serie_temporal_ciudades.png' | cuatro curvas horarias y la línea del umbral |
| 'mapa_calor_pm25.png' | promedio por ciudad y hora del día, con el número en cada celda |

### El plan de Spark y el antes y después de los tamaños

El paso 6 deja dos archivos en 'evidencias\explain' y responde dos preguntas
distintas.

'plan_consulta_pesada.txt' guarda el plan de ejecución completo de la consulta
de episodios, que es la más pesada del proyecto porque usa dos funciones de
ventana y tres capas de CTE anidadas. Es la evidencia de que el trabajo corre
sobre Spark de verdad: el plan físico muestra los dos 'Exchange' que mueven
datos entre procesos, las dos agrupaciones 'HashAggregate' en dos fases, los
'Sort' que las funciones de ventana exigen, y un 'FileScan parquet' con los
filtros ya empujados hacia el lector de archivos. Ninguna de esas líneas se
puede escribir a mano, que es justamente lo que las hace valer como evidencia.

'tamano_zonas.txt' guarda la tabla de tamaños y de lecturas de cada zona, con
los bytes por lectura para que la comparación sea justa y no dependa de que una
zona tenga más filas que otra.

| Zona | Lecturas | Archivos | Bytes | Bytes por lectura |
| ---- | -------- | -------- | ----- | ----------------- |
| raw snapshot | 672 | 1 | 47,897 | 71 |
| raw sucio | 672 | 1 | 52,710 | 78 |
| bronze | 696 | 2 | 14,844 | 21 |
| silver | 586 | 32 | 117,919 | 201 |
| gold | 35 | 4 | 9,531 | 272 |

Dos detalles de esa tabla que conviene tener listos. El primero es que la zona
gold suma 35 filas porque son cuatro salidas agregadas: 23 episodios, 4 ciudades
en el resumen de umbral, 6 filas de medias y 2 en el resumen de episodios, ese
último bajo porque agrupa la tabla de episodios y solo dos ciudades llegaron a
tener alguno.

El segundo es que el JSON crudo solo se puede leer con la opción 'multiLine'. El
archivo es un arreglo que ocupa varias líneas, y sin esa opción Spark toma cada
línea por separado, no encuentra JSON válido en ninguna y devuelve todo como
registros corruptos en vez de fallar con un mensaje claro. El paso 6 lo deja
escrito en el log.

El cronometraje de lectura también se imprime, pero no se saca ninguna
conclusión de velocidad de ahí. Con 586 filas la diferencia entre leer crudo y
leer Parquet queda dentro del ruido de arranque de la máquina virtual.

### Por qué el Parquet no siempre pesa menos

La tabla anterior ya trae el dato completo, pero vale la pena repetir la
conclusión, porque es la pregunta que con más seguridad va a llegar en la
defensa.

El JSON crudo pesa 47.897 bytes y la zona silver pesa 117.919, o sea más del
doble. Normalizando por lectura la diferencia es aún más clara: 71 bytes contra
201. La respuesta es que silver son 32 carpetas particionadas y cada una lleva
su propia fila de esquema, sus propios archivos de índice y sus propios footers,
así que con 586 filas el sobrepeso de la estructura gana.

Con millones de filas esa misma estructura deja de pesar y la compresión manda.
Es un dato que conviene llevar a la defensa, porque la ventaja de Parquet es una
función del volumen, no una constante.

## Windows te bloquea un archivo

Si al abrir 'lab03.bat' Windows te dice que está bloqueado o te sale el aviso
del Escudo de Windows, no es un virus. Es la marca que Windows le pone a los
archivos que vienen de afuera, que se llama Mark of the Web, y lo hace porque no
conoce el archivo. Un archivo que viene de 'git clone' no debería tener esa
marca, pero si lo bajaste con el navegador o lo copiaste de un pen drive, la
puede tener. Se quita con esta línea, que solo le saca la marca y no toca nada
más:

```powershell
Get-ChildItem -Recurse -Include *.bat, *.ps1 | Unblock-File
```

Lo que **no** vamos a hacer, y conviene decirlo porque es tentador, es
desactivar Windows Defender o agregar excepciones. No hace falta y no
corresponde. El 'lab03.bat' está escrito justamente para que no haya nada que
señalar: no descarga nada, no tiene código empaquetado, no usa llamadas
reflejadas y nunca baja un archivo y lo ejecuta en el mismo paso. Solo mira si
las piezas están y, si están, corre las que ya están escritas.

## Reproducir el entorno desde cero

```powershell
.\bootstrap.ps1
```

Recrea la venv e instala las versiones pineadas de 'requirements.txt'. Es
idempotente, se puede correr las veces que haga falta porque antes de cada paso
pregunta si la cosa ya está hecha.

## Ejecutar el pipeline completo

Los seis comandos de la sección anterior hacen todo el trabajo, y se corren en
orden desde la raíz del repositorio. Todavía no hay un 'run_all.ps1' que los
junte en uno solo, así que hoy se ejecutan uno por uno.

## Verificar reproducibilidad

La comprobación es correr el pipeline dos veces y comparar los hashes de las
salidas. El comando que se usa para eso es este, que calcula el SHA-256 de cada
Parquet que dejó el último paso.

```powershell
Get-ChildItem "$repo\datos\gold" -Recurse -File | Get-FileHash -Algorithm SHA256 |
    ForEach-Object { "$($_.Hash)  $($_.Path)" }
```

Si las dos corridas dan los mismos hashes, el pipeline es reproducible. La
semilla fija es lo que garantiza que el arranque sea idéntico, y el resto
depende de no meter ningún valor que dependa de la fecha de hoy.

---

## Documentos

| Documento | Rúbrica |
| --------- | ------- |
| 'docs/informe_tecnico.md' | Documentación (15 pts) |
| 'docs/tabla_comparativa.md' | Análisis comparativo (10 pts) |
| 'docs/veredicto_arquitecturas.md' | Decisión del equipo |
| 'docs/guion_presentacion.md' | Presentación y tiempo (20 pts) |
| 'evidencias/bitacora.md' | Evidencia del proceso |
| 'evidencias/entorno/entorno.txt' | Huella del entorno, la genera '00_capturar_entorno.ps1' |
| 'evidencias/explain/plan_consulta_pesada.txt' | Plan de ejecución de Spark |
| 'evidencias/explain/tamano_zonas.txt' | Tamaño de cada zona del lago |

## Problemas que encontramos y cómo se resolvieron

Esto se documenta a propósito, porque es lo que vale el criterio de resolución
de problemas.

| Problema | Causa real | Solución |
| -------- | ---------- | -------- |
| 'ModuleNotFoundError: No module named 'pyspark'' | el pip del PATH es el de Python 3.13 y 'python' es el 3.12 | usar siempre la venv y la forma '-m pip' |
| 'Unsupported class file major version' | 'JAVA_HOME' vacío, así que Spark tomaba el Java 20 del PATH | apuntar 'JAVA_HOME' al Temurin 17 |
| 'HADOOP_HOME and hadoop.home.dir are unset' al escribir Parquet | falta el binario de permisos de Windows | instalar winutils |
| 'UnsatisfiedLinkError' en 'NativeIO$Windows.access0' | Hadoop busca 'winutils.dll' y solo existe 'hadoop.dll' | copiar el DLL con el otro nombre |
| 'Hadoop home directory C:BigDataLab03winutils is not an absolute path' | la JVM se come los contrabarras de las propiedades | pasar la dirección con barras normales |
| 'ModuleNotFoundError: No module named 'distutils'' en 'toPandas()' | Python 3.12 quitó 'distutils' de la estándar | instalar 'setuptools' |
| 'PySpark does not yet fully support pandas >= 3.0.0' | pip resolvió pandas 3 sin consultarlo | pinear 'pandas==2.3.3' |
| 'TABLE_OR_VIEW_NOT_FOUND: lecturas' al pedir el EXPLAIN | la consulta pesada da la vista por supuesta porque la crea el paso 4 | registrar la vista antes de explicar |
