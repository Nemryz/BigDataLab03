# BigDataLab03

Este repositorio es dónde se ubicará el Laboratorio N°3 para la asignatura de Big Data.

Implementación de las tres arquitecturas de referencia (Lambda, Kappa y Data
Lakehouse) sobre un entorno local en Windows, con pipeline simulado, evidencia
reproducible y análisis comparativo.

## Arquitectura del repositorio

Todo vive dentro de una sola carpeta, la del propio repositorio, y esa decisión explica casi todo lo demás. La raíz está en una ruta corta y sin espacios porque Spark en Windows se rompe cuando la ruta tiene espacios, y porque el sistema tiene un tope de 260 carácteres que se agota apenas metemos carpetas profundas. La carpeta pesada queda ignorada por git, porque pesa mucho y no le interesa a nadie el historial de un gigabyte de librerías.

```
C:\BigDataLab03\
  .venv\                       Python 3.12 con PySpark 3.5.9
  winutils\                    binarios de Hadoop que Windows necesita para escribir
  datos\                       zonas raw, bronze, silver y gold
  checkpoints\                 estado del Structured Streaming
  .ivy2\                       memoria del conector Spark con Kafka
  kafka\                       Apache Kafka 4.1.2
  src\comun\                   módulos compartidos por las tres ramas
  data-lakehouse\              rama Data Lakehouse
  lambda\                      rama Lambda
  kappa\                       rama Kappa
  evidencias\                  logs, pantallazos, huella del entorno
  docs\                        informe, tabla comparativa, veredicto
```

Las carpetas de cada rama llevan un número al principio, y ese número es el orden en que se van a construir. Mirar el árbol ya dice en qué vamos.

| Carpeta | Ramas | Contenido |
| --------- | ------- | ----------- |
| `01-generacion` | lakehouse | el script que fabrica el dataset con semilla fija |
| `01-ingesta` | lakehouse, lambda, kappa | el generador o el productor que alimenta el sistema |
| `02-ingesta` | lakehouse | la lectura de la zona cruda |
| `02-velocidad` | lambda | la capa de baja latencia |
| `03-lotes` | lambda | la capa por lotes |
| `03-transformacion` | lakehouse | limpieza y Parquet particionado |
| `04-consultas` | lakehouse | SQL analítico |
| `05-visualizacion` | lakehouse | gráficos con matplotlib |
| `04-servicio` | lambda | la capa SQLite que une las dos vías |
| `02-procesamiento` | kappa | ventanas de tumbling, sliding y sesión |
| `03-reprocesamiento` | kappa | replay del historial con lógica cambiada |
| `salidas\` | todas | resultados de la rama |

## Requisitos

| Componente | Versión | Por qué esa |
| Python | 3.12.10 | venv propia dentro del repo |
| Temurin JDK | 17.0.20.1 | Spark 3.5 exige Java 8, 11 o 17 |
| PySpark | 3.5.9 | empaqueta Hadoop 3.3.4, que tiene winutils publicado |
| pandas | 2.3.3 | PySpark 4.x no soporta del todo pandas 3 en adelante |
| winutils | 3.3.5 | el binario nativo que Windows necesita para escribir |
| Apache Kafka | 4.1.2 | solo para Lambda y Kappa, en modo KRaft |

> Ojo con los Pythons. En mi pc hay un 3.12 y un 3.13 instalados, y el pip del PATH pertenece al 3.13 mientras que 'python' es el 3.12. Por eso todos los comandos de este documento usan la ruta absoluta a la venv y la forma '-m pip'. Si usás 'pip' suelto, instalás en el intérprete equivocado y después el import falla sin explicación.

# Instalación paso a paso

Cada bloque se ejecuta en PowerShell. Abre una ventana nueva al final, porque la variable 'JAVA_HOME' recién se lee en las sesiones que se abren después de configurarla.

## Paso 1: Java 17

Spark 3.5 corre con Java 8, 11 o 17, y nada más. Como dije antes, mi pc traía Java 18 y Java 20, que no sirven para el funcionamiento de todo el laboratorio, entonces tocará instalarlo.

```powershell
winget install EclipseAdoptium.Temurin.17.JDK --accept-source-agreements --accept-package-agreements
```

Apuntamos 'JAVA_HOME' al JDK nuevo y lo ponemos delante en el PATH, porque el 'java' del
PATH apuntaba al shim de Oracle con Java 20 y Spark usa la variable primero.

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

Tiene que decir '17.0.x'. Si dice 18 o 20, la variable no tomó y hay que repetir el paso.

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

El primero deja el PySpark. El segundo es el que importa, con pandas y numpy en las versiones que calzan con PySpark 3.5.9 y con matplotlib, porque si se instalan sueltos pip resuelve versiones que se pisan entre ellas.

Y el tercero es el que más confunde, 'setuptools' parece innecesario pero no lo es, ya que Python 3.12 quitó 'distutils' de la biblioteca estándar y PySpark todavía lo usa, así que sin este paquete la función 'toPandas()' revienta con un error de módulo inexistente.

Para verificar todo:

```powershell
& $py -c "import pyspark, pandas, numpy, pyarrow, setuptools; print(pyspark.__version__, pandas.__version__, numpy.__version__, pyarrow.__version__)"
& $py -m pip check
```

Lo primero tiene que imprimir '3.5.9 2.3.3 2.5.3 25.0.1' y lo segundo tiene que decir
'No broken requirements found'. Si 'pip check' se queja de numpy, es que se coló una
versión vieja y hay que reinstalar el segundo paso.

## Paso 3: winutils

Este es el que más tiempo costó y el que más conviene explicar, porque su error miente demasiado, en serio... demasiado. "Leer Parquet y contar filas funciona sin winutils. Escribir no" Y cuando falla, el mensaje dice que falta 'HADOOP_HOME', como si la ruta estuviera mal, cuando en realidad lo que falta es un archivo.

La razón es que al escribir un archivo Spark le pone permisos, y en Windows esa operación la hace un binario de Hadoop que no viene con nada. Lo descargamos y lo dejamos en su carpeta.

```powershell
$bin = "$repo\winutils\bin"
New-Item -ItemType Directory -Force -Path $bin | Out-Null
$base = "https://github.com/cdarlint/winutils/raw/master/hadoop-3.3.5/bin"

curl.exe -sL -o "$bin\winutils.exe" "$base/winutils.exe"
curl.exe -sL -o "$bin\hadoop.dll"  "$base/hadoop.dll"
Copy-Item "$bin\hadoop.dll" "$bin\winutils.dll" -Force
```

Las tres líneas importan demasiado dado que los dos archivos que se descargan son el ejecutable y la biblioteca. El 'Copy-Item' es el que casi nadie encuentra, y hace falta porque Hadoop busca una biblioteca llamada 'winutils' mientras que lo que se descarga se llama 'hadoop.dll', así que sin la copia todas las lecturas funcionan y ninguna escritura.

Para verificar la copia también la puede hacer el propio código

```powershell
& $py -c "import sys; sys.path.insert(0, '$repo\src\comun'); import config; config.preparar_winutils(); print('winutils.dll ->', (config.HADOOP_HOME / 'bin' / 'winutils.dll').exists())"
```

## Paso 4: Apache Kafka

Kafka solo hace falta para las ramas Lambda y Kappa, y no lo necesita el Lakehouse.

```powershell
curl.exe -L -o "C:\BigDataLab03\kafka.tgz" "https://archive.apache.org/dist/kafka/4.1.2/kafka_2.13-4.1.2.tgz"
tar -xzf "C:\BigDataLab03\kafka.tgz" -C "C:\BigDataLab03"
Rename-Item "C:\BigDataLab03\kafka_2.13-4.1.2" "kafka"
```

Kafka 4.x trabaja en modo KRaft y ya no usa ZooKeeper, así que cualquier tutorial anterior que mencione 'zookeeper-server-start' está obsoleto. Después se formatea el almacenamiento y se arranca, y eso lo hace `start_kafka.ps1`.

Para que funcione con un tipo de verificación tiene que responder el puerto 9092

```powershell
Test-NetConnection -ComputerName localhost -Port 9092
```

## Paso 5: Smoke test

Prueba de humo, o sea que verifica que las cuatro piezas anteriores se puedan usar juntas
antes de construir nada encima.

```powershell
& $py "$repo\src\comun\smoke_test.py" 2>&1 | Tee-Object "$repo\evidencias\logs\00_smoke_test.log"
```

Tiene que terminar con 'SMOKE TEST: OK'.

## Reproducir el entorno desde cero

```powershell
.\bootstrap.ps1
```

Recrea la venv e instala las versiones pineadas de `requisitos.txt`.

## Ejecutar el pipeline completo

```powershell
.\run_all.ps1
```

## Verificar reproducibilidad

```powershell
.\verificar_reproducibilidad.ps1
```

Ejecuta el pipeline dos veces y compara los hashes SHA-256 de las salidas.

---

## Documentos

| Documento | Rúbrica |
| 'docs/informe_tecnico.md' | Documentación (15 pts) |
| 'docs/tabla_comparativa.md' | Análisis comparativo (10 pts) |
| 'docs/veredicto_arquitecturas.md' | Decisión del equipo |
| 'docs/guion_presentacion.md' | Presentación y tiempo (20 pts) |
| 'evidencias/bitacora.md' | Evidencia del proceso |

## Problemas que encontramos y cómo se resolvieron

Esto se documenta a propósito, porque es lo que vale el criterio de resolución de problemas.

| Síntoma | Causa real | Solución |
| --------- | ----------- | ---------- |
| 'ModuleNotFoundError: No module named 'pyspark'' | el pip del PATH es el de Python 3.13 y 'python' es el 3.12 | usar siempre la venv y la forma '-m pip' |
| 'Unsupported class file major version' | 'JAVA_HOME' vacío, así que Spark tomaba el Java 20 del PATH | apuntar 'JAVA_HOME' al Temurin 17 |
| 'HADOOP_HOME and hadoop.home.dir are unset' al escribir Parquet | falta el binario de permisos de Windows | instalar winutils |
| 'UnsatisfiedLinkError' en 'NativeIO$Windows.access0' | Hadoop busca 'winutils.dll' y solo existe 'hadoop.dll' | copiar el DLL con el otro nombre |
| 'Hadoop home directory C:BigDataLab03winutils is not an absolute path' | la JVM se come los contrabarras de las propiedades | pasar la dirección con barras normales |
| 'ModuleNotFoundError: No module named 'distutils'' en 'toPandas()' | Python 3.12 quitó 'distutils' de la estándar | instalar 'setuptools' |
| 'PySpark does not yet fully support pandas >= 3.0.0' | pip resolvió pandas 3 sin consultarlo | pinear 'pandas==2.3.3' |
