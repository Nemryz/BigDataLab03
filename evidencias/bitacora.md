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

## Errores típicos y qué hacer

Si el productor dice que el broker no está corriendo, se enciende con start_kafka.ps1 y se vuelve a probar.

Si la limpieza falla con acceso denegado sobre la carpeta de datos, es porque Kafka dejó archivos de checkpoint en solo lectura y Windows no deja borrarlos mientras el atributo siga puesto.

Si el arranque del productor se queda colgado sin imprimir nada, era el proceso largo heredando el manijón de la salida, y eso ya está resuelto con la lectura en hilo aparte con tiempo tope.

Si el conector de Kafka no encuentra las clases, es porque se pidió con sufijo 2.13 y Spark trae 2.12.

Si el arranque dice que no encuentra la configuración de un bloque, es el aviso inofensivo de la reconfiguración dinámica y no impide que el almacenamiento se formatee.

Si PySpark no aparece en el import, el pip que corrió pertenecía al otro intérprete.

Si la escritura de Parquet falla con un error que parece una ruta mal escrita, en realidad faltan permisos y hay que revisar winutils.

Si algo falla y no se sabe dónde, se empieza por la prueba de humo, porque ordena las comprobaciones de menor a mayor dificultad y marca en qué punto se rompió.
