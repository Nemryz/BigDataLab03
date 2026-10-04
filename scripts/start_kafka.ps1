# Arranca el broker de Kafka y espera a que este listo.

# El broker se levanta en segundo plano y su salida completa se guarda en un log, porque esa salida es la evidencia de que arranco bien, y porque es la unica forma de saber que esta vivo sin adivinar o hacer algo de magia negra. 

# El script no adivina, espera a leer la linea exacta que dice que el servidor termino de arrancar, y si no aparece en el tiempo dado se declara fallido.

# Se puede correr las veces que haga falta, si el broker ya esta vivo lo dice y no levanta un segundo, porque dos brokers en el mismo puerto no conviven.

$ErrorActionPreference = "Stop"

$repo = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
$bat = Join-Path $repo "kafka\bin\windows"
$log = Join-Path $repo "evidencias\logs\03_kafka_broker.log"
$puerto = 9092

# Kafka necesita un Java y no lo busca solo, asi que se lo pasamos desde donde el proyecto ya lo tiene resuelto, que es la variable que escribimos al instalar el JDK. Esto se ve si se presiona el archivo bootstrap.ps1, que es el que instala el JDK y fija la variable. Si no esta definida, el script no puede arrancar el broker y se corta con un mensaje claro.
$javaHome = [Environment]::GetEnvironmentVariable('JAVA_HOME', 'User')
if (-not $javaHome) {
    Write-Host "No esta definido JAVA_HOME, corre scripts/bootstrap.ps1 primero"
    exit 1
}
$env:JAVA_HOME = $javaHome
$env:Path = "$javaHome\bin;$env:Path"

# Fijamos la memoria del broker antes de arrancarlo, y no es solo por querer menos memoria.

# El script de arranque de Kafka usa wmic para preguntar si el sistema es de 32 o de 64 bits, y wmic ya no viene en las versiones nuevas de Windows 11.

# Cuando falta, el script escribe un error y el broker no levanta.

# Fijando la variable nosotros, el bloque que llama a wmic ni se ejecuta, porque la condición de que esté vacía da falso.

# De paso controlamos la memoria, que es lo que nos interesa 
$env:KAFKA_HEAP_OPTS = "-Xmx256M -Xms128M"

# Si el puerto ya responde, el broker de una corrida anterior sigue vivo
$escuchando = Test-NetConnection -ComputerName localhost -Port $puerto -InformationLevel Quiet -WarningAction SilentlyContinue
if ($escuchando) {
    Write-Host "El broker ya esta escuchando en el puerto $puerto, no se levanta otro"
    exit 0
}

if (-not (Test-Path (Join-Path $repo "kafka\kraft-logs\meta.properties"))) {
    # El almacenamiento viene sin formatear cada vez que se reinstala Kafka, y sin eso el broker no arranca.

    # Se formatea acá en vez de dejar un paso manual suelto, porque el README le dice al lector que esto lo hace este script, y una instrucción que no coincide con lo que hace el código es el tipo de cosa que cuesta encontrar el día de la defensa.

    # El storage es un directorio de datos y no de configuración, así que borrarlo no pierde nada del proyecto.
    $env:KAFKA_HEAP_OPTS = "-Xmx256M -Xms128M"
    $config = Join-Path $repo "kafka\config\server.properties"

    Write-Host "Formateando el almacenamiento de Kafka"

    # El identificador del cluster se genera en el momento, porque no es un valor que se pueda inventar ni recordar, es un identificador que Kafka pide que tenga forma de uuid.

    # Este comando imprime primero un aviso de su propio log4j que no significa un error, así que se toma la última línea de salida y no la primera.
    $uuid = (cmd /c "`"$($repo)\kafka\bin\windows\kafka-storage.bat`" random-uuid 2>nul" |
        Where-Object { $_ -and $_.Trim() } | Select-Object -Last 1)
    $uuid = "$uuid".Trim()
    if ($uuid.Length -lt 20) {
        Write-Host "  no se pudo generar el identificador del cluster, se obtuvo '$uuid'"
        exit 1
    }

    # El flag --standalone es obligatorio en Kafka 4.x cuando el nodo es broker y controlador a la vez y no hay una lista de votantes declarada, que es justo nuestra configuración.

    # Sin él el formateo se corta pidiendo uno de esos tres flags y no escribe nada, y encima devuelve el código cero, así que un script que solo mirara el código diría que salió bien.
    $salida = cmd /c "`"$($repo)\kafka\bin\windows\kafka-storage.bat`" format -t $uuid -c `"$config`" --standalone 2>&1"
    $salida | ForEach-Object { Write-Host "  $_" }

    if (-not (Test-Path (Join-Path $repo "kafka\kraft-logs\meta.properties"))) {
        Write-Host "  el formateo no dejo meta.properties, revisa el mensaje de arriba"
        exit 1
    }
    Write-Host "  storage formateado con el cluster $uuid"
}

New-Item -ItemType Directory -Force -Path (Split-Path $log -Parent) | Out-Null
if (Test-Path $log) { Remove-Item $log -Force }
if (Test-Path "$log.err") { Remove-Item "$log.err" -Force }

Write-Host "Arrancando el broker de Kafka"
Write-Host "  java      $javaHome"
Write-Host "  memoria   $env:KAFKA_HEAP_OPTS"
Write-Host "  broker    localhost`:$puerto"
Write-Host "  log       $log"

$proceso = Start-Process -FilePath (Join-Path $bat "kafka-server-start.bat") `
    -ArgumentList (Join-Path $repo "kafka\config\server.properties") `
    -WorkingDirectory $bat `
    -RedirectStandardOutput $log `
    -RedirectStandardError "$log.err" `
    -PassThru -WindowStyle Hidden

Write-Host "  proceso   $($proceso.Id)"
Write-Host "  esperando a que el servidor levante"

# Se lee el log cada medio segundo hasta que aparezca la linea de arranque o se agote el tiempo.

# El limite esta porque un proceso colgado no devuelve el control, y un script bloqueado en un laboratorio es peor que uno que falla con un mensaje claro.
$limite = 60
$esperado = 0
for ($i = 0; $i -lt $limite; $i++) {
    Start-Sleep -Milliseconds 500
    if (Test-Path $log) {
        $contenido = Get-Content $log -Raw -ErrorAction SilentlyContinue
        if ($contenido -match "KafkaRaftServer nodeId=1.*started|started \(kafka.server.KafkaRaftServer\)") {
            $esperado = 1
            break
        }
    }
    if ($proceso.HasExited) {
        Write-Host "  el proceso termino antes de tiempo, codigo $($proceso.ExitCode)"
        Write-Host " Ultimas lineas del log"
        Get-Content $log -Tail 10 -ErrorAction SilentlyContinue | ForEach-Object { Write-Host "    $_" }
        exit 1
    }
}

if ($esperado -ne 1) {
    Write-Host "  no arranco en $limite segundos, revisando el log"
    Get-Content $log -Tail 20 -ErrorAction SilentlyContinue | ForEach-Object { Write-Host "    $_" }
    exit 1
}

Write-Host ""
Write-Host "El broker arranco bien"
$conexion = Test-NetConnection -ComputerName localhost -Port $puerto -InformationLevel Quiet -WarningAction SilentlyContinue
Write-Host "  puerto $puerto responde  $conexion"
