# Detiene el broker de Kafka de forma ordenada y guarda la evidencia del cierre.
# Se manda la orden de parada en vez de matar el proceso a la fuerza, porque Kafka
# escribe los datos en disco y si lo matamos de golpe puede dejar archivos a medio escribir.
# Killing no es lo mismo que cerrar, y con un solo topic de prueba el riesgo es bajo, pero
# el orden se respeta igual porque es lo correcto y ademas lo que se explica en la defensa.
# Es el complemento de start_kafka.ps1 y se puede correr las veces que haga falta.

$ErrorActionPreference = "Continue"

$repo = Split-Path -Parent $MyInvocation.MyCommand.Path
$bat = Join-Path $repo "kafka\bin\windows"
$log = Join-Path $repo "evidencias\logs\04_kafka_cierre.log"
$puerto = 9092

$javaHome = [Environment]::GetEnvironmentVariable('JAVA_HOME', 'User')
if ($javaHome) {
    $env:JAVA_HOME = $javaHome
    $env:Path = "$javaHome\bin;$env:Path"
}

$escuchando = Test-NetConnection -ComputerName localhost -Port $puerto -InformationLevel Quiet -WarningAction SilentlyContinue
if (-not $escuchando) {
    Write-Host "El broker no esta corriendo, no hay nada que detener"
    exit 0
}

Write-Host "Deteniendo el broker de Kafka"
cmd /c "`"$bat\kafka-server-stop.bat`" 2>nul" | Out-Null

# Se espera a que el puerto se libere, con un tope, porque la orden de parada es una
# peticion y el broker puede tardar un par de segundos en irse
$esperado = 0
for ($i = 0; $i -lt 40; $i++) {
    Start-Sleep -Milliseconds 500
    $escuchando = Test-NetConnection -ComputerName localhost -Port $puerto -InformationLevel Quiet -WarningAction SilentlyContinue
    if (-not $escuchando) {
        $esperado = 1
        break
    }
}

# El estado final del puerto queda anotado en un archivo aparte, porque sirve de evidencia
# de que el broker quedo realmente abajo y no solo que mandamos la orden
$lineas = @()
$lineas += "cierre $(Get-Date).ToUniversalTime()"
$lineas += "puerto $puerto responds  $escuchando"
$lineas += "detenido correctamente  $($esperado -eq 1)"
if (Test-Path (Join-Path $repo "evidencias\logs\03_kafka_broker.log")) {
    $lineas += ""
    $lineas += "Ultimas lineas del broker antes de cerrar"
    $lineas += (Get-Content (Join-Path $repo "evidencias\logs\03_kafka_broker.log") -Tail 12)
}
$utf8SinBom = New-Object System.Text.UTF8Encoding($false)
[System.IO.File]::WriteAllLines($log, $lineas, $utf8SinBom)

if ($esperado -eq 1) {
    Write-Host "El broker se detuvo, el puerto $puerto quedo libre"
    Write-Host "  evidencia en $log"
    exit 0
}

Write-Host "El broker no se detuvo del todo, el puerto $puerto sigue ocupado"
Write-Host "  evidencia en $log"
exit 1
