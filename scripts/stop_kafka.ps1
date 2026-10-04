# Detiene el broker de Kafka de forma ordenada y guarda la evidencia del cierre.

# El proceso se corta con la misma orden que usa el script de parada que trae Kafka en Windows, que es dar de baja el proceso cuyo nombre es kafka.Kafka.

# Kafka no ofrece una parada suave desde fuera en Windows, y su script oficial se apoya en wmic, que ya no viene en las versiones nuevas de Windows 11, así que ahí la orden no llega a ningún lado y encima devuelve el código cero como si hubiera salido bien.

# Se hace directamente desde PowerShell para que la orden sí llegue, y el riesgo de dejar archivos a medio escribir es bajo con un solo topic de prueba.

# Es el complemento de start_kafka.ps1 y se puede correr las veces que haga falta.

$ErrorActionPreference = "Continue"

$repo = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
$log = Join-Path $repo "evidencias\logs\04_kafka_cierre.log"
$puerto = 9092

$escuchando = Test-NetConnection -ComputerName localhost -Port $puerto -InformationLevel Quiet -WarningAction SilentlyContinue
if (-not $escuchando) {
    Write-Host "El broker no esta corriendo, no hay nada que detener"
    exit 0
}

Write-Host "Deteniendo el broker de Kafka"
$procesos = Get-CimInstance Win32_Process -Filter "Name = 'java.exe'" | Where-Object { $_.CommandLine -like "*kafka.Kafka*" }
$procesos | ForEach-Object { Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue }

# Se espera a que el puerto se libere, con un tope, porque aunque el proceso baja de inmediato el sistema tarda un instante en soltar la conexión
$esperado = 0
for ($i = 0; $i -lt 40; $i++) {
    Start-Sleep -Milliseconds 500
    $escuchando = Test-NetConnection -ComputerName localhost -Port $puerto -InformationLevel Quiet -WarningAction SilentlyContinue
    if (-not $escuchando) {
        $esperado = 1
        break
    }
}

# El estado final del puerto queda anotado en un archivo aparte, porque sirve de evidencia de que el broker quedo realmente abajo y no solo que mandamos la orden
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
