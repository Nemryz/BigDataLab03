# Captura la huella del entorno del proyecto.
# Se ejecuta desde la raiz del repositorio y guarda todo en evidencias\entorno.
# Sirve para dos cosas, para el informe tecnico y para demostrar que el resultado depende
# de versiones concretas y no de la suerte.
# El archivo se escribe sin BOM a proposito, porque con BOM el pip install -r falla y
# no sabemos por que, ya que el error que sale no menciona nada de codificacion.
# Lo mismo con la version de Java, que se captura pasando por cmd porque en PowerShell
# 5.1 esa orden escribe en la salida de errores y eso rompe el script si esta estricto.

$ErrorActionPreference = "Stop"

$repo = Split-Path -Parent $MyInvocation.MyCommand.Path
$destino = Join-Path $repo "evidencias\entorno"
$py = Join-Path $repo ".venv\Scripts\python.exe"
$marca = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH-mm-ssZ")

New-Item -ItemType Directory -Force -Path $destino | Out-Null

Write-Host "Capturando el entorno en $destino"

$lineas = @()
$lineas += "Proyecto        BigDataLab03"
$lineas += "Capturado UTC   $marca"
$lineas += "Repositorio     $repo"
$lineas += ""
$lineas += "--- Git ---"
$lineas += "commit  $(& git -C $repo rev-parse --short HEAD 2>&1)"
$lineas += "rama    $(& git -C $repo rev-parse --abbrev-ref HEAD 2>&1)"
$lineas += "archivos con cambios  $(& git -C $repo status --porcelain 2>&1 | Measure-Object | ForEach-Object { $_.Count })"
$lineas += ""
$lineas += "--- Java ---"
$javaHome = [Environment]::GetEnvironmentVariable('JAVA_HOME', 'User')
$lineas += "JAVA_HOME  $javaHome"
if ($javaHome -and (Test-Path (Join-Path $javaHome 'bin\java.exe'))) {
    $lineas += "version usada por el proyecto"
    $lineas += (& cmd /c "`"$javaHome\bin\java.exe`" -version 2>&1")
} else {
    $lineas += "version usada por el proyecto  NO DISPONIBLE"
}
$lineas += "java del PATH del sistema"
$lineas += (& cmd /c 'java -version 2>&1')
$lineas += ""
$lineas += "--- Python ---"
$lineas += "interprete  $py"
$lineas += (& $py -V 2>&1)
$lineas += ""
$lineas += "--- Librerias ---"
$lineas += (& $py -c "import pyspark, pandas, numpy, pyarrow, py4j, setuptools, matplotlib; print('pyspark     ', pyspark.__version__); print('pandas      ', pandas.__version__); print('numpy       ', numpy.__version__); print('pyarrow     ', pyarrow.__version__); print('py4j        ', py4j.__version__); print('setuptools  ', setuptools.__version__); print('matplotlib  ', matplotlib.__version__)" 2>&1)
$lineas += ""
$lineas += "--- Winutils ---"
foreach ($archivo in @("winutils.exe", "hadoop.dll", "winutils.dll")) {
    $ruta = Join-Path $repo "winutils\bin\$archivo"
    if (Test-Path $ruta) {
        $lineas += "$archivo  $([math]::Round((Get-Item $ruta).Length / 1KB, 0)) KB"
    } else {
        $lineas += "$archivo  FALTA"
    }
}
$lineas += ""
$lineas += "--- Kafka ---"
if (Test-Path (Join-Path $repo "kafka")) {
    $lineas += "instalado en la carpeta del proyecto"
} else {
    $lineas += "todavia no instalado, solo hace falta para Lambda y Kappa"
}
$lineas += ""
$lineas += "--- Maquina ---"
$lineas += "sistema  $((Get-CimInstance Win32_OperatingSystem).Caption)"
$lineas += "memoria  $([math]::Round((Get-CimInstance Win32_ComputerSystem).TotalPhysicalMemory / 1GB, 1)) GB"
$lineas += "procesadores  $((Get-CimInstance Win32_Processor).NumberOfLogicalProcessors) logicos"
$lineas += "disco libre  $([math]::Round((Get-PSDrive C).Free / 1GB, 1)) GB"
$lineas += ""
$lineas += "--- Paquetes instalados ---"
$lineas += (& $py -m pip freeze 2>&1)

# Escribimos sin BOM, que es el detalle que hace que este archivo funcione
$utf8SinBom = New-Object System.Text.UTF8Encoding($false)
[System.IO.File]::WriteAllLines((Join-Path $destino "entorno.txt"), $lineas, $utf8SinBom)

# El archivo de requisitos se refresca con lo que hay ahora instalado
[System.IO.File]::WriteAllLines((Join-Path $repo "requirements.txt"), (& $py -m pip freeze 2>&1), $utf8SinBom)

# Y una copia sellada con la fecha, para tener varias capturas si el entorno cambia
[System.IO.File]::WriteAllLines((Join-Path $destino "entorno-$marca.txt"), $lineas, $utf8SinBom)

Write-Host "Listo"
Write-Host "  evidencias\entorno\entorno.txt"
Write-Host "  evidencias\entorno\entorno-$marca.txt"
Write-Host "  requirements.txt"
