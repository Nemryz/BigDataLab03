# Deja el entorno listo desde cero, para reproducir el laboratorio en otra maquina.
# Los cinco pasos son los mismos que estan en el README, solo que aqui estan en automatico.
# El script se puede correr las veces que haga falta porque antes de cada paso pregunta si
# la cosa ya esta hecha, y si lo esta la salta. Eso importa porque el README es para leer
# y este es para ejecutar, y si uno corre los dos sin querer no deberia romper nada.
# Al final se corre el smoke test, que es el que dice si quedo bien o si algo falta.

$ErrorActionPreference = "Stop"

$repo = Split-Path -Parent $MyInvocation.MyCommand.Path
$py = Join-Path $repo ".venv\Scripts\python.exe"
$log = Join-Path $repo "evidencias\logs\01_bootstrap.log"
New-Item -ItemType Directory -Force -Path (Split-Path $log -Parent) | Out-Null

$salida = @()
function escribir($texto) {
    Write-Host $texto
    $script:salida += $texto
}

escribir "Bootstrap de BigDataLab03"
escribir "Repositorio  $repo"
escribir ""

# Paso 1, Java 17
escribir "Paso 1 de 5, Java 17"
$javaHome = [Environment]::GetEnvironmentVariable('JAVA_HOME', 'User')
$temurin = @(Get-ChildItem "C:\Program Files\Eclipse Adoptium" -Directory -Filter "jdk-17*" -ErrorAction SilentlyContinue)
if ($javaHome -and $temurin.Count -gt 0) {
    escribir "  ya estaba instalado en $javaHome"
} else {
    escribir "  instalando con winget, puede tardar un rato"
    & winget install EclipseAdoptium.Temurin.17.JDK --accept-source-agreements --accept-package-agreements | Out-Null
    $temurin = @(Get-ChildItem "C:\Program Files\Eclipse Adoptium" -Directory -Filter "jdk-17*" -ErrorAction SilentlyContinue)
    if ($temurin.Count -eq 0) {
        escribir "  ERROR, no se encontro el JDK despues de instalar"
        exit 1
    }
    [Environment]::SetEnvironmentVariable("JAVA_HOME", $temurin[0].FullName, "User")
    $rutaUsuario = [Environment]::GetEnvironmentVariable('Path', 'User')
    if ($rutaUsuario -notlike "*JAVA_HOME*") {
        [Environment]::SetEnvironmentVariable('Path', "%JAVA_HOME%\bin;" + $rutaUsuario, 'User')
    }
    escribir "  instalado y JAVA_HOME apuntando a $($temurin[0].FullName)"
}
escribir "  OJO, esto no aplica a la consola que lo esta corriendo, abre una nueva al final"
escribir ""

# Paso 2, la venv
escribir "Paso 2 de 5, la venv"
if (Test-Path $py) {
    escribir "  ya existia en $py"
} else {
    escribir "  creando con Python 3.12"
    $python312 = Get-ChildItem "C:\Users\*\AppData\Local\Programs\Python\Python312\python.exe" -ErrorAction SilentlyContinue | Select-Object -First 1
    if (-not $python312) {
        escribir "  ERROR, no se encontro Python 3.12 en este equipo"
        exit 1
    }
    & $python312.FullName -m venv (Join-Path $repo ".venv")
    escribir "  creada"
}
escribir ""

# Paso 3, PySpark con las versiones pineadas
escribir "Paso 3 de 5, PySpark y librerias"
# La version se lee pasando por cmd con la salida de errores descartada, porque si PySpark
# todavia no esta el import falla y en PowerShell ese fallo corta el script entero
# Todas las llamadas a programas de afuera van pasando por cmd con la salida de errores
# descartada. Es la unica forma que funciona en PowerShell 5.1, porque con
# ErrorActionPreference en Stop, el 2>&1 de PowerShell convierte la salida de errores del
# programa en registros de error antes de llegar a la tuberia, y ahi corta el script.
# Con pip pasa siempre, porque escribe avisos inocuos ahi, como el de que hay version nueva.
$versionActual = (cmd /c "`"$py`" -c `"import pyspark; print(pyspark.__version__)`" 2>nul")
if ("$versionActual".Trim() -eq "3.5.9") {
    escribir "  PySpark 3.5.9 ya estaba"
} else {
    $requisitos = Join-Path $repo "requisitos.txt"
    if (Test-Path $requisitos) {
        # Se instala desde el archivo de requisitos y no desde la lista de base, porque
        # matplotlib no fija la version de contourpy y pip puede resolvarla distinta en
        # cada equipo. Con el archivo pineado las dos instalaciones quedan iguales
        escribir "  instalando desde requisitos.txt, con las versiones exactas"
        cmd /c "`"$py`" -m pip install -r `"$requisitos`" 2>nul" | Out-Null
    } else {
        escribir "  no hay requisitos.txt, se instalan las versiones base, 400 MB la primera vez"
        cmd /c "`"$py`" -m pip install --upgrade pip 2>nul" | Out-Null
        cmd /c "`"$py`" -m pip install `"pyspark[sql]==3.5.9`" matplotlib 2>nul" | Out-Null
        cmd /c "`"$py`" -m pip install `"pandas==2.3.3`" `"numpy==2.5.3`" setuptools 2>nul" | Out-Null
    }
    if ($LASTEXITCODE -ne 0) {
        escribir "  ERROR, pip devolvio el codigo $LASTEXITCODE"
        exit 1
    }
    escribir "  instalado"
}
escribir ""

# Paso 4, winutils
escribir "Paso 4 de 5, winutils"
$bin = Join-Path $repo "winutils\bin"
if (Test-Path (Join-Path $bin "winutils.dll")) {
    escribir "  ya estaba en $bin"
} else {
    New-Item -ItemType Directory -Force -Path $bin | Out-Null
    $base = "https://github.com/cdarlint/winutils/raw/master/hadoop-3.3.5/bin"
    escribir "  descargando"
    cmd /c "curl.exe -sL -o `"$bin\winutils.exe`" `"$base/winutils.exe`" 2>nul" | Out-Null
    cmd /c "curl.exe -sL -o `"$bin\hadoop.dll`" `"$base/hadoop.dll`" 2>nul" | Out-Null
    Copy-Item (Join-Path $bin "hadoop.dll") (Join-Path $bin "winutils.dll") -Force
    if (-not (Test-Path (Join-Path $bin "winutils.dll"))) {
        escribir "  ERROR, la descarga de winutils fallo"
        exit 1
    }
    escribir "  descargados, y la copia con el otro nombre tambien"
}
escribir ""

# Paso 5, Kafka
escribir "Paso 5 de 5, Apache Kafka, solo si falta"
if (Test-Path (Join-Path $repo "kafka")) {
    escribir "  ya estaba instalado"
} else {
    escribir "  descargando, son 127 MB"
    cmd /c "curl.exe -sL -o `"$repo\kafka.tgz`" https://archive.apache.org/dist/kafka/4.1.2/kafka_2.13-4.1.2.tgz 2>nul" | Out-Null
    cmd /c "tar -xzf `"$repo\kafka.tgz`" -C `"$repo`" 2>nul" | Out-Null
    if (-not (Test-Path (Join-Path $repo "kafka_2.13-4.1.2"))) {
        escribir "  ERROR, la descarga de Kafka fallo"
        exit 1
    }
    Rename-Item (Join-Path $repo "kafka_2.13-4.1.2") "kafka"
    Remove-Item (Join-Path $repo "kafka.tgz") -Force
    # La configuración que trae Kafka apunta a /tmp, que es una ruta de Linux y en Windows
    # deja los datos del broker en la raiz del disco. La dejamos en una carpeta del proyecto
    $config = Join-Path $repo "kafka\config\server.properties"
    if (Test-Path $config) {
        (Get-Content $config -Raw) -replace 'log\.dirs=.*', "log.dirs=$($repo.Replace('\', '/'))/kafka/kraft-logs" |
            Set-Content -Path $config -Encoding ASCII
    }
    escribir "  instalado en la carpeta kafka"
}
escribir ""

# Verificacion final
escribir "Verificacion"
$utf8SinBom = New-Object System.Text.UTF8Encoding($false)
[System.IO.File]::WriteAllLines($log, $salida, $utf8SinBom)
& cmd /c "`"$py`" `"$repo\src\comun\smoke_test.py`" > `"$repo\evidencias\logs\00_smoke_test.log`" 2>&1"
$codigo = $LASTEXITCODE
$ultima = Get-Content (Join-Path $repo "evidencias\logs\00_smoke_test.log") -Encoding UTF8 |
    Select-String -Pattern "SMOKE TEST" | Select-Object -Last 1
escribir "  $($ultima.Line)"
escribir "  log en evidencias\logs\00_smoke_test.log"
escribir ""
escribir "Si recien instalaste Java, abre una consola nueva antes de seguir"

if ($codigo -ne 0) {
    escribir ""
    escribir "El smoke test fallo, revisa el log antes de seguir"
    exit 1
}
escribir "Listo"
