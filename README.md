# BigDataLab03

BIG DATA — Laboratorio 03: arquitectura de datos para analisis en streaming y batch.

Implementacion de tres arquitecturas de referencia (**Lambda**, **Kappa** y **Data
Lakehouse**) sobre un entorno local en Windows, con pipeline simulado, evidencia
reproducible y analisis comparativo.

---

## Estado

| Fase | Contenido | Estado |
|------|-----------|--------|
| 0 | Base del entorno (JDK 17, venv, PySpark) | en curso |
| 1 | Kafka nativo en Windows (KRaft) | pendiente |
| 2 | Lambda, Kappa y Lakehouse | pendiente |
| 3 | Bitacoras reproducibles | pendiente |
| 4 | Informe, tabla comparativa y entrega | pendiente |

---

## Arquitectura del repositorio

El codigo y los artefactos del entregable viven en este repositorio. El runtime
pesado vive **fuera**, en `C:\lab03`, por dos motivos concretos:

1. **Spark en Windows rompe con rutas que contienen espacios.** La ruta del repo
   incluye `OneDrive\Escritorio\Big Data\...`.
2. **OneDrive corrompe archivos escritos a medias.** Los checkpoints de
   Structured Streaming y los archivos Parquet no pueden escribirse dentro de
   una carpeta sincronizada.

```
C:\lab03\                      <- runtime (fuera del repo)
  .venv\                       <- Python 3.12 + PySpark 4.2.0
  .ivy2\                       <- cache del conector Spark-Kafka
  datos\                       <- raw / bronze / silver / gold
  checkpoints\                 <- estado de Structured Streaming
  kafka\                       <- Apache Kafka 4.1.2

Laboratorio03\                 <- este repositorio
  src\comun\                   <- configuracion, sesion Spark, evidencia
  src\lakehouse\               <- rama Data Lakehouse
  src\lambda\                  <- rama Lambda
  src\kappa\                  <- rama Kappa
  resultados\                  <- csv / parquet / sqlite / png
  evidencias\                  <- logs, pantallazos, huella del entorno
  docs\                        <- informe, tabla comparativa, veredicto
```

---

## Requisitos

| Componente | Version | Nota |
|------------|---------|------|
| Python | 3.12.x | venv propia en `C:\lab03\.venv` |
| Temurin JDK | 17.0.x | Spark 4.x exige Java 17 o 21 |
| PySpark | 4.2.0 | `pandas` pineado a 2.3.3 |
| Apache Kafka | 4.1.2 | solo para Lambda y Kappa, modo KRaft |

> En esta maquina hay dos Pythons instalados (3.12 y 3.13) y el `pip` del PATH
> pertenece al 3.13 mientras que `python` es el 3.12. **Por eso todos los comandos
> usan la ruta absoluta a la venv y la forma `-m pip`.** Usar `pip` suelto
> instala en el interprete equivocado.

---

## Reproducir el entorno desde cero

```powershell
.\bootstrap.ps1
```

Recrea la venv e instala las versiones pineadas de `requisitos.txt`.

## Ejecutar el pipeline completo

```powershell
.\run_all.ps1
```

Regenera todas las evidencias en orden.

## Verificar reproducibilidad

```powershell
.\verificar_reproducibilidad.ps1
```

Ejecuta el pipeline dos veces y compara los hashes SHA-256 de las salidas.

---

## Documentos

| Documento | Rubrica |
|-----------|---------|
| `docs/informe_tecnico.md` | Documentacion (15 pts) |
| `docs/tabla_comparativa.md` | Analisis comparativo (10 pts) |
| `docs/veredicto_arquitecturas.md` | Decision del equipo |
| `docs/guion_presentacion.md` | Presentacion y tiempo (20 pts) |
| `evidencias/bitacora.md` | Evidencia del proceso |

---

## Convenciones

- Comentarios y docstrings en espanol, codigo y nombres de funciones en ingles.
- Todo script arranca con `evidencia.imprimir_header(__file__)`.
- Todo script lleva el guard `if __name__ == "__main__":` (obligatorio en
  Windows: los workers de PySpark usan `spawn`).
- El paso de ejecucion se captura siempre con `Tee-Object` a `evidencias/logs/`.
