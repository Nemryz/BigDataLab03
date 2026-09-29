r"""Toma el snapshot limpio de la API y le arruina los datos a propósito.

Esta es la primera zona del lago, la que va de limpia a sucia. Existe por una razón que
tiene que ver con lo que nos califican, y es que si los datos llegan perfectos no hay nada
que demostrar. Un evaluador que ve un pipeline andando sobre datos que ya venían limpios no
puede saber si sabemos limpiar algo, y la limpieza es la mitad del trabajo de un lago de
datos. Así que arruinamos el archivo a propósito y después lo arreglamos, que es la forma
de poder mostrar las dos mitades.

El snapshot bueno no se toca, porque es la evidencia de que los datos son reales. El
cambio va a una copia, y las dos conviven en la misma carpeta para que se vea de entrada
que un lago de datos tiene varias capas y que cada una es inmutable respecto de la otra.

Lo que arruinamos son cuatro cosas, que son las que aparecen de verdad cuando un sistema
trae datos de afuera. Primero, valores faltantes, porque el sensor no siempre midió.
Segundo, valores que vinieron como texto en vez de número, porque el que exportó no se
cuidó del tipo. Tercero, valores negativos, que no tienen sentido físico y que sirven para
probar que el filtro de rango hace algo. Y cuarto, horas repetidas, que pasan cuando el
envío se reintenta y el sistema receptor no descarta lo que ya tenía.

La semilla sale de la configuración y no se cambia, y por eso dos corridas del script
arruinan exactamente los mismos valores. Si la dejamos al azar, los resultados de la
limpieza cambiarían en cada corrida y no se podrían comparar, que es justo lo que
queremos evitar.
"""

import json
import os
import random
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "src", "comun"))

import config
import fuentes
from evidencia import imprimir_header

# El archivo bueno y el archivo arruinado, que viven en la misma zona cruda
ORIGEN = config.DATOS / "raw" / "aire_horario_manifiesto.json"

# Cuánto de cada cosa se arruina. Son proporciones chicas a propósito, porque si se
# arruina mucho el resultado final parece basura y no un dataset con problemas realistic
PROPORCION_NULOS = 0.08
PROPORCION_TEXTO = 0.03
PROPORCION_NEGATIVOS = 0.02
PROPORCION_HORAS_REPETIDAS = 0.04

# Las tres cosas que se usan para simular un texto donde deberia haber un número
TEXTOS_ROTOS = ("26,8", "N/A", "  12.5  ", "--", "")


def _archivo_de_origen():
    """Devuelve la ruta del snapshot bueno, leyéndola del manifiesto.

    Se lee el manifiesto y no se busca con un glob porque quedó un snapshot viejo de una
    corrida anterior, cuando el rango llegaba hasta hoy y por eso daba distinto. Si nos
    trustamos del glob podríamos tomar el archivo equivocado sin enterarnos, y el
    manifiesto es justamente lo que dice cuál es el bueno.
    """
    if not ORIGEN.exists():
        print(f"No existe el manifiesto {ORIGEN}, corre descargar.py primero")
        return None
    manifiesto = json.loads(ORIGEN.read_text(encoding="utf-8"))
    return ORIGEN.parent / manifiesto["archivo"]


def _arruinar_valores(ciudad, azar):
    """Le mete los cuatro tipos de desperfecto a los valores de una ciudad.

    Se separa en su propia función porque lo mismo hay que hacerlo para las cuatro ciudades
    y porque así queda claro que el desperfecto se aplica a los datos y no a la estructura.
    """
    horario = ciudad["hourly"]
    contaminantes = [c for c in horario if c != "time"]
    modificados = {"nulos": 0, "texto": 0, "negativos": 0, "horas_repetidas": 0}

    for clave in contaminantes:
        valores = horario[clave]
        for indice in range(len(valores)):
            if valores[indice] is None:
                continue
            dado = azar.random()
            if dado < PROPORCION_NULOS:
                valores[indice] = None
                modificados["nulos"] += 1
            elif dado < PROPORCION_NULOS + PROPORCION_TEXTO:
                valores[indice] = azar.choice(TEXTOS_ROTOS)
                modificados["texto"] += 1
            elif dado < PROPORCION_NULOS + PROPORCION_TEXTO + PROPORCION_NEGATIVOS:
                valores[indice] = -abs(valores[indice]) - azar.randint(1, 9)
                modificados["negativos"] += 1

    # Las horas repetidas se agregan al final de la lista, copiando una hora que ya existe
    # y sus valores, que es exactamente lo que pasa cuando un reintento de entrega duplica
    total = len(horario["time"])
    repetidas = int(total * PROPORCION_HORAS_REPETIDAS)
    for _ in range(repetidas):
        origen = azar.randrange(total)
        horario["time"].append(horario["time"][origen])
        for clave in contaminantes:
            horario[clave].append(horario[clave][origen])
        modificados["horas_repetidas"] += 1

    return modificados


def main():
    """Arruina el snapshot y guarda la copia, luego imprime un resumen de lo que hizo."""
    imprimir_header("ensuciar.py")

    archivo_origen = _archivo_de_origen()
    if archivo_origen is None:
        return 1
    if not archivo_origen.exists():
        print(f"El manifiesto apunta a {archivo_origen} y ese archivo no esta")
        return 1

    azar = random.Random(config.SEED)
    datos = json.loads(archivo_origen.read_text(encoding="utf-8"))

    print("")
    print(f"Origen   {archivo_origen.name}")
    print(f"Semilla  {config.SEED}")
    print(f"Ciudades {len(datos)}")
    print("")
    print("Arruinando")

    total = {"nulos": 0, "texto": 0, "negativos": 0, "horas_repetidas": 0}
    for ciudad in datos:
        cambios = _arruinar_valores(ciudad, azar)
        for clave in total:
            total[clave] += cambios[clave]
        print(f"  {ciudad['latitude']},{ciudad['longitude']}  "
              f"nulos {cambios['nulos']}  texto {cambios['texto']}  "
              f"negativos {cambios['negativos']}  horas repetidas {cambios['horas_repetidas']}")

    destino = config.DATOS / "raw" / "aire_horario_sucio.json"
    destino.write_text(json.dumps(datos, indent=2, ensure_ascii=False), encoding="utf-8")

    print("")
    print(f"Guardado {destino}")
    print(f"Tamano   {destino.stat().st_size} bytes")
    print(f"Total    nulos {total['nulos']}  texto {total['texto']}  "
          f"negativos {total['negativos']}  horas repetidas {total['horas_repetidas']}")
    print(f"Umbral de episodio que se usara despues  {fuentes.UMBRAL_EPISODIO}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
