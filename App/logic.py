import time
import sys
import csv
from collections import Counter, defaultdict
from datetime import datetime
from math import radians, sin, cos, sqrt, atan2

default_limit = 1000
sys.setrecursionlimit(default_limit*10)

def new_logic():
    """
    Crea el catalogo para almacenar las estructuras de datos
    """
    #TODO: Llama a las funciónes de creación de las estructuras de datos
    catalog = {
        "trayectos": []}
    return catalog


# Funciones para la carga de datos

def load_data(catalog, filename):
    """
    Carga los datos del reto
    """
    # TODO: Realizar la carga de datos
    start_time = time.perf_counter()
    data = []
    try:
        with open(filename, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                data.append(row)
    except Exception as e:
        print(f"Error al leer el archivo: {e}")
        return None

    end_time = time.perf_counter()
    elapsed = (end_time - start_time) * 1000  # milisegundos
    print(f"Archivo cargado correctamente en {elapsed:.2f} ms. Total de trayectos: {len(data)}")
    catalog["taxis"] = data
    return catalog
   

# Funciones de consulta sobre el catálogo

def get_data(catalog, id):
    """
    Retorna un dato por su ID.
    """
    #TODO: Consulta en las Llamar la función del modelo para obtener un dato
    try:
        return catalog["taxis"]["index"]
    except IndexError:
        return None
    except KeyError:
        print("El catálogo no contiene la clave 'taxis'.")
        return None


def req_1(catalog,num_pasajeros):
    """
    Retorna el resultado del requerimiento 1
    """
    
    inicio = time.time()  # Pour mesurer le temps d'exécution

    trayectos_filtrados = [
        t for t in catalog.get("taxis", [])
        if int(t["passenger_count"]) == num_pasajeros
    ]
    
    inicio = time.time()

    suma_duracion = suma_costo = suma_distancia = suma_peajes = suma_propina = 0
    total_trayectos = len(trayectos_filtrados)

    # Dictionnaires pour compter fréquences
    freq_pagos = {}
    freq_fechas = {}

    # Suivi direct du max
    max_pago = None
    max_pago_count = 0
    max_fecha = None
    max_fecha_count = 0

    for t in trayectos_filtrados:
        inicio_tray = datetime.strptime(t["pickup_datetime"], "%Y-%m-%d %H:%M:%S")
        fin_tray = datetime.strptime(t["dropoff_datetime"], "%Y-%m-%d %H:%M:%S")
        duracion_min = (fin_tray - inicio_tray).total_seconds() / 60

        suma_duracion += duracion_min
        suma_costo += float(t["total_amount"])
        suma_distancia += float(t["trip_distance"])
        suma_peajes += float(t["tolls_amount"])
        suma_propina += float(t["tip_amount"])

        # --- Contar fecha ---
        fecha = inicio_tray.strftime("%Y-%m-%d")
        if fecha not in freq_fechas:
            freq_fechas[fecha] = 0
        freq_fechas[fecha] += 1
        if freq_fechas[fecha] > max_fecha_count:
            max_fecha = fecha
            max_fecha_count = freq_fechas[fecha]

        # --- Contar tipo de pago ---
        pago = t["payment_type"]
        if pago not in freq_pagos:
            freq_pagos[pago] = 0
        freq_pagos[pago] += 1
        if freq_pagos[pago] > max_pago_count:
            max_pago = pago
            max_pago_count = freq_pagos[pago]

    tiempo_promedio = suma_duracion / total_trayectos
    costo_promedio = suma_costo / total_trayectos
    distancia_promedio = suma_distancia / total_trayectos
    peaje_promedio = suma_peajes / total_trayectos
    propina_promedio = suma_propina / total_trayectos

    fin = time.time()
    tiempo_ejecucion_ms = (fin - inicio) * 1000

    return {
        "tiempo_ejecucion_ms": tiempo_ejecucion_ms,
        "total_trayectos": total_trayectos,
        "tiempo_promedio_min": tiempo_promedio,
        "costo_total_promedio": costo_promedio,
        "distancia_promedio_millas": distancia_promedio,
        "peaje_promedio": peaje_promedio,
        "propina_promedio": propina_promedio,
        "tipo_pago_mas_usado": f"{max_pago} - {max_pago_count}",
        "fecha_inicio_mas_frecuente": max_fecha
    }
    


def req_2(catalog, metodo_pago):
    """
    Retorna el resultado del requerimiento 2
    """
    
    inicio_tiempo = time.time()

    viajes_filtrados = [v for v in catalog if v["payment_type"] == metodo_pago]

    if not viajes_filtrados:
        return {"mensaje": "No se encontraron viajes con ese método de pago"}

    total_duracion = 0
    total_costo = 0
    total_distancia = 0
    total_peajes = 0
    total_propinas = 0
    pasajeros = []
    fechas_finalizacion = []

    for v in viajes_filtrados:
        inicio = datetime.strptime(v["pickup_datetime"], "%Y-%m-%d %H:%M:%S")
        fin = datetime.strptime(v["dropoff_datetime"], "%Y-%m-%d %H:%M:%S")
        duracion = (fin - inicio).total_seconds() / 60
        total_duracion += duracion

        total_costo += float(v["total_amount"])
        total_distancia += float(v["trip_distance"])
        total_peajes += float(v["tolls_amount"])
        total_propinas += float(v["tip_amount"])

        pasajeros.append(int(v["passenger_count"]))
        fechas_finalizacion.append(fin.strftime("%Y-%m-%d"))

    n_viajes = len(viajes_filtrados)

    pasajero_mas_frecuente, cantidad = Counter(pasajeros).most_common(1)[0]
    fecha_mas_frecuente = Counter(fechas_finalizacion).most_common(1)[0][0]

    fin_tiempo = time.time()
    tiempo_ms = (fin_tiempo - inicio_tiempo) * 1000

    return {
        "tiempo_ms": tiempo_ms,
        "total_viajes": n_viajes,
        "duracion_promedio_min": total_duracion / n_viajes,
        "costo_promedio": total_costo / n_viajes,
        "distancia_promedio": total_distancia / n_viajes,
        "peajes_promedio": total_peajes / n_viajes,
        "propinas_promedio": total_propinas / n_viajes,
        "pasajero_mas_frecuente": f"{pasajero_mas_frecuente} - {cantidad}",
        "fecha_finalizacion_mas_frecuente": fecha_mas_frecuente
    }

    # TODO: Modificar el requerimiento 2
    pass


def req_3(catalog):
    """
    Retorna el resultado del requerimiento 3
    """
    # TODO: Modificar el requerimiento 3
    pass


def req_4(catalog, filtro, fecha_inicio_str, fecha_fin_str):
    inicio = time.time()
    fecha_inicio = datetime.strptime(fecha_inicio_str, "%Y-%m-%d").date()
    fecha_fin = datetime.strptime(fecha_fin_str, "%Y-%m-%d").date()

    combinaciones = defaultdict(list)
    centroides= cargar_centroides("Data/nyc-neighborhoods.csv")

    for t in catalog["taxis"]:
        try:
            fecha = datetime.strptime(t["pickup_datetime"], "%Y-%m-%d %H:%M:%S").date()
            if not (fecha_inicio <= fecha <= fecha_fin):
                continue

            ori = barrio_mas_cercano(float(t["pickup_latitude"]), float(t["pickup_longitude"]), centroides)
            dest = barrio_mas_cercano(float(t["dropoff_latitude"]), float(t["dropoff_longitude"]), centroides)
            dur = (datetime.strptime(t["dropoff_datetime"], "%Y-%m-%d %H:%M:%S") -
                   datetime.strptime(t["pickup_datetime"], "%Y-%m-%d %H:%M:%S")).total_seconds()/60
            combinaciones[(ori, dest)].append((float(t["trip_distance"]), dur, float(t["total_amount"])))
        except:
            continue

    promedios = {k: (sum(d[0] for d in v)/len(v),
                     sum(d[1] for d in v)/len(v),
                     sum(d[2] for d in v)/len(v))
                 for k, v in combinaciones.items()}

    if not promedios:
        return {"mensaje": "No hay trayectos en ese rango de fechas."}

    if filtro.upper() == "MAYOR":
        sel = max(promedios.items(), key=lambda x: x[1][2])
    elif filtro.upper() == "MENOR":
        sel = min(promedios.items(), key=lambda x: x[1][2])
    else:
        return {"mensaje": "Filtro inválido"}

    fin = time.time()
    tiempo_ejecucion_ms = (fin - inicio) * 1000
    (ori, dest), (dist_prom, tiempo_prom, costo_prom) = sel

    return {
        "tiempo_ejecucion_ms": tiempo_ejecucion_ms,
        "filtro_costo": filtro.upper(),
        "total_trayectos": sum(len(v) for v in combinaciones.values()),
        "origen": ori,
        "destino": dest,
        "distancia_promedio_millas": dist_prom,
        "tiempo_promedio_min": tiempo_prom,
        "costo_total_promedio": costo_prom
    }
    

def cargar_centroides(filename):
    centroides = {}
    try:
        with open(filename, newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile, delimiter=';')
            for row in reader:
                barrio = row["neighborhood"]
                lat = float(row["latitude"].replace(",", "."))
                lon = float(row["longitude"].replace(",", "."))
                centroides[barrio] = (lat, lon)
    except Exception as e:
        print(f"Error al leer archivo de barrios: {e}")
    return centroides

def haversine(lat1, lon1, lat2, lon2):
    R = 3958.8  # miles
    lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = sin(dlat/2)**2 + cos(lat1)*cos(lat2)*sin(dlon/2)**2
    c = 2 * atan2(sqrt(a), sqrt(1-a))
    return R * c

def barrio_mas_cercano(lat, lon, centroides):
    min_dist = float('inf')
    barrio_sel = None
    for barrio, (lat_c, lon_c) in centroides.items():
        dist = haversine(lat, lon, lat_c, lon_c)
        if dist < min_dist:
            min_dist = dist
            barrio_sel = barrio
    return barrio_sel



def req_5(catalog, filtro, fecha_inicio_str, fecha_fin_str):

    inicio = time.time()

    # Conversion de fechas
    fecha_inicio = datetime.strptime(fecha_inicio_str, "%Y-%m-%d").date()
    fecha_fin = datetime.strptime(fecha_fin_str, "%Y-%m-%d").date()

    trayectos = catalog.get("taxis", [])

    # Diccionario para acumular datos por franja horaria
    franjas = defaultdict(lambda: {
        "costos": [],
        "duraciones": [],
        "pasajeros": [],
        "trayectos": []
    })

    total_filtrados = 0

    for t in trayectos:
        try:
            pickup = datetime.strptime(t["pickup_datetime"], "%Y-%m-%d %H:%M:%S")
            dropoff = datetime.strptime(t["dropoff_datetime"], "%Y-%m-%d %H:%M:%S")
        except Exception:
            continue

        # Filtrar por rango de fechas (solo la fecha de inicio)
        if not (fecha_inicio <= pickup.date() <= fecha_fin):
            continue

        total_filtrados += 1
        franja = pickup.hour  # ej. 13 → franja [13 - 14)

        duracion_min = (dropoff - pickup).total_seconds() / 60
        costo = float(t["total_amount"])
        pasajeros = int(t["passenger_count"])

        franjas[franja]["costos"].append(costo)
        franjas[franja]["duraciones"].append(duracion_min)
        franjas[franja]["pasajeros"].append(pasajeros)
        franjas[franja]["trayectos"].append({
            "costo": costo,
            "dropoff": dropoff
        })

    if total_filtrados == 0:
        return {"mensaje": "No hay trayectos en ese rango de fechas."}

    # Calcular estadísticas por franja
    stats_franjas = []
    for franja, datos in franjas.items():
        if not datos["costos"]:
            continue

        costo_prom = sum(datos["costos"]) / len(datos["costos"])
        duracion_prom = sum(datos["duraciones"]) / len(datos["duraciones"])
        pasajeros_prom = sum(datos["pasajeros"]) / len(datos["pasajeros"])

        # Mayor y menor costo total (con desempate por fecha más reciente)
        mayor_tray = max(datos["trayectos"], key=lambda x: (x["costo"], x["dropoff"]))
        menor_tray = min(datos["trayectos"], key=lambda x: (x["costo"], -x["dropoff"].timestamp()))

        stats_franjas.append({
            "franja": f"[{franja} - {franja+1})",
            "costo_prom": costo_prom,
            "num_trayectos": len(datos["costos"]),
            "duracion_prom": duracion_prom,
            "pasajeros_prom": pasajeros_prom,
            "costo_max": mayor_tray["costo"],
            "costo_min": menor_tray["costo"]
        })

    # Seleccionar franja segun filtro
    if filtro == "MAYOR":
        mejor = max(stats_franjas, key=lambda x: x["costo_prom"])
    elif filtro == "MENOR":
        mejor = min(stats_franjas, key=lambda x: x["costo_prom"])
    else:
        return {"error": "Filtro inválido, use 'MAYOR' o 'MENOR'"}

    fin = time.time()
    tiempo_ms = (fin - inicio) * 1000

    return {
        "tiempo_ejecucion_ms": tiempo_ms,
        "filtro": filtro,
        "total_trayectos": total_filtrados,
        "resultado": mejor
    }

def req_6(catalog, neighborhoods, barrio_inicio, fecha_inicial, fecha_final):
    """
    Retorna el resultado del requerimiento 6
    """
    fecha_inicial = datetime.strptime(fecha_inicial, "%Y-%m-%d")
    fecha_final = datetime.strptime(fecha_final, "%Y-%m-%d")

    viajes_filtrados = []
    
    for viaje in catalog:
        fecha_viaje = datetime.strptime(viaje["pickup_datetime"], "%Y-%m-%d %H:%M:%S")
        if not (fecha_inicial <= fecha_viaje <= fecha_final):
            continue

        barrio = encontrar_barrio(float(viaje["pickup_latitude"]),
                                  float(viaje["pickup_longitude"]),
                                  neighborhoods)
        if barrio != barrio_inicio:
            continue

        viajes_filtrados.append(viaje)

    if not viajes_filtrados:
        return {"mensaje": "No se encontraron trayectos en el rango y barrio indicados"}

    total_distancia = 0
    total_duracion = 0
    destinos = []

    pagos = defaultdict(lambda: {"count": 0, "total": 0, "duracion": 0})

    for v in viajes_filtrados:
        inicio = datetime.strptime(v["pickup_datetime"], "%Y-%m-%d %H:%M:%S")
        fin = datetime.strptime(v["dropoff_datetime"], "%Y-%m-%d %H:%M:%S")
        duracion = (fin - inicio).total_seconds() / 60

        total_duracion += duracion
        total_distancia += float(v["trip_distance"])

        barrio_destino = encontrar_barrio(float(v["dropoff_latitude"]),
                                          float(v["dropoff_longitude"]),
                                          neighborhoods)
        destinos.append(barrio_destino)

        metodo = v["payment_type"]
        pagos[metodo]["count"] += 1
        pagos[metodo]["total"] += float(v["total_amount"])
        pagos[metodo]["duracion"] += duracion

    n_viajes = len(viajes_filtrados)
    distancia_prom = total_distancia / n_viajes
    duracion_prom = total_duracion / n_viajes
    barrio_destino_mas_frecuente = Counter(destinos).most_common(1)[0][0]

    max_usado = max(pagos.items(), key=lambda x: x[1]["count"])[0]
    max_recaudo = max(pagos.items(), key=lambda x: x[1]["total"])[0]

    resultados_pagos = []
    for metodo, info in pagos.items():
        resultados_pagos.append({
            "metodo": metodo,
            "cantidad_trayectos": info["count"],
            "promedio_pago": info["total"] / info["count"],
            "es_mas_usado": metodo == max_usado,
            "es_mas_recaudo": metodo == max_recaudo,
            "duracion_promedio": info["duracion"] / info["count"]
        })

    return {
        "total_viajes": n_viajes,
        "distancia_promedio": distancia_prom,
        "duracion_promedio": duracion_prom,
        "barrio_destino_mas_visitado": barrio_destino_mas_frecuente,
        "metodos_pago": resultados_pagos
    }
    
def haversine(lat1, lon1, lat2, lon2):
    R = 3958.8  # miles
    lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = sin(dlat/2)**2 + cos(lat1)*cos(lat2)*sin(dlon/2)**2
    c = 2 * atan2(sqrt(a), sqrt(1-a))
    return R * c

def encontrar_barrio(lat, lon, neighborhoods):
    min_dist = float("inf")
    barrio_cercano = None
    for n in neighborhoods:
        dist = haversine(lat, lon, float(n["latitude"]), float(n["longitude"]))
        if dist < min_dist:
            min_dist = dist
            barrio_cercano = n["neighborhood"]
    return barrio_cercano

# TODO: Modificar el requerimiento 6
    pass

# Funciones para medir tiempos de ejecucion

def get_time():
    """
    devuelve el instante tiempo de procesamiento en milisegundos
    """
    return float(time.perf_counter()*1000)


def delta_time(start, end):
    """
    devuelve la diferencia entre tiempos de procesamiento muestreados
    """
    elapsed = float(end - start)
    return elapsed
