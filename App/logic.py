import time
import sys
import csv
from collections import Counter, defaultdict
from datetime import datetime
from math import radians, sin, cos, sqrt, atan2
import DataStructures.array_list as list

default_limit = 1000
sys.setrecursionlimit(default_limit*10)

def new_logic():
    """
    Crea el catalogo para almacenar las estructuras de datos
    """
    #TODO: Llama a las funciónes de creación de las estructuras de datos
    #catalog = {
    #    "trayectos": []}
    catalog=list.new_list()
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
                #data.append(row)
                list.add_last(catalog,row)
    except Exception as e:
        print(f"Error al leer el archivo: {e}")
        return None

    end_time = time.perf_counter()
    elapsed = (end_time - start_time) * 1000  # milisegundos
    print(f"Archivo cargado correctamente en {elapsed:.2f} ms. Total de trayectos: {len(data)}")
    return catalog
   

# Funciones de consulta sobre el catálogo

def get_data(catalog, id):
    """
    Retorna un dato por su ID.
    """
    try:
        return list.get_element(catalog, id)
    except IndexError:
        return None


def req_1(catalog,num_pasajeros):
    """
    Retorna el resultado del requerimiento 1
    """
    trayectos_filtrados = list.new_list()
    inicio = time.time() 
    for i in range(list.size(catalog)):
        t=get_data(catalog,i)
        if int(t["passenger_count"])==num_pasajeros:
            list.add_last(trayectos_filtrados,t)
        

    suma_duracion = suma_costo = suma_distancia = suma_peajes = suma_propina = 0
    total_trayectos = list.size(trayectos_filtrados)
    if total_trayectos == 0:
        return {
            "mensaje": "No hay trayectos con ese número de pasajeros"
        }

    # Dictionnaires pour compter fréquences
    freq_pagos = {}
    freq_fechas = {}

    # Suivi direct du max
    max_pago = None
    max_pago_count = 0
    max_fecha = None
    max_fecha_count = 0

    for i in range(list.size(trayectos_filtrados)):
        t=list.get_element(trayectos_filtrados,i)
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
    


def req_2(my_list, metodo_pago):
    """
    Retorna el resultado del requerimiento 2
    """
    
    inicio_tiempo = time.time()

    n = list.size(my_list)
    viajes_filtrados = list.new_list()
    
    for i in range(n):
        v = list.get_element(my_list, i)
        if v["payment_type"] == metodo_pago:
            list.add_last(viajes_filtrados, v)

    if list.is_empty(viajes_filtrados):
        return {"mensaje": "No se encontraron viajes con ese método de pago"}
    
    total_duracion = 0
    total_costo = 0
    total_distancia = 0
    total_peajes = 0
    total_propinas = 0
    pasajeros = []
    fechas_finalizacion = []

    m = list.size(viajes_filtrados)
    
    for v in range(m):
        j = list.get_element(viajes_filtrados, v)
        inicio = datetime.strptime(j["pickup_datetime"], "%Y-%m-%d %H:%M:%S")
        fin = datetime.strptime(j["dropoff_datetime"], "%Y-%m-%d %H:%M:%S")
        duracion = (fin - inicio).total_seconds() / 60
        
        total_duracion += duracion
        total_costo += float(j["total_amount"])
        total_distancia += float(j["trip_distance"])
        total_peajes += float(j["tolls_amount"])
        total_propinas += float(j["tip_amount"])

        pasajeros.append(int(j["passenger_count"]))
        fechas_finalizacion.append(fin.strftime("%Y-%m-%d"))

    pasajero_mas_frecuente, cantidad = Counter(pasajeros).most_common(1)[0]
    fecha_mas_frecuente = Counter(fechas_finalizacion).most_common(1)[0][0]

    fin_tiempo = time.time()
    tiempo_ms = (fin_tiempo - inicio_tiempo) * 1000

    return {
        "tiempo_ms": tiempo_ms,
        "total_viajes": m,
        "duracion_promedio_min": total_duracion / m,
        "costo_promedio": total_costo / m,
        "distancia_promedio": total_distancia / m,
        "peajes_promedio": total_peajes / m,
        "propinas_promedio": total_propinas / m,
        "pasajero_mas_frecuente": f"{pasajero_mas_frecuente} - {cantidad}",
        "fecha_finalizacion_mas_frecuente": fecha_mas_frecuente
    }
    
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

    centroides = cargar_centroides("Data/nyc-neighborhoods.csv")

    combinaciones = list.new_list()

    for i in range(list.size(catalog)):
        t = list.get_element(catalog, i)
        try:
            fecha = datetime.strptime(t["pickup_datetime"], "%Y-%m-%d %H:%M:%S").date()
            if not (fecha_inicio <= fecha <= fecha_fin):
                continue

            ori = barrio_mas_cercano(float(t["pickup_latitude"]), float(t["pickup_longitude"]), centroides)
            dest = barrio_mas_cercano(float(t["dropoff_latitude"]), float(t["dropoff_longitude"]), centroides)
            dur = (datetime.strptime(t["dropoff_datetime"], "%Y-%m-%d %H:%M:%S") -
                   datetime.strptime(t["pickup_datetime"], "%Y-%m-%d %H:%M:%S")).total_seconds()/60
            if dur <= 0 or float(t["trip_distance"]) <= 0 or float(t["total_amount"]) <= 0:
                continue

            viaje = {"dist": float(t["trip_distance"]), "dur": dur, "costo": float(t["total_amount"])}

            pos = -1
            for idx in range(list.size(combinaciones)):
                combo = list.get_element(combinaciones, idx)
                if combo.get("ori") == ori and combo.get("dest") == dest:
                    pos = idx
                    break

            if pos != -1:
                combo = list.get_element(combinaciones, pos)
                viajes_list = combo["viajes"]         
                list.add_last(viajes_list, viaje)
            else:
                nuevo = {"ori": ori, "dest": dest, "viajes": list.new_list()}
                list.add_last(nuevo["viajes"], viaje)
                list.add_last(combinaciones, nuevo)
        except:
            continue
        

    promedios = list.new_list()
    for j in range(list.size(combinaciones)):
        oridest = list.get_element(combinaciones, j)
        viajes = oridest["viajes"]

        if list.size(viajes) == 0:
            continue

        suma_dist = suma_dur = suma_costo = 0
        for k in range(list.size(viajes)):
            v = list.get_element(viajes, k)
            suma_dist += v["dist"]
            suma_dur += v["dur"]
            suma_costo += v["costo"]

        dist_prom = suma_dist / list.size(viajes)
        dur_prom = suma_dur / list.size(viajes)
        costo_prom = suma_costo / list.size(viajes)

        list.add_last(promedios, {
            "ori": oridest["ori"],
            "dest": oridest["dest"],
            "dist_prom": dist_prom,
            "dur_prom": dur_prom,
            "costo_prom": costo_prom,
            "total": list.size(viajes)
        })

    if list.size(promedios) == 0:
        return {"mensaje": "No hay trayectos en ese rango de fechas."}

    sel = None
    if filtro.upper() == "MAYOR":
        sel = max(promedios["elements"], key=lambda x: x["costo_prom"])
    elif filtro.upper() == "MENOR":
        sel = min(promedios["elements"], key=lambda x: x["costo_prom"])
    else:
        return {"mensaje": "Filtro inválido"}

    fin = time.time()
    tiempo_ejecucion_ms = (fin - inicio) * 1000

    return {
        "tiempo_ejecucion_ms": tiempo_ejecucion_ms,
        "filtro_costo": filtro.upper(),
        "total_trayectos": sum(p["total"] for p in promedios["elements"]),
        "origen": sel["ori"],
        "destino": sel["dest"],
        "distancia_promedio_millas": sel["dist_prom"],
        "tiempo_promedio_min": sel["dur_prom"],
        "costo_total_promedio": sel["costo_prom"]
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

    fecha_inicio = datetime.strptime(fecha_inicio_str, "%Y-%m-%d").date()
    fecha_fin = datetime.strptime(fecha_fin_str, "%Y-%m-%d").date()

    franjas = {}

    total_filtrados = 0

    for i in range(list.size(catalog)):
        t = list.get_element(catalog, i)

        try:
            pickup = datetime.strptime(t["pickup_datetime"], "%Y-%m-%d %H:%M:%S")
            dropoff = datetime.strptime(t["dropoff_datetime"], "%Y-%m-%d %H:%M:%S")
        except Exception:
            continue

        if not (fecha_inicio <= pickup.date() <= fecha_fin):
            continue

        total_filtrados += 1
        franja = pickup.hour  

        duracion_min = (dropoff - pickup).total_seconds() / 60
        costo = float(t["total_amount"])
        pasajeros = int(t["passenger_count"])

        if franja not in franjas:
            franjas[franja] = {
                "costos": list.new_list(),
                "duraciones": list.new_list(),
                "pasajeros": list.new_list(),
                "trayectos": list.new_list()
            }

        list.add_last(franjas[franja]["costos"], costo)
        list.add_last(franjas[franja]["duraciones"], duracion_min)
        list.add_last(franjas[franja]["pasajeros"], pasajeros)
        list.add_last(franjas[franja]["trayectos"], {
            "costo": costo,
            "dropoff": dropoff
        })

    if total_filtrados == 0:
        return {"mensaje": "No hay trayectos en ese rango de fechas."}

    stats_franjas = list.new_list()
    for franja, datos in franjas.items():
        if list.size(datos["costos"]) == 0:
            continue

        costo_prom = sum(datos["costos"]["elements"]) / list.size(datos["costos"])
        duracion_prom = sum(datos["duraciones"]["elements"]) / list.size(datos["duraciones"])
        pasajeros_prom = sum(datos["pasajeros"]["elements"]) / list.size(datos["pasajeros"])

        mayor_tray = max(datos["trayectos"]["elements"], key=lambda x: (x["costo"], x["dropoff"]))
        menor_tray = min(datos["trayectos"]["elements"], key=lambda x: (x["costo"], -x["dropoff"].timestamp()))

        list.add_last(stats_franjas, {
            "franja": f"[{franja} - {franja+1})",
            "costo_prom": costo_prom,
            "num_trayectos": list.size(datos["costos"]),
            "duracion_prom": duracion_prom,
            "pasajeros_prom": pasajeros_prom,
            "costo_max": mayor_tray["costo"],
            "costo_min": menor_tray["costo"]
        })

    if filtro == "MAYOR":
        mejor = max(stats_franjas["elements"], key=lambda x: x["costo_prom"])
    elif filtro == "MENOR":
        mejor = min(stats_franjas["elements"], key=lambda x: x["costo_prom"])
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
    

def req_6(my_list, neighborhoods, barrio_inicio, fecha_inicial, fecha_final):
    
    fecha_inicial = datetime.strptime(fecha_inicial, "%Y-%m-%d")
    fecha_final = datetime.strptime(fecha_final, "%Y-%m-%d")

    viajes_filtrados = list.new_list
    n = list.size(my_list)
    
    for i in range(n):
        viaje = list.get_element(my_list, i)
        fecha_viaje = datetime.strptime(viaje["pickup_datetime"], "%Y-%m-%d %H:%M:%S")
        if not (fecha_inicial <= fecha_viaje <= fecha_final):
            continue

        barrio = encontrar_barrio(float(viaje["pickup_latitude"]),
                                  float(viaje["pickup_longitude"]),
                                  neighborhoods)
        if barrio == barrio_inicio:
            list.add_last(viajes_filtrados, viaje)


    if list.is_empty(viajes_filtrados):
        return {"No se encontraron trayectos en el rango y barrio indicados"}

    total_distancia = 0
    total_duracion = 0
    destinos = []
    pagos = defaultdict(lambda: {"count": 0, "total": 0, "duracion": 0})

    m = list.size(viajes_filtrados)
    for i in range(m):
        v = list.get_element(viajes_filtrados, i)
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

    n_viajes = list.size(viajes_filtrados)
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
    n = list.size(neighborhoods)
    for i in range(n):
        j = list.get_element(neighborhoods, i)
        dist = haversine(lat, lon, float(j["latitude"]), float(j["longitude"]))
        if dist < min_dist:
            min_dist = dist
            barrio_cercano = j["neighborhood"]
    return barrio_cercano

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
