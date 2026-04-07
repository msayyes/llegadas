import time
import requests
import re
from datetime import datetime, timezone
from playwright.sync_api import sync_playwright

# ==========================================
# 1. CONFIGURACIÓN (TUS LLAVES)
# ==========================================
SUPABASE_URL = "https://pozwondqqzurujbsanhn.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InBvendvbmRxcXp1cnVqYnNhbmhuIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc3MjY4MDI2MiwiZXhwIjoyMDg4MjU2MjYyfQ.7sa0HnppwjWlZhh_cZRqcW-qMmlAex8vY3-4dNWFcRU" 

HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "return=minimal"
}

def recolectar_valenciaport_pcs():
    print("🤖 Iniciando Bot Recolector (Doble Batida + Atajo 60 Días + Anti-Duplicados)...")

    # 1. BD de barcos (Limpiando espacios dobles en la memoria)
    r_buques = requests.get(f"{SUPABASE_URL}/rest/v1/buques?select=id,nombre", headers=HEADERS)
    barcos_conocidos = {re.sub(r'\s+', ' ', b["nombre"].replace('\xa0', ' ').strip().upper()): b["id"] for b in r_buques.json() if b.get("nombre")}

    # 2. BD de Llegadas Activas
    r_activas = requests.get(f"{SUPABASE_URL}/rest/v1/llegadas_valencia?select=buque_id&estado_actual=neq.TERMINADO", headers=HEADERS)
    llegadas_activas_ids = [item["buque_id"] for item in r_activas.json()]

    # 3. Diccionario Equivalencias (Línea PCS -> Tu Servicio)
    try:
        r_equiv = requests.get(f"{SUPABASE_URL}/rest/v1/equivalencias_lineas?select=linea_pcs,servicio_codigo", headers=HEADERS)
        equivalencias = {e["linea_pcs"].strip().upper(): e["servicio_codigo"] for e in r_equiv.json() if e.get("linea_pcs")}
        print(f"📚 Diccionario cargado con {len(equivalencias)} traducciones.")
    except:
        equivalencias = {}
        print("⚠️ No se pudo cargar la tabla de equivalencias.")

    buques_escrapeados_hoy =[] 
    llegadas_procesadas = {} 

    # ==========================================
    # FUNCIÓN INTERNA PARA ESCANEAR PÁGINAS
    # ==========================================
    def escanear_paginas(page, nombre_fase):
        numero_pagina = 1
        while True:
            print(f"\n⏳[{nombre_fase}] Escaneando página {numero_pagina}...")
            filas = page.locator("table tbody tr").all()

            for fila in filas:
                try:
                    texto_fila = fila.inner_text().upper()
                    es_msc = "M.S.C." in texto_fila or "MSC " in texto_fila or "MSC TERMINAL" in texto_fila
                    es_csp = "CSP" in texto_fila
                    es_apm = "APM" in texto_fila
                    
                    if es_csp or es_msc or es_apm or "CONTENEDORES" in texto_fila:
                        celdas = fila.locator("td").all_inner_texts()
                        if len(celdas) < 10: continue # Seguridad anti-filas rotas
                        
                        # --- LIMPIEZA NOMBRE ---
                        nombre_crudo = celdas[0].strip().upper()
                        if len(nombre_crudo) < 3 or any(c.isdigit() for c in nombre_crudo):
                            nombre_crudo = celdas[1].strip().upper()
                        nombre = re.sub(r'\s+', ' ', nombre_crudo.replace('\xa0', ' ').strip())
                            
                        # --- TERMINAL ---
                        terminal = "Desconocida"
                        if es_csp: terminal = "CSP Iberian"
                        elif es_msc: terminal = "MSC Terminal"
                        elif es_apm: terminal = "APM Terminals"

                        # --- ESTADOS ---
                        estado_barco = "PREVISTO"
                        if "OPERANDO" in texto_fila: estado_barco = "OPERANDO"
                        elif "AUTORIZADA" in texto_fila or "AUTORIZADO" in texto_fila: estado_barco = "AUTORIZADO"

                        # --- COLUMNAS 8 Y 9 (CONSIGNATARIO Y LÍNEA) ---
                        COL_CONSIGNATARIO = 8 
                        COL_LINEA = 9 
                        consignatario = celdas[COL_CONSIGNATARIO].strip().title() if len(celdas) > COL_CONSIGNATARIO else "N/A"
                        linea_regular = celdas[COL_LINEA].strip().upper() if len(celdas) > COL_LINEA else "N/A"

                        # --- FECHAS ETA/ETD ---
                        fechas_encontradas =[]
                        for celda in celdas:
                            matches = re.finditer(r'(\d{2})[/.-](\d{2})(?:[/.-](\d{2,4}))?(?:\s+(\d{2}):(\d{2}))?', celda)
                            for match in matches:
                                dia, mes = match.group(1), match.group(2)
                                ano = match.group(3)
                                if not ano: ano = str(datetime.now().year)
                                elif len(ano) == 2: ano = "20" + ano 
                                hora = match.group(4) or "00"
                                minuto = match.group(5) or "00"
                                f_iso = f"{ano}-{mes}-{dia} {hora}:{minuto}:00+00"
                                fechas_encontradas.append(f_iso)
                        
                        fecha_eta_real = fechas_encontradas[0] if len(fechas_encontradas) > 0 else datetime.now(timezone.utc).isoformat()
                        fecha_etd_real = fechas_encontradas[1] if len(fechas_encontradas) > 1 else None

                        # --- TRADUCCIÓN DE SERVICIO ---
                        servicio_traducido = equivalencias.get(linea_regular)
                        buque_payload = {"nombre": nombre}
                        if servicio_traducido:
                            buque_payload["servicio_codigo"] = servicio_traducido

                        print(f"   --> 🚢 {nombre} | ETA: {fecha_eta_real[:16]} | Lín: {linea_regular} -> Srv: {servicio_traducido or 'N/A'}")

                        # --- CREACIÓN EN BASE DE DATOS ---
                        if nombre not in barcos_conocidos:
                            requests.post(f"{SUPABASE_URL}/rest/v1/buques", headers=HEADERS, json=buque_payload)
                            r_id = requests.get(f"{SUPABASE_URL}/rest/v1/buques?nombre=eq.{nombre.replace(' ', '%20')}&select=id", headers=HEADERS)
                            if len(r_id.json()) > 0: barcos_conocidos[nombre] = r_id.json()[0]["id"]
                        else:
                            if servicio_traducido:
                                requests.patch(f"{SUPABASE_URL}/rest/v1/buques?id=eq.{barcos_conocidos[nombre]}", headers=HEADERS, json={"servicio_codigo": servicio_traducido})
                        
                        buque_id_actual = barcos_conocidos[nombre]
                        if buque_id_actual not in buques_escrapeados_hoy:
                            buques_escrapeados_hoy.append(buque_id_actual)
                        
                        registro_actual = {
                            "buque_id": buque_id_actual,
                            "terminal": terminal,
                            "estado_actual": estado_barco, 
                            "eta": fecha_eta_real,
                            "etd": fecha_etd_real,
                            "consignatario": consignatario,
                            "linea_regular": linea_regular
                        }

                        # --- ANTI DUPLICADOS CON DOBLE ESCALA ---
                        if buque_id_actual in llegadas_procesadas:
                            eta_existente = llegadas_procesadas[buque_id_actual]["eta"]
                            if fecha_eta_real[:10] != eta_existente[:10]: 
                                if fecha_eta_real < eta_existente:
                                    registro_actual["eta_2"] = eta_existente
                                    registro_actual["etd_2"] = llegadas_procesadas[buque_id_actual].get("etd")
                                    llegadas_procesadas[buque_id_actual] = registro_actual
                                else:
                                    llegadas_procesadas[buque_id_actual]["eta_2"] = fecha_eta_real
                                    llegadas_procesadas[buque_id_actual]["etd_2"] = fecha_etd_real
                            else:
                                if estado_barco == "OPERANDO" or llegadas_procesadas[buque_id_actual]["estado_actual"] == "PREVISTO":
                                    llegadas_procesadas[buque_id_actual] = registro_actual
                        else:
                            llegadas_procesadas[buque_id_actual] = registro_actual

                except Exception as e:
                    continue

            # --- PASO DE PÁGINA ---
            try:
                boton_siguiente = page.locator(".ui-iggrid-nextpagelabel").first
                if boton_siguiente.is_visible():
                    padre_desactivado = boton_siguiente.evaluate("el => el.parentElement.classList.contains('ui-state-disabled')")
                    if padre_desactivado:
                        print(f"🚫 [{nombre_fase}] Última página alcanzada.")
                        break
                        
                    print(f"⏭️ [{nombre_fase}] Haciendo clic en Siguiente...")
                    boton_siguiente.click(force=True)
                    time.sleep(5) 
                    numero_pagina += 1
                else:
                    print(f"🚫 [{nombre_fase}] No se encontró el botón de siguiente.")
                    break
            except Exception as e:
                print(f"🚫 [{nombre_fase}] Fin de la paginación. ({e})")
                break

    # ==========================================
    # EJECUCIÓN PRINCIPAL DEL NAVEGADOR
    # ==========================================
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, slow_mo=300) # Headless=True para Github Actions
        page = browser.new_page(user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64)")

        print("🌐 Entrando a ValenciaportPCS...")
        page.goto("https://www.valenciaportpcs.net/portcalls/Search?lang=es", timeout=60000)
        
        try: page.locator("text='Aceptar', text='Accept'").first.click(timeout=3000)
        except: pass

        # ------------------------------------------
        # ▶️ FASE 1: LEYENDO VISTA ACTUAL (Presente)
        # ------------------------------------------
        print("\n==========================================")
        print("▶️ FASE 1: LEYENDO VISTA ACTUAL (Por Defecto)")
        print("==========================================")
        try:
            boton_padre = page.locator("a, button").filter(has=page.locator("span.hidden-xs:has-text('Buscar')")).first
            if boton_padre.is_visible(): boton_padre.click(delay=200, force=True)
            else: page.locator("span.hidden-xs:has-text('Buscar')").first.click(force=True)
            time.sleep(6) 
            page.wait_for_selector("table tbody tr", timeout=20000)
        except: pass

        escanear_paginas(page, "FASE 1")

        # ------------------------------------------
        # ▶️ FASE 2: EL ATAJO DE 60 DÍAS (Futuro)
        # ------------------------------------------
        print("\n==========================================")
        print("▶️ FASE 2: ACTIVANDO ATAJO '60 DÍAS'")
        print("==========================================")
        try:
            caja_fecha = page.locator("input.ui-igedit-field, input#date-picker").first
            if caja_fecha.is_visible(timeout=5000):
                caja_fecha.click()
                time.sleep(1.5) 
                
                # TU ATAJO ESTRELLA
                print("   --> Buscando el atajo de 60 Días...")
                atajo_60 = page.locator("a[shortcut='custom']").filter(has_text=re.compile(r"60\s*Dias", re.IGNORECASE)).first
                if atajo_60.is_visible():
                    atajo_60.click(force=True)
                    print("   --> ✅ ¡Atajo de 60 Días clicado con éxito!")
                    time.sleep(1.5)
                    
                    # Volvemos a darle a Buscar
                    boton_padre = page.locator("a, button").filter(has=page.locator("span.hidden-xs:has-text('Buscar')")).first
                    if boton_padre.is_visible(): boton_padre.click(delay=200, force=True)
                    else: page.locator("span.hidden-xs:has-text('Buscar')").first.click(force=True)
                    
                    print("   --> ⏳ Esperando recarga de tabla (6 seg)...")
                    time.sleep(6)
                    page.wait_for_selector("table tbody tr", timeout=20000)
                    
                    escanear_paginas(page, "FASE 2")
                else:
                    print("   --> ⚠️ No se encontró el botón de 60 Días en el calendario.")
        except Exception as e:
            print(f"   --> ⚠️ Fallo en la Fase 2: {e}")

        browser.close()

    # ==========================================
    # GUARDAR DATOS EN SUPABASE
    # ==========================================
    if len(llegadas_procesadas) > 0:
        print(f"\n💾 Procesando {len(llegadas_procesadas)} llegadas únicas extraídas de ambas fases...")
        ahora_utc = datetime.now(timezone.utc).isoformat()
        
        barcos_zarpados = 0
        for b_id in llegadas_activas_ids:
            if b_id not in buques_escrapeados_hoy:
                requests.patch(f"{SUPABASE_URL}/rest/v1/llegadas_valencia?buque_id=eq.{b_id}", headers=HEADERS, json={"estado_actual": "TERMINADO"})
                requests.patch(f"{SUPABASE_URL}/rest/v1/buques?id=eq.{b_id}", headers=HEADERS, json={
    "ultima_visita": ahora_utc[:10],
    "destino_declarado": None,   # limpia el destino al zarpar
    "eta_declarada": None        # limpia la ETA al zarpar
})
                barcos_zarpados += 1
        
        print(f"👋 {barcos_zarpados} barcos han zarpado. Historial actualizado.")

        for b_id, llegada in llegadas_procesadas.items():
            # Eliminamos los registros anteriores activos (Purga de duplicados)
            requests.delete(f"{SUPABASE_URL}/rest/v1/llegadas_valencia?buque_id=eq.{b_id}&estado_actual=neq.TERMINADO", headers=HEADERS)
            # Insertamos el registro limpio y procesado
            requests.post(f"{SUPABASE_URL}/rest/v1/llegadas_valencia", headers=HEADERS, json=llegada)

        print("🎉 ¡Base de Datos sincronizada con ValenciaportPCS y el Diccionario de Servicios!")
    else:
        print("⚠️ Se han leído 0 barcos.")

if __name__ == "__main__":
    recolectar_valenciaport_pcs()