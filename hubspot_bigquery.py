import json
import time
import os
import requests
from google.cloud import bigquery

# --- CONFIGURACIÓN DE ENTORNO ---
# Estas variables deben configurarse con la credencial de hubspot
HUBSPOT_API_KEY = os.getenv("HUBSPOT_MKT_ENV")
BIGQUERY_PROJECT = "kambista"

# Mapeo: Columna de BigQuery -> Propiedad Interna de HubSpot. Ya que son dos objetos distintos (empresa y contacto), se crean dos mapeos separados.
COMPANY_MAPPING = {
    "profile_id": "ruc",
    "socialIdentity": "name",
    "department": "city",
    "highRisk": "high_risk",
    "ticket_mean": "ticket_promedio",
    "current_pattern": "patron_operacional",
    "first_op_time_hubspot": "primera_operacion",
    "last_op_time_hubspot": "ultima_operacion"
}
CONTACT_MAPPING = {
    "fullname": "firstname",
    "email": "email",
    "phoneNumber_peru": "phone",
    "dni": "dni",
    "register_date_hubspot": "fecha_de_registro",
    "last_login_hubspot": "ultimo_loggin",
    "compliance_company_additionalInformation": "informacion_adicional_status__additionalinformation_",
    "iv_customer_status": "validacion_de_identidad__status_"
}

QUERY_STRING = (    #Esto es la consulta SQL que se va a ejecutar en BigQuery. Se seleccionan las columnas necesarias y se filtran los registros.
   # AQUI VA EL QUERY QUE CREA LA TABLA EN DONDE LA FUNCIÓN DE ENVÍO EXTRAERÁ LA INFORMACIÓN

def to_timestamp(date_str): # Convierte una fecha en formato "DD-MM-YYYY" a timestamp en milisegundos.
    from datetime import datetime, timezone
    try:
        dt = datetime.strptime(date_str, "%d-%m-%Y")
        dt = dt.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=timezone.utc)
        return int(dt.timestamp() * 1000)
    except Exception:
        return None

def fetch_data_from_bigquery(): # Conecta con BigQuery (autenticación automática) y extrae los datos.
    print("Iniciando conexión a BigQuery y consulta...")
    try:
        client = bigquery.Client(project=BIGQUERY_PROJECT)
        query_job = client.query(QUERY_STRING)
        records = [dict(row.items()) for row in query_job.result()]
        print(f"✅ Éxito: {len(records)} registros listos para sincronizar.")
        return records
    except Exception as e:
        print(f"❌ Error al consultar BigQuery: {e}")
        print("Usando datos simulados para pruebas locales...")
        
# 1. Primero obtiene el valor de profile_id (RUC) del registro.
def upsert_company(record, headers, company_mapping):
    profile_id = record.get("profile_id")
    if not profile_id:
        print("Saltando registro: profile_id (RUC) no encontrado.")
        return None
    search_url = "https://api.hubapi.com/crm/v3/objects/companies/search" # 2. Luego, realiza una búsqueda en HubSpot con el URL de husbpot para ver si ya existe una empresa con ese RUC.
    search_payload = {
        "filterGroups": [
            {"filters": [{"propertyName": "ruc", "operator": "EQ", "value": str(profile_id)}]} # 3. Crea un filtro en el que se busca el RUC igual al profile_id obtenido.
        ],
        "properties": list(company_mapping.values())
    }
    response = requests.post(search_url, headers=headers, data=json.dumps(search_payload)) # 4. Envía la solicitud POST a Hsubspot para ver si la empresa existe con su ruc.
    company_id = None
    if response.status_code == 200:
        results = response.json().get("results", [])
        if results:
            company_id = results[0]["id"] # 5. Si existen resultados, obtiene el ID de la empresa existente. Esto sirve para saber si se debe actualizar o crear una nueva empresa.
    company_props = {}
    for bq_col, hs_prop in company_mapping.items():
        if bq_col in record:
            val = record[bq_col]
            if hs_prop in ["primera_operacion", "ultima_operacion"]: # Si es fecha, convertir a timestamp para enviarlo a Husbpot
                ts = to_timestamp(val)
                if ts:
                    company_props[hs_prop] = ts
                else:
                    company_props[hs_prop] = val
            elif hs_prop == "ticket_promedio": # Si ticket_promedio es None, enviar campo vacío
                if val is None:
                    company_props[hs_prop] = ""
                else:
                    company_props[hs_prop] = str(val)
            elif hs_prop == "patron_operacional": # Acá en bigquery está con tilde, pero en HubSpot no. Así que lo ajusta para enviarlo.
                if val is not None:
                    if isinstance(val, str) and val.strip().lower() == "más de 2 meses":
                        company_props[hs_prop] = "Mas de 2 meses"
                    else:
                        company_props[hs_prop] = str(val)
            elif hs_prop == "high_risk": # Acá convierte los valores booleanos a "True"/"False" (Lo recibe en minúscula y lo pone en mayúscula porque la propieda es así) en string y los pone en la propiedad correspondiente, si no es ninguno de esos valores, lo deja igual.
                if isinstance(val, str):
                    if val.lower() == "true":
                        company_props[hs_prop] = "True"
                    elif val.lower() == "false":
                        company_props[hs_prop] = "False"
                    else:
                        company_props[hs_prop] = val
                else:
                    company_props[hs_prop] = val
            else:
                company_props[hs_prop] = str(val)
    payload = {"properties": company_props} # Con el company ID encontrado, actualiza la empresa
    if company_id:
        url = f"https://api.hubapi.com/crm/v3/objects/companies/{company_id}"
        resp = requests.patch(url, headers=headers, data=json.dumps(payload))
        if resp.status_code in (200, 204):
            print(f"Empresa actualizada (RUC/profile_id: {profile_id})")
        else:
            print(f"Error actualizando empresa (RUC/profile_id: {profile_id}): {resp.text}")
    else:
        url = "https://api.hubapi.com/crm/v3/objects/companies" # Si no existe, crea una nueva empresa
        resp = requests.post(url, headers=headers, data=json.dumps(payload))
        if resp.status_code in (200, 201):
            company_id = resp.json().get("id")
            print(f"Empresa creada (RUC/profile_id: {profile_id})")
        else:
            print(f"Error creando empresa (RUC/profile_id: {profile_id}): {resp.text}")
            company_id = None
    return company_id

def upsert_contact(record, headers, contact_mapping): # Busca y actualiza o crea un contacto en HubSpot basado en el DNI y email. EL DNI Y EL EMAIL SON CAMPOS ÚNICOS, por eso no hay repeticiones.
    dni = record.get("dni")
    email = record.get("email")
    if not dni and not email:
        print("Saltando registro: DNI y email no encontrados.")
        return None
    search_url = "https://api.hubapi.com/crm/v3/objects/contacts/search"
    filters = []
    if dni:
        filters.append({"propertyName": "dni", "operator": "EQ", "value": str(dni)}) # Busca por DNI, Operator y EQ significa "igual a"
    if email:
        filters.append({"propertyName": "email", "operator": "EQ", "value": str(email)}) # Busca por email
    search_payload = {"filterGroups": [{"filters": filters}], "properties": list(contact_mapping.values())}
    response = requests.post(search_url, headers=headers, data=json.dumps(search_payload))
    contact_id = None
    if response.status_code == 200:
        results = response.json().get("results", [])
        if results:
            contact_id = results[0]["id"]
    contact_props = {} # Prepara las propiedades del contacto para crear o actualizar
    for bq_col, hs_prop in contact_mapping.items():
        if bq_col in record:
            val = record[bq_col]
            if hs_prop in ["fecha_de_registro", "ultimo_loggin"]:
                ts = to_timestamp(val)
                if ts:
                    contact_props[hs_prop] = ts
                else:
                    contact_props[hs_prop] = val
            elif hs_prop in ["validacion_de_identidad__status_", "informacion_adicional_status__additionalinformation_"]:
                if val is not None:
                    try:
                        contact_props[hs_prop] = int(float(val))
                    except Exception:
                        contact_props[hs_prop] = val
            else:
                contact_props[hs_prop] = str(val)
    payload = {"properties": contact_props}
    if contact_id: # Si el contacto ya existe, lo actualiza
        url = f"https://api.hubapi.com/crm/v3/objects/contacts/{contact_id}"
        resp = requests.patch(url, headers=headers, data=json.dumps(payload))
        if resp.status_code in (200, 204):
            print(f"Contacto actualizado (DNI: {dni}, Email: {email})")
        else:
            print(f"Error actualizando contacto (DNI: {dni}, Email: {email}): {resp.text}")
    else:
        url = "https://api.hubapi.com/crm/v3/objects/contacts" # Si no existe, crea un nuevo contacto
        resp = requests.post(url, headers=headers, data=json.dumps(payload))
        if resp.status_code in (200, 201):
            contact_id = resp.json().get("id")
            print(f"Contacto creado (DNI: {dni}, Email: {email})")
        else:
            print(f"Error creando contacto (DNI: {dni}, Email: {email}): {resp.text}")
            contact_id = None
    return contact_id

def associate_company_contact(company_id, contact_id, headers): # Crea una asociación entre la empresa y el contacto en HubSpot. Por eso primero se crea la empresa y el contacto, y luego se asocian.
    if not company_id or not contact_id:
        print("No se puede asociar: faltan IDs.")
        return
    url = "https://api.hubapi.com/crm/v3/associations/contact/company/batch/create"
    payload = {
        "inputs": [
            {
                "from": {"id": contact_id},
                "to": {"id": company_id},
                "type": "contact_to_company"
            }
        ]
    }
    resp = requests.post(url, headers=headers, data=json.dumps(payload))
    if resp.status_code in (200, 201, 204):
        print(f"Asociación creada entre contacto {contact_id} y empresa {company_id}")
    else:
        print(f"Error creando asociación: {resp.text}")

def main():
    print("🤖 Iniciando Job de Sincronización BQ a HubSpot.")
    records_to_sync = fetch_data_from_bigquery()
    if not HUBSPOT_API_KEY:
        print("❌ Error: HUBSPOT_API_KEY no está configurada. Deteniendo proceso.")
        return
    if records_to_sync:
        headers = {
            "Authorization": f"Bearer {HUBSPOT_API_KEY}",
            "Content-Type": "application/json"
        }
        total = len(records_to_sync)
        for record in records_to_sync:
            company_id = upsert_company(record, headers, COMPANY_MAPPING)
            contact_id = upsert_contact(record, headers, CONTACT_MAPPING)
            if company_id and contact_id:
                associate_company_contact(company_id, contact_id, headers)
            time.sleep(0.5)
        print(f"\n--- RESUMEN DE PROCESO ---\nTotal de registros procesados: {total}")
    else:
        print("Finalizando. No se encontraron datos para sincronizar.")

if __name__ == "__main__":
    main()
