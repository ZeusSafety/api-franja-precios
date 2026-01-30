import functions_framework
import pymysql
import json
import requests
import os
import logging

# ==========================================================
# CONFIGURACIÓN Y VARIABLES DE ENTORNO
# ==========================================================
# Se obtienen de las variables de entorno de la Cloud Function
DB_USER = os.getenv("DB_USER", "zeussafety-2024")
DB_PASSWORD = os.getenv("DB_PASSWORD", "ZeusSafety2025")
DB_NAME = os.getenv("DB_NAME", "Zeus_Safety_Data_Integration")
INSTANCE_CONNECTION_NAME = os.getenv("INSTANCE_CONNECTION_NAME", "stable-smithy-435414-m6:us-central1:zeussafety-2024")
API_TOKEN = "https://api-verificacion-token-2946605267.us-central1.run.app"

def get_connection():
    """Establece conexión con Cloud SQL vía Unix Socket (estándar Zeus)"""
    return pymysql.connect(
        user=DB_USER,
        password=DB_PASSWORD,
        unix_socket=f"/cloudsql/{INSTANCE_CONNECTION_NAME}",
        db=DB_NAME,
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=True
    )

# ==========================================================
# HANDLER PARA GET (LISTAR POR MERCADO)
# ==========================================================
def extraer_precios(request, headers):
    mercado = request.args.get("mercado")
    if not mercado:
        return (json.dumps({"error": "Falta parámetro 'mercado'"}), 400, headers)

    conn = get_connection()
    try:
        with conn.cursor() as cursor:
            # Llama al procedimiento almacenado de listado
            cursor.execute("CALL ListarProductosPorMercado(%s)", (mercado,))
            registros = cursor.fetchall()
            return (json.dumps(registros, default=str), 200, headers)
    except Exception as e:
        logging.error(f"Error en extraer_precios: {e}")
        return (json.dumps({"error": str(e)}), 500, headers)
    finally:
        conn.close()

# ==========================================================
# HANDLER PARA POST (INSERTAR / ACTUALIZAR)
# ==========================================================
def procesar_post(request, headers):
    metodo = request.args.get("method")
    data = request.get_json(silent=True) or {}
    conn = get_connection()

    try:
        with conn.cursor() as cursor:
            # CASO A: ACTUALIZAR PRECIOS (Lógica Upsert)
            if metodo == "actualizar_precios_mercado":
                params = (
                    data.get("mercado"),
                    data.get("codigo"),
                    data.get("docena"),
                    data.get("caja_1"),
                    data.get("caja_5"),
                    data.get("caja_10"),
                    data.get("caja_20"),
                    data.get("texto_copiar") # Puede ser None para activar el Trigger
                )
                cursor.execute("CALL ActualizarPreciosMercado(%s, %s, %s, %s, %s, %s, %s, %s)", params)
                return (json.dumps({"success": True, "message": "Precios actualizados"}), 200, headers)

            # CASO B: CREAR PRODUCTO BASE
            elif metodo == "crear_producto_base":
                sql = """
                    INSERT INTO Productos_franja (Codigo, Producto, Cantidad_En_Caja, ficha_tecnica)
                    VALUES (%s, %s, %s, %s)
                """
                cursor.execute(sql, (
                    data.get("codigo"),
                    data.get("producto"),
                    data.get("cantidad_caja"),
                    data.get("ficha_tecnica")
                ))
                # Tu TRIGGER 'despues_insertar_producto' se encarga del resto
                return (json.dumps({"success": True, "message": "Producto base creado"}), 201, headers)

            return (json.dumps({"error": "Método POST no reconocido"}), 404, headers)

    except Exception as e:
        logging.error(f"Error en procesar_post: {e}")
        return (json.dumps({"error": str(e)}), 500, headers)
    finally:
        conn.close()

# ==========================================================
# FUNCIÓN PRINCIPAL (ENTRY POINT)
# ==========================================================
@functions_framework.http
def crud_franja_precios(request):
    headers = {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
        "Access-Control-Allow-Headers": "Content-Type, Authorization"
    }

    if request.method == "OPTIONS":
        return ("", 204, headers)

    # Autenticación Zeus
    auth_header = request.headers.get("Authorization")
    if not auth_header:
        return (json.dumps({"error": "No token"}), 401, headers)

    try:
        val_resp = requests.post(API_TOKEN, headers={"Authorization": auth_header}, timeout=10)
        if val_resp.status_code != 200:
            return (json.dumps({"error": "Token inválido"}), 401, headers)
    except Exception as e:
        return (json.dumps({"error": f"Error auth: {str(e)}"}), 503, headers)

    # Enrutamiento
    if request.method == "GET":
        return extraer_precios(request, headers)
    elif request.method == "POST":
        return procesar_post(request, headers)
    
    return (json.dumps({"error": "Method Not Allowed"}), 405, headers)