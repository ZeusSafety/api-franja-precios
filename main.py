import functions_framework
import pymysql
import json
import requests
import os
import logging
import pandas as pd

# ==========================================================
# CONFIGURACIÓN Y VARIABLES DE ENTORNO
# ==========================================================
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")
INSTANCE_CONNECTION_NAME = os.getenv("INSTANCE_CONNECTION_NAME")
API_TOKEN = "https://api-verificacion-token-2946605267.us-central1.run.app"

def get_connection():
    """Establece conexión con Cloud SQL vía Unix Socket"""
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
def get_handler(request, headers):
    conn = get_connection()
    mercado = request.args.get("mercado") # Ej: Malvinas_online
    
    if not mercado:
        return (json.dumps({"error": "Debe especificar el parámetro 'mercado'"}), 400, headers)

    try:
        with conn.cursor() as cursor:
            # Llama a tu procedimiento almacenado
            cursor.execute("CALL ListarProductosPorMercado(%s)", (mercado,))
            result = cursor.fetchall()
            return (json.dumps(result, default=str), 200, headers)
    except Exception as e:
        return (json.dumps({"error": str(e)}), 500, headers)
    finally:
        conn.close()

# ==========================================================
# HANDLER PARA POST (UPSERT Y NUEVOS PRODUCTOS)
# ==========================================================
def post_handler(request, headers):
    conn = get_connection()
    data = request.get_json(silent=True) or {}
    method = request.args.get("method", "").upper()

    try:
        with conn.cursor() as cursor:
            # CASO 1: ACTUALIZAR PRECIOS (Lógica Upsert con tu SP)
            if method == "ACTUALIZAR_PRECIOS_MERCADO":
                # Mapeo de parámetros para CALL ActualizarPreciosMercado
                params = (
                    data.get("mercado"),
                    data.get("codigo"),
                    data.get("docena"),
                    data.get("caja_1"),
                    data.get("caja_5"),
                    data.get("caja_10"),
                    data.get("caja_20"),
                    data.get("texto_copiar") # Puede ser NULL, tus triggers lo generarán
                )
                
                cursor.execute("CALL ActualizarPreciosMercado(%s, %s, %s, %s, %s, %s, %s, %s)", params)
                return (json.dumps({"success": True, "message": "Upsert procesado"}), 200, headers)

            # CASO 2: INSERTAR PRODUCTO BASE (Dispara el trigger de sincronización)
            elif method == "CREAR_PRODUCTO_BASE":
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
                # Nota: Tu TRIGGER 'despues_insertar_producto' creará las filas en las 4 tablas automáticamente.
                return (json.dumps({"success": True, "message": "Producto creado y mercados sincronizados"}), 201, headers)

            else:
                return (json.dumps({"error": "Método POST no reconocido"}), 400, headers)

    except Exception as e:
        return (json.dumps({"error": str(e)}), 500, headers)
    finally:
        conn.close()

# ==========================================================
# FUNCIÓN PRINCIPAL (ENTRY POINT)
# ==========================================================
@functions_framework.http
def crud_franja_precios(request):
    # 1. Configurar Headers CORS
    headers = {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
        "Access-Control-Allow-Headers": "Content-Type, Authorization"
    }

    if request.method == "OPTIONS":
        return ("", 204, headers)

    # 2. Validación de Token (Microservicio Externo)
    auth_header = request.headers.get("Authorization")
    if not auth_header:
        return (json.dumps({"error": "No se proporcionó token de autorización"}), 401, headers)

    try:
        val_resp = requests.post(API_TOKEN, headers={"Authorization": auth_header}, timeout=10)
        if val_resp.status_code != 200:
            return (json.dumps({"error": "Token inválido o expirado"}), 401, headers)
    except Exception as e:
        return (json.dumps({"error": f"Servicio de autenticación no disponible: {str(e)}"}), 503, headers)

    # 3. Routing de Métodos HTTP
    if request.method == "GET":
        return get_handler(request, headers)
    elif request.method == "POST":
        return post_handler(request, headers)
    
    return (json.dumps({"error": "Método no permitido"}), 405, headers)