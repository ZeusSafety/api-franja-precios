import functions_framework
import pymysql
import json
import requests
import os
import logging

# ==========================================================
# CONFIGURACIÓN (Zeus Safety Standard)
# ==========================================================
DB_USER = "zeussafety-2024"
DB_PASSWORD = "ZeusSafety2025"
DB_NAME = "Zeus_Safety_Data_Integration"
INSTANCE_CONNECTION_NAME = "stable-smithy-435414-m6:us-central1:zeussafety-2024"
API_TOKEN = "https://api-verificacion-token-2946605267.us-central1.run.app"

def get_connection():
    """Establece conexión con Cloud SQL"""
    return pymysql.connect(
        user=DB_USER,
        password=DB_PASSWORD,
        unix_socket=f"/cloudsql/{INSTANCE_CONNECTION_NAME}",
        db=DB_NAME,
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=True
    )

# --- LISTAR ---
def extraer_precios(request, headers):
    mercado = request.args.get("mercado")
    if not mercado:
        return (json.dumps({"error": "Falta parámetro 'mercado'"}), 400, headers)

    conn = get_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute("CALL ListarProductosPorMercado(%s)", (mercado,))
            registros = cursor.fetchall()
            return (json.dumps(registros, default=str), 200, headers)
    except Exception as e:
        return (json.dumps({"error": str(e)}), 500, headers)
    finally:
        conn.close()

# --- INSERTAR, ACTUALIZAR, ELIMINAR ---
def procesar_post(request, headers):
    metodo = request.args.get("method")
    data = request.get_json(silent=True) or {}
    conn = get_connection()

    try:
        with conn.cursor() as cursor:
            # 1. ACTUALIZAR
            if metodo == "actualizar_precios_mercado":
                params = (
                    data.get("mercado"),
                    data.get("codigo"),
                    data.get("docena"),
                    data.get("caja_1"),
                    data.get("caja_5"),
                    data.get("caja_10"),
                    data.get("caja_20"),
                    data.get("texto_copiar")
                )
                cursor.execute("CALL ActualizarPreciosMercado(%s, %s, %s, %s, %s, %s, %s, %s)", params)
                return (json.dumps({"success": True, "message": "Precios actualizados"}), 200, headers)

            # 2. CREAR (Devuelve el ID generado)
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
                nuevo_id = cursor.lastrowid # <--- Aquí obtienes el ID que pedías
                return (json.dumps({"success": True, "id": nuevo_id, "message": "Producto creado"}), 201, headers)

            # 3. ELIMINAR (Mejorado para validar existencia)
            elif metodo == "eliminar_producto":
                identificador = data.get("id") or data.get("codigo")
                if not identificador:
                    return (json.dumps({"error": "Falta id o codigo"}), 400, headers)
                
                if str(identificador).isdigit():
                    sql = "DELETE FROM Productos_franja WHERE id = %s"
                else:
                    sql = "DELETE FROM Productos_franja WHERE Codigo = %s"
                
                cursor.execute(sql, (identificador,))
                
                # VERIFICACIÓN DE FILAS AFECTADAS
                if cursor.rowcount == 0:
                    return (json.dumps({
                        "success": False, 
                        "message": f"No se encontró ningún producto con el identificador: {identificador}"
                    }), 404, headers)
                else:
                    return (json.dumps({
                        "success": True, 
                        "message": f"Producto {identificador} eliminado correctamente"
                    }), 200, headers)

# --- ENTRY POINT ---
@functions_framework.http
def crud_franja_precios(request):
    headers = {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
        "Access-Control-Allow-Headers": "Content-Type, Authorization"
    }

    if request.method == "OPTIONS":
        return ("", 204, headers)

    auth_header = request.headers.get("Authorization")
    if not auth_header:
        return (json.dumps({"error": "No token"}), 401, headers)

    # Validación de Token
    try:
        val_resp = requests.post(API_TOKEN, headers={"Authorization": auth_header}, timeout=10)
        if val_resp.status_code != 200:
            return (json.dumps({"error": "Token inválido"}), 401, headers)
    except:
        return (json.dumps({"error": "Error de autenticacion"}), 503, headers)

    if request.method == "GET":
        return extraer_precios(request, headers)
    elif request.method == "POST":
        return procesar_post(request, headers)
    
    return (json.dumps({"error": "Metodo no permitido"}), 405, headers)