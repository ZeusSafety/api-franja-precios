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

# --- MÉTODO PARA LISTAR (GET) ---
def extraer_precios(request, headers):
    mercado = request.args.get("mercado")
    if not mercado:
        return (json.dumps({"error": "Falta parámetro 'mercado' en la URL"}), 400, headers)

    conn = get_connection()
    try:
        with conn.cursor() as cursor:
            # Llama al SP de listado
            cursor.execute("CALL ListarProductosPorMercado(%s)", (mercado,))
            registros = cursor.fetchall()
            return (json.dumps(registros, default=str), 200, headers)
    except Exception as e:
        return (json.dumps({"error": f"Error en DB: {str(e)}"}), 500, headers)
    finally:
        conn.close()

# --- MÉTODOS DE ESCRITURA (POST) ---
def procesar_post(request, headers):
    metodo = request.args.get("method")
    data = request.get_json(silent=True) or {}
    conn = get_connection()

    try:
        with conn.cursor() as cursor:
            # 1. ACTUALIZAR PRECIOS (Corregido el error del paréntesis)
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
                ) # <--- Parentesis cerrado correctamente
                
                cursor.execute("CALL ActualizarPreciosMercado(%s, %s, %s, %s, %s, %s, %s, %s)", params)
                return (json.dumps({"success": True, "message": "Precios actualizados exitosamente"}), 200, headers)

            # 2. CREAR PRODUCTO BASE (Retorna el ID nuevo)
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
                nuevo_id = cursor.lastrowid
                return (json.dumps({"success": True, "id": nuevo_id, "message": "Producto base creado"}), 201, headers)

            # 3. ELIMINAR PRODUCTO (Valida si realmente existía)
            elif metodo == "eliminar_producto":
                identificador = data.get("id") or data.get("codigo")
                if not identificador:
                    return (json.dumps({"error": "Falta id o codigo para eliminar"}), 400, headers)
                
                # Detecta si es ID numérico o Código de texto
                if str(identificador).isdigit():
                    sql = "DELETE FROM Productos_franja WHERE id = %s"
                else:
                    sql = "DELETE FROM Productos_franja WHERE Codigo = %s"
                
                cursor.execute(sql, (identificador,))
                
                if cursor.rowcount == 0:
                    return (json.dumps({"success": False, "message": f"No se encontró nada con: {identificador}"}), 404, headers)
                
                return (json.dumps({"success": True, "message": f"Eliminado correctamente: {identificador}"}), 200, headers)

            return (json.dumps({"error": f"Método '{metodo}' no es válido"}), 404, headers)

    except Exception as e:
        logging.error(f"Error en POST: {str(e)}")
        return (json.dumps({"error": str(e)}), 500, headers)
    finally:
        conn.close()

# --- ENTRY POINT PRINCIPAL ---
@functions_framework.http
def crud_franja_precios(request):
    # Configuración de CORS
    headers = {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
        "Access-Control-Allow-Headers": "Content-Type, Authorization"
    }

    # Responder a Preflight de Navegadores
    if request.method == "OPTIONS":
        return ("", 204, headers)

    # Validación de Seguridad (Token)
    auth_header = request.headers.get("Authorization")
    if not auth_header:
        return (json.dumps({"error": "No se proporcionó Token de acceso"}), 401, headers)

    try:
        val_resp = requests.post(API_TOKEN, headers={"Authorization": auth_header}, timeout=10)
        if val_resp.status_code != 200:
            return (json.dumps({"error": "Token inválido o expirado"}), 401, headers)
    except Exception as e:
        return (json.dumps({"error": f"Fallo servicio de autenticación: {str(e)}"}), 503, headers)

    # Ruteo de Métodos
    if request.method == "GET":
        return extraer_precios(request, headers)
    elif request.method == "POST":
        return procesar_post(request, headers)
    
    return (json.dumps({"error": "Método HTTP no soportado"}), 405, headers)