from fastapi import FastAPI, UploadFile, File, Form, BackgroundTasks, HTTPException, Depends, Header, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, RedirectResponse, StreamingResponse
from botocore.config import Config
from pydantic import BaseModel
from typing import Optional, List, Dict, Any
from dotenv import load_dotenv
from datetime import datetime
import boto3
import json
import httpx
import os
import uuid
import zipfile
from supabase import create_client

load_dotenv()

app = FastAPI(title="Kusipix API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ─── CLIENTES AWS ─────────────────────────────────────────────────────────────

rekognition = boto3.client(
    "rekognition",
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name=os.getenv("AWS_REGION", "us-east-1")
)

sqs = boto3.client(
    "sqs",
    region_name=os.getenv("AWS_REGION", "us-east-1"),
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY")
)

SQS_QUEUE_URL = os.getenv("SQS_QUEUE_URL", "")

# ─── SUPABASE (service role para operaciones admin) ───────────────────────────

supabase = create_client(
    os.getenv("SUPABASE_URL"),
    os.getenv("SUPABASE_SERVICE_KEY")  # service role key para bypass RLS
)

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_ANON_KEY = os.getenv("SUPABASE_ANON_KEY")
BACKEND_URL = os.getenv("BACKEND_URL", "https://api.kusipix.com")
REKOGNITION_COLLECTION = os.getenv("REKOGNITION_COLLECTION", "kusipix-faces")

# ─── AUTH: Verificar token JWT de Supabase ────────────────────────────────────

async def get_current_user(authorization: str = Header(None)):
    """Extrae el usuario autenticado del token Bearer de Supabase"""
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="No autorizado")
    token = authorization.replace("Bearer ", "")
    try:
        # Verificar token con Supabase
        user_client = create_client(SUPABASE_URL, SUPABASE_ANON_KEY)
        user_client.auth._session = None
        # Usar el token para obtener el usuario
        resp = httpx.get(
            f"{SUPABASE_URL}/auth/v1/user",
            headers={"Authorization": f"Bearer {token}", "apikey": SUPABASE_ANON_KEY}
        )
        if resp.status_code != 200:
            raise HTTPException(status_code=401, detail="Token inválido")
        user_data = resp.json()
        return user_data
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=401, detail=f"Error de autenticación: {str(e)}")

async def get_fotografo(user=Depends(get_current_user)):
    """Obtiene el perfil del fotógrafo a partir del usuario autenticado"""
    result = supabase.table("fotografos").select("*").eq("auth_user_id", user["id"]).single().execute()
    if not result.data:
        raise HTTPException(status_code=404, detail="Perfil de fotógrafo no encontrado")
    return result.data

# ─── HEALTH CHECK ─────────────────────────────────────────────────────────────

@app.get("/")
def health():
    return {"status": "ok", "platform": "Kusipix", "version": "1.0.0"}

# ─── SUBIDA DE FOTOS (Presigned URL → Supabase Storage) ──────────────────────

class GenerarUrlSubidaRequest(BaseModel):
    filename: str
    content_type: str
    evento_id: str

@app.post("/api/generar-url-subida")
async def generar_url_subida(body: GenerarUrlSubidaRequest, fotografo=Depends(get_fotografo)):
    """Genera URL presignada para subir foto original a Supabase Storage"""
    ext = body.filename.rsplit(".", 1)[-1].lower()
    foto_id = str(uuid.uuid4())
    path = f"{fotografo['id']}/{body.evento_id}/{foto_id}.{ext}"

    # Verificar créditos
    if (fotografo.get("creditos_disponibles") or 0) < 1:
        return JSONResponse(status_code=400, content={"error": "Créditos insuficientes"})

    # Verificar que el evento pertenece al fotógrafo
    ev = supabase.table("eventos").select("id").eq("id", body.evento_id).eq("fotografo_id", fotografo["id"]).execute()
    if not ev.data:
        return JSONResponse(status_code=403, content={"error": "Evento no encontrado"})

    # Generar URL presignada para Supabase Storage
    result = supabase.storage.from_("fotos-originales").create_signed_upload_url(path)

    return {
        "upload_url": result.get("signedUrl") or result.get("signed_url"),
        "path": path,
        "foto_id": foto_id,
        "token": result.get("token"),
    }

class ConfirmarSubidaRequest(BaseModel):
    path: str
    filename: str
    evento_id: str
    foto_id: str
    tamano_bytes: Optional[int] = None
    album_id: Optional[str] = None

@app.post("/api/confirmar-subida")
async def confirmar_subida(body: ConfirmarSubidaRequest, background_tasks: BackgroundTasks, fotografo=Depends(get_fotografo)):
    """Confirma la subida y encola el procesamiento"""
    # Verificar créditos de nuevo
    creditos = fotografo.get("creditos_disponibles") or 0
    if creditos < 1:
        return JSONResponse(status_code=400, content={"error": "Créditos insuficientes"})

    # Verificar evento
    ev = supabase.table("eventos").select("*").eq("id", body.evento_id).eq("fotografo_id", fotografo["id"]).execute()
    if not ev.data:
        return JSONResponse(status_code=403, content={"error": "Evento no encontrado"})
    evento = ev.data[0]

    # Insertar foto en DB
    foto_data = {
        "id": body.foto_id,
        "evento_id": body.evento_id,
        "fotografo_id": fotografo["id"],
        "url_original": body.path,
        "nombre_archivo": body.filename,
        "tamano_bytes": body.tamano_bytes,
        "procesada": False,
        "tiene_rostros": False,
        "cantidad_rostros": 0,
        "face_ids": [],
    }
    if body.album_id:
        foto_data["album_id"] = body.album_id

    supabase.table("fotos").insert(foto_data).execute()

    # Descontar crédito
    supabase.table("fotografos").update({
        "creditos_disponibles": creditos - 1
    }).eq("id", fotografo["id"]).execute()

    supabase.table("transacciones_creditos").insert({
        "fotografo_id": fotografo["id"],
        "tipo": "consumo",
        "cantidad": -1,
        "saldo_despues": creditos - 1,
        "descripcion": f"Foto: {body.filename}"
    }).execute()

    # Actualizar contador del evento
    supabase.table("eventos").update({
        "total_fotos": (evento.get("total_fotos") or 0) + 1
    }).eq("id", body.evento_id).execute()

    # Encolar procesamiento (marca de agua + reconocimiento facial)
    if SQS_QUEUE_URL:
        try:
            sqs.send_message(
                QueueUrl=SQS_QUEUE_URL,
                MessageBody=json.dumps({
                    "foto_id": body.foto_id,
                    "path": body.path,
                    "filename": body.filename,
                    "evento_id": body.evento_id,
                    "fotografo_id": fotografo["id"],
                    "modo_busqueda": evento.get("modo_busqueda", "facial_dorsal"),
                    "marca_agua_url": fotografo.get("marca_agua_url"),
                    "usar_marca_plataforma": not fotografo.get("marca_agua_url"),
                })
            )
        except Exception as e:
            print(f"SQS error: {e}")
            # Procesar en background si SQS falla
            background_tasks.add_task(procesar_foto_inline, body.foto_id, body.path, body.filename, body.evento_id, fotografo["id"], evento.get("modo_busqueda"), fotografo.get("marca_agua_url"))

    return {"foto_id": body.foto_id, "estado": "en_cola", "creditos_restantes": creditos - 1}

class ConfirmarSubidaLoteRequest(BaseModel):
    fotos: List[Dict[str, Any]]
    evento_id: str

@app.post("/api/confirmar-subida-lote")
async def confirmar_subida_lote(body: ConfirmarSubidaLoteRequest, fotografo=Depends(get_fotografo)):
    """Confirma subida en lote y encola procesamiento"""
    creditos = fotografo.get("creditos_disponibles") or 0
    n = len(body.fotos)

    if creditos < n:
        return JSONResponse(status_code=400, content={
            "error": f"Créditos insuficientes. Tienes {creditos}, necesitas {n}."
        })

    ev = supabase.table("eventos").select("*").eq("id", body.evento_id).eq("fotografo_id", fotografo["id"]).execute()
    if not ev.data:
        return JSONResponse(status_code=403, content={"error": "Evento no encontrado"})
    evento = ev.data[0]

    # Insert en lote
    filas = []
    for f in body.fotos:
        fila = {
            "id": f.get("foto_id", str(uuid.uuid4())),
            "evento_id": body.evento_id,
            "fotografo_id": fotografo["id"],
            "url_original": f["path"],
            "nombre_archivo": f["filename"],
            "tamano_bytes": f.get("tamano_bytes"),
            "procesada": False,
            "tiene_rostros": False,
            "cantidad_rostros": 0,
            "face_ids": [],
        }
        if f.get("album_id"):
            fila["album_id"] = f["album_id"]
        filas.append(fila)

    resultado = supabase.table("fotos").insert(filas).execute()
    insertadas = resultado.data or []

    # Descontar créditos
    nuevo_saldo = creditos - len(insertadas)
    supabase.table("fotografos").update({
        "creditos_disponibles": nuevo_saldo
    }).eq("id", fotografo["id"]).execute()

    supabase.table("transacciones_creditos").insert({
        "fotografo_id": fotografo["id"],
        "tipo": "consumo",
        "cantidad": -len(insertadas),
        "saldo_despues": nuevo_saldo,
        "descripcion": f"Lote: {len(insertadas)} fotos - {evento.get('nombre', '')}"
    }).execute()

    # Actualizar contador
    supabase.table("eventos").update({
        "total_fotos": (evento.get("total_fotos") or 0) + len(insertadas)
    }).eq("id", body.evento_id).execute()

    # Encolar en SQS por lotes de 10
    if SQS_QUEUE_URL:
        mensajes = []
        for i, fila in enumerate(insertadas):
            original = body.fotos[i] if i < len(body.fotos) else {}
            mensajes.append({
                "Id": str(i),
                "MessageBody": json.dumps({
                    "foto_id": fila["id"],
                    "path": fila["url_original"],
                    "filename": fila["nombre_archivo"],
                    "evento_id": body.evento_id,
                    "fotografo_id": fotografo["id"],
                    "modo_busqueda": evento.get("modo_busqueda", "facial_dorsal"),
                    "marca_agua_url": fotografo.get("marca_agua_url"),
                    "usar_marca_plataforma": not fotografo.get("marca_agua_url"),
                })
            })
        enviados = 0
        for i in range(0, len(mensajes), 10):
            chunk = mensajes[i:i+10]
            try:
                sqs.send_message_batch(QueueUrl=SQS_QUEUE_URL, Entries=chunk)
                enviados += len(chunk)
            except Exception as e:
                print(f"SQS batch error: {e}")

    return {
        "insertadas": len(insertadas),
        "creditos_restantes": nuevo_saldo,
    }

# ─── CHEQUEO DE DUPLICADOS ────────────────────────────────────────────────────

class ChequearDuplicadosRequest(BaseModel):
    evento_id: str
    archivos: List[Dict[str, Any]]

@app.post("/api/chequear-duplicados")
async def chequear_duplicados(body: ChequearDuplicadosRequest, fotografo=Depends(get_fotografo)):
    """Verifica duplicados por nombre + tamaño"""
    if not body.archivos:
        return {"duplicados": {}}

    nombres = list({a.get("nombre", "").strip() for a in body.archivos if a.get("nombre")})
    if not nombres:
        return {"duplicados": {}}

    coincidencias = []
    for i in range(0, len(nombres), 500):
        chunk = nombres[i:i+500]
        r = supabase.table("fotos").select("nombre_archivo, tamano_bytes").eq("evento_id", body.evento_id).in_("nombre_archivo", chunk).execute()
        coincidencias.extend(r.data or [])

    por_nombre = {}
    for row in coincidencias:
        n = (row.get("nombre_archivo") or "").strip()
        t = row.get("tamano_bytes")
        por_nombre.setdefault(n, []).append(t)

    duplicados = {}
    for a in body.archivos:
        n = (a.get("nombre") or "").strip()
        t = a.get("tamano")
        if not n or n not in por_nombre:
            continue
        tamanos = por_nombre[n]
        if t is not None and any(te == t for te in tamanos if te is not None):
            duplicados[n] = {"tipo": "exacto"}
        elif all(te is None for te in tamanos):
            duplicados[n] = {"tipo": "solo_nombre"}

    return {"duplicados": duplicados}

# ─── PROCESAMIENTO DE FOTOS (inline, backup si SQS falla) ────────────────────

async def procesar_foto_inline(foto_id, path, filename, evento_id, fotografo_id, modo_busqueda, marca_agua_url):
    """Procesa foto sin SQS - genera preview con marca de agua y detecta caras/dorsales"""
    try:
        # Descargar foto original desde Supabase Storage
        data = supabase.storage.from_("fotos-originales").download(path)
        if not data:
            return

        # Generar preview con marca de agua usando Pillow
        from PIL import Image, ImageDraw, ImageFont
        import io

        img = Image.open(io.BytesIO(data))
        draw = ImageDraw.Draw(img)

        # Marca de agua de texto simple (se mejora con logo después)
        w, h = img.size
        texto = "© Kusipix"
        try:
            font = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", max(24, w // 30))
        except Exception:
            font = ImageFont.load_default()

        # Marca de agua diagonal repetida
        for y in range(0, h, h // 4):
            for x in range(0, w, w // 3):
                draw.text((x, y), texto, fill=(255, 255, 255, 80), font=font)

        # Resize preview (max 1200px)
        max_dim = 1200
        if w > max_dim or h > max_dim:
            ratio = min(max_dim / w, max_dim / h)
            img = img.resize((int(w * ratio), int(h * ratio)), Image.LANCZOS)

        # Guardar preview
        preview_buf = io.BytesIO()
        img.save(preview_buf, "JPEG", quality=85)
        preview_buf.seek(0)

        preview_path = f"{fotografo_id}/{evento_id}/{foto_id}_preview.jpg"
        try:
            supabase.storage.from_("fotos-preview").upload(
                preview_path, preview_buf.getvalue(), {"content-type": "image/jpeg"}
            )
        except Exception:
            # Si ya existe, sobreescribir
            supabase.storage.from_("fotos-preview").update(
                preview_path, preview_buf.getvalue(), {"content-type": "image/jpeg"}
            )

        preview_url = f"{SUPABASE_URL}/storage/v1/object/public/fotos-preview/{preview_path}"

        # Reconocimiento facial
        face_ids = []
        cantidad_rostros = 0
        dorsales = []

        if modo_busqueda in ("facial", "facial_dorsal"):
            try:
                resp = rekognition.index_faces(
                    CollectionId=REKOGNITION_COLLECTION,
                    Image={"Bytes": data},
                    ExternalImageId=foto_id,
                    DetectionAttributes=["DEFAULT"],
                    MaxFaces=10,
                )
                for face in resp.get("FaceRecords", []):
                    face_ids.append(face["Face"]["FaceId"])
                cantidad_rostros = len(face_ids)
            except Exception as e:
                print(f"Rekognition IndexFaces error: {e}")

        if modo_busqueda in ("dorsal", "facial_dorsal"):
            try:
                resp = rekognition.detect_text(Image={"Bytes": data})
                for det in resp.get("TextDetections", []):
                    if det["Type"] == "LINE":
                        texto_det = det["DetectedText"].strip()
                        if texto_det.isdigit() and 1 <= len(texto_det) <= 5:
                            dorsales.append(texto_det)
            except Exception as e:
                print(f"Rekognition DetectText error: {e}")

        # Actualizar foto en DB
        update_data = {
            "url_preview": preview_url,
            "procesada": True,
            "tiene_rostros": cantidad_rostros > 0,
            "cantidad_rostros": cantidad_rostros,
            "face_ids": face_ids,
            "ancho": w,
            "alto": h,
        }
        if dorsales:
            update_data["dorsal"] = dorsales[0]  # primer dorsal detectado

        supabase.table("fotos").update(update_data).eq("id", foto_id).execute()

        # Verificar si es la última foto pendiente del evento
        try:
            pendientes = supabase.table("fotos").select("id", count="exact").eq("evento_id", evento_id).eq("procesada", False).execute()
            if (pendientes.count or 0) == 0:
                # Todas procesadas — notificar al fotógrafo si tiene esa preferencia
                fot_pref = supabase.table("fotografos").select("email, nombre, notif_procesamiento").eq("id", fotografo_id).execute().data
                if fot_pref and fot_pref[0].get("notif_procesamiento") is not False:
                    fot = fot_pref[0]
                    ev = supabase.table("eventos").select("nombre, total_fotos").eq("id", evento_id).execute().data
                    ev_nombre = ev[0]["nombre"] if ev else "tu evento"
                    total = ev[0].get("total_fotos", 0) if ev else 0
                    resend_key = os.getenv("RESEND_API_KEY", "")
                    if resend_key:
                        import httpx as hx2
                        hx2.post("https://api.resend.com/emails",
                            headers={"Authorization": f"Bearer {resend_key}"},
                            json={
                                "from": "Kusipix <noreply@kusipix.com>",
                                "to": [fot["email"]],
                                "subject": f"✅ Fotos de {ev_nombre} listas para el público",
                                "html": f'''<div style="font-family:sans-serif;max-width:580px;margin:0 auto;background:#0f1117;color:#e2e8f0;border-radius:16px;overflow:hidden">
                                    <div style="background:linear-gradient(135deg,#22c55e,#3b82f6);padding:32px;text-align:center">
                                        <div style="font-size:48px;margin-bottom:8px">✅</div>
                                        <h2 style="color:white;margin:0;font-size:22px">Fotos procesadas</h2>
                                    </div>
                                    <div style="padding:28px">
                                        <p style="font-size:15px;color:#8892a4;margin:0 0 16px">Hola {fot["nombre"]},</p>
                                        <p style="font-size:15px;color:#e2e8f0;margin:0 0 20px">Las <strong>{total} fotos</strong> de <strong>{ev_nombre}</strong> ya están procesadas y disponibles para tus clientes.</p>
                                        <div style="text-align:center">
                                            <a href="https://kusipix.com/panel" style="display:inline-block;background:#22c55e;color:white;padding:12px 28px;border-radius:8px;text-decoration:none;font-size:14px;font-weight:600">Ver mi evento</a>
                                        </div>
                                    </div>
                                    <div style="background:#13151e;padding:16px 28px;text-align:center;border-top:1px solid #2a2d3a">
                                        <p style="font-size:12px;color:#64748b;margin:0">Kusipix · kusipix.com</p>
                                    </div>
                                </div>'''
                            },
                            timeout=10
                        )
        except Exception as notif_err:
            print(f"Error notif procesamiento: {notif_err}")

    except Exception as e:
        print(f"Error procesando foto {foto_id}: {e}")
        supabase.table("fotos").update({"procesada": True}).eq("id", foto_id).execute()

class CrearPagoRequest(BaseModel):
    evento_id: str
    foto_ids: List[str]
    nombre: Optional[str] = None
    email: Optional[str] = None
    telefono: Optional[str] = None
    cupon_codigo: Optional[str] = None
    comprar_todas: bool = False

def calcular_monto_venta(evento: dict, foto_ids: list, cupon_codigo: Optional[str], comprar_todas: bool):
    """
    Calcula el monto final de una venta aplicando, en orden excluyente:
    1. Pack completo (precio_todas) si comprar_todas=True
    2. Cupón si se entrega cupon_codigo valido
    3. Descuento por cantidad (tramos configurados por el fotografo)
    Retorna dict con: monto, tipo_compra, cupon_id, descuento_aplicado, error (si aplica)
    """
    n = len(foto_ids)
    precio_foto = evento.get("precio_foto") or 0

    # 1. Pack completo (excluyente con todo lo demas)
    if comprar_todas:
        precio_todas = evento.get("precio_todas")
        if not precio_todas:
            return {"error": "Este evento no tiene pack completo configurado"}
        return {
            "monto": precio_todas,
            "tipo_compra": "todas",
            "cupon_id": None,
            "cupon_codigo": None,
            "descuento_aplicado": 0,
        }

    monto_base = precio_foto * n

    # 2. Cupon (excluyente con descuento por cantidad)
    if cupon_codigo:
        res = supabase.rpc("aplicar_cupon", {
            "p_evento_id": evento["id"],
            "p_codigo": cupon_codigo,
            "p_monto_original": monto_base,
        }).execute()
        data = res.data or {}
        if not data.get("valido"):
            return {"error": data.get("error", "Cupón inválido")}
        return {
            "monto": data["monto_final"],
            "tipo_compra": "individual" if n == 1 else "pack",
            "cupon_id": data["cupon_id"],
            "cupon_codigo": data["codigo"],
            "descuento_aplicado": data["descuento"],
        }

    # 3. Descuento por cantidad
    porcentaje = supabase.rpc("calcular_descuento_cantidad", {
        "p_evento_id": evento["id"],
        "p_fotografo_id": evento["fotografo_id"],
        "p_cantidad": n,
    }).execute()
    pct = (porcentaje.data or 0) if porcentaje.data is not None else 0
    descuento = round(monto_base * pct / 100)
    return {
        "monto": monto_base - descuento,
        "tipo_compra": "individual" if n == 1 else "pack",
        "cupon_id": None,
        "cupon_codigo": None,
        "descuento_aplicado": descuento,
    }

# ─── PAGO CON FLOW ───────────────────────────────────────────────────────────

import hmac
import hashlib

def flow_sign(params: dict, secret: str) -> str:
    """Genera firma HMAC-SHA256 para Flow"""
    keys = sorted(params.keys())
    to_sign = "".join(f"{k}{params[k]}" for k in keys)
    return hmac.new(secret.encode(), to_sign.encode(), hashlib.sha256).hexdigest()

@app.post("/api/pago/flow/crear")
def crear_pago_flow(body: CrearPagoRequest):
    """Crea pago con Flow usando credenciales del fotógrafo"""
    ev = supabase.table("eventos").select("*").eq("id", body.evento_id).execute().data
    if not ev:
        return JSONResponse(status_code=404, content={"error": "Evento no encontrado"})
    evento = ev[0]

    fot = supabase.table("fotografos").select(
        "flow_api_key, flow_secret_key, flow_ambiente"
    ).eq("id", evento["fotografo_id"]).execute()
    if not fot.data:
        return JSONResponse(status_code=400, content={"error": "Fotógrafo no encontrado"})
    creds = fot.data[0]

    if not creds.get("flow_api_key") or not creds.get("flow_secret_key"):
        return JSONResponse(status_code=400, content={"error": "Flow no configurado"})

    if not body.comprar_todas and len(body.foto_ids) == 0:
        return JSONResponse(status_code=400, content={"error": "No hay fotos seleccionadas"})

    calculo = calcular_monto_venta(evento, body.foto_ids, body.cupon_codigo, body.comprar_todas)
    if calculo.get("error"):
        return JSONResponse(status_code=400, content={"error": calculo["error"]})
    monto = calculo["monto"]
    n = len(body.foto_ids)
    buy_order = "KF" + uuid.uuid4().hex[:16]

    # Ambiente
    if creds.get("flow_ambiente") == "production":
        flow_url = "https://www.flow.cl/api"
    else:
        flow_url = "https://sandbox.flow.cl/api"

    api_key = creds["flow_api_key"]
    secret = creds["flow_secret_key"]
    url_retorno = f"{BACKEND_URL}/api/pago/flow/retorno"

    params = {
        "apiKey": api_key,
        "commerceOrder": buy_order,
        "subject": f"Fotos evento - {evento.get('nombre', 'Kusipix')}",
        "currency": "CLP",
        "amount": int(monto),
        "email": body.email or "",
        "urlConfirmation": url_retorno,
        "urlReturn": url_retorno,
        "paymentMethod": 9,  # Todos los medios
    }
    params["s"] = flow_sign(params, secret)

    # Crear orden en Flow
    resp = httpx.post(f"{flow_url}/payment/create", data=params, timeout=30)
    if resp.status_code != 200:
        return JSONResponse(status_code=400, content={"error": f"Error Flow: {resp.text}"})

    data = resp.json()
    if data.get("code") and data["code"] != 0:
        return JSONResponse(status_code=400, content={"error": data.get("message", "Error Flow")})

    flow_token = data.get("token")
    redirect_url = data.get("url") + "?token=" + flow_token

    # En pack completo, foto_ids debe venir con TODAS las fotos del participante
    # (ya calculadas por el frontend via la busqueda facial/dorsal previa)
    foto_ids_finales = body.foto_ids

    # Registrar venta
    venta_result = supabase.table("ventas").insert({
        "evento_id": body.evento_id,
        "fotografo_id": evento["fotografo_id"],
        "comprador_nombre": body.nombre,
        "comprador_email": body.email,
        "monto_total": monto,
        "metodo_pago": "flow",
        "referencia_pago": flow_token,
        "estado": "pendiente",
        "tipo_compra": calculo["tipo_compra"],
        "cantidad_fotos": len(foto_ids_finales),
        "cupon_usado": calculo.get("cupon_codigo"),
        "descuento_aplicado": calculo.get("descuento_aplicado", 0),
    }).execute()

    if venta_result.data and foto_ids_finales:
        venta_id = venta_result.data[0]["id"]
        precio_unitario = monto // len(foto_ids_finales) if len(foto_ids_finales) else 0
        supabase.table("venta_fotos").insert([
            {"venta_id": venta_id, "foto_id": fid, "precio": precio_unitario}
            for fid in foto_ids_finales
        ]).execute()

    return {"url": redirect_url, "token": flow_token, "monto": monto}

@app.get("/api/pago/flow/retorno")
@app.post("/api/pago/flow/retorno")
async def pago_flow_retorno(token: str = None):
    """Retorno de Flow después del pago"""
    if not token:
        return RedirectResponse("https://kusipix.com?pago=rechazado", status_code=303)

    # Buscar venta
    venta = supabase.table("ventas").select("*, eventos(slug, fotografo_id)").eq("referencia_pago", token).execute().data
    if not venta:
        return RedirectResponse("https://kusipix.com?pago=error", status_code=303)
    v = venta[0]

    fot_id = v.get("fotografo_id")
    creds = supabase.table("fotografos").select("flow_api_key, flow_secret_key, flow_ambiente").eq("id", fot_id).execute().data
    cred = creds[0] if creds else {}

    if cred.get("flow_ambiente") == "production":
        flow_url = "https://www.flow.cl/api"
    else:
        flow_url = "https://sandbox.flow.cl/api"

    api_key = cred.get("flow_api_key", "")
    secret = cred.get("flow_secret_key", "")

    params = {"apiKey": api_key, "token": token}
    params["s"] = flow_sign(params, secret)

    resp = httpx.get(f"{flow_url}/payment/getStatus", params=params, timeout=30)
    data = resp.json() if resp.status_code == 200 else {}
    aprobado = data.get("status") == 2  # 2 = pagado en Flow

    slug_evento = v.get("eventos", {}).get("slug", "") if isinstance(v.get("eventos"), dict) else ""
    token_descarga = v.get("token_descarga", "")

    if aprobado:
        supabase.table("ventas").update({"estado": "completado"}).eq("referencia_pago", token).execute()
        supabase.table("eventos").update({
            "total_ventas": (supabase.table("eventos").select("total_ventas").eq("id", v["evento_id"]).execute().data[0].get("total_ventas") or 0) + 1,
            "monto_total_ventas": (supabase.table("eventos").select("monto_total_ventas").eq("id", v["evento_id"]).execute().data[0].get("monto_total_ventas") or 0) + v["monto_total"],
        }).eq("id", v["evento_id"]).execute()

        # Enviar email
        if v.get("comprador_email"):
            try:
                resend_key = os.getenv("RESEND_API_KEY", "")
                if resend_key:
                    nombre = v.get("comprador_nombre") or "Cliente"
                    descarga_url = f"https://kusipix.com/descargas/{token_descarga}"
                    httpx.post("https://api.resend.com/emails",
                        headers={"Authorization": f"Bearer {resend_key}"},
                        json={"from": "Kusipix <noreply@kusipix.com>", "to": [v["comprador_email"]],
                              "subject": "Tus fotos de Kusipix están listas 📸",
                              "html": f'<div style="font-family:sans-serif;max-width:600px;margin:0 auto;padding:20px"><h2 style="color:#7c5cf0">Tus fotos están listas</h2><p>Hola {nombre},</p><p>Tu pago fue exitoso. Descarga tus fotos aquí:</p><div style="text-align:center;margin:30px 0"><a href="{descarga_url}" style="background:#7c5cf0;color:white;padding:14px 28px;border-radius:8px;text-decoration:none;font-weight:bold">Descargar mis fotos</a></div><p style="color:#666;font-size:13px">Kusipix - La alegría de encontrarte</p></div>'},
                        timeout=10)
            except Exception as e:
                print(f"Error email Flow: {e}")

        # Notificar al fotografo
        await notificar_venta_fotografo(v, v["evento_id"], fot_id)

    estado = "ok" if aprobado else "rechazado"
    return RedirectResponse(f"https://kusipix.com/evento/{slug_evento}?pago={estado}&token={token_descarga}", status_code=303)


# ─── EMAIL INVITACIÓN COLABORADOR ────────────────────────────────────────────

class InvitacionEmailRequest(BaseModel):
    invitado_email: str
    dueno_nombre: str
    evento_nombre: str
    tipo_acuerdo: str
    porcentaje_comision: Optional[float] = None
    limite_fotos: Optional[int] = None

@app.post("/api/notificar-invitacion")
async def notificar_invitacion(body: InvitacionEmailRequest):
    """Envía email al fotógrafo invitado a colaborar en un evento"""
    print(f"[INVITACION] Recibida solicitud para: {body.invitado_email}")
    try:
        resend_key = os.getenv("RESEND_API_KEY", "")
        print(f"[INVITACION] RESEND_API_KEY presente: {bool(resend_key)}, longitud: {len(resend_key) if resend_key else 0}")
        if not resend_key:
            print("[INVITACION] ERROR: Sin API key de Resend")
            return {"ok": False, "error": "Sin API key de Resend"}

        acuerdo = f"Comisión del {body.porcentaje_comision}% sobre tus ventas" if body.tipo_acuerdo == "comision" else "Pago fijo (coordinar directamente con el organizador)"
        limite = f"Límite: {body.limite_fotos:,} fotos".replace(",", ".") if body.limite_fotos else "Sin límite de fotos"

        print(f"[INVITACION] Enviando email a {body.invitado_email}...")
        resp = httpx.post(
            "https://api.resend.com/emails",
            headers={"Authorization": f"Bearer {resend_key}"},
            json={
                "from": "Kusipix <noreply@kusipix.com>",
                "to": [body.invitado_email],
                "subject": f"Invitacion de {body.dueno_nombre} para {body.evento_nombre}",
                "html": f"""<div style="font-family:sans-serif;max-width:580px;margin:0 auto;background:#0f1117;color:#e2e8f0;border-radius:16px;overflow:hidden">
                    <div style="background:linear-gradient(135deg,#7c5cf0,#3b82f6);padding:32px;text-align:center">
                        <div style="font-size:48px;margin-bottom:8px">📸</div>
                        <h2 style="color:white;margin:0;font-size:22px">Invitación a colaborar</h2>
                    </div>
                    <div style="padding:28px">
                        <p style="font-size:15px;color:#8892a4;margin:0 0 16px">Hola,</p>
                        <p style="font-size:15px;color:#e2e8f0;margin:0 0 20px"><strong>{body.dueno_nombre}</strong> te ha invitado a colaborar como fotógrafo en:</p>
                        <div style="background:#1a1d27;border-radius:12px;padding:20px;margin-bottom:20px;border:1px solid #2a2d3a">
                            <div style="font-size:18px;font-weight:700;color:#e2e8f0;margin-bottom:12px">{body.evento_nombre}</div>
                            <div style="font-size:14px;color:#8892a4;margin-bottom:6px">💰 Acuerdo: {acuerdo}</div>
                            <div style="font-size:14px;color:#8892a4">📷 {limite}</div>
                        </div>
                        <div style="text-align:center;margin-bottom:20px">
                            <a href="https://kusipix.com/panel" style="display:inline-block;background:#7c5cf0;color:white;padding:14px 32px;border-radius:10px;text-decoration:none;font-size:15px;font-weight:600">Ver invitación en mi panel</a>
                        </div>
                        <p style="font-size:12px;color:#64748b;text-align:center">⚠️ Kusipix no intermedia ni garantiza pagos entre fotógrafos. Los acuerdos son responsabilidad de las partes.</p>
                    </div>
                    <div style="background:#13151e;padding:16px 28px;text-align:center;border-top:1px solid #2a2d3a">
                        <p style="font-size:12px;color:#64748b;margin:0">Kusipix · kusipix.com</p>
                    </div>
                </div>"""
            },
            timeout=10
        )
        print(f"[INVITACION] Resend response: {resp.status_code} - {resp.text[:200]}")
        if resp.status_code == 200:
            return {"ok": True}
        else:
            return {"ok": False, "error": f"Resend error {resp.status_code}: {resp.text[:200]}"}
    except Exception as e:
        print(f"[INVITACION] Exception: {type(e).__name__}: {e}")
        return {"ok": False, "error": str(e)}


# ─── DESCARGA GRATUITA ───────────────────────────────────────────────────────

class DescargaGratisRequest(BaseModel):
    evento_id: str
    foto_ids: List[str]
    nombre: Optional[str] = "Descarga gratuita"
    email: Optional[str] = ""

@app.post("/api/descarga-gratis")
def descarga_gratis(body: DescargaGratisRequest):
    """Genera token de descarga gratuita para eventos sin costo"""
    ev = supabase.table("eventos").select("*").eq("id", body.evento_id).execute()
    if not ev.data:
        return JSONResponse(status_code=404, content={"error": "Evento no encontrado"})
    evento = ev.data[0]

    if not evento.get("es_gratuito"):
        return JSONResponse(status_code=403, content={"error": "Este evento no es gratuito"})

    if not body.foto_ids:
        return JSONResponse(status_code=400, content={"error": "No hay fotos seleccionadas"})

    # Crear venta con monto 0
    token_descarga = str(uuid.uuid4())
    venta_result = supabase.table("ventas").insert({
        "evento_id": body.evento_id,
        "fotografo_id": evento["fotografo_id"],
        "comprador_nombre": body.nombre or "Descarga gratuita",
        "comprador_email": body.email or "",
        "monto_total": 0,
        "metodo_pago": "gratuito",
        "referencia_pago": token_descarga,
        "estado": "completado",
        "tipo_compra": "gratuito",
        "cantidad_fotos": len(body.foto_ids),
        "token_descarga": token_descarga,
    }).execute()

    if not venta_result.data:
        return JSONResponse(status_code=500, content={"error": "Error al crear descarga"})

    venta_id = venta_result.data[0]["id"]

    # Asociar fotos
    supabase.table("venta_fotos").insert([
        {"venta_id": venta_id, "foto_id": fid, "precio": 0}
        for fid in body.foto_ids
    ]).execute()

    return {"token": token_descarga, "ok": True}


# ─── BÚSQUEDA PÚBLICA ────────────────────────────────────────────────────────

@app.post("/api/buscar-por-selfie")
async def buscar_por_selfie(selfie: UploadFile = File(...), evento_id: Optional[str] = None):
    """Búsqueda por reconocimiento facial - público"""
    contenido = await selfie.read()
    try:
        resultado = rekognition.search_faces_by_image(
            CollectionId=REKOGNITION_COLLECTION,
            Image={"Bytes": contenido},
            MaxFaces=100,
            FaceMatchThreshold=75
        )
    except Exception as e:
        return {"mensaje": "No se pudo procesar la selfie", "fotos": []}

    matches = resultado.get("FaceMatches", [])
    if not matches:
        return {"mensaje": "No se encontraron fotos", "fotos": []}

    face_ids = [m["Face"]["FaceId"] for m in matches]

    # Buscar fotos que contienen esos face_ids usando SQL directo
    import json
    fotos_encontradas = []
    for fid in face_ids:
        try:
            query = supabase.table("fotos").select("id, url_preview, dorsal, evento_id, tiene_rostros")
            # Usar filter con operador cs (contains) para jsonb array
            query = query.filter("face_ids", "cs", json.dumps([fid]))
            if evento_id:
                query = query.eq("evento_id", evento_id)
            data = query.execute().data or []
            fotos_encontradas.extend(data)
        except Exception as e:
            print(f"Error buscando face_id {fid}: {e}")

    # Deduplicar
    seen = set()
    fotos_unicas = []
    for f in fotos_encontradas:
        if f["id"] not in seen:
            seen.add(f["id"])
            fotos_unicas.append(f)

    return {"mensaje": f"Se encontraron {len(fotos_unicas)} fotos", "fotos": fotos_unicas}

@app.get("/api/buscar-por-dorsal")
def buscar_por_dorsal(dorsal: str, evento_id: Optional[str] = None):
    """Búsqueda por número de dorsal - público"""
    query = supabase.table("fotos").select("id, url_preview, dorsal, evento_id").eq("dorsal", dorsal)
    if evento_id:
        query = query.eq("evento_id", evento_id)
    resultado = query.execute()
    return {"mensaje": f"Se encontraron {len(resultado.data)} fotos", "fotos": resultado.data}

# ─── EVENTO PÚBLICO ──────────────────────────────────────────────────────────

@app.get("/api/evento/{slug}")
def evento_publico(slug: str):
    """Info pública de un evento para la página de compra"""
    ev = supabase.table("eventos").select("*").eq("slug", slug).eq("activo", True).eq("publico", True).execute()
    if not ev.data:
        return JSONResponse(status_code=404, content={"error": "Evento no encontrado"})
    evento = ev.data[0]

    # Info del fotógrafo (nombre, logo, colores)
    fot = supabase.table("fotografos").select("nombre, logo_url, color_primario, color_secundario, slug").eq("id", evento["fotografo_id"]).execute()
    fotografo_info = fot.data[0] if fot.data else {}

    return {
        "evento": evento,
        "fotografo": fotografo_info,
    }

@app.get("/api/evento/{slug}/fotos")
def fotos_evento_publico(slug: str, dorsal: Optional[str] = None, album_id: Optional[str] = None, pagina: int = 0, por_pagina: int = 50):
    """Lista fotos públicas de un evento (con marca de agua)"""
    ev = supabase.table("eventos").select("id").eq("slug", slug).eq("activo", True).eq("publico", True).execute()
    if not ev.data:
        return JSONResponse(status_code=404, content={"error": "Evento no encontrado"})

    evento_id = ev.data[0]["id"]
    desde = pagina * por_pagina

    query = supabase.table("fotos").select("id, url_preview, dorsal, tiene_rostros, cantidad_rostros, created_at").eq("evento_id", evento_id).eq("procesada", True)
    if dorsal:
        query = query.eq("dorsal", dorsal)
    if album_id:
        query = query.eq("album_id", album_id)

    resultado = query.order("created_at", desc=True).range(desde, desde + por_pagina - 1).execute()
    total = supabase.table("fotos").select("id", count="exact").eq("evento_id", evento_id).eq("procesada", True).execute().count or 0

    return {"fotos": resultado.data, "total": total, "pagina": pagina}

# ─── COMPRA DE CRÉDITOS (Flow del admin) ─────────────────────────────────────

class ComprarCreditosRequest(BaseModel):
    paquete_id: str
    fotografo_id: Optional[str] = None

@app.post("/api/creditos/comprar")
async def comprar_creditos(request: Request, body: ComprarCreditosRequest):
    """Crea pago en Flow para comprar créditos usando las credenciales del admin"""
    # Obtener fotógrafo autenticado
    auth_header = request.headers.get("Authorization", "")
    token = auth_header.replace("Bearer ", "")
    
    # Verificar usuario via Supabase
    user_resp = httpx.get(
        f"{os.getenv('SUPABASE_URL')}/auth/v1/user",
        headers={"Authorization": f"Bearer {token}", "apikey": os.getenv("SUPABASE_ANON_KEY")}
    )
    if user_resp.status_code != 200:
        return JSONResponse(status_code=401, content={"error": "No autenticado"})
    
    user = user_resp.json()
    fot = supabase.table("fotografos").select("id, email, nombre").eq("auth_user_id", user["id"]).execute()
    if not fot.data:
        return JSONResponse(status_code=404, content={"error": "Fotógrafo no encontrado"})
    fotografo = fot.data[0]

    # Obtener paquete
    pkg = supabase.table("paquetes_creditos").select("*").eq("id", body.paquete_id).execute()
    if not pkg.data:
        return JSONResponse(status_code=404, content={"error": "Paquete no encontrado"})
    paquete = pkg.data[0]

    # Credenciales Flow del admin
    flow_api_key = os.getenv("FLOW_API_KEY", "")
    flow_secret_key = os.getenv("FLOW_SECRET_KEY", "")
    flow_ambiente = os.getenv("FLOW_AMBIENTE", "sandbox")

    if not flow_api_key or not flow_secret_key:
        return JSONResponse(status_code=500, content={"error": "Flow no configurado en el servidor"})

    flow_url = "https://www.flow.cl/api" if flow_ambiente == "production" else "https://sandbox.flow.cl/api"

    buy_order = "KC" + uuid.uuid4().hex[:16]
    monto = int(paquete["precio"])

    params = {
        "apiKey": flow_api_key,
        "commerceOrder": buy_order,
        "subject": f"Kusipix - {paquete['nombre']} ({paquete['cantidad']} créditos)",
        "currency": "CLP",
        "amount": monto,
        "email": fotografo["email"],
        "urlConfirmation": f"{BACKEND_URL}/api/creditos/flow-confirmacion",
        "urlReturn": f"{BACKEND_URL}/api/creditos/flow-retorno",
        "paymentMethod": 9,
    }
    params["s"] = flow_sign(params, flow_secret_key)

    resp = httpx.post(f"{flow_url}/payment/create", data=params, timeout=30)
    if resp.status_code != 200:
        return JSONResponse(status_code=400, content={"error": f"Error Flow: {resp.text}"})

    data = resp.json()
    flow_token = data.get("token")
    redirect_url = data.get("url") + "?token=" + flow_token

    # Guardar referencia de compra pendiente
    supabase.table("transacciones_creditos").insert({
        "fotografo_id": fotografo["id"],
        "tipo": "compra_pendiente",
        "cantidad": paquete["cantidad"],
        "saldo_despues": 0,
        "descripcion": f"Compra pendiente: {paquete['nombre']} ({paquete['cantidad']} créditos) - {buy_order}",
    }).execute()

    return {"url": redirect_url, "token": flow_token, "monto": monto}


@app.post("/api/creditos/flow-confirmacion")
async def creditos_flow_confirmacion(request: Request):
    """Callback de Flow cuando el pago se confirma (server-to-server)"""
    form = await request.form()
    token = form.get("token") or ""
    
    flow_api_key = os.getenv("FLOW_API_KEY", "")
    flow_secret_key = os.getenv("FLOW_SECRET_KEY", "")
    flow_ambiente = os.getenv("FLOW_AMBIENTE", "sandbox")
    flow_url = "https://www.flow.cl/api" if flow_ambiente == "production" else "https://sandbox.flow.cl/api"

    params = {"apiKey": flow_api_key, "token": token}
    params["s"] = flow_sign(params, flow_secret_key)

    resp = httpx.get(f"{flow_url}/payment/getStatus", params=params, timeout=30)
    if resp.status_code != 200:
        print(f"[CREDITOS] Error getStatus: {resp.text}")
        return {"ok": False}

    data = resp.json()
    if data.get("status") != 2:  # 2 = pagado
        print(f"[CREDITOS] Pago no completado: status={data.get('status')}")
        return {"ok": False}

    commerce_order = data.get("commerceOrder", "")
    email = data.get("payer", "")

    # Buscar fotógrafo y paquete
    fot = supabase.table("fotografos").select("id, creditos_disponibles, email").eq("email", email).execute()
    if not fot.data:
        print(f"[CREDITOS] Fotógrafo no encontrado: {email}")
        return {"ok": False}
    fotografo = fot.data[0]

    # Encontrar la transacción pendiente
    pending = supabase.table("transacciones_creditos").select("*").eq("fotografo_id", fotografo["id"]).eq("tipo", "compra_pendiente").like("descripcion", f"%{commerce_order}%").execute()
    if not pending.data:
        print(f"[CREDITOS] Transacción pendiente no encontrada: {commerce_order}")
        return {"ok": False}

    creditos_comprados = pending.data[0]["cantidad"]
    nuevo_saldo = (fotografo.get("creditos_disponibles") or 0) + creditos_comprados

    # Acreditar créditos
    supabase.table("fotografos").update({
        "creditos_disponibles": nuevo_saldo
    }).eq("id", fotografo["id"]).execute()

    # Actualizar transacción
    supabase.table("transacciones_creditos").update({
        "tipo": "compra_paquete",
        "saldo_despues": nuevo_saldo,
        "descripcion": pending.data[0]["descripcion"].replace("Compra pendiente:", "Compra completada:")
    }).eq("id", pending.data[0]["id"]).execute()

    print(f"[CREDITOS] Compra exitosa: {email} +{creditos_comprados} créditos (saldo: {nuevo_saldo})")
    return {"ok": True}


@app.get("/api/creditos/flow-retorno")
@app.post("/api/creditos/flow-retorno")
async def creditos_flow_retorno(request: Request, token: str = None):
    """Retorno del usuario después de pagar en Flow"""
    if not token:
        form = await request.form()
        token = form.get("token") or ""

    flow_api_key = os.getenv("FLOW_API_KEY", "")
    flow_secret_key = os.getenv("FLOW_SECRET_KEY", "")
    flow_ambiente = os.getenv("FLOW_AMBIENTE", "sandbox")
    flow_url = "https://www.flow.cl/api" if flow_ambiente == "production" else "https://sandbox.flow.cl/api"

    params = {"apiKey": flow_api_key, "token": token}
    params["s"] = flow_sign(params, flow_secret_key)

    resp = httpx.get(f"{flow_url}/payment/getStatus", params=params, timeout=30)
    data = resp.json() if resp.status_code == 200 else {}
    aprobado = data.get("status") == 2

    estado = "ok" if aprobado else "error"
    return RedirectResponse(f"https://kusipix.com/panel?compra={estado}", status_code=303)


# ─── HELPER: Notificar fotógrafo por venta ───────────────────────────────────

async def notificar_venta_fotografo(venta: dict, evento_id: str, fotografo_id: str):
    """Envía email al fotógrafo avisando de una nueva venta"""
    try:
        resend_key = os.getenv("RESEND_API_KEY", "")
        if not resend_key:
            return

        fot_data = supabase.table("fotografos").select("email, nombre, notif_ventas").eq("id", fotografo_id).execute().data
        if not fot_data:
            return
        fot = fot_data[0]

        # Respetar preferencia de notificaciones
        if fot.get("notif_ventas") is False:
            return

        ev_data = supabase.table("eventos").select("nombre").eq("id", evento_id).execute().data
        ev_nombre = ev_data[0]["nombre"] if ev_data else "tu evento"

        monto = int(venta.get("monto_total", 0))
        monto_fmt = f"${monto:,}".replace(",", ".")
        cantidad = venta.get("cantidad_fotos", 1)
        comprador = venta.get("comprador_email", "—")
        nombre_fot = fot.get("nombre", "Fotógrafo")

        httpx.post(
            "https://api.resend.com/emails",
            headers={"Authorization": f"Bearer {resend_key}"},
            json={
                "from": "Kusipix <noreply@kusipix.com>",
                "to": [fot["email"]],
                "subject": f"💰 Nueva venta — {monto_fmt} CLP en {ev_nombre}",
                "html": f"""<div style="font-family:sans-serif;max-width:580px;margin:0 auto;background:#0f1117;color:#e2e8f0;border-radius:16px;overflow:hidden">
                    <div style="background:linear-gradient(135deg,#7c5cf0,#3b82f6);padding:32px;text-align:center">
                        <div style="font-size:48px;margin-bottom:8px">💰</div>
                        <h2 style="color:white;margin:0;font-size:22px">Nueva venta</h2>
                    </div>
                    <div style="padding:28px">
                        <p style="font-size:15px;color:#8892a4;margin:0 0 20px">Hola {nombre_fot},</p>
                        <div style="background:#1a1d27;border-radius:12px;padding:20px;margin-bottom:20px;border:1px solid #2a2d3a">
                            <div style="display:flex;justify-content:space-between;margin-bottom:12px">
                                <span style="color:#8892a4;font-size:14px">Evento</span>
                                <span style="color:#e2e8f0;font-weight:500">{ev_nombre}</span>
                            </div>
                            <div style="display:flex;justify-content:space-between;margin-bottom:12px">
                                <span style="color:#8892a4;font-size:14px">Comprador</span>
                                <span style="color:#e2e8f0">{comprador}</span>
                            </div>
                            <div style="display:flex;justify-content:space-between;margin-bottom:12px">
                                <span style="color:#8892a4;font-size:14px">Fotos vendidas</span>
                                <span style="color:#e2e8f0">{cantidad}</span>
                            </div>
                            <div style="display:flex;justify-content:space-between;border-top:1px solid #2a2d3a;padding-top:14px;margin-top:4px">
                                <span style="color:#8892a4;font-size:14px;font-weight:500">Total</span>
                                <span style="color:#7c5cf0;font-weight:700;font-size:22px">{monto_fmt} CLP</span>
                            </div>
                        </div>
                        <div style="text-align:center">
                            <a href="https://kusipix.com/panel" style="display:inline-block;background:#7c5cf0;color:white;padding:12px 28px;border-radius:8px;text-decoration:none;font-size:14px;font-weight:600">Ver mis ventas</a>
                        </div>
                    </div>
                    <div style="background:#13151e;padding:16px 28px;text-align:center;border-top:1px solid #2a2d3a">
                        <p style="font-size:12px;color:#64748b;margin:0">Kusipix · kusipix.com</p>
                    </div>
                </div>"""
            },
            timeout=10
        )
    except Exception as e:
        print(f"Error notificando fotografo: {e}")


# ─── PAGOS (Transbank por fotógrafo) ──────────────────────────────────────────

@app.post("/api/pago/crear")
def crear_pago(body: CrearPagoRequest):
    """Crea transacción de pago usando credenciales Transbank del fotógrafo"""
    from transbank.webpay.webpay_plus.transaction import Transaction
    from transbank.common.options import WebpayOptions
    from transbank.common.integration_commerce_codes import IntegrationCommerceCodes
    from transbank.common.integration_api_keys import IntegrationApiKeys

    ev = supabase.table("eventos").select("*").eq("id", body.evento_id).execute().data
    if not ev:
        return JSONResponse(status_code=404, content={"error": "Evento no encontrado"})
    evento = ev[0]

    # Obtener credenciales Transbank del fotógrafo dueño del evento
    fot = supabase.table("fotografos").select("transbank_codigo_comercio, transbank_api_key, transbank_shared_secret, transbank_ambiente").eq("id", evento["fotografo_id"]).execute()
    if not fot.data:
        return JSONResponse(status_code=400, content={"error": "Fotógrafo no configurado"})
    creds = fot.data[0]

    if not body.comprar_todas and len(body.foto_ids) == 0:
        return JSONResponse(status_code=400, content={"error": "No hay fotos seleccionadas"})

    calculo = calcular_monto_venta(evento, body.foto_ids, body.cupon_codigo, body.comprar_todas)
    if calculo.get("error"):
        return JSONResponse(status_code=400, content={"error": calculo["error"]})
    monto = calculo["monto"]

    buy_order = "KP" + uuid.uuid4().hex[:18]
    session_id = uuid.uuid4().hex[:18]

    # En pack completo, foto_ids debe venir con TODAS las fotos del participante
    foto_ids_finales = body.foto_ids

    # Registrar venta
    venta_result = supabase.table("ventas").insert({
        "evento_id": body.evento_id,
        "fotografo_id": evento["fotografo_id"],
        "comprador_nombre": body.nombre,
        "comprador_email": body.email,
        "comprador_telefono": body.telefono,
        "monto_total": monto,
        "metodo_pago": "transbank",
        "referencia_pago": buy_order,
        "estado": "pendiente",
        "tipo_compra": calculo["tipo_compra"],
        "cantidad_fotos": len(foto_ids_finales),
        "cupon_usado": calculo.get("cupon_codigo"),
        "descuento_aplicado": calculo.get("descuento_aplicado", 0),
    }).execute()

    # Guardar fotos en venta_fotos
    if venta_result.data and foto_ids_finales:
        venta_id = venta_result.data[0]["id"]
        precio_unitario = monto // len(foto_ids_finales) if len(foto_ids_finales) else 0
        supabase.table("venta_fotos").insert([
            {"venta_id": venta_id, "foto_id": fid, "precio": precio_unitario}
            for fid in foto_ids_finales
        ]).execute()

    # Crear transacción Transbank con credenciales del fotógrafo
    if creds.get("transbank_ambiente") == "PRODUCTION" and creds.get("transbank_codigo_comercio"):
        options = WebpayOptions(
            creds["transbank_codigo_comercio"],
            creds["transbank_api_key"],
            "https://webpay3g.transbank.cl"
        )
    else:
        options = WebpayOptions(
            IntegrationCommerceCodes.WEBPAY_PLUS,
            IntegrationApiKeys.WEBPAY,
            "https://webpay3gint.transbank.cl"
        )

    tx = Transaction(options)
    resp = tx.create(buy_order, session_id, monto, f"{BACKEND_URL}/api/pago/retorno")

    supabase.table("ventas").update({"referencia_pago": resp["token"]}).eq("referencia_pago", buy_order).execute()

    return {"url": resp["url"], "token": resp["token"], "buy_order": buy_order, "monto": monto}

@app.get("/api/pago/retorno")
@app.post("/api/pago/retorno")
async def pago_retorno(token_ws: str = None):
    """Retorno de Transbank después del pago"""
    from transbank.webpay.webpay_plus.transaction import Transaction
    from transbank.common.options import WebpayOptions
    from transbank.common.integration_commerce_codes import IntegrationCommerceCodes
    from transbank.common.integration_api_keys import IntegrationApiKeys

    if not token_ws:
        return RedirectResponse("https://kusipix.com?pago=rechazado", status_code=303)

    # Buscar la venta por token
    venta = supabase.table("ventas").select("*, eventos(slug, fotografo_id)").eq("referencia_pago", token_ws).execute().data
    if not venta:
        return RedirectResponse("https://kusipix.com?pago=error", status_code=303)
    v = venta[0]

    # Obtener credenciales del fotógrafo
    fot_id = v.get("fotografo_id")
    creds = supabase.table("fotografos").select("transbank_codigo_comercio, transbank_api_key, transbank_ambiente, slug").eq("id", fot_id).execute().data
    cred = creds[0] if creds else {}

    if cred.get("transbank_ambiente") == "PRODUCTION" and cred.get("transbank_codigo_comercio"):
        options = WebpayOptions(cred["transbank_codigo_comercio"], cred["transbank_api_key"], "https://webpay3g.transbank.cl")
    else:
        options = WebpayOptions(IntegrationCommerceCodes.WEBPAY_PLUS, IntegrationApiKeys.WEBPAY, "https://webpay3gint.transbank.cl")

    tx = Transaction(options)
    try:
        result = tx.commit(token_ws)
        aprobado = result.get("response_code") == 0
    except Exception:
        aprobado = False

    if aprobado:
        supabase.table("ventas").update({
            "estado": "completado",
        }).eq("referencia_pago", token_ws).execute()

        # Actualizar contadores del evento
        supabase.table("eventos").update({
            "total_ventas": (supabase.table("eventos").select("total_ventas").eq("id", v["evento_id"]).execute().data[0].get("total_ventas") or 0) + 1,
            "monto_total_ventas": (supabase.table("eventos").select("monto_total_ventas").eq("id", v["evento_id"]).execute().data[0].get("monto_total_ventas") or 0) + v["monto_total"],
        }).eq("id", v["evento_id"]).execute()

    estado = "ok" if aprobado else "rechazado"
    slug_evento = v.get("eventos", {}).get("slug", "") if isinstance(v.get("eventos"), dict) else ""
    token_descarga = v.get("token_descarga", "")

    if aprobado:
        await notificar_venta_fotografo(v, v["evento_id"], fot_id)

    if aprobado and v.get("comprador_email"):
        try:
            resend_key = os.getenv("RESEND_API_KEY", "")
            if resend_key:
                nombre = v.get("comprador_nombre") or "Cliente"
                descarga_url = f"https://kusipix.com/descargas/{token_descarga}"
                import httpx as hx
                hx.post("https://api.resend.com/emails",
                    headers={"Authorization": f"Bearer {resend_key}"},
                    json={"from": "Kusipix <noreply@kusipix.com>", "to": [v["comprador_email"]],
                          "subject": "Tus fotos de Kusipix estan listas",
                          "html": f"<p>Hola {nombre}, tu pago fue exitoso. <a href=\"{descarga_url}\">Descarga tus fotos aqui</a></p>"},
                    timeout=10)
        except Exception as e:
            print(f"Error email: {e}")

    return RedirectResponse(f"https://kusipix.com/evento/{slug_evento}?pago={estado}&token={token_descarga}", status_code=303)

# ─── DESCUENTOS Y CUPONES ────────────────────────────────────────────────────

class ReglaDescuentoRequest(BaseModel):
    evento_id: Optional[str] = None  # None = regla global del fotografo
    cantidad_minima: int
    porcentaje_descuento: float

def _fotografo_id_desde_auth(authorization: Optional[str]):
    """Obtiene el fotografo_id a partir del JWT en el header Authorization"""
    if not authorization or not authorization.startswith("Bearer "):
        return None
    token = authorization.split(" ")[1]
    try:
        user = supabase.auth.get_user(token)
        auth_user_id = user.user.id
        fot = supabase.table("fotografos").select("id").eq("auth_user_id", auth_user_id).execute()
        if fot.data:
            return fot.data[0]["id"]
    except Exception as e:
        print(f"Error auth: {e}")
    return None

@app.get("/api/descuentos/reglas")
def listar_reglas_descuento(authorization: Optional[str] = Header(None)):
    fotografo_id = _fotografo_id_desde_auth(authorization)
    if not fotografo_id:
        return JSONResponse(status_code=401, content={"error": "No autorizado"})
    res = supabase.table("reglas_descuento_cantidad").select("*").eq("fotografo_id", fotografo_id).order("cantidad_minima").execute()
    return {"reglas": res.data}

@app.post("/api/descuentos/reglas")
def crear_regla_descuento(body: ReglaDescuentoRequest, authorization: Optional[str] = Header(None)):
    fotografo_id = _fotografo_id_desde_auth(authorization)
    if not fotografo_id:
        return JSONResponse(status_code=401, content={"error": "No autorizado"})
    if body.cantidad_minima < 2:
        return JSONResponse(status_code=400, content={"error": "La cantidad mínima debe ser 2 o más"})
    if body.porcentaje_descuento <= 0 or body.porcentaje_descuento > 90:
        return JSONResponse(status_code=400, content={"error": "El descuento debe ser entre 1% y 90%"})
    res = supabase.table("reglas_descuento_cantidad").insert({
        "fotografo_id": fotografo_id,
        "evento_id": body.evento_id,
        "cantidad_minima": body.cantidad_minima,
        "porcentaje_descuento": body.porcentaje_descuento,
    }).execute()
    return {"regla": res.data[0] if res.data else None}

@app.delete("/api/descuentos/reglas/{regla_id}")
def eliminar_regla_descuento(regla_id: str, authorization: Optional[str] = Header(None)):
    fotografo_id = _fotografo_id_desde_auth(authorization)
    if not fotografo_id:
        return JSONResponse(status_code=401, content={"error": "No autorizado"})
    supabase.table("reglas_descuento_cantidad").delete().eq("id", regla_id).eq("fotografo_id", fotografo_id).execute()
    return {"ok": True}


class CuponRequest(BaseModel):
    evento_id: str
    codigo: str
    tipo_descuento: str  # 'porcentaje' | 'monto_fijo'
    valor: float
    limite_usos: Optional[int] = None
    fecha_expiracion: Optional[str] = None

@app.get("/api/cupones")
def listar_cupones(evento_id: Optional[str] = None, authorization: Optional[str] = Header(None)):
    fotografo_id = _fotografo_id_desde_auth(authorization)
    if not fotografo_id:
        return JSONResponse(status_code=401, content={"error": "No autorizado"})
    q = supabase.table("cupones").select("*").eq("fotografo_id", fotografo_id)
    if evento_id:
        q = q.eq("evento_id", evento_id)
    res = q.order("created_at", desc=True).execute()
    return {"cupones": res.data}

@app.post("/api/cupones")
def crear_cupon(body: CuponRequest, authorization: Optional[str] = Header(None)):
    fotografo_id = _fotografo_id_desde_auth(authorization)
    if not fotografo_id:
        return JSONResponse(status_code=401, content={"error": "No autorizado"})
    if body.tipo_descuento not in ("porcentaje", "monto_fijo"):
        return JSONResponse(status_code=400, content={"error": "Tipo de descuento inválido"})
    if body.valor <= 0:
        return JSONResponse(status_code=400, content={"error": "El valor debe ser mayor a 0"})
    try:
        res = supabase.table("cupones").insert({
            "fotografo_id": fotografo_id,
            "evento_id": body.evento_id,
            "codigo": body.codigo.strip().upper(),
            "tipo_descuento": body.tipo_descuento,
            "valor": body.valor,
            "limite_usos": body.limite_usos,
            "fecha_expiracion": body.fecha_expiracion,
        }).execute()
    except Exception as e:
        if "duplicate" in str(e).lower() or "unique" in str(e).lower():
            return JSONResponse(status_code=400, content={"error": "Ya existe un cupón con ese código en este evento"})
        return JSONResponse(status_code=400, content={"error": "No se pudo crear el cupón"})
    return {"cupon": res.data[0] if res.data else None}

@app.delete("/api/cupones/{cupon_id}")
def eliminar_cupon(cupon_id: str, authorization: Optional[str] = Header(None)):
    fotografo_id = _fotografo_id_desde_auth(authorization)
    if not fotografo_id:
        return JSONResponse(status_code=401, content={"error": "No autorizado"})
    supabase.table("cupones").delete().eq("id", cupon_id).eq("fotografo_id", fotografo_id).execute()
    return {"ok": True}


class CotizarRequest(BaseModel):
    evento_id: str
    foto_ids: List[str] = []
    cupon_codigo: Optional[str] = None
    comprar_todas: bool = False

class ActualizarPrecioEventoRequest(BaseModel):
    precio_foto: Optional[int] = None
    precio_todas: Optional[int] = None  # enviar null para desactivar el pack

@app.put("/api/eventos/{evento_id}/precio")
def actualizar_precio_evento(evento_id: str, body: ActualizarPrecioEventoRequest, authorization: Optional[str] = Header(None)):
    fotografo_id = _fotografo_id_desde_auth(authorization)
    if not fotografo_id:
        return JSONResponse(status_code=401, content={"error": "No autorizado"})
    ev = supabase.table("eventos").select("id, fotografo_id, es_gratuito").eq("id", evento_id).execute().data
    if not ev or ev[0]["fotografo_id"] != fotografo_id:
        return JSONResponse(status_code=404, content={"error": "Evento no encontrado"})
    if ev[0]["es_gratuito"]:
        return JSONResponse(status_code=400, content={"error": "No se puede fijar precio en un evento gratuito"})

    updates = {}
    if body.precio_foto is not None:
        if body.precio_foto <= 0:
            return JSONResponse(status_code=400, content={"error": "El precio por foto debe ser mayor a 0"})
        updates["precio_foto"] = body.precio_foto
    # precio_todas puede ser explicitamente None para desactivar el pack, por eso se chequea aparte
    if "precio_todas" in body.model_fields_set:
        if body.precio_todas is not None and body.precio_todas <= 0:
            return JSONResponse(status_code=400, content={"error": "El precio del pack debe ser mayor a 0"})
        updates["precio_todas"] = body.precio_todas

    if not updates:
        return JSONResponse(status_code=400, content={"error": "No hay cambios para aplicar"})

    supabase.table("eventos").update(updates).eq("id", evento_id).execute()
    return {"ok": True}


@app.get("/api/eventos/{evento_id}/resumen-ventas")
def resumen_ventas_evento(evento_id: str, authorization: Optional[str] = Header(None)):
    """Trae en una sola llamada: precio, pack completo, tramos de descuento y cupones del evento.
    Usado por el dashboard de detalle del evento en el panel del fotografo."""
    fotografo_id = _fotografo_id_desde_auth(authorization)
    if not fotografo_id:
        return JSONResponse(status_code=401, content={"error": "No autorizado"})

    ev = supabase.table("eventos").select("id, nombre, precio_foto, precio_todas, fotografo_id").eq("id", evento_id).execute().data
    if not ev or ev[0]["fotografo_id"] != fotografo_id:
        return JSONResponse(status_code=404, content={"error": "Evento no encontrado"})
    evento = ev[0]

    tramos_evento = supabase.table("reglas_descuento_cantidad").select("*").eq("evento_id", evento_id).eq("activo", True).order("cantidad_minima").execute().data
    tramos_globales = supabase.table("reglas_descuento_cantidad").select("*").eq("fotografo_id", fotografo_id).is_("evento_id", "null").eq("activo", True).order("cantidad_minima").execute().data
    cupones = supabase.table("cupones").select("*").eq("evento_id", evento_id).order("created_at", desc=True).execute().data

    return {
        "precio_foto": evento.get("precio_foto"),
        "precio_todas": evento.get("precio_todas"),
        "tramos_evento": tramos_evento,
        "tramos_globales": tramos_globales,
        "cupones": cupones,
    }


@app.post("/api/cotizar")
def cotizar_precio(body: CotizarRequest):
    """Endpoint publico: calcula el precio final SIN crear la venta ni consumir el cupon.
    Se usa para mostrar el precio en el checkout antes de pagar."""
    ev = supabase.table("eventos").select("*").eq("id", body.evento_id).execute().data
    if not ev:
        return JSONResponse(status_code=404, content={"error": "Evento no encontrado"})
    evento = ev[0]

    if body.comprar_todas:
        precio_todas = evento.get("precio_todas")
        if not precio_todas:
            return JSONResponse(status_code=400, content={"error": "Este evento no tiene pack completo"})
        return {"monto": precio_todas, "tipo_compra": "todas", "descuento_aplicado": 0}

    n = len(body.foto_ids)
    if n == 0:
        return JSONResponse(status_code=400, content={"error": "No hay fotos seleccionadas"})
    monto_base = (evento.get("precio_foto") or 0) * n

    if body.cupon_codigo:
        # Solo previsualiza: valida sin consumir usando SELECT directo (no la RPC que incrementa uso)
        cup = supabase.table("cupones").select("*").eq("evento_id", body.evento_id).ilike("codigo", body.cupon_codigo).eq("activo", True).execute()
        if not cup.data:
            return JSONResponse(status_code=400, content={"error": "Cupón no encontrado"})
        c = cup.data[0]
        if c.get("fecha_expiracion") and c["fecha_expiracion"] < datetime.utcnow().isoformat():
            return JSONResponse(status_code=400, content={"error": "Cupón expirado"})
        if c.get("limite_usos") is not None and c["usos_actuales"] >= c["limite_usos"]:
            return JSONResponse(status_code=400, content={"error": "Cupón agotado"})
        if c["tipo_descuento"] == "porcentaje":
            descuento = round(monto_base * c["valor"] / 100)
        else:
            descuento = min(c["valor"], monto_base)
        return {"monto": monto_base - descuento, "tipo_compra": "individual" if n == 1 else "pack", "descuento_aplicado": descuento}

    porcentaje = supabase.rpc("calcular_descuento_cantidad", {
        "p_evento_id": body.evento_id,
        "p_fotografo_id": evento["fotografo_id"],
        "p_cantidad": n,
    }).execute()
    pct = (porcentaje.data or 0) if porcentaje.data is not None else 0
    descuento = round(monto_base * pct / 100)
    return {"monto": monto_base - descuento, "tipo_compra": "individual" if n == 1 else "pack", "descuento_aplicado": descuento}


# ─── DESCARGA DE FOTOS COMPRADAS ─────────────────────────────────────────────

@app.get("/api/descargas/{token}")
def descargar_fotos(token: str):
    """Genera URLs de descarga para fotos compradas"""
    v = supabase.table("ventas").select("*").eq("token_descarga", token).eq("estado", "completado").execute()
    if not v.data:
        return JSONResponse(status_code=404, content={"error": "Compra no encontrada"})
    venta = v.data[0]

    # Obtener exactamente las fotos de esta venta
    vf = supabase.table("venta_fotos").select("foto_id, precio").eq("venta_id", venta["id"]).execute()
    foto_ids = [f["foto_id"] for f in (vf.data or [])]

    if not foto_ids:
        return JSONResponse(status_code=404, content={"error": "No hay fotos en esta compra"})

    fotos = supabase.table("fotos").select("id, url_original, url_preview, nombre_archivo").in_("id", foto_ids).execute()
    resultado = []
    for f in (fotos.data or []):
        try:
            # Signed URL para descarga de la foto original (sin marca de agua)
            signed = supabase.storage.from_("fotos-originales").create_signed_url(f["url_original"], 3600)
            download_url = signed.get("signedURL") or signed.get("signedUrl") or signed.get("signed_url") or ""
        except Exception as e:
            print(f"Error signed URL: {e}")
            download_url = ""

        # Preview URL para miniatura (pública)
        preview_url = f.get("url_preview") or ""

        resultado.append({
            "id": f["id"],
            "nombre": f.get("nombre_archivo", f["id"] + ".jpg"),
            "url": f"{BACKEND_URL}/api/descargar/{token}/{f['id']}",
            "preview": preview_url,
        })

    return {"fotos": resultado, "total": len(resultado)}

# ─── DESCARGA PROXY (fuerza descarga en navegador) ───────────────────────────

@app.get("/api/descargar/{token}/{foto_id}")
async def descargar_foto_proxy(token: str, foto_id: str):
    """Proxy de descarga - fuerza descarga del archivo en el navegador"""
    # Verificar que la compra es válida
    v = supabase.table("ventas").select("id").eq("token_descarga", token).eq("estado", "completado").execute()
    if not v.data:
        return JSONResponse(status_code=404, content={"error": "Compra no encontrada"})

    venta_id = v.data[0]["id"]

    # Verificar que la foto pertenece a esta venta
    vf = supabase.table("venta_fotos").select("foto_id").eq("venta_id", venta_id).eq("foto_id", foto_id).execute()
    if not vf.data:
        return JSONResponse(status_code=403, content={"error": "Foto no pertenece a esta compra"})

    # Obtener la foto
    foto = supabase.table("fotos").select("url_original, nombre_archivo").eq("id", foto_id).execute()
    if not foto.data:
        return JSONResponse(status_code=404, content={"error": "Foto no encontrada"})

    f = foto.data[0]
    nombre = f.get("nombre_archivo", foto_id + ".jpg")

    # Descargar desde Supabase Storage
    try:
        data = supabase.storage.from_("fotos-originales").download(f["url_original"])
        ext = nombre.rsplit(".", 1)[-1].lower() if "." in nombre else "jpg"
        content_types = {"jpg": "image/jpeg", "jpeg": "image/jpeg", "png": "image/png", "webp": "image/webp"}
        ct = content_types.get(ext, "image/jpeg")

        return StreamingResponse(
            iter([data]),
            media_type=ct,
            headers={
                "Content-Disposition": f'attachment; filename="{nombre}"',
                "Content-Length": str(len(data))
            }
        )
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": f"Error descargando: {str(e)}"})


# ─── PROGRESO DE PROCESAMIENTO ───────────────────────────────────────────────

@app.get("/api/progreso/{evento_id}")
def ver_progreso(evento_id: str):
    """Estado de procesamiento de fotos de un evento"""
    total = supabase.table("fotos").select("id", count="exact").eq("evento_id", evento_id).execute().count or 0
    procesadas = supabase.table("fotos").select("id", count="exact").eq("evento_id", evento_id).eq("procesada", True).execute().count or 0
    porcentaje = round((procesadas / total * 100) if total > 0 else 0)
    return {
        "total": total,
        "procesadas": procesadas,
        "pendientes": total - procesadas,
        "porcentaje": porcentaje,
        "listo": porcentaje == 100
    }


# ─── REPROCESAR FOTOS PENDIENTES ─────────────────────────────────────────────

@app.post("/api/reprocesar/{evento_id}")
async def reprocesar_fotos(evento_id: str, background_tasks: BackgroundTasks, fotografo=Depends(get_fotografo)):
    ev = supabase.table("eventos").select("*").eq("id", evento_id).eq("fotografo_id", fotografo["id"]).execute()
    if not ev.data:
        return JSONResponse(status_code=403, content={"error": "Evento no encontrado"})
    evento = ev.data[0]
    fotos = supabase.table("fotos").select("*").eq("evento_id", evento_id).eq("procesada", False).execute()
    pendientes = fotos.data or []
    if not pendientes:
        return {"mensaje": "No hay fotos pendientes", "total": 0}
    for foto in pendientes:
        background_tasks.add_task(
            procesar_foto_inline, foto["id"], foto["url_original"], foto["nombre_archivo"],
            evento_id, fotografo["id"], evento.get("modo_busqueda", "facial_dorsal"),
            fotografo.get("marca_agua_url")
        )
    return {"mensaje": f"Reprocesando {len(pendientes)} fotos", "total": len(pendientes)}


# ─── SUBIDA DIRECTA AL BACKEND ────────────────────────────────────────────────

@app.post("/api/subir-foto")
async def subir_foto_directo(
    background_tasks: BackgroundTasks,
    foto: UploadFile = File(...),
    evento_id: str = Form(...),
    album_id: Optional[str] = Form(None),
    fotografo=Depends(get_fotografo)
):
    # Verificar evento — puede ser dueño o colaborador
    ev = supabase.table("eventos").select("*").eq("id", evento_id).execute()
    if not ev.data:
        return JSONResponse(status_code=404, content={"error": "Evento no encontrado"})
    evento = ev.data[0]

    es_dueno = evento["fotografo_id"] == fotografo["id"]
    es_colaborador = False
    colaboracion = None

    if not es_dueno:
        col = supabase.table("evento_fotografos").select("*").eq("evento_id", evento_id).eq("fotografo_id", fotografo["id"]).eq("estado", "aceptado").execute()
        if not col.data:
            return JSONResponse(status_code=403, content={"error": "No tienes permiso para subir fotos a este evento"})
        es_colaborador = True
        colaboracion = col.data[0]
        if colaboracion.get("limite_fotos") is not None:
            if (colaboracion.get("fotos_subidas") or 0) >= colaboracion["limite_fotos"]:
                return JSONResponse(status_code=400, content={"error": f"Alcanzaste tu límite de {colaboracion['limite_fotos']} fotos en este evento"})
        dueno = supabase.table("fotografos").select("id, creditos_disponibles").eq("id", evento["fotografo_id"]).execute()
        fotografo_pagador = dueno.data[0] if dueno.data else fotografo
    else:
        fotografo_pagador = fotografo

    creditos = fotografo_pagador.get("creditos_disponibles") or 0
    modo = evento.get("modo_busqueda", "facial_dorsal")
    creditos_consumir = 2 if modo == "facial_dorsal" else 1

    if creditos < creditos_consumir:
        return JSONResponse(status_code=400, content={"error": f"Créditos insuficientes. Se necesitan {creditos_consumir} créditos para este evento."})

    foto_id = str(uuid.uuid4())
    ext = foto.filename.rsplit(".", 1)[-1].lower() if "." in foto.filename else "jpg"
    path = f"{fotografo['id']}/{evento_id}/{foto_id}.{ext}"
    contenido = await foto.read()
    supabase.storage.from_("fotos-originales").upload(path, contenido, {"content-type": foto.content_type or "image/jpeg"})
    foto_data = {
        "id": foto_id, "evento_id": evento_id, "fotografo_id": fotografo["id"],
        "url_original": path, "nombre_archivo": foto.filename,
        "tamano_bytes": len(contenido), "procesada": False,
        "tiene_rostros": False, "cantidad_rostros": 0, "face_ids": [],
    }
    if album_id:
        foto_data["album_id"] = album_id
    supabase.table("fotos").insert(foto_data).execute()
    # Créditos según modo: facial+dorsal = 2, cualquier otro = 1
    modo = evento.get("modo_busqueda", "facial_dorsal")
    creditos_consumir = 2 if modo == "facial_dorsal" else 1
    nuevo_saldo = creditos - creditos_consumir
    supabase.table("fotografos").update({"creditos_disponibles": nuevo_saldo}).eq("id", fotografo_pagador["id"]).execute()
    desc = f"Foto: {foto.filename} (modo: {modo})"
    if es_colaborador:
        desc += f" — colaborador {fotografo['email']}"
    supabase.table("transacciones_creditos").insert({
        "fotografo_id": fotografo_pagador["id"], "tipo": "consumo", "cantidad": -creditos_consumir,
        "saldo_despues": nuevo_saldo, "descripcion": desc
    }).execute()
    
    # Actualizar contador de fotos del colaborador
    if es_colaborador and colaboracion:
        supabase.table("evento_fotografos").update({
            "fotos_subidas": (colaboracion.get("fotos_subidas") or 0) + 1
        }).eq("id", colaboracion["id"]).execute()
    supabase.table("eventos").update({"total_fotos": (evento.get("total_fotos") or 0) + 1}).eq("id", evento_id).execute()
    background_tasks.add_task(
        procesar_foto_inline, foto_id, path, foto.filename,
        evento_id, fotografo["id"], evento.get("modo_busqueda", "facial_dorsal"),
        fotografo.get("marca_agua_url")
    )
    return {"foto_id": foto_id, "estado": "procesando", "creditos_restantes": creditos - 1}
