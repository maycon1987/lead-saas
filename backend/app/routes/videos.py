from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from supabase import create_client
import os


router = APIRouter()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY") or os.getenv("SUPABASE_SERVICE_ROLE_KEY")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)


# =========================
# MODELO DE ENTRADA
# =========================
class VideoInput(BaseModel):
    titulo: str
    descricao: str = ""
    fornecedor: str = ""
    link: str


# =========================
# POST - salvar vídeo (link)
# =========================
@router.post("/postar-video")
def postar_video(data: VideoInput):
    try:
        registro = {
            "titulo": data.titulo,
            "descricao": data.descricao,
            "fornecedor": data.fornecedor,
            "link": data.link
        }

        supabase.table("videos_fornecedores").insert(registro).execute()

        return {
            "status": "ok",
            "message": "Vídeo salvo com sucesso"
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# =========================
# GET - listar vídeos
# =========================
@router.get("/listar-videos")
def listar_videos():
    try:
        res = (
            supabase
            .table("videos_fornecedores")
            .select("*")
            .order("id", desc=True)
            .execute()
        )

        return {
            "status": "ok",
            "videos": res.data
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
