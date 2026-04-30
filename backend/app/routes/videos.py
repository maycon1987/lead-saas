from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from supabase import create_client
from typing import Optional
import os

router = APIRouter()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY") or os.getenv("SUPABASE_SERVICE_ROLE_KEY")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)


class VideoInput(BaseModel):
    titulo: str = ""
    descricao: str = ""
    fornecedor: str = ""
    link: Optional[str] = None
    url: Optional[str] = None
    nicho: str = ""
    categoria: str = ""
    cidade: str = ""
    endereco: str = ""
    whatsapp: str = ""
    instagram: str = ""
    site: str = ""
    destaque: bool = False
    ativo: bool = True


@router.post("/postar-video")
def postar_video(data: VideoInput):
    video_link = data.link or data.url

    if not video_link:
        raise HTTPException(status_code=400, detail="Informe link ou url do vídeo.")

    registro = {
        "titulo": data.titulo,
        "descricao": data.descricao,
        "fornecedor": data.fornecedor,
        "link": video_link,
        "nicho": data.nicho,
        "categoria": data.categoria,
        "cidade": data.cidade,
        "endereco": data.endereco,
        "whatsapp": data.whatsapp,
        "instagram": data.instagram,
        "site": data.site,
        "destaque": data.destaque,
        "ativo": data.ativo,
    }

    try:
        supabase.table("videos_fornecedores").insert(registro).execute()
        return {"status": "ok", "video": registro}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/listar-videos")
def listar_videos(
    nicho: str = "",
    categoria: str = "",
    cidade: str = "",
    destaque: bool = False,
    limite: int = 30
):
    try:
        query = supabase.table("videos_fornecedores").select("*").eq("ativo", True)

        if nicho:
            query = query.ilike("nicho", f"%{nicho}%")
        if categoria:
            query = query.ilike("categoria", f"%{categoria}%")
        if cidade:
            query = query.ilike("cidade", f"%{cidade}%")
        if destaque:
            query = query.eq("destaque", True)

        resposta = (
            query
            .order("destaque", desc=True)
            .order("id", desc=True)
            .limit(limite)
            .execute()
        )

        return {"status": "ok", "videos": resposta.data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
