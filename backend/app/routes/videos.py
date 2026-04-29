from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from supabase import create_client
import os


router = APIRouter()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY") or os.getenv("SUPABASE_SERVICE_ROLE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    supabase = None
else:
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)


class VideoInput(BaseModel):
    titulo: str
    descricao: str = ""
    fornecedor: str = ""
    link: str

    # novos campos
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
    if supabase is None:
        raise HTTPException(
            status_code=500,
            detail="Supabase não configurado. Verifique SUPABASE_URL e SUPABASE_KEY."
        )

    try:
        registro = {
            "titulo": data.titulo,
            "descricao": data.descricao,
            "fornecedor": data.fornecedor,
            "link": data.link,
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

        supabase.table("videos_fornecedores").insert(registro).execute()

        return {
            "status": "ok",
            "message": "Vídeo salvo com sucesso",
            "video": registro
        }

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
    if supabase is None:
        raise HTTPException(
            status_code=500,
            detail="Supabase não configurado. Verifique SUPABASE_URL e SUPABASE_KEY."
        )

    try:
        query = (
            supabase
            .table("videos_fornecedores")
            .select("*")
            .eq("ativo", True)
        )

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

        return {
            "status": "ok",
            "videos": resposta.data
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/videos/{video_id}/view")
def registrar_visualizacao(video_id: int):
    if supabase is None:
        raise HTTPException(
            status_code=500,
            detail="Supabase não configurado. Verifique SUPABASE_URL e SUPABASE_KEY."
        )

    try:
        atual = (
            supabase
            .table("videos_fornecedores")
            .select("visualizacoes")
            .eq("id", video_id)
            .single()
            .execute()
        )

        views = 0
        if atual.data and atual.data.get("visualizacoes") is not None:
            views = atual.data.get("visualizacoes")

        novo_total = views + 1

        supabase.table("videos_fornecedores").update({
            "visualizacoes": novo_total
        }).eq("id", video_id).execute()

        return {
            "status": "ok",
            "visualizacoes": novo_total
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
