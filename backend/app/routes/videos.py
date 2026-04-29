
from fastapi import APIRouter, UploadFile, File, Form, HTTPException
from supabase import create_client
import os
import uuid


router = APIRouter()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY") or os.getenv("SUPABASE_SERVICE_ROLE_KEY")

BUCKET = os.getenv("SUPABASE_VIDEOS_BUCKET", "videos-fornecedores")

if not SUPABASE_URL or not SUPABASE_KEY:
    supabase = None
else:
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)


@router.post("/postar-video")
async def postar_video(
    file: UploadFile = File(...),
    titulo: str = Form(...),
    descricao: str = Form(""),
    fornecedor: str = Form(""),
    link: str = Form("")
):
    if supabase is None:
        raise HTTPException(
            status_code=500,
            detail="Supabase não configurado. Verifique SUPABASE_URL e SUPABASE_KEY no Railway."
        )

    try:
        extensao = file.filename.split(".")[-1].lower()
        nome_arquivo = f"{uuid.uuid4()}.{extensao}"

        conteudo = await file.read()

        supabase.storage.from_(BUCKET).upload(
            nome_arquivo,
            conteudo,
            {
                "content-type": file.content_type
            }
        )

        video_url = supabase.storage.from_(BUCKET).get_public_url(nome_arquivo)

        registro = {
            "titulo": titulo,
            "descricao": descricao,
            "fornecedor": fornecedor,
            "link": link,
            "video_url": video_url
        }

        supabase.table("videos_fornecedores").insert(registro).execute()

        return {
            "status": "ok",
            "message": "Vídeo publicado com sucesso",
            "video_url": video_url
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/listar-videos")
def listar_videos():
    if supabase is None:
        raise HTTPException(
            status_code=500,
            detail="Supabase não configurado. Verifique SUPABASE_URL e SUPABASE_KEY no Railway."
        )

    try:
        resposta = (
            supabase
            .table("videos_fornecedores")
            .select("*")
            .order("id", desc=True)
            .execute()
        )

        return {
            "status": "ok",
            "videos": resposta.data
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
