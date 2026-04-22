from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from typing import Optional

from app.services.collector import run_search

app = FastAPI(title="Buscador de Leads", version="2.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class FiltrosBusca(BaseModel):
    apenas_com_whatsapp: bool = False
    apenas_com_instagram: bool = False
    apenas_com_site: bool = False
    apenas_com_email: bool = False
    apenas_com_cnpj: bool = False
    tipo_empresa: str = "Todos"
    avaliacao_minima: float = 0


class SearchRequest(BaseModel):
    cidade: str = Field(..., min_length=2)
    palavra_chave_principal: str = Field(..., min_length=2)
    palavras_chave_extras: Optional[str] = ""
    limite_resultados: int = 5
    filtros: FiltrosBusca


@app.get("/")
def root():
    return {"status": "ok", "message": "API rodando 🚀"}


@app.get("/health")
def health():
    return {"status": "healthy"}


@app.post("/searches")
def create_search(payload: SearchRequest):
    try:
        resultado = run_search(payload.model_dump())
        return resultado
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
