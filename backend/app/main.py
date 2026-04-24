from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field, ConfigDict
from typing import Optional, Any, Dict

from app.services.collector import run_search


app = FastAPI(title="Buscador de Leads", version="2.2.0")


app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class FiltrosBusca(BaseModel):
    model_config = ConfigDict(extra="allow")

    apenas_com_whatsapp: bool = False
    apenas_com_instagram: bool = False
    apenas_com_site: bool = False
    apenas_com_email: bool = False
    apenas_com_cnpj: bool = False
    tipo_empresa: str = "Todos"
    avaliacao_minima: float = 0


class SearchRequest(BaseModel):
    model_config = ConfigDict(extra="allow")

    estado: Optional[str] = ""
    cidade: str = Field(..., min_length=2)
    palavra_chave_principal: str = Field(..., min_length=2)
    palavras_chave_extras: Optional[str] = ""
    limite_resultados: Optional[int] = 10

    # Pode vir do frontend como rapida/completa
    modo_busca: Optional[str] = "rapida"

    # Opções selecionadas no frontend
    buscar_whatsapp: Optional[bool] = False
    buscar_site: Optional[bool] = False
    buscar_telefone: Optional[bool] = False
    buscar_email: Optional[bool] = False
    buscar_instagram: Optional[bool] = False
    buscar_facebook: Optional[bool] = False

    buscar_cnpj: Optional[bool] = False
    buscar_cnae: Optional[bool] = False
    buscar_capital_social: Optional[bool] = False
    buscar_natureza_juridica: Optional[bool] = False
    buscar_situacao_cadastral: Optional[bool] = False

    filtros: FiltrosBusca = FiltrosBusca()


@app.get("/")
def root():
    return {"status": "ok", "message": "API rodando 🚀"}


@app.get("/health")
def health():
    return {"status": "healthy"}


@app.post("/searches")
def create_search(payload: SearchRequest):
    try:
        payload_dict: Dict[str, Any] = payload.model_dump()
        resultado = run_search(payload_dict)
        return resultado
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
