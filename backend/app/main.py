from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field
from typing import List, Optional
from datetime import datetime
import os
import json
import re

# IMPORT CORRIGIDO (ABSOLUTO)
from app.services.collector import run_collection

app = FastAPI()

# CORS liberado
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

BASE_OUTPUT_DIR = "storage/runs"


class SearchFilters(BaseModel):
    apenas_com_whatsapp: bool = False
    apenas_com_instagram: bool = False
    apenas_com_site: bool = False
    apenas_com_email: bool = False
    apenas_com_cnpj: bool = False
    tipo_empresa: str = "Todos"
    avaliacao_minima: float = 0.0


class SearchRequest(BaseModel):
    cidade: str = Field(..., example="Campinas")
    palavra_chave_principal: str = Field(..., example="embalagens")
    palavras_chave_extras: Optional[str] = Field(default="")
    limite_resultados: int = Field(default=5)
    filtros: SearchFilters = Field(default_factory=SearchFilters)


def slugify(text: str) -> str:
    text = text.strip().lower()
    text = text.replace(" ", "_")
    text = re.sub(r"[^a-z0-9_]+", "", text)
    return text or "busca"


def create_run_folder(first_keyword: str) -> str:
    os.makedirs(BASE_OUTPUT_DIR, exist_ok=True)

    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    safe_keyword = slugify(first_keyword)
    folder_name = f"{timestamp}_{safe_keyword}"

    run_path = os.path.join(BASE_OUTPUT_DIR, folder_name)
    os.makedirs(run_path, exist_ok=True)

    return run_path


def write_log(run_path: str, message: str) -> None:
    log_path = os.path.join(run_path, "log.txt")
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    with open(log_path, "a", encoding="utf-8") as f:
        f.write(f"{timestamp} - {message}\n")


def montar_nichos(principal: str, extras: str) -> List[str]:
    nichos = []

    if principal:
        nichos.append(principal.strip())

    if extras:
        extras_lista = [x.strip() for x in extras.split(",") if x.strip()]
        nichos.extend(extras_lista)

    return list(dict.fromkeys(nichos))


@app.get("/")
def home():
    return {"status": "ok", "message": "API rodando 🚀"}


@app.get("/teste")
def teste():
    return {"msg": "Funcionando 100%"}


@app.get("/app")
def frontend():
    return FileResponse("static/index.html")


@app.post("/searches")
def create_search(data: SearchRequest):
    nichos = montar_nichos(
        principal=data.palavra_chave_principal,
        extras=data.palavras_chave_extras or ""
    )

    first_keyword = nichos[0] if nichos else "busca"
    run_path = create_run_folder(first_keyword)

    request_data = {
        "cidade": data.cidade,
        "nichos": nichos,
        "created_at": datetime.now().isoformat(),
        "status": "running",
    }

    request_json_path = os.path.join(run_path, "request.json")

    with open(request_json_path, "w", encoding="utf-8") as f:
        json.dump(request_data, f, ensure_ascii=False, indent=2)

    write_log(run_path, "Nova busca criada")
    write_log(run_path, f"Cidade: {data.cidade}")
    write_log(run_path, f"Nichos: {', '.join(nichos)}")

    result = run_collection(
        cidade=data.cidade,
        nichos=nichos,
        run_path=run_path,
        limite_resultados=data.limite_resultados,
        filtros=data.filtros.model_dump(),
    )

    return {
        "status": "ok",
        "total_leads": result["total"],
        "leads": result["leads"]
    }