import os
import requests
import time
import re
from typing import List, Dict

from app.services.cnpjbiz_enricher import enrich_from_cnpjbiz


GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
SERPER_API_KEY = os.getenv("SERPER_API_KEY")
CNPJ_BASE_URL = os.getenv("CNPJ_BASE_API_URL", "https://brasilapi.com.br/api/cnpj/v1")

TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", 30))


# =========================
# Helpers
# =========================

def limpar_texto(txt):
    if not txt:
        return ""
    return re.sub(r"\s+", " ", txt).strip()


def extrair_cnpj(texto):
    if not texto:
        return None
    match = re.search(r"\d{2}\.\d{3}\.\d{3}/\d{4}-\d{2}", texto)
    if match:
        return match.group(0)
    match = re.search(r"\d{14}", texto)
    if match:
        return match.group(0)
    return None


# =========================
# Google Places
# =========================

def buscar_places(query, cidade):
    url = "https://maps.googleapis.com/maps/api/place/textsearch/json"

    params = {
        "query": f"{query} em {cidade}",
        "key": GOOGLE_API_KEY
    }

    resp = requests.get(url, params=params, timeout=TIMEOUT)
    data = resp.json()

    return data.get("results", [])


def buscar_detalhes_place(place_id):
    url = "https://maps.googleapis.com/maps/api/place/details/json"

    params = {
        "place_id": place_id,
        "fields": "name,formatted_phone_number,website,formatted_address,rating,user_ratings_total,photos",
        "key": GOOGLE_API_KEY
    }

    resp = requests.get(url, params=params, timeout=TIMEOUT)
    data = resp.json()

    return data.get("result", {})


def pegar_foto(photo_reference):
    return f"https://maps.googleapis.com/maps/api/place/photo?maxwidth=400&photo_reference={photo_reference}&key={GOOGLE_API_KEY}"


# =========================
# Scraping básico do site
# =========================

def extrair_contatos_site(url):
    try:
        html = requests.get(url, timeout=TIMEOUT).text

        email = re.findall(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", html)
        whatsapp = re.findall(r"\+?\d{10,13}", html)

        instagram = re.findall(r"instagram\.com/[A-Za-z0-9_.]+", html)
        facebook = re.findall(r"facebook\.com/[A-Za-z0-9_.]+", html)

        return {
            "email": email[0] if email else "",
            "whatsapp": whatsapp[0] if whatsapp else "",
            "instagram": f"https://{instagram[0]}" if instagram else "",
            "facebook": f"https://{facebook[0]}" if facebook else ""
        }

    except:
        return {}


# =========================
# Serper (fallback inteligente)
# =========================

def buscar_google_serper(query):
    if not SERPER_API_KEY:
        return None

    url = "https://google.serper.dev/search"

    headers = {
        "X-API-KEY": SERPER_API_KEY,
        "Content-Type": "application/json"
    }

    payload = {"q": query}

    resp = requests.post(url, json=payload, headers=headers, timeout=TIMEOUT)

    data = resp.json()

    if "organic" in data and len(data["organic"]) > 0:
        snippet = data["organic"][0].get("snippet", "")
        return extrair_cnpj(snippet)

    return None


# =========================
# BrasilAPI
# =========================

def buscar_cnpj_brasilapi(cnpj):
    try:
        url = f"{CNPJ_BASE_URL}/{cnpj}"
        resp = requests.get(url, timeout=TIMEOUT)

        if resp.status_code != 200:
            return {}

        return resp.json()
    except:
        return {}


# =========================
# Classificação
# =========================

def classificar_empresa(texto):
    texto = texto.lower()

    if "fabrica" in texto or "indústria" in texto:
        return "Fabricante"
    if "distribuidora" in texto:
        return "Distribuidor"
    if "atacado" in texto:
        return "Atacadista"
    if "import" in texto:
        return "Importador"

    return "Revendedor"


# =========================
# Função principal
# =========================

def run_search(payload: Dict):

    cidade = payload.get("cidade")
    palavra = payload.get("palavra_chave_principal")
    limite = payload.get("limite_resultados", 10)

    modo = payload.get("modo_busca", "rapida")

    # Auto detectar completo
    if payload.get("buscar_cnpj") or payload.get("buscar_cnae"):
        modo = "completa"

    resultados = buscar_places(palavra, cidade)

    leads = []

    for r in resultados[:limite]:

        detalhe = buscar_detalhes_place(r["place_id"])

        lead = {
            "nome": r.get("name"),
            "endereco": detalhe.get("formatted_address"),
            "telefone": detalhe.get("formatted_phone_number", ""),
            "site": detalhe.get("website", ""),
            "rating": detalhe.get("rating", 0),
            "reviews": detalhe.get("user_ratings_total", 0),
            "tipo_empresa": classificar_empresa(r.get("name", "")),
        }

        # Foto
        photos = detalhe.get("photos")
        if photos:
            lead["foto"] = pegar_foto(photos[0]["photo_reference"])

        # =========================
        # MODO RÁPIDO
        # =========================
        if modo == "rapida":
            if lead.get("site"):
                contatos = extrair_contatos_site(lead["site"])
                lead.update(contatos)

            leads.append(lead)
            continue

        # =========================
        # MODO COMPLETO
        # =========================

        cnpj = None

        # 1. tenta no site
        if lead.get("site"):
            html = requests.get(lead["site"], timeout=TIMEOUT).text
            cnpj = extrair_cnpj(html)

        # 2. tenta via google (SERPER)
        if not cnpj:
            cnpj = buscar_google_serper(f"{lead['nome']} {cidade} cnpj")

        lead["cnpj"] = cnpj or ""

        # 3. BrasilAPI
        if cnpj:
            dados = buscar_cnpj_brasilapi(cnpj)
            lead["cnae"] = dados.get("cnae_fiscal_descricao")
            lead["capital_social"] = dados.get("capital_social")
            lead["natureza_juridica"] = dados.get("natureza_juridica")
            lead["situacao_cadastral"] = dados.get("descricao_situacao_cadastral")

        # 4. CNPJ.BIZ (com Playwright)
        if cnpj:
            try:
                dados_biz = enrich_from_cnpjbiz(cnpj)
                lead.update(dados_biz)
            except:
                pass

        # 5. contatos
        if lead.get("site"):
            contatos = extrair_contatos_site(lead["site"])
            lead.update(contatos)

        leads.append(lead)

    return {
        "status": "ok",
        "total_leads": len(leads),
        "leads": leads
    }
