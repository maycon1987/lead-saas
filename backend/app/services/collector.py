import os
import requests
import re
from urllib.parse import quote_plus

GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", 30))

CNPJ_API = os.getenv("CNPJ_BASE_API_URL", "https://brasilapi.com.br/api/cnpj/v1")

# =========================
# UTILIDADES
# =========================

def limpar_texto(txt):
    if not txt:
        return ""
    return re.sub(r"\s+", " ", txt).strip()

def extrair_email(texto):
    emails = re.findall(r"[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+", texto or "")
    return emails[0] if emails else None

def extrair_whatsapp(texto):
    numeros = re.findall(r"\d{10,13}", texto or "")
    return numeros[0] if numeros else None

# =========================
# CLASSIFICAÇÃO INTELIGENTE
# =========================

def classificar_empresa(nome, cnae, descricao):
    texto = f"{nome} {cnae} {descricao}".lower()

    if "fabric" in texto:
        return "Fabricante"

    if "atacad" in texto or "distrib" in texto:
        return "Distribuidor / Atacadista"

    if "import" in texto:
        return "Importador"

    return "Revendedor"

# =========================
# GOOGLE PLACES
# =========================

def buscar_google_places(query):
    url = f"https://maps.googleapis.com/maps/api/place/textsearch/json?query={quote_plus(query)}&key={GOOGLE_API_KEY}"

    try:
        resp = requests.get(url, timeout=REQUEST_TIMEOUT).json()
        return resp.get("results", [])
    except:
        return []

def buscar_detalhes_place(place_id):
    url = f"https://maps.googleapis.com/maps/api/place/details/json?place_id={place_id}&key={GOOGLE_API_KEY}"

    try:
        resp = requests.get(url, timeout=REQUEST_TIMEOUT).json()
        return resp.get("result", {})
    except:
        return {}

def montar_foto(place):
    if "photos" in place:
        photo_ref = place["photos"][0]["photo_reference"]
        return f"https://maps.googleapis.com/maps/api/place/photo?maxwidth=400&photo_reference={photo_ref}&key={GOOGLE_API_KEY}"
    return None

# =========================
# CNPJ
# =========================

def buscar_cnpj(cnpj):
    if not cnpj:
        return {}

    try:
        url = f"{CNPJ_API}/{cnpj}"
        return requests.get(url, timeout=REQUEST_TIMEOUT).json()
    except:
        return {}

# =========================
# FALLBACK INTELIGENTE
# =========================

def fallback_busca(nome, cidade):
    query = f"{nome} {cidade}"
    resultados = buscar_google_places(query)

    if not resultados:
        return None

    return resultados[0]

# =========================
# PROCESSAMENTO PRINCIPAL
# =========================

def buscar_leads(dados):
    cidade = dados.get("cidade")
    palavra = dados.get("palavra_chave_principal")
    limite = dados.get("limite_resultados", 5)

    query = f"{palavra} em {cidade}"

    resultados = buscar_google_places(query)

    leads = []

    for item in resultados[:limite]:

        nome = item.get("name")
        endereco = item.get("formatted_address")
        rating = item.get("rating", 0)
        place_id = item.get("place_id")

        detalhes = buscar_detalhes_place(place_id)

        telefone = detalhes.get("formatted_phone_number")
        site = detalhes.get("website")

        foto = montar_foto(item)

        descricao = " ".join(item.get("types", []))

        # =========================
        # TENTAR PEGAR CNPJ DO SITE
        # =========================
        cnpj = None
        email = None
        whatsapp = None

        if site:
            try:
                html = requests.get(site, timeout=10).text
                cnpj_match = re.findall(r"\d{14}", html)
                if cnpj_match:
                    cnpj = cnpj_match[0]

                email = extrair_email(html)
                whatsapp = extrair_whatsapp(html)

            except:
                pass

        # =========================
        # ENRIQUECER COM CNPJ
        # =========================
        dados_cnpj = buscar_cnpj(cnpj) if cnpj else {}

        cnae = dados_cnpj.get("cnae_fiscal_descricao", "")
        situacao = dados_cnpj.get("descricao_situacao_cadastral")

        # =========================
        # FALLBACK (SE FALTAR DADOS)
        # =========================
        if not telefone or not site:
            fallback = fallback_busca(nome, cidade)

            if fallback:
                detalhes_fb = buscar_detalhes_place(fallback["place_id"])

                telefone = telefone or detalhes_fb.get("formatted_phone_number")
                site = site or detalhes_fb.get("website")

        # =========================
        # CLASSIFICAÇÃO
        # =========================
        tipo_empresa = classificar_empresa(nome, cnae, descricao)

        lead = {
            "nome": nome,
            "endereco": endereco,
            "telefone": telefone,
            "site": site,
            "email": email,
            "whatsapp": whatsapp,
            "cnpj": cnpj,
            "cnae": cnae,
            "situacao_cadastral": situacao,
            "tipo_empresa": tipo_empresa,
            "rating": rating,
            "foto": foto,
        }

        leads.append(lead)

    return {
        "status": "ok",
        "cidade": cidade,
        "total_leads": len(leads),
        "leads": leads
    }
