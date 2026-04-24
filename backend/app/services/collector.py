import os
import re
import time
from typing import Any, Dict, List, Optional

import requests

from app.services.cnpjbiz_enricher import (
    enrich_from_cnpj_base,
    enrich_from_cnpjbiz,
    validar_cnpj,
)

GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY") or os.getenv("GOOGLE_PLACES_API_KEY", "")
SERPER_API_KEY = os.getenv("SERPER_API_KEY", "")
PLACES_TEXT_SEARCH_URL = "https://places.googleapis.com/v1/places:searchText"
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "30"))


def _safe_get(d: Dict[str, Any], *keys, default=None):
    cur = d
    for k in keys:
        if not isinstance(cur, dict):
            return default
        cur = cur.get(k)
        if cur is None:
            return default
    return cur


def _normalize_text(value: Optional[str]) -> str:
    if not value:
        return ""
    return re.sub(r"\s+", " ", value).strip()


def _slugify(text: str) -> str:
    text = text.lower().strip()
    text = re.sub(r"[^a-z0-9\s\-]", "", text)
    text = re.sub(r"\s+", "-", text)
    return text


def _only_digits(value: str) -> str:
    return re.sub(r"\D", "", value or "")


def _first_non_empty(*values):
    for value in values:
        if value:
            return value
    return ""


def _extract_cnpj(text: str) -> str:
    if not text:
        return ""

    matches = re.findall(r"(\d{2}\.?\d{3}\.?\d{3}/?\d{4}\-?\d{2}|\d{14})", text)

    for item in matches:
        digits = _only_digits(item)
        if validar_cnpj(digits):
            return digits

    return ""


def _extract_email(text: str) -> str:
    if not text:
        return ""
    m = re.search(r"([A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,})", text)
    return m.group(1) if m else ""


def _extract_instagram(text: str) -> str:
    if not text:
        return ""
    m = re.search(r"(https?://(?:www\.)?instagram\.com/[A-Za-z0-9._\-/?=&]+)", text, re.IGNORECASE)
    return m.group(1) if m else ""


def _extract_facebook(text: str) -> str:
    if not text:
        return ""
    m = re.search(r"(https?://(?:www\.)?facebook\.com/[A-Za-z0-9._\-/?=&]+)", text, re.IGNORECASE)
    return m.group(1) if m else ""


def _extract_whatsapp(text: str) -> str:
    if not text:
        return ""

    m = re.search(r"(https?://wa\.me/\d+)", text, re.IGNORECASE)
    if m:
        return m.group(1)

    m2 = re.search(r"(\+?55\s?\(?\d{2}\)?\s?\d{4,5}\-?\d{4})", text)
    return m2.group(1) if m2 else ""


def _build_queries(cidade: str, principal: str, extras: str) -> List[str]:
    queries = []

    cidade = _normalize_text(cidade)
    principal = _normalize_text(principal)
    extras = _normalize_text(extras)

    if principal:
        queries.append(f"{principal} em {cidade}")

    if extras:
        extras_list = [x.strip() for x in extras.split(",") if x.strip()]
        for extra in extras_list:
            queries.append(f"{principal} {extra} em {cidade}")
            queries.append(f"{extra} em {cidade}")

    queries.append(f"{principal} {cidade}")
    queries.append(f"empresa de {principal} em {cidade}")
    queries.append(f"loja de {principal} em {cidade}")

    final = []
    seen = set()

    for q in queries:
        key = q.lower()
        if key not in seen:
            seen.add(key)
            final.append(q)

    return final


def _request_places(query: str, max_result_count: int = 10) -> List[Dict[str, Any]]:
    if not GOOGLE_API_KEY:
        raise Exception("Variável GOOGLE_API_KEY não configurada no Railway.")

    headers = {
        "Content-Type": "application/json",
        "X-Goog-Api-Key": GOOGLE_API_KEY,
        "X-Goog-FieldMask": ",".join([
            "places.id",
            "places.displayName",
            "places.formattedAddress",
            "places.nationalPhoneNumber",
            "places.internationalPhoneNumber",
            "places.websiteUri",
            "places.googleMapsUri",
            "places.rating",
            "places.userRatingCount",
            "places.businessStatus",
            "places.primaryType",
            "places.primaryTypeDisplayName",
            "places.location",
            "places.shortFormattedAddress",
            "places.photos",
        ])
    }

    payload = {
        "textQuery": query,
        "maxResultCount": max_result_count,
        "languageCode": "pt-BR"
    }

    response = requests.post(
        PLACES_TEXT_SEARCH_URL,
        headers=headers,
        json=payload,
        timeout=REQUEST_TIMEOUT
    )

    if response.status_code >= 400:
        raise Exception(f"Erro Google Places: {response.status_code} - {response.text}")

    data = response.json()
    return data.get("places", [])


def _enrich_from_website(website_url: str) -> Dict[str, Any]:
    info = {
        "email": "",
        "instagram": "",
        "facebook": "",
        "whatsapp": "",
        "cnpj": "",
    }

    if not website_url:
        return info

    try:
        resp = requests.get(
            website_url,
            timeout=15,
            headers={"User-Agent": "Mozilla/5.0"}
        )

        if resp.status_code >= 400:
            return info

        html = resp.text

        info["email"] = _extract_email(html)
        info["instagram"] = _extract_instagram(html)
        info["facebook"] = _extract_facebook(html)
        info["whatsapp"] = _extract_whatsapp(html)
        info["cnpj"] = _extract_cnpj(html)

    except Exception:
        pass

    return info


def _buscar_cnpj_com_serper(nome_empresa: str, cidade: str) -> str:
    """
    Busca CNPJ em resultados do Google via Serper.
    Melhor do que tentar raspar google.com diretamente.
    """
    if not SERPER_API_KEY or not nome_empresa:
        return ""

    consultas = [
        f"{nome_empresa} cnpj {cidade}",
        f"{nome_empresa} cnpj",
        f"{nome_empresa} razão social cnpj {cidade}",
        f"site:cnpj.biz {nome_empresa}",
        f"site:econodata.com.br {nome_empresa} {cidade}",
        f"site:casadosdados.com.br {nome_empresa} {cidade}",
        f"\"{nome_empresa}\" \"CNPJ\"",
    ]

    headers = {
        "X-API-KEY": SERPER_API_KEY,
        "Content-Type": "application/json",
    }

    for consulta in consultas:
        try:
            resp = requests.post(
                "https://google.serper.dev/search",
                headers=headers,
                json={
                    "q": consulta,
                    "gl": "br",
                    "hl": "pt-br",
                    "num": 10,
                },
                timeout=REQUEST_TIMEOUT
            )

            if resp.status_code >= 400:
                continue

            data = resp.json()

            textos = []

            # resposta orgânica
            for item in data.get("organic", []) or []:
                textos.append(item.get("title", ""))
                textos.append(item.get("snippet", ""))
                textos.append(item.get("link", ""))

            # resposta enriquecida / answer box
            answer_box = data.get("answerBox") or {}
            if isinstance(answer_box, dict):
                textos.append(answer_box.get("title", ""))
                textos.append(answer_box.get("answer", ""))
                textos.append(answer_box.get("snippet", ""))

            # knowledge graph
            knowledge = data.get("knowledgeGraph") or {}
            if isinstance(knowledge, dict):
                textos.append(knowledge.get("title", ""))
                textos.append(knowledge.get("description", ""))

            texto_total = "\n".join([t for t in textos if t])
            cnpj = _extract_cnpj(texto_total)

            if cnpj and validar_cnpj(cnpj):
                return cnpj

        except Exception:
            pass

        time.sleep(0.3)

    return ""


def _get_place_photo(place: Dict[str, Any]) -> Dict[str, Any]:
    result = {
        "foto_maps_url": "",
        "foto_maps_atribuicoes": [],
    }

    photos = place.get("photos") or []
    if not photos:
        return result

    first_photo = photos[0]
    photo_name = first_photo.get("name", "")
    author_attributions = first_photo.get("authorAttributions", []) or []

    if not photo_name or not GOOGLE_API_KEY:
        result["foto_maps_atribuicoes"] = author_attributions
        return result

    try:
        photo_url = (
            f"https://places.googleapis.com/v1/{photo_name}/media"
            f"?key={GOOGLE_API_KEY}&maxWidthPx=600&skipHttpRedirect=true"
        )

        resp = requests.get(photo_url, timeout=REQUEST_TIMEOUT)

        if resp.status_code < 400:
            data = resp.json()
            result["foto_maps_url"] = data.get("photoUri", "")
            result["foto_maps_atribuicoes"] = author_attributions

    except Exception:
        pass

    return result


def _classify_company(place: Dict[str, Any], cnpj_base_data: Dict[str, Any], cnpjbiz_data: Dict[str, Any]) -> str:
    primary_type = (_safe_get(place, "primaryType") or "").lower()
    primary_name = (_safe_get(place, "primaryTypeDisplayName", "text") or "").lower()

    cnae = _first_non_empty(
        cnpj_base_data.get("cnae_principal", ""),
        cnpjbiz_data.get("cnpjbiz_cnae_principal", "")
    ).lower()

    joined = f"{primary_type} {primary_name} {cnae}"

    if any(x in joined for x in ["fabricação", "fabricacao", "fabricante", "industria", "indústria", "factory", "manufacturer"]):
        return "Fabricante"

    if any(x in joined for x in ["atacado", "atacadista", "distribuidor", "distribuidora", "distribution", "wholesale"]):
        return "Distribuidor"

    if any(x in joined for x in ["importador", "importadora", "importação", "importacao", "import"]):
        return "Importador"

    if any(x in joined for x in ["varejo", "revenda", "revendedor", "comércio varejista", "comercio varejista", "loja", "retail"]):
        return "Revendedor"

    return "Todos"


def _calculate_score(lead: Dict[str, Any]) -> int:
    score = 0

    if lead.get("whatsapp"):
        score += 20
    if lead.get("instagram"):
        score += 10
    if lead.get("site"):
        score += 15
    if lead.get("email"):
        score += 15
    if lead.get("cnpj"):
        score += 15
    if lead.get("rating", 0) >= 4.0:
        score += 10
    if lead.get("reviews", 0) >= 10:
        score += 10
    if lead.get("status_empresa") == "OPERATIONAL":
        score += 5

    return min(score, 100)


def _maps_place_to_lead(place: Dict[str, Any], cidade: str, palavra_chave_principal: str) -> Dict[str, Any]:
    nome_empresa = _safe_get(place, "displayName", "text", default="") or "Sem nome"
    website = _safe_get(place, "websiteUri", default="") or ""

    website_info = _enrich_from_website(website) if website else {}

    phone = (
        _safe_get(place, "nationalPhoneNumber")
        or _safe_get(place, "internationalPhoneNumber")
        or ""
    )

    cnpj_digits = _only_digits(website_info.get("cnpj", ""))

    # fallback profissional: se o site não trouxe CNPJ, procura via Serper
    if not validar_cnpj(cnpj_digits):
        cnpj_serper = _buscar_cnpj_com_serper(nome_empresa, cidade)
        if validar_cnpj(cnpj_serper):
            cnpj_digits = cnpj_serper

    cnpj_base_data = {}
    cnpjbiz_data = {}

    if validar_cnpj(cnpj_digits):
        cnpj_base_data = enrich_from_cnpj_base(cnpj_digits)
        cnpjbiz_data = enrich_from_cnpjbiz(cnpj_digits)

    email_final = _first_non_empty(
        website_info.get("email"),
        cnpjbiz_data.get("cnpjbiz_email", "")
    )

    whatsapp_final = _first_non_empty(
        website_info.get("whatsapp"),
        cnpjbiz_data.get("cnpjbiz_whatsapp", "")
    )

    telefone_final = _first_non_empty(
        phone,
        cnpjbiz_data.get("cnpjbiz_telefone", "")
    )

    foto_maps = _get_place_photo(place)

    lead = {
        "id": _safe_get(place, "id", default=""),
        "nome": nome_empresa,
        "razao_social": _first_non_empty(
            cnpj_base_data.get("razao_social", ""),
            cnpjbiz_data.get("cnpjbiz_razao_social", "")
        ),
        "nome_fantasia": _first_non_empty(
            cnpj_base_data.get("nome_fantasia", ""),
            cnpjbiz_data.get("cnpjbiz_nome_fantasia", "")
        ),
        "endereco": _safe_get(place, "formattedAddress", default="") or "",
        "endereco_curto": _safe_get(place, "shortFormattedAddress", default="") or "",
        "cidade": cidade,
        "telefone": telefone_final,
        "whatsapp": whatsapp_final,
        "email": email_final,
        "instagram": website_info.get("instagram", ""),
        "facebook": website_info.get("facebook", ""),
        "site": website,
        "google_maps_url": _safe_get(place, "googleMapsUri", default="") or "",
        "rating": _safe_get(place, "rating", default=0) or 0,
        "reviews": _safe_get(place, "userRatingCount", default=0) or 0,
        "cnpj": cnpj_digits if validar_cnpj(cnpj_digits) else "",
        "tipo_empresa": "",
        "status_empresa": _safe_get(place, "businessStatus", default="") or "",
        "palavra_chave": palavra_chave_principal,
        "lead_score": 0,

        "situacao_cadastral": _first_non_empty(
            cnpj_base_data.get("situacao_cadastral", ""),
            cnpjbiz_data.get("cnpjbiz_situacao_cadastral", "")
        ),
        "data_abertura": _first_non_empty(
            cnpj_base_data.get("data_abertura", ""),
            cnpjbiz_data.get("cnpjbiz_data_abertura", "")
        ),
        "cnae_principal": _first_non_empty(
            cnpj_base_data.get("cnae_principal", ""),
            cnpjbiz_data.get("cnpjbiz_cnae_principal", "")
        ),
        "porte": _first_non_empty(
            cnpj_base_data.get("porte", ""),
            cnpjbiz_data.get("cnpjbiz_porte", "")
        ),
        "natureza_juridica": _first_non_empty(
            cnpj_base_data.get("natureza_juridica", ""),
            cnpjbiz_data.get("cnpjbiz_natureza_juridica", "")
        ),
        "capital_social": _first_non_empty(
            cnpj_base_data.get("capital_social", ""),
            cnpjbiz_data.get("cnpjbiz_capital_social", "")
        ),
        "matriz_filial": _first_non_empty(
            cnpj_base_data.get("matriz_filial", ""),
            cnpjbiz_data.get("cnpjbiz_matriz_filial", "")
        ),
        "uf": cnpj_base_data.get("uf", ""),
        "municipio": cnpj_base_data.get("municipio", ""),
        "cnpjbiz_url": cnpjbiz_data.get("cnpjbiz_url", ""),

        "foto_maps_url": foto_maps.get("foto_maps_url", ""),
        "foto_maps_atribuicoes": foto_maps.get("foto_maps_atribuicoes", []),
    }

    lead["tipo_empresa"] = _classify_company(place, cnpj_base_data, cnpjbiz_data)
    lead["lead_score"] = _calculate_score(lead)

    return lead


def _city_matches(cidade_busca: str, endereco: str) -> bool:
    if not cidade_busca or not endereco:
        return True

    cidade_busca = cidade_busca.lower().strip()
    endereco = endereco.lower()

    return cidade_busca in endereco


def _apply_filters(leads: List[Dict[str, Any]], filtros: Dict[str, Any]) -> List[Dict[str, Any]]:
    filtrados = []

    apenas_com_whatsapp = filtros.get("apenas_com_whatsapp", False)
    apenas_com_instagram = filtros.get("apenas_com_instagram", False)
    apenas_com_site = filtros.get("apenas_com_site", False)
    apenas_com_email = filtros.get("apenas_com_email", False)
    apenas_com_cnpj = filtros.get("apenas_com_cnpj", False)
    tipo_empresa = filtros.get("tipo_empresa", "Todos")
    avaliacao_minima = filtros.get("avaliacao_minima", 0)

    for lead in leads:
        if apenas_com_whatsapp and not lead.get("whatsapp"):
            continue
        if apenas_com_instagram and not lead.get("instagram"):
            continue
        if apenas_com_site and not lead.get("site"):
            continue
        if apenas_com_email and not lead.get("email"):
            continue
        if apenas_com_cnpj and not lead.get("cnpj"):
            continue
        if tipo_empresa and tipo_empresa != "Todos" and lead.get("tipo_empresa") != tipo_empresa:
            continue
        if float(lead.get("rating", 0) or 0) < float(avaliacao_minima or 0):
            continue

        filtrados.append(lead)

    return filtrados


def run_search(payload: Dict[str, Any]) -> Dict[str, Any]:
    cidade = _normalize_text(payload.get("cidade", ""))
    palavra_chave_principal = _normalize_text(payload.get("palavra_chave_principal", ""))
    palavras_chave_extras = _normalize_text(payload.get("palavras_chave_extras", ""))
    limite_resultados = int(payload.get("limite_resultados", 5))
    filtros = payload.get("filtros", {}) or {}

    if not cidade:
        raise Exception("Cidade não informada.")
    if not palavra_chave_principal:
        raise Exception("Palavra-chave principal não informada.")

    queries = _build_queries(cidade, palavra_chave_principal, palavras_chave_extras)

    encontrados: List[Dict[str, Any]] = []
    seen_ids = set()
    seen_cnpj = set()

    for query in queries:
        try:
            places = _request_places(query=query, max_result_count=max(10, limite_resultados * 3))
        except Exception:
            continue

        for place in places:
            place_id = _safe_get(place, "id", default="")
            if not place_id or place_id in seen_ids:
                continue

            lead = _maps_place_to_lead(place, cidade, palavra_chave_principal)

            if not _city_matches(cidade, lead.get("endereco", "")):
                continue

            seen_ids.add(place_id)

            cnpj = lead.get("cnpj", "")
            if cnpj and cnpj in seen_cnpj:
                continue

            if cnpj:
                seen_cnpj.add(cnpj)

            encontrados.append(lead)

        time.sleep(0.3)

        if len(encontrados) >= limite_resultados * 5:
            break

    encontrados = _apply_filters(encontrados, filtros)

    encontrados.sort(
        key=lambda x: (x.get("lead_score", 0), x.get("reviews", 0), x.get("rating", 0)),
        reverse=True
    )

    encontrados = encontrados[:limite_resultados]

    return {
        "status": "ok",
        "cidade": cidade,
        "palavra_chave_principal": palavra_chave_principal,
        "palavras_chave_extras": palavras_chave_extras,
        "total_leads": len(encontrados),
        "run_folder": f"online_{_slugify(cidade)}_{_slugify(palavra_chave_principal)}",
        "nichos": [palavra_chave_principal] + ([x.strip() for x in palavras_chave_extras.split(",") if x.strip()] if palavras_chave_extras else []),
        "leads": encontrados
    }
