import os
import re
import json
import math
import time
from typing import Any, Dict, List, Optional

import requests


GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY") or os.getenv("GOOGLE_PLACES_API_KEY", "")

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

    # variações automáticas para melhorar resultado
    queries.append(f"{principal} {cidade}")
    queries.append(f"empresa de {principal} em {cidade}")
    queries.append(f"loja de {principal} em {cidade}")

    # remove duplicadas preservando ordem
    final = []
    seen = set()
    for q in queries:
        key = q.lower()
        if key not in seen:
            seen.add(key)
            final.append(q)

    return final


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
    m = re.search(r"(https?://wa\.me/[0-9]+)", text, re.IGNORECASE)
    if m:
        return m.group(1)

    m2 = re.search(r"(\+?55\s?\(?\d{2}\)?\s?\d{4,5}\-?\d{4})", text)
    return m2.group(1) if m2 else ""


def _extract_email(text: str) -> str:
    if not text:
        return ""
    m = re.search(r"([A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,})", text)
    return m.group(1) if m else ""


def _extract_cnpj(text: str) -> str:
    if not text:
        return ""
    m = re.search(r"(\d{2}\.?\d{3}\.?\d{3}/?\d{4}\-?\d{2})", text)
    return m.group(1) if m else ""


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
            "places.shortFormattedAddress"
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
            headers={
                "User-Agent": "Mozilla/5.0"
            }
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


def _classify_company(place: Dict[str, Any]) -> str:
    primary_type = (_safe_get(place, "primaryType") or "").lower()
    primary_name = (_safe_get(place, "primaryTypeDisplayName", "text") or "").lower()
    joined = f"{primary_type} {primary_name}"

    if any(x in joined for x in ["manufacturer", "fabric", "factory", "fabrica", "industria"]):
        return "Fabricante"
    if any(x in joined for x in ["wholesaler", "distribution", "distributor", "atacado"]):
        return "Distribuidor"
    return "Todos"


def _maps_place_to_lead(place: Dict[str, Any], cidade: str, palavra_chave_principal: str) -> Dict[str, Any]:
    website = _safe_get(place, "websiteUri", default="") or ""
    website_info = _enrich_from_website(website) if website else {}

    phone = (
        _safe_get(place, "nationalPhoneNumber")
        or _safe_get(place, "internationalPhoneNumber")
        or ""
    )

    lead = {
        "id": _safe_get(place, "id", default=""),
        "nome": _safe_get(place, "displayName", "text", default="") or "Sem nome",
        "endereco": _safe_get(place, "formattedAddress", default="") or "",
        "endereco_curto": _safe_get(place, "shortFormattedAddress", default="") or "",
        "cidade": cidade,
        "telefone": phone,
        "whatsapp": website_info.get("whatsapp", ""),
        "email": website_info.get("email", ""),
        "instagram": website_info.get("instagram", ""),
        "facebook": website_info.get("facebook", ""),
        "site": website,
        "google_maps_url": _safe_get(place, "googleMapsUri", default="") or "",
        "rating": _safe_get(place, "rating", default=0) or 0,
        "reviews": _safe_get(place, "userRatingCount", default=0) or 0,
        "cnpj": website_info.get("cnpj", ""),
        "tipo_empresa": _classify_company(place),
        "status_empresa": _safe_get(place, "businessStatus", default="") or "",
        "palavra_chave": palavra_chave_principal,
        "lead_score": 0,
    }

    lead["lead_score"] = _calculate_score(lead)
    return lead


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

    # busca em múltiplas variações
    for query in queries:
        try:
            places = _request_places(query=query, max_result_count=max(10, limite_resultados * 2))
        except Exception:
            continue

        for place in places:
            place_id = _safe_get(place, "id", default="")
            if not place_id or place_id in seen_ids:
                continue

            seen_ids.add(place_id)
            lead = _maps_place_to_lead(place, cidade, palavra_chave_principal)
            encontrados.append(lead)

        # pequena pausa para não bater tudo de uma vez
        time.sleep(0.3)

        if len(encontrados) >= limite_resultados * 4:
            break

    # aplica filtros
    encontrados = _apply_filters(encontrados, filtros)

    # ordena melhor lead primeiro
    encontrados.sort(key=lambda x: (x.get("lead_score", 0), x.get("reviews", 0), x.get("rating", 0)), reverse=True)

    # corta no limite final
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
