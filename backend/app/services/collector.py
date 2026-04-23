import os
import re
import time
from typing import Any, Dict, List, Optional

import requests
from supabase import create_client, Client

from app.services.cnpjbiz_enricher import (
    enrich_from_cnpj_base,
    enrich_from_cnpjbiz,
    validar_cnpj,
)

GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY") or os.getenv("GOOGLE_PLACES_API_KEY", "")
PLACES_TEXT_SEARCH_URL = "https://places.googleapis.com/v1/places:searchText"
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "30"))

SUPABASE_URL = os.getenv("SUPABASE_URL", "")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "")

supabase: Optional[Client] = None
if SUPABASE_URL and SUPABASE_KEY:
    try:
        supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    except Exception:
        supabase = None


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
    m = re.search(r"(https?://wa\.me/\d+)", text, re.IGNORECASE)
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

    matches = re.findall(r"(\d{2}\.?\d{3}\.?\d{3}/?\d{4}\-?\d{2}|\d{14})", text)
    for m in matches:
        digits = _only_digits(m)
        if validar_cnpj(digits):
            return digits
    return ""


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


def _buscar_cnpj_no_google(nome_empresa: str, cidade: str) -> str:
    if not nome_empresa:
        return ""

    consultas = [
        f"{nome_empresa} cnpj {cidade}",
        f"{nome_empresa} cnpj",
        f"site:cnpj.biz {nome_empresa}",
        f"site:econodata.com.br {nome_empresa} {cidade}",
        f"\"{nome_empresa}\" \"cnpj\"",
    ]

    headers = {
        "User-Agent": "Mozilla/5.0"
    }

    for consulta in consultas:
        try:
            url = f"https://www.google.com/search?q={requests.utils.quote(consulta)}&hl=pt-BR"
            resp = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)

            if resp.status_code >= 400:
                continue

            html = resp.text
            cnpj = _extract_cnpj(html)
            if cnpj and validar_cnpj(cnpj):
                return cnpj

        except Exception:
            continue

        time.sleep(0.4)

    return ""


def _classify_company(place: Dict[str, Any], cnpj_base_data: Dict[str, Any], cnpjbiz_data: Dict[str, Any]) -> str:
    primary_type = (_safe_get(place, "primaryType") or "").lower()
    primary_name = (_safe_get(place, "primaryTypeDisplayName", "text") or "").lower()

    cnae = _first_non_empty(
        cnpj_base_data.get("cnae_principal", ""),
        cnpjbiz_data.get("cnpjbiz_cnae_principal", "")
    ).lower()

    joined = f"{primary_type} {primary_name} {cnae}"

    if any(x in joined for x in ["manufacturer", "fabric", "factory", "fabrica", "industria", "fabricação", "fabricacao"]):
        return "Fabricante"
    if any(x in joined for x in ["wholesale", "wholesaler", "distribution", "distributor", "atacado", "atacadista"]):
        return "Distribuidor"
    if any(x in joined for x in ["import", "importacao", "importação", "importador"]):
        return "Importador"
    if any(x in joined for x in ["retail", "comercio", "loja", "varejo", "revenda", "revendedor"]):
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
    website = _safe_get(place, "websiteUri", default="") or ""
    nome_empresa = _safe_get(place, "displayName", "text", default="") or ""

    website_info = _enrich_from_website(website) if website else {}

    phone = (
        _safe_get(place, "nationalPhoneNumber")
        or _safe_get(place, "internationalPhoneNumber")
        or ""
    )

    cnpj = website_info.get("cnpj", "")
    cnpj_digits = _only_digits(cnpj) if cnpj else ""

    if not validar_cnpj(cnpj_digits):
        cnpj_google = _buscar_cnpj_no_google(nome_empresa, cidade)
        if validar_cnpj(cnpj_google):
            cnpj_digits = cnpj_google

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

    lead = {
        "place_id": _safe_get(place, "id", default=""),
        "nome": nome_empresa or "Sem nome",
        "razao_social": _first_non_empty(
            cnpj_base_data.get("razao_social", ""),
            cnpjbiz_data.get("cnpjbiz_razao_social", "")
        ),
        "nome_fantasia": _first_non_empty(
            cnpj_base_data.get("nome_fantasia", ""),
            cnpjbiz_data.get("cnpjbiz_nome_fantasia", "")
        ),
        "endereco": _safe_get(place, "formattedAddress", default="") or "",
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
        "cnpjbiz_url": cnpjbiz_data.get("cnpjbiz_url", ""),
        "lead_score": 0,
        "status_empresa": _safe_get(place, "businessStatus", default="") or "",
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


def _save_search_run(cidade: str, palavra_chave_principal: str, palavras_chave_extras: str, limite_resultados: int, total_encontrado: int) -> Optional[str]:
    if not supabase:
        return None

    try:
        result = supabase.table("search_runs").insert({
            "cidade": cidade,
            "palavra_chave_principal": palavra_chave_principal,
            "palavras_chave_extras": palavras_chave_extras,
            "limite_resultados": limite_resultados,
            "total_encontrado": total_encontrado,
            "status": "ok",
        }).execute()

        if result.data and len(result.data) > 0:
            return result.data[0]["id"]
    except Exception as e:
        print("Erro ao salvar search_run:", e)

    return None


def _upsert_lead(lead: Dict[str, Any]) -> Optional[str]:
    if not supabase:
        return None

    try:
        cnpj = lead.get("cnpj", "")
        place_id = lead.get("place_id", "")

        existing = None

        if cnpj:
            existing_result = supabase.table("leads").select("id").eq("cnpj", cnpj).limit(1).execute()
            if existing_result.data:
                existing = existing_result.data[0]

        if not existing and place_id:
            existing_result = supabase.table("leads").select("id").eq("place_id", place_id).limit(1).execute()
            if existing_result.data:
                existing = existing_result.data[0]

        if existing:
            lead_id = existing["id"]
            supabase.table("leads").update(lead).eq("id", lead_id).execute()
            return lead_id

        insert_result = supabase.table("leads").insert(lead).execute()
        if insert_result.data and len(insert_result.data) > 0:
            return insert_result.data[0]["id"]

    except Exception as e:
        print("Erro ao salvar lead:", e)

    return None


def _save_search_run_lead(search_run_id: str, lead_id: str, palavra_chave_usada: str) -> None:
    if not supabase or not search_run_id or not lead_id:
        return

    try:
        supabase.table("search_run_leads").insert({
            "search_run_id": search_run_id,
            "lead_id": lead_id,
            "palavra_chave_usada": palavra_chave_usada,
        }).execute()
    except Exception as e:
        print("Erro ao salvar search_run_lead:", e)


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
    seen_place_ids = set()
    seen_cnpjs = set()

    for query in queries:
        try:
            places = _request_places(query=query, max_result_count=max(10, limite_resultados * 3))
        except Exception:
            continue

        for place in places:
            place_id = _safe_get(place, "id", default="")
            if not place_id or place_id in seen_place_ids:
                continue

            lead = _maps_place_to_lead(place, cidade, palavra_chave_principal)

            if not _city_matches(cidade, lead.get("endereco", "")):
                continue

            seen_place_ids.add(place_id)

            cnpj = lead.get("cnpj", "")
            if cnpj and cnpj in seen_cnpjs:
                continue

            if cnpj:
                seen_cnpjs.add(cnpj)

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

    search_run_id = _save_search_run(
        cidade=cidade,
        palavra_chave_principal=palavra_chave_principal,
        palavras_chave_extras=palavras_chave_extras,
        limite_resultados=limite_resultados,
        total_encontrado=len(encontrados),
    )

    for lead in encontrados:
        lead_id = _upsert_lead(lead)
        if lead_id and search_run_id:
            _save_search_run_lead(search_run_id, lead_id, palavra_chave_principal)

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
