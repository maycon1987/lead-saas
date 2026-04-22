import os
import re
from typing import Dict, Any, Optional

import requests
from playwright.sync_api import sync_playwright


REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "30"))
CNPJBIZ_ENABLED = os.getenv("CNPJBIZ_ENABLED", "true").lower() == "true"
CNPJBIZ_BASE_URL = os.getenv("CNPJBIZ_BASE_URL", "https://cnpj.biz")
CNPJBIZ_HEADLESS = os.getenv("CNPJBIZ_HEADLESS", "true").lower() == "true"

CNPJ_BASE_API_URL = os.getenv("CNPJ_BASE_API_URL", "https://brasilapi.com.br/api/cnpj/v1")
CNPJ_BASE_TOKEN = os.getenv("CNPJ_BASE_TOKEN", "")


def only_digits(value: str) -> str:
    return re.sub(r"\D", "", value or "")


def _extract_email(text: str) -> str:
    if not text:
        return ""
    m = re.search(r'([A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,})', text)
    return m.group(1) if m else ""


def _extract_phone(text: str) -> str:
    if not text:
        return ""
    m = re.search(r'(\(?\d{2}\)?\s?\d{4,5}\-?\d{4})', text)
    return m.group(1) if m else ""


def _extract_whatsapp(text: str) -> str:
    if not text:
        return ""
    wa = re.search(r'(https?://wa\.me/\d+)', text, re.IGNORECASE)
    if wa:
        return wa.group(1)

    m = re.search(r'(\+?55\s?\(?\d{2}\)?\s?\d{4,5}\-?\d{4})', text)
    return m.group(1) if m else ""


def enrich_from_cnpj_base(cnpj: str) -> Dict[str, Any]:
    """
    Busca dados cadastrais públicos por CNPJ.
    Usa BrasilAPI como padrão, mas pode usar outra base via env.
    """
    result = {
        "cnpj": cnpj,
        "razao_social": "",
        "nome_fantasia": "",
        "situacao_cadastral": "",
        "data_abertura": "",
        "cnae_principal": "",
        "porte": "",
        "natureza_juridica": "",
        "capital_social": "",
        "matriz_filial": "",
        "uf": "",
        "municipio": "",
    }

    cnpj = only_digits(cnpj)
    if len(cnpj) != 14:
        return result

    url = f"{CNPJ_BASE_API_URL.rstrip('/')}/{cnpj}"
    headers = {}
    if CNPJ_BASE_TOKEN:
        headers["Authorization"] = f"Bearer {CNPJ_BASE_TOKEN}"

    try:
        resp = requests.get(url, timeout=REQUEST_TIMEOUT, headers=headers)
        if resp.status_code >= 400:
            return result

        data = resp.json()

        result["razao_social"] = data.get("razao_social") or data.get("nome") or ""
        result["nome_fantasia"] = data.get("nome_fantasia") or ""
        result["situacao_cadastral"] = data.get("descricao_situacao_cadastral") or data.get("situacao_cadastral") or ""
        result["data_abertura"] = data.get("data_inicio_atividade") or data.get("data_abertura") or ""
        result["porte"] = data.get("porte") or data.get("descricao_porte") or ""
        result["natureza_juridica"] = data.get("natureza_juridica") or ""
        result["capital_social"] = str(data.get("capital_social") or "")
        result["matriz_filial"] = data.get("descricao_identificador_matriz_filial") or data.get("matriz_filial") or ""
        result["uf"] = data.get("uf") or ""
        result["municipio"] = data.get("municipio") or ""

        cnaes = data.get("cnaes_secundarios") or data.get("cnae_fiscal_descricao") or ""
        if isinstance(cnaes, list) and cnaes:
            primeiro = cnaes[0]
            if isinstance(primeiro, dict):
                result["cnae_principal"] = primeiro.get("descricao") or ""
            else:
                result["cnae_principal"] = str(primeiro)
        elif isinstance(cnaes, str):
            result["cnae_principal"] = cnaes

    except Exception:
        pass

    return result


def enrich_from_cnpjbiz(cnpj: str) -> Dict[str, Any]:
    """
    Tenta abrir a página do cnpj.biz e clicar em botões que revelem contato.
    Como o site pode mudar, essa rotina usa seletores genéricos e fallback por texto.
    """
    result = {
        "cnpjbiz_url": "",
        "cnpjbiz_email": "",
        "cnpjbiz_telefone": "",
        "cnpjbiz_whatsapp": "",
        "cnpjbiz_razao_social": "",
        "cnpjbiz_nome_fantasia": "",
        "cnpjbiz_capital_social": "",
        "cnpjbiz_situacao_cadastral": "",
        "cnpjbiz_cnae_principal": "",
    }

    if not CNPJBIZ_ENABLED:
        return result

    cnpj = only_digits(cnpj)
    if len(cnpj) != 14:
        return result

    url = f"{CNPJBIZ_BASE_URL.rstrip('/')}/{cnpj}"
    result["cnpjbiz_url"] = url

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=CNPJBIZ_HEADLESS)
            page = browser.new_page()
            page.goto(url, timeout=REQUEST_TIMEOUT * 1000)
            page.wait_for_timeout(3000)

            # tenta clicar em possíveis botões de revelar contato
            textos_possiveis = [
                "mostrar telefone",
                "ver telefone",
                "telefone",
                "mostrar whatsapp",
                "ver whatsapp",
                "whatsapp",
                "mostrar email",
                "ver email",
                "e-mail",
                "email",
            ]

            for texto in textos_possiveis:
                try:
                    locator = page.get_by_text(texto, exact=False)
                    if locator.count() > 0:
                        locator.first.click(timeout=2000)
                        page.wait_for_timeout(1200)
                except Exception:
                    pass

            html = page.content()
            texto = page.inner_text("body")

            result["cnpjbiz_email"] = _extract_email(texto) or _extract_email(html)
            result["cnpjbiz_telefone"] = _extract_phone(texto) or _extract_phone(html)
            result["cnpjbiz_whatsapp"] = _extract_whatsapp(texto) or _extract_whatsapp(html)

            # tentativa simples de capturar alguns campos textuais
            for label, key in [
                ("Razão Social", "cnpjbiz_razao_social"),
                ("Nome Fantasia", "cnpjbiz_nome_fantasia"),
                ("Capital Social", "cnpjbiz_capital_social"),
                ("Situação Cadastral", "cnpjbiz_situacao_cadastral"),
                ("CNAE Principal", "cnpjbiz_cnae_principal"),
            ]:
                try:
                    m = re.search(rf"{label}\s*([\s\S]{{0,80}})", texto, re.IGNORECASE)
                    if m:
                        value = m.group(1).split("\n")[0].strip(" :-")
                        result[key] = value
                except Exception:
                    pass

            browser.close()

    except Exception:
        pass

    return result
