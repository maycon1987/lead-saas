import os
import re
from typing import Dict, Any

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


def validar_cnpj(cnpj: str) -> bool:
    cnpj = only_digits(cnpj)

    if len(cnpj) != 14:
        return False

    if cnpj == cnpj[0] * 14:
        return False

    def calc_digito(cnpj_parcial: str, pesos):
        soma = sum(int(num) * peso for num, peso in zip(cnpj_parcial, pesos))
        resto = soma % 11
        return "0" if resto < 2 else str(11 - resto)

    pesos1 = [5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
    pesos2 = [6, 5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]

    digito1 = calc_digito(cnpj[:12], pesos1)
    digito2 = calc_digito(cnpj[:12] + digito1, pesos2)

    return cnpj[-2:] == digito1 + digito2


def _clean_value(value: str) -> str:
    if not value:
        return ""
    value = re.sub(r"\s+", " ", value).strip(" :-\n\t")
    return value


def _extract_email(text: str) -> str:
    if not text:
        return ""
    m = re.search(r"([A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,})", text)
    return m.group(1) if m else ""


def _extract_phone(text: str) -> str:
    if not text:
        return ""
    m = re.search(r"(\(?\d{2}\)?\s?\d{4,5}\-?\d{4})", text)
    return m.group(1) if m else ""


def _extract_whatsapp(text: str) -> str:
    if not text:
        return ""
    wa = re.search(r"(https?://wa\.me/\d+)", text, re.IGNORECASE)
    if wa:
        return wa.group(1)

    m = re.search(r"(\+?55\s?\(?\d{2}\)?\s?\d{4,5}\-?\d{4})", text)
    return m.group(1) if m else ""


def _extract_labeled_value(text: str, labels: list[str], max_len: int = 180) -> str:
    if not text:
        return ""

    for label in labels:
        patterns = [
            rf"{label}\s*[:\-]?\s*(.+)",
            rf"{label}\s*\n\s*(.+)",
        ]
        for pattern in patterns:
            m = re.search(pattern, text, re.IGNORECASE)
            if m:
                value = _clean_value(m.group(1))
                if value:
                    return value[:max_len]
    return ""


def enrich_from_cnpj_base(cnpj: str) -> Dict[str, Any]:
    result = {
        "cnpj": "",
        "cnpj_valido": False,
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
    result["cnpj"] = cnpj
    result["cnpj_valido"] = validar_cnpj(cnpj)

    if not result["cnpj_valido"]:
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

        cnae_desc = data.get("cnae_fiscal_descricao") or ""
        if cnae_desc:
            result["cnae_principal"] = cnae_desc
        else:
            cnaes = data.get("cnaes_secundarios") or []
            if isinstance(cnaes, list) and cnaes:
                primeiro = cnaes[0]
                if isinstance(primeiro, dict):
                    result["cnae_principal"] = primeiro.get("descricao") or ""
                else:
                    result["cnae_principal"] = str(primeiro)

    except Exception:
        pass

    return result


def enrich_from_cnpjbiz(cnpj: str) -> Dict[str, Any]:
    result = {
        "cnpjbiz_url": "",
        "cnpjbiz_email": "",
        "cnpjbiz_telefone": "",
        "cnpjbiz_whatsapp": "",
        "cnpjbiz_razao_social": "",
        "cnpjbiz_nome_fantasia": "",
        "cnpjbiz_capital_social": "",
        "cnpjbiz_situacao_cadastral": "",
        "cnpjbiz_data_abertura": "",
        "cnpjbiz_cnae_principal": "",
        "cnpjbiz_porte": "",
        "cnpjbiz_natureza_juridica": "",
        "cnpjbiz_matriz_filial": "",
        "cnpjbiz_cnpj_valido": False,
    }

    if not CNPJBIZ_ENABLED:
        return result

    cnpj = only_digits(cnpj)
    result["cnpjbiz_cnpj_valido"] = validar_cnpj(cnpj)

    if not result["cnpjbiz_cnpj_valido"]:
        return result

    url = f"{CNPJBIZ_BASE_URL.rstrip('/')}/{cnpj}"
    result["cnpjbiz_url"] = url

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=CNPJBIZ_HEADLESS)
            page = browser.new_page()
            page.goto(url, timeout=REQUEST_TIMEOUT * 1000)
            page.wait_for_timeout(4000)

            # tentativa de clicar em botões para revelar contato
            textos_possiveis = [
                "mostrar telefone",
                "ver telefone",
                "telefone",
                "mostrar whatsapp",
                "ver whatsapp",
                "whatsapp",
                "mostrar email",
                "ver email",
                "email",
                "e-mail",
                "mostrar contato",
                "ver contato",
            ]

            for texto in textos_possiveis:
                try:
                    locator = page.get_by_text(texto, exact=False)
                    if locator.count() > 0:
                        locator.first.click(timeout=1500)
                        page.wait_for_timeout(900)
                except Exception:
                    pass

            texto = page.inner_text("body")
            html = page.content()

            result["cnpjbiz_email"] = _extract_email(texto) or _extract_email(html)
            result["cnpjbiz_telefone"] = _extract_phone(texto) or _extract_phone(html)
            result["cnpjbiz_whatsapp"] = _extract_whatsapp(texto) or _extract_whatsapp(html)

            result["cnpjbiz_razao_social"] = _extract_labeled_value(
                texto, ["Razão Social", "Razao Social"]
            )
            result["cnpjbiz_nome_fantasia"] = _extract_labeled_value(
                texto, ["Nome Fantasia"]
            )
            result["cnpjbiz_capital_social"] = _extract_labeled_value(
                texto, ["Capital Social"]
            )
            result["cnpjbiz_situacao_cadastral"] = _extract_labeled_value(
                texto, ["Situação Cadastral", "Situacao Cadastral"]
            )
            result["cnpjbiz_data_abertura"] = _extract_labeled_value(
                texto, ["Data de Abertura", "Início de Atividade", "Inicio de Atividade"]
            )
            result["cnpjbiz_cnae_principal"] = _extract_labeled_value(
                texto, ["CNAE Principal"]
            )
            result["cnpjbiz_porte"] = _extract_labeled_value(
                texto, ["Porte"]
            )
            result["cnpjbiz_natureza_juridica"] = _extract_labeled_value(
                texto, ["Natureza Jurídica", "Natureza Juridica"]
            )
            result["cnpjbiz_matriz_filial"] = _extract_labeled_value(
                texto, ["Matriz ou Filial", "Matriz/Filial", "Identificador Matriz Filial"]
            )

            browser.close()

    except Exception:
        pass

    return result
