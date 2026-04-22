def run_collection(cidade, nichos, run_path, limite_resultados, filtros):
    print("RODANDO COLETA...")

    # Simulação de resposta
    return {
        "success": True,
        "total": 2,
        "excel_file": "teste.xlsx",
        "json_file": "teste.json",
        "json_clean_file": "teste_clean.json",
        "leads": [
            {"nome": "Empresa 1", "telefone": "123"},
            {"nome": "Empresa 2", "telefone": "456"},
        ]
    }
