"""
coletar.py
Coleta dados do Rio Coruripe da SEMARH-AL e ANA Hidroweb
Roda automaticamente pelo GitHub Actions a cada 1 hora
"""

import json
import re
import urllib.request
import urllib.error
from datetime import datetime, timedelta
import xml.etree.ElementTree as ET

# ── Configuração ──────────────────────────────────────────────
STATION_CODE   = "27290000"   # Estação ANA mais próxima do Rio Coruripe
COTA_ATENCAO   = 1.80         # metros — nível de atenção (SEMARH-AL)
COTA_ALERTA    = 2.70         # metros — nível de alerta
OUTPUT_FILE    = "dados.json"
HISTORICO_MAX  = 168          # horas salvas (7 dias)

# ── Funções auxiliares ────────────────────────────────────────
def http_get(url, timeout=10):
    req = urllib.request.Request(url, headers={"User-Agent": "RioCoruripe-Monitor/1.0"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read().decode("utf-8", errors="replace")

def classify(nivel):
    if nivel is None:
        return "unknown"
    if nivel >= COTA_ALERTA:
        return "critical"
    if nivel >= COTA_ATENCAO:
        return "alert"
    if nivel >= COTA_ATENCAO * 0.7:
        return "attention"
    return "normal"

# ── Fonte 1: ANA Hidroweb (SOAP) ─────────────────────────────
def fetch_ana():
    hoje = datetime.now()
    inicio = hoje - timedelta(days=7)
    fmt = lambda d: f"{d.day:02d}/{d.month:02d}/{d.year}"

    url = (
        "https://telemetriaws1.ana.gov.br/serviceana.asmx/"
        f"DadosHidrometeorologicos"
        f"?codEstacao={STATION_CODE}"
        f"&dataInicio={fmt(inicio)}&dataFim={fmt(hoje)}"
    )

    print(f"[ANA] Buscando estação {STATION_CODE}...")
    try:
        xml_text = http_get(url)
        root = ET.fromstring(xml_text)
        registros = []

        for row in root.iter("Table"):
            data_hora = row.findtext("DataHora") or row.findtext("Data")
            nivel_raw  = row.findtext("Nivel") or row.findtext("Cota")

            if not data_hora or nivel_raw is None:
                continue
            try:
                nivel = float(nivel_raw)
            except ValueError:
                continue

            # ANA pode retornar em cm ou m — normaliza para metros
            if nivel > 20:
                nivel = nivel / 100.0

            # Parse da data
            for fmt_str in ("%d/%m/%Y %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%d/%m/%Y %H:%M"):
                try:
                    ts = datetime.strptime(data_hora.strip(), fmt_str)
                    break
                except ValueError:
                    ts = None
            if ts is None:
                continue

            registros.append({
                "ts": ts.strftime("%Y-%m-%dT%H:%M:00"),
                "nivel": round(nivel, 3)
            })

        registros.sort(key=lambda x: x["ts"])
        print(f"[ANA] {len(registros)} registros encontrados.")
        return registros, "ANA Hidroweb"

    except Exception as e:
        print(f"[ANA] Falhou: {e}")
        return None, None

# ── Fonte 2: SEMARH-AL (scraping do boletim) ─────────────────
def fetch_semarh():
    url = "https://sistemasweb.itec.al.gov.br/semarh/boletim_alerta/"
    print("[SEMARH] Buscando boletim...")
    try:
        html = http_get(url)

        # Tenta extrair nível do Rio Coruripe em cm
        padrao = re.search(
            r"Coruripe.{0,300}?(\d{1,4}[,\.]\d{1,2})\s*(?:cm|m)",
            html, re.IGNORECASE | re.DOTALL
        )
        if padrao:
            valor = float(padrao.group(1).replace(",", "."))
            nivel = valor / 100.0 if valor > 20 else valor
            agora = datetime.now().strftime("%Y-%m-%dT%H:%M:00")
            print(f"[SEMARH] Nível atual: {nivel:.2f} m")
            return [{"ts": agora, "nivel": round(nivel, 3)}], "SEMARH-AL"

        print("[SEMARH] Padrão não encontrado no HTML.")
        return None, None

    except Exception as e:
        print(f"[SEMARH] Falhou: {e}")
        return None, None

# ── Carrega histórico anterior ────────────────────────────────
def load_historico():
    try:
        with open(OUTPUT_FILE, "r", encoding="utf-8") as f:
            old = json.load(f)
            return old.get("historico", [])
    except Exception:
        return []

# ── Merge e deduplicação ──────────────────────────────────────
def merge(historico, novos):
    existing_ts = {r["ts"] for r in historico}
    for r in novos:
        if r["ts"] not in existing_ts:
            historico.append(r)
            existing_ts.add(r["ts"])
    historico.sort(key=lambda x: x["ts"])

    # Mantém apenas os últimos HISTORICO_MAX registros
    if len(historico) > HISTORICO_MAX:
        historico = historico[-HISTORICO_MAX:]
    return historico

# ── Salva dados.json ──────────────────────────────────────────
def salvar(historico, fonte, nivel_atual):
    # Calcula max/min 24h
    agora = datetime.now()
    corte_24h = (agora - timedelta(hours=24)).strftime("%Y-%m-%dT%H:%M:00")
    ultimas_24h = [r["nivel"] for r in historico if r["ts"] >= corte_24h]

    # Variação na última hora
    variacao = None
    if len(historico) >= 2:
        variacao = round((historico[-1]["nivel"] - historico[-2]["nivel"]) * 100, 1)

    status = classify(nivel_atual)

    payload = {
        "atualizado_em": agora.strftime("%Y-%m-%dT%H:%M:00"),
        "fonte": fonte or "indisponivel",
        "estacao": STATION_CODE,
        "nivel_atual": nivel_atual,
        "status": status,
        "variacao_cm": variacao,
        "max_24h": max(ultimas_24h) if ultimas_24h else None,
        "min_24h": min(ultimas_24h) if ultimas_24h else None,
        "cota_atencao": COTA_ATENCAO,
        "cota_alerta": COTA_ALERTA,
        "historico": historico
    }

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    print(f"[OK] dados.json salvo — status: {status} — nível: {nivel_atual} m — fonte: {fonte}")

# ── Main ──────────────────────────────────────────────────────
def main():
    historico = load_historico()
    novos = []
    fonte = None

    # Tenta ANA primeiro
    registros, fonte = fetch_ana()
    if registros:
        novos = registros

    # Se ANA falhou, tenta SEMARH
    if not novos:
        registros, fonte = fetch_semarh()
        if registros:
            novos = registros

    if novos:
        historico = merge(historico, novos)
        nivel_atual = historico[-1]["nivel"] if historico else None
    else:
        print("[AVISO] Nenhuma fonte disponível. Mantendo último histórico.")
        nivel_atual = historico[-1]["nivel"] if historico else None
        fonte = "cache"

    salvar(historico, fonte, nivel_atual)

if __name__ == "__main__":
    main()
