"""
coletar.py
Coleta dados do Rio Coruripe via SEMARH-AL
Roda automaticamente pelo GitHub Actions a cada 1 hora
"""

import json
import re
import urllib.request
from datetime import datetime, timedelta

# ── Configuração ──────────────────────────────────────────────
COTA_ATENCAO  = 1.80
COTA_ALERTA   = 2.70
OUTPUT_FILE   = "dados.json"
HISTORICO_MAX = 168  # 7 dias

URLS_SEMARH = [
    "https://sistemasweb.itec.al.gov.br/semarh/boletim_alerta/",
    "http://www.semarh.al.gov.br/sala-de-situacao/boletins/boletim-de-alertas",
    "http://semarh.al.gov.br/monitoramento",
]

# ── Funções auxiliares ────────────────────────────────────────
def http_get(url, timeout=15):
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; RioCoruripe-Monitor/1.0)",
        "Accept": "text/html,application/xhtml+xml,*/*",
    }
    req = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        charset = r.headers.get_content_charset() or "utf-8"
        return r.read().decode(charset, errors="replace")

def classify(nivel):
    if nivel is None:       return "unknown"
    if nivel >= COTA_ALERTA:    return "critical"
    if nivel >= COTA_ATENCAO:   return "alert"
    if nivel >= COTA_ATENCAO * 0.7: return "attention"
    return "normal"

def extrair_nivel(html):
    padroes = [
        r"[Cc]oruripe[^<]{0,300}?(\d{1,2}[,\.]\d{1,2})\s*m(?:etros?)?",
        r"[Cc]oruripe[^<]{0,300}?(\d{2,4})\s*cm",
        r"[Cc]oruripe</td>[^<]{0,100}<td[^>]*>([0-9,\.]+)",
        r"coruripe[^0-9]{0,50}([0-9]+[,\.][0-9]+)",
    ]
    for padrao in padroes:
        match = re.search(padrao, html, re.IGNORECASE | re.DOTALL)
        if match:
            try:
                valor = float(match.group(1).replace(",", "."))
                if valor > 20:
                    valor = valor / 100.0
                if 0 <= valor <= 10:
                    print(f"[SEMARH] Nivel encontrado: {valor:.2f} m")
                    return round(valor, 3)
            except ValueError:
                continue
    return None

def fetch_semarh():
    for url in URLS_SEMARH:
        print(f"[SEMARH] Tentando: {url}")
        try:
            html = http_get(url)
            with open("debug_semarh.html", "w", encoding="utf-8") as f:
                f.write(html[:5000])
            nivel = extrair_nivel(html)
            if nivel is not None:
                agora = datetime.now().strftime("%Y-%m-%dT%H:%M:00")
                return [{"ts": agora, "nivel": nivel}], "SEMARH-AL", url
            else:
                print(f"[SEMARH] Nivel nao encontrado nesta URL")
        except Exception as e:
            print(f"[SEMARH] Falhou {url}: {e}")
    return None, None, None

def load_historico():
    try:
        with open(OUTPUT_FILE, "r", encoding="utf-8") as f:
            return json.load(f).get("historico", [])
    except:
        return []

def merge(historico, novos):
    existing_ts = {r["ts"] for r in historico}
    for r in novos:
        if r["ts"] not in existing_ts:
            historico.append(r)
    historico.sort(key=lambda x: x["ts"])
    return historico[-HISTORICO_MAX:]

def salvar(historico, fonte, nivel_atual, url_fonte=None):
    agora = datetime.now()
    corte = (agora - timedelta(hours=24)).strftime("%Y-%m-%dT%H:%M:00")
    ultimas = [r["nivel"] for r in historico if r["ts"] >= corte]
    variacao = None
    if len(historico) >= 2:
        variacao = round((historico[-1]["nivel"] - historico[-2]["nivel"]) * 100, 1)

    payload = {
        "atualizado_em": agora.strftime("%Y-%m-%dT%H:%M:00"),
        "fonte": fonte or "indisponivel",
        "url_fonte": url_fonte,
        "estacao": "Rio Coruripe - SEMARH-AL",
        "nivel_atual": nivel_atual,
        "status": classify(nivel_atual),
        "variacao_cm": variacao,
        "max_24h": max(ultimas) if ultimas else None,
        "min_24h": min(ultimas) if ultimas else None,
        "cota_atencao": COTA_ATENCAO,
        "cota_alerta": COTA_ALERTA,
        "historico": historico
    }
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    print(f"[OK] Salvo — status: {payload['status']} — nivel: {nivel_atual} m — fonte: {fonte}")

def main():
    historico = load_historico()
    novos, fonte, url_fonte = fetch_semarh()

    if novos:
        historico = merge(historico, novos)
        nivel_atual = historico[-1]["nivel"]
    else:
        print("[AVISO] SEMARH indisponivel. Mantendo cache.")
        nivel_atual = historico[-1]["nivel"] if historico else None
        fonte = "cache"
        url_fonte = None

    salvar(historico, fonte, nivel_atual, url_fonte)

if __name__ == "__main__":
    main()
