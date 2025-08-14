import requests
import pandas as pd
import time
import threading
from concurrent.futures import ThreadPoolExecutor
import os
import argparse
import json
import re

# =========================
# Config & Globals
# =========================
BM_FILE = 'bms.json'
LOG_FILE = 'sent_log.csv'
TEMPLATE_LANG = 'pt_BR'
LOCK = threading.Lock()

# Tor proxy (Tails default port 9050)
TOR_PROXY = {
    "http": "socks5h://127.0.0.1:9050",
    "https": "socks5h://127.0.0.1:9050"
}

# =========================
# BM helpers
# =========================
def carregar_bms():
    if not os.path.exists(BM_FILE):
        return {}
    with open(BM_FILE, 'r', encoding='utf-8') as f:
        return json.load(f)

def salvar_bms(bms):
    with open(BM_FILE, 'w', encoding='utf-8') as f:
        json.dump(bms, f, indent=4, ensure_ascii=False)

def cadastrar_bm():
    bms = carregar_bms()
    nome = input("Nome da BM: ")
    phone_number_id = input("Phone Number ID: ")
    token = input("Token: ")
    templates_raw = input("Templates (separados por vírgula): ")
    templates = [t.strip() for t in templates_raw.split(',') if t.strip()]

    bms[nome] = {
        "phone_number_id": phone_number_id,
        "token": token,
        "templates": templates
    }

    salvar_bms(bms)
    print(f"✅ BM '{nome}' cadastrada com sucesso.")

# =========================
# Template inspection (optional but useful)
# =========================
def fetch_templates(waba_id, token, template_name=None, lang=None):
    url = f"https://graph.facebook.com/v23.0/{waba_id}/message_templates"
    params = {
        "fields": "name,language,status,category,components",
        "limit": 100
    }
    if template_name:
        params["name"] = template_name
    r = requests.get(url, params=params, headers={"Authorization": f"Bearer {token}"}, timeout=30)
    r.raise_for_status()
    data = r.json().get("data", [])
    if lang:
        data = [t for t in data if t.get("language") == lang]
    return data

def print_template_info(templates):
    if not templates:
        print("⚠️ Nenhum template encontrado com os filtros informados.")
        return
    for t in templates:
        print("="*70)
        print(f"name: {t.get('name')}")
        print(f"language: {t.get('language')}")
        print(f"status: {t.get('status')} | category: {t.get('category')}")
        print("components:")
        comps = t.get("components", [])
        for c in comps:
            ctype = (c.get("type") or "").upper()
            print(f"  - type: {ctype}")
            if ctype == "BUTTONS" or ctype == "BUTTON" or ctype == "buttons":
                # Some responses pack buttons under 'buttons'
                buttons = c.get("buttons") or []
                for i, b in enumerate(buttons):
                    print(f"      button[{i}]: sub_type={b.get('type') or b.get('sub_type')} text={b.get('text')} url={b.get('url')}")
            else:
                # Show raw for non-button components
                print(f"    {c}")

# =========================
# Message building/sending
# =========================
def build_body_component(otp_code: str):
    return {
        "type": "body",
        "parameters": [
            {"type": "text", "text": otp_code}
        ]
    }

def parse_url_param_specs(spec_list, lead_row, otp_code):
    """
    Parse a list like:
      ["otp", "col:token", "lit:VALOR_FIXO"]
    Return a list of parameter dicts suitable for the button 'parameters' field.
    """
    params = []
    for spec in (spec_list or []):
        spec = str(spec).strip()
        if spec == "otp":
            params.append({"type": "text", "text": otp_code})
        elif spec.startswith("col:"):
            col = spec.split(":", 1)[1]
            if col not in lead_row:
                raise KeyError(f"CSV column '{col}' not found for URL parameter.")
            params.append({"type": "text", "text": str(lead_row[col]).strip()})
        elif spec.startswith("lit:"):
            val = spec.split(":", 1)[1]
            params.append({"type": "text", "text": str(val)})
        else:
            raise ValueError(f"Invalid --url-param value: {spec} (use 'otp', 'col:<colname>' or 'lit:<value>')")
    return params

def build_url_button_component(index_str: str, button_params: list):
    """
    Build the URL button component. 'button_params' must contain exactly as many
    entries as there are placeholders in the template URL ({{1}}, {{2}}, ...).
    """
    return {
        "type": "button",
        "sub_type": "url",
        "index": str(index_str),
        "parameters": button_params
    }

def enviar_auth_template(
    lead,
    phone_number_id,
    token,
    template_name,
    template_lang=TEMPLATE_LANG,
    log_enabled=True,
    use_tor=True,
    use_url_button=False,
    url_button_index="0",
    url_param_specs=None
):
    telefone = str(lead['telefone'])
    otp_code = str(lead['mensagem']).strip()

    api_url = f"https://graph.facebook.com/v23.0/{phone_number_id}/messages"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {token}"
    }

    components = [build_body_component(otp_code)]

    if use_url_button:
        button_params = parse_url_param_specs(url_param_specs, lead, otp_code)
        components.append(build_url_button_component(url_button_index, button_params))

    payload = {
        "messaging_product": "whatsapp",
        "recipient_type": "individual",
        "to": telefone,
        "type": "template",
        "template": {
            "name": template_name,
            "language": {"code": template_lang},
            "components": components
        }
    }

    proxies = TOR_PROXY if use_tor else None

    try:
        resp = requests.post(api_url, headers=headers, json=payload, proxies=proxies, timeout=30)
        if not resp.ok:
            # Log rich error to understand 131008/132018 cases
            try:
                err = resp.json()
            except Exception:
                err = {"raw": resp.text}
            print(f"{telefone}: {resp.status_code} | code={err.get('error',{}).get('code')} "
                  f"| fbtrace_id={err.get('error',{}).get('fbtrace_id')} | details={err}")
        else:
            print(f"{telefone}: {resp.status_code} | OK")
            if log_enabled:
                with LOCK:
                    with open(LOG_FILE, "a") as f:
                        f.write(f"{telefone}\n")
    except Exception as e:
        print(f"Erro ao enviar para {telefone}: {e}")

# =========================
# Send loop
# =========================
def modo_envio(
    random_mode=False,
    use_tor=True,
    leads_file="100k.csv",
    template_lang=TEMPLATE_LANG,
    use_url_button=False,
    url_button_index="0",
    url_param_specs=None,
    max_workers=1
):
    bms = carregar_bms()
    if not bms:
        print("❌ Nenhuma BM cadastrada. Use '--cadastrar' para adicionar uma.")
        return

    print("\nBMs disponíveis:")
    for i, nome in enumerate(bms.keys()):
        print(f"{i + 1}. {nome}")

    escolha = input("Escolha o número da BM que deseja usar: ")
    try:
        index = int(escolha) - 1
        bm_nome = list(bms.keys())[index]
    except (ValueError, IndexError):
        print("❌ Escolha inválida.")
        return

    bm = bms[bm_nome]
    phone_number_id = bm['phone_number_id']
    token = bm['token']
    templates = bm['templates']

    # CSV esperado: colunas 'telefone' e 'mensagem'
    leads = pd.read_csv(leads_file)

    if not os.path.exists(LOG_FILE):
        open(LOG_FILE, "w").close()

    with open(LOG_FILE, "r") as f:
        enviados = set(line.strip() for line in f)

    leads_filtrados = leads[~leads['telefone'].astype(str).isin(enviados)].reset_index(drop=True)

    if random_mode:
        leads_filtrados = leads_filtrados.sample(frac=1).reset_index(drop=True)

    num_templates = len(templates)
    total_leads = len(leads_filtrados)
    leads_filtrados['template_name'] = [templates[i % num_templates] for i in range(total_leads)]

    print(f"\n📤 Iniciando envio para {total_leads} leads...")
    print(f"📌 Template(s): {', '.join(templates)} | idioma={template_lang} | url_button={use_url_button} (index={url_button_index})")
    if use_url_button:
        print(f"   URL params: {url_param_specs}")

    def runner(_, lead_row):
        enviar_auth_template(
            lead_row,
            phone_number_id,
            token,
            template_name=lead_row['template_name'],
            template_lang=template_lang,
            log_enabled=not random_mode,
            use_tor=use_tor,
            use_url_button=use_url_button,
            url_button_index=url_button_index,
            url_param_specs=url_param_specs
        )

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        executor.map(runner, leads_filtrados.index, leads_filtrados.itertuples(index=False, name=None))

# =========================
# CLI
# =========================
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--cadastrar', action='store_true', help='Cadastrar nova BM')
    parser.add_argument('--random', '-r', action='store_true', help='Enviar em ordem aleatória e sem log')
    parser.add_argument('--no-tor', action='store_true', help='Desabilitar proxy Tor')
    parser.add_argument('--leads', default='100k.csv', help='Caminho do CSV de leads')
    parser.add_argument('--lang', default=TEMPLATE_LANG, help='Código do idioma do template (ex: pt_BR)')
    parser.add_argument('--workers', type=int, default=1, help='Número de workers (default 1)')

    # URL button controls
    parser.add_argument('--use-url-button', action='store_true', help='Enviar parâmetro(s) para o botão URL (index 0 por padrão)')
    parser.add_argument('--button-index', default='0', help='Índice do botão URL (padrão "0")')
    parser.add_argument('--url-param', action='append',
                        help="Adicione parâmetros para o botão URL (repita a flag). Use 'otp', 'col:<coluna>', ou 'lit:<valor>'. "
                             "Ex.: --url-param otp --url-param col:token --url-param lit:fixo")

    # Template inspection
    parser.add_argument('--inspect-template', action='store_true', help='Listar estrutura do template via API e sair')
    parser.add_argument('--waba', help='WhatsApp Business Account ID para inspecionar templates')
    parser.add_argument('--template', help='Nome exato do template para filtrar na inspeção')

    args = parser.parse_args()

    if args.cadastrar:
        cadastrar_bm()
    elif args.inspect_template:
        if not args.waba:
            print("❌ Para --inspect-template, informe --waba <WABA_ID> (e opcionalmente --template e --lang)")
        else:
            # Vamos pegar um token de alguma BM salva (ou você pode trocar para passar via CLI se preferir)
            bms = carregar_bms()
            if not bms:
                print("❌ Nenhuma BM cadastrada (precisamos de um token para a chamada).")
            else:
                # usa o primeiro token encontrado (ou adapte para escolher)
                first_bm = next(iter(bms.values()))
                token = first_bm['token']
                data = fetch_templates(args.waba, token, template_name=args.template, lang=args.lang)
                print_template_info(data)
    else:
        modo_envio(
            random_mode=args.random,
            use_tor=not args.no_tor,
            leads_file=args.leads,
            template_lang=args.lang,
            use_url_button=args.use_url_button,
            url_button_index=args.button_index,
            url_param_specs=args.url_param,   # list like ["otp", "col:token", "lit:VAL"]
            max_workers=args.workers
        )
