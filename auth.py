import requests
import pandas as pd
import time
import threading
from concurrent.futures import ThreadPoolExecutor
import os
import argparse
import json

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
    with open(BM_FILE, 'r') as f:
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
# Message building/sending
# =========================
def build_components_with_otp_and_url_button(otp_code, button_index="0", include_url_button=True):
    """
    Builds components to match the example payload:
      - BODY with one text parameter (OTP)
      - URL button at the chosen index with one text parameter (OTP)
    """
    components = [
        {
            "type": "body",
            "parameters": [
                {"type": "text", "text": otp_code}
            ]
        }
    ]
    if include_url_button:
        components.append(
            {
                "type": "button",
                "sub_type": "url",
                "index": str(button_index),
                "parameters": [
                    {"type": "text", "text": otp_code}
                ]
            }
        )
    return components

def enviar_auth_template(
    lead,
    phone_number_id,
    token,
    template_lang=TEMPLATE_LANG,
    log_enabled=True,
    use_tor=True,
    include_url_button=True,
    button_index="0"
):
    telefone = str(lead['telefone'])
    # CSV: 'mensagem' deve conter o OTP/código
    otp_code = str(lead['mensagem']).strip()
    template_name = lead['template_name']

    api_url = f"https://graph.facebook.com/v23.0/{phone_number_id}/messages"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {token}"
    }

    payload = {
        "messaging_product": "whatsapp",
        "recipient_type": "individual",
        "to": telefone,
        "type": "template",
        "template": {
            "name": template_name,
            "language": {"code": template_lang},
            "components": build_components_with_otp_and_url_button(
                otp_code=otp_code,
                button_index=button_index,
                include_url_button=include_url_button
            )
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
    leads_file="base10pra100k.csv",
    template_lang=TEMPLATE_LANG,
    include_url_button=True,
    button_index="0",
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
    print(f"📌 Template(s): {', '.join(templates)} | idioma={template_lang} | url_button={include_url_button} (index={button_index})")

    def runner(lead_row):
        enviar_auth_template(
            lead_row,
            phone_number_id,
            token,
            template_lang=template_lang,
            log_enabled=not random_mode,
            use_tor=use_tor,
            include_url_button=include_url_button,
            button_index=button_index
        )

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        executor.map(lambda item: runner(item[1]), leads_filtrados.iterrows())

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
    parser.add_argument('--no-url-button', action='store_true', help='NÃO enviar parâmetro de botão URL (apenas BODY)')
    parser.add_argument('--button-index', default='0', help='Índice do botão URL (padrão "0")')
    parser.add_argument('--workers', type=int, default=1, help='Número de workers para envio (default 1)')

    args = parser.parse_args()

    if args.cadastrar:
        cadastrar_bm()
    else:
        modo_envio(
            random_mode=args.random,
            use_tor=not args.no_tor,
            leads_file=args.leads,
            template_lang=args.lang,
            include_url_button=not args.no_url_button,  # by default, behaves like your example (button included)
            button_index=args.button_index,
            max_workers=args.workers
        )
