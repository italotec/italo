import requests
import pandas as pd
import time
import threading
from concurrent.futures import ThreadPoolExecutor
import os
import argparse
import json
import random
import string
import uuid

BM_FILE = 'bms.json'
LOG_FILE = 'sent_log.csv'
TEMPLATE_LANG = 'pt_BR'
LOCK = threading.Lock()

# Tor proxy (Tails default port 9050)
TOR_PROXY = {
    "http": "socks5h://127.0.0.1:9050",
    "https": "socks5h://127.0.0.1:9050"
}

def carregar_bms():
    if not os.path.exists(BM_FILE):
        return {}
    with open(BM_FILE, 'r') as f:
        return json.load(f)

def salvar_bms(bms):
    with open(BM_FILE, 'w') as f:
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

def enviar_auth_template(lead, phone_number_id, token, log_enabled=True, use_tor=True):
    telefone = str(lead['telefone'])
    # coluna 'mensagem' aqui é o OTP/código em si
    otp_code = str(lead['mensagem']).strip()
    template_name = lead['template_name']

    api_url = f"https://graph.facebook.com/v23.0/{phone_number_id}/messages"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {token}"
    }

    # IMPORTANTE:
    # - Cloud API não usa 'namespace'
    # - O código deve ir como parâmetro de BODY dentro de template.components
    payload = {
        "messaging_product": "whatsapp",
        "to": telefone,
        "type": "template",
        "template": {
            "name": template_name,
            "language": {"code": TEMPLATE_LANG},
            "components": [
                {
                    "type": "body",
                    "parameters": [
                        {"type": "text", "text": otp_code}
                    ]
                }
                # Para templates de autenticação "copy code" e "one-tap",
                # normalmente NÃO há parâmetros no botão. Se seu provedor exigir
                # duplicar o código no botão, descomente o bloco abaixo:
                # ,
                # {
                #     "type": "button",
                #     "sub_type": "url",
                #     "index": "0",
                #     "parameters": [
                #         {"type": "text", "text": otp_code}
                #     ]
                # }
            ]
        }
    }

    proxies = TOR_PROXY if use_tor else None

    try:
        resp = requests.post(api_url, headers=headers, json=payload, proxies=proxies, timeout=30)
        print(f"{telefone}: {resp.status_code} | {resp.text}")
        if resp.ok and log_enabled:
            with LOCK:
                with open(LOG_FILE, "a") as f:
                    f.write(f"{telefone}\n")
    except Exception as e:
        print(f"Erro ao enviar para {telefone}: {e}")

def modo_envio(random_mode=False, use_tor=True, leads_file="100k.csv"):
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

    # Espera-se CSV com colunas: telefone, mensagem (mensagem=OTP), ...
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
    print(f"📌 Template(s): {', '.join(templates)} | idioma={TEMPLATE_LANG}")

    with ThreadPoolExecutor(max_workers=1) as executor:
        executor.map(
            lambda lead: enviar_auth_template(
                lead, phone_number_id, token, log_enabled=not random_mode, use_tor=use_tor
            ),
            [lead for _, lead in leads_filtrados.iterrows()]
        )

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--cadastrar', action='store_true', help='Cadastrar nova BM')
    parser.add_argument('--random', '-r', action='store_true', help='Enviar mensagens em ordem aleatória e sem log')
    parser.add_argument('--no-tor', action='store_true', help='Desabilitar proxy Tor')
    parser.add_argument('--leads', default='100k.csv', help='Caminho do CSV de leads')
    args = parser.parse_args()

    if args.cadastrar:
        cadastrar_bm()
    else:
        modo_envio(random_mode=args.random, use_tor=not args.no_tor, leads_file=args.leads)
