# sender.py
import requests
import pandas as pd
import threading
from concurrent.futures import ThreadPoolExecutor
import os
import argparse
import json
import random
import string
import uuid
from datetime import datetime
from zoneinfo import ZoneInfo

BM_FILE = 'bms.json'
LOG_FILE = 'sent_log.csv'
TEMPLATE_LANG = 'pt_BR'
LOCK = threading.Lock()

# Proxy do Tor (Tails usa porta 9050 por padrão)
TOR_PROXY = {
    "http": "socks5h://127.0.0.1:9050",
    "https": "socks5h://127.0.0.1:9050"
}

# === Random generators ===
def random_namespace():
    u = str(uuid.uuid4())
    parts = u.split('-')
    return f"{parts[0]}_{parts[1]}_{parts[2]}_{parts[3]}_{parts[4]}"

def random_parameter_name(length=6):
    return random.choice(string.ascii_lowercase) + ''.join(random.choices(string.ascii_lowercase + string.digits, k=length-1))

NAMESPACE_VALUE = random_namespace()
PARAM_NAME_VALUE = random_parameter_name()

# === Funções de BM ===
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
    waba_id = input("WABA ID: ")
    templates_raw = input("Templates (separados por vírgula): ")
    templates = [t.strip() for t in templates_raw.split(',')]
    bms[nome] = {
        "phone_number_id": phone_number_id,
        "token": token,
        "waba_id": waba_id,
        "templates": templates
    }
    salvar_bms(bms)
    print(f"BM '{nome}' cadastrada com sucesso.")

# === Enviar template ===
def enviar_template(lead, phone_number_id, token, log_enabled=True):
    telefone = str(lead.get('telefone', '')).strip()
    nome = str(lead.get('nome', '')).strip()
    template_name = str(lead.get('template_name', '')).strip()
    if not telefone or not template_name:
        print(f"Lead faltando telefone ou template_name: {lead}")
        return
    api_url = f"https://graph.facebook.com/v23.0/{phone_number_id}/messages"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {token}"
    }
    components = [
        {
            "type": "body",
            "parameters": [
                {"type": "text", "parameter_name": PARAM_NAME_VALUE, "text": nome},
                {"type": "text", "parameter_name": "serie", "text": telefone},
                {"type": "text", "parameter_name": "indicacao", "text": "serie"}
            ]
        }
    ]
    payload = {
        "type": "template",
        "messaging_product": "whatsapp",
        "template": {
            "namespace": NAMESPACE_VALUE,
            "name": template_name,
            "language": {"code": TEMPLATE_LANG},
            "components": components
        },
        "to": telefone
    }
    try:
        response = requests.post(api_url, headers=headers, json=payload, proxies=TOR_PROXY, timeout=30)
        print(f"{telefone}: {response.status_code} | namespace={NAMESPACE_VALUE}")
        if response.status_code == 200 and log_enabled:
            with LOCK:
                with open(LOG_FILE, "a") as f:
                    f.write(f"{telefone}\n")
    except Exception as e:
        print(f"Erro ao enviar para {telefone}: {e}")

# === Registrar disparo no Flask ===
def registrar_disparo(phone_number_id, flask_url="http://localhost:5000/update-disparo"):
    tz_sp = ZoneInfo("America/Sao_Paulo")
    now = datetime.now(tz_sp)
    time_str = now.strftime("%H:%M %d/%m")
    payload = {
        "phone_number_id": phone_number_id,
        "time": time_str
    }
    try:
        resp = requests.post(flask_url, json=payload, timeout=5, verify=False)  # ← ADICIONE verify=False
        if resp.status_code == 200:
            print(f"Disparo registrado: {time_str}")
        else:
            print(f"Falha: {resp.status_code} {resp.text}")
    except Exception as e:
        print(f"Erro Flask: {e}")

# === Modo envio ===
def modo_envio(random_mode=False, monitor=False, flask_url=None):
    bms = carregar_bms()
    if not bms:
        print("Nenhuma BM cadastrada. Use '--cadastrar' para adicionar uma.")
        return

    print("\nBMs disponíveis:")
    for i, nome in enumerate(bms.keys()):
        print(f"{i + 1}. {nome}")
    escolha = input("Escolha o número da BM que deseja usar: ")
    try:
        index = int(escolha) - 1
        bm_nome = list(bms.keys())[index]
    except (ValueError, IndexError):
        print("Escolha inválida.")
        return

    bm = bms[bm_nome]
    phone_number_id = bm['phone_number_id']
    token = bm['token']
    templates = bm['templates']

    # Registrar disparo se -m for usado
    if monitor and flask_url:
        registrar_disparo(phone_number_id, flask_url)

    # Carregar leads
    leads = pd.read_csv("base10pra100k.csv")
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

    print(f"\nIniciando envio para {total_leads} leads...")
    print(f"Usando namespace: {NAMESPACE_VALUE} | param_name: {PARAM_NAME_VALUE}")

    with ThreadPoolExecutor(max_workers=1) as executor:
        executor.map(
            lambda lead: enviar_template(lead, phone_number_id, token, log_enabled=not random_mode),
            [lead for _, lead in leads_filtrados.iterrows()]
        )

# === Main ===
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Sender de Templates WhatsApp")
    parser.add_argument('--cadastrar', action='store_true', help='Cadastrar nova BM')
    parser.add_argument('--random', '-r', action='store_true', help='Enviar em ordem aleatória e sem log')
    parser.add_argument('-m', '--monitor', action='store_true', help='Registrar disparo no painel Flask')
    parser.add_argument('--flask-url', type=str, default="http://localhost:5000/update-disparo",
                        help='URL do endpoint Flask (padrão: http://localhost:5000/update-disparo)')

    args = parser.parse_args()

    if args.cadastrar:
        cadastrar_bm()
    else:
        modo_envio(random_mode=args.random, monitor=args.monitor, flask_url=args.flask_url)
