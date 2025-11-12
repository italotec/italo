# sender.py (versão corrigida)
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
TEMPLATE_LANG = 'en'
LOCK = threading.Lock()

# Proxy do Tor
TOR_PROXY = {
    "http": "socks5h://127.0.0.1:9050",
    "https": "socks5h://127.0.0.1:9050"
}

# NAMESPACE FIXO - MUDE PARA O SEU REAL (do Meta)
NAMESPACE_VALUE = "butecs"  # ← SUBSTITUA PELO SEU NAMESPACE OFICIAL
PARAM_NAME_VALUE = "joga"  # Ou gere random se quiser

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
    print(f"✅ BM '{nome}' cadastrada.")

def enviar_template(lead, phone_number_id, token, log_enabled=True):
    telefone = str(lead.get('telefone', '')).strip()
    nome = str(lead.get('nome', '')).strip()
    mensagem = str(lead.get('mensagem', '')).strip()
    template_name = str(lead.get('template_name', '')).strip()
    if not telefone or not template_name:
        print(f"⚠️ Lead faltando: {lead}")
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
                #{"type": "text", "parameter_name": PARAM_NAME_VALUE, "text": nome},
                {"type": "text", "parameter_name": "estilo", "text": telefone},
                #{"type": "text", "parameter_name": "indicacao", "text": "serie"}
            ]
        }
    ]
    payload = {
        "type": "template",
        "messaging_product": "whatsapp",
        "template": {
            "namespace": NAMESPACE_VALUE,  # ← FIXO AQUI
            "name": template_name,
            "language": {"code": TEMPLATE_LANG},
            "components": components
        },
        "to": telefone
    }
    try:
        response = requests.post(api_url, headers=headers, json=payload, proxies=TOR_PROXY, timeout=30)
        print(f"{telefone}: {response.status_code} | {response.text[:100]}... | namespace={NAMESPACE_VALUE} | param={PARAM_NAME_VALUE}")
        if response.status_code == 200 and log_enabled:
            with LOCK:
                with open(LOG_FILE, "a") as f:
                    f.write(f"{telefone}\n")
    except Exception as e:
        print(f"Erro envio {telefone}: {e}")

def registrar_disparo(phone_number_id, flask_url="http://localhost:5000/update-disparo"):
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    tz_sp = ZoneInfo("America/Sao_Paulo")
    now = datetime.now(tz_sp)
    time_str = now.strftime("%H:%M %d/%m")
    payload = {"phone_number_id": phone_number_id, "time": time_str}

    session = requests.Session()
    session.proxies = {}           # SEM PROXY
    session.verify = False         # SEM SSL CHECK
    session.headers.update({"Content-Type": "application/json"})

    try:
        resp = session.post(flask_url, json=payload, timeout=10)
        if resp.status_code == 200:
            print(f"Disparo registrado: {time_str}")
        else:
            print(f"Falha: {resp.status_code} | {resp.text}")
    except Exception as e:
        print(f"Erro ao conectar com Flask: {e}")
        
def modo_envio(random_mode=False, monitor=False, flask_url=None):
    bms = carregar_bms()
    if not bms:
        print("❌ Nenhuma BM. Use --cadastrar.")
        return

    print("\nBMs:")
    for i, nome in enumerate(bms.keys()):
        print(f"{i + 1}. {nome}")
    escolha = input("Escolha BM: ")
    try:
        bm_nome = list(bms.keys())[int(escolha) - 1]
    except:
        print("❌ Inválida.")
        return

    bm = bms[bm_nome]
    phone_number_id = bm['phone_number_id']
    token = bm['token']
    templates = bm['templates']

    # Registrar se -m
    if monitor and flask_url:
        registrar_disparo(phone_number_id, flask_url)

    leads = pd.read_csv("nosv7.csv")
    if not os.path.exists(LOG_FILE):
        open(LOG_FILE, "w").close()
    with open(LOG_FILE, "r") as f:
        enviados = set(line.strip() for line in f)
    leads_filtrados = leads[~leads['telefone'].astype(str).isin(enviados)].reset_index(drop=True)

    if random_mode:
        leads_filtrados = leads_filtrados.sample(frac=1).reset_index(drop=True)

    total_leads = len(leads_filtrados)
    leads_filtrados['template_name'] = [templates[i % len(templates)] for i in range(total_leads)]

    print(f"\n📤 {total_leads} leads | namespace={NAMESPACE_VALUE}")

    with ThreadPoolExecutor(max_workers=40) as executor:
        executor.map(
            lambda lead: enviar_template(lead, phone_number_id, token, log_enabled=not random_mode),
            [lead for _, lead in leads_filtrados.iterrows()]
        )

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Sender WhatsApp")
    parser.add_argument('--cadastrar', action='store_true', help='Cadastrar BM')
    parser.add_argument('--random', '-r', action='store_true', help='Aleatório, sem log')
    parser.add_argument('-m', '--monitor', action='store_true', help='Registrar disparo no Flask')
    parser.add_argument('--flask-url', type=str, default="https://natalycomercio.com/update-disparo", help='URL Flask')
    parser.add_argument('--test', action='store_true', help='Teste envio isolado')

    args = parser.parse_args()

    if args.cadastrar:
        cadastrar_bm()
    elif args.test:
        bms = carregar_bms()
        if bms:
            bm = list(bms.values())[0]
            enviar_template({'telefone': '5571988608723', 'nome': 'Teste', 'template_name': 'reta'}, bm['phone_number_id'], bm['token'])
        else:
            print("❌ Nenhuma BM.")
    else:
        modo_envio(random_mode=args.random, monitor=args.monitor, flask_url=args.flask_url)
