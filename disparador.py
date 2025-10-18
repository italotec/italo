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

def carregar_bms():
    if not os.path.exists(BM_FILE):
        return {}
    with open(BM_FILE, 'r') as f:
        return json.load(f)

def salvar_bms(bms):
    with open(BM_FILE, 'w') as f:
        json.dump(bms, f, indent=4)

def cadastrar_bm():
    bms = carregar_bms()
    nome = input("Nome da BM: ")
    phone_number_id = input("Phone Number ID: ")
    token = input("Token: ")
    templates_raw = input("Templates (separados por vírgula): ")
    templates = [t.strip() for t in templates_raw.split(',')]

    bms[nome] = {
        "phone_number_id": phone_number_id,
        "token": token,
        "templates": templates
    }
    salvar_bms(bms)
    print(f"✅ BM '{nome}' cadastrada com sucesso.")

def enviar_template(lead, phone_number_id, token, log_enabled=True):
    telefone = str(lead.get('telefone', '')).strip()
    nome = str(lead.get('nome', '')).strip()
    template_name = str(lead.get('template_name', '')).strip()

    if not telefone or not template_name:
        print(f"⚠️ Lead faltando telefone ou template_name: {lead}")
        return

    api_url = f"https://graph.facebook.com/v23.0/{phone_number_id}/messages"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {token}"
    }

    # Apenas BODY com variáveis (o botão é fixo no template, então não precisa enviar componente de button)
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
        print(f"{telefone}: {response.status_code} | {response.text} | namespace={NAMESPACE_VALUE} | param={PARAM_NAME_VALUE}")
        if response.status_code == 200 and log_enabled:
            with LOCK:
                with open(LOG_FILE, "a") as f:
                    f.write(f"{telefone}\n")
    except Exception as e:
        print(f"Erro ao enviar para {telefone}: {e}")

def modo_envio(random_mode=False):
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

    # CSV precisa ter: telefone, nome (opcional)
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

    print(f"\n📤 Iniciando envio para {total_leads} leads...")
    print(f"📌 Usando namespace: {NAMESPACE_VALUE} | param_name: {PARAM_NAME_VALUE}")

    with ThreadPoolExecutor(max_workers=1) as executor:
        executor.map(
            lambda lead: enviar_template(lead, phone_number_id, token, log_enabled=not random_mode),
            [lead for _, lead in leads_filtrados.iterrows()]
        )

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--cadastrar', action='store_true', help='Cadastrar nova BM')
    parser.add_argument('--random', '-r', action='store_true', help='Enviar mensagens em ordem aleatória e sem log')
    args = parser.parse_args()

    if args.cadastrar:
        cadastrar_bm()
    else:
        modo_envio(random_mode=args.random)
