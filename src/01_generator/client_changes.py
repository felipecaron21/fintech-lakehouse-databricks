"""Mudanças diárias de clientes com replay determinístico a partir do D0 (ADR-08).

Regras:
- Um dia tem mudanças com probabilidade PROB_DIA_COM_MUDANCAS; nesses dias,
  PROP_CLIENTES_QUE_MUDAM dos clientes muda exatamente um grupo de campos.
- Tudo é sorteado com semente derivada da data: o mesmo dia produz sempre
  as mesmas mudanças, em qualquer execução e em qualquer ordem de execução.
- Os valores novos saem no mesmo formato da fonte (ex.: "$64200").

Dependência: faker==40.40.0 (versão fixada; outra versão muda os resultados).
"""

import copy
import random
from datetime import date, timedelta

from faker import Faker

D0 = date(2017, 1, 1)
PROB_DIA_COM_MUDANCAS = 0.30
PROP_CLIENTES_QUE_MUDAM = 0.01

GRUPOS = ["email", "endereco", "renda", "divida", "score"]
DOMINIOS_EMAIL = ["gmail.com", "yahoo.com", "hotmail.com", "outlook.com", "aol.com", "icloud.com"]

_fake = Faker("en_US")


# ---------------------------------------------------------------------------
# Formato da fonte: valores monetários como "$59696"
# ---------------------------------------------------------------------------

def _ler_dinheiro(valor: str) -> int:
    return int(valor.replace("$", ""))


def _formatar_dinheiro(valor: int) -> str:
    return f"${valor}"


# ---------------------------------------------------------------------------
# 1. Quais clientes mudam num dia
# ---------------------------------------------------------------------------

def sortear_mudancas(dia: date, ids_ordenados: list) -> list:
    """Retorna as mudanças de um dia: lista de (id_cliente, grupo, semente_da_mudanca).

    `ids_ordenados` precisa estar sempre na mesma ordem (ordenado por id),
    senão o sorteio escolhe clientes diferentes em execuções diferentes.
    """
    sorteador = random.Random(int(dia.strftime("%Y%m%d")))

    if sorteador.random() >= PROB_DIA_COM_MUDANCAS:
        return []

    quantidade = max(1, round(len(ids_ordenados) * PROP_CLIENTES_QUE_MUDAM))
    escolhidos = sorteador.sample(ids_ordenados, quantidade)

    return [(id_cliente, sorteador.choice(GRUPOS), sorteador.randrange(2**31))
            for id_cliente in escolhidos]


# ---------------------------------------------------------------------------
# 2. Como um cliente muda
# ---------------------------------------------------------------------------

def aplicar_mudanca(cliente: dict, grupo: str, semente: int) -> dict:
    """Retorna uma cópia do cliente com um grupo de campos alterado."""
    sorteador = random.Random(semente)
    novo = dict(cliente)

    if grupo == "email":
        # Troca de provedor: mantém o padrão nome.sobrenome+id e muda o domínio
        usuario, dominio_atual = cliente["email"].split("@")
        opcoes = [d for d in DOMINIOS_EMAIL if d != dominio_atual]
        novo["email"] = f"{usuario}@{sorteador.choice(opcoes)}"

    elif grupo == "endereco":
        # Novo endereço, com coordenadas de uma localidade real dos EUA
        _fake.seed_instance(semente)
        novo["address"] = f"{_fake.building_number()} {_fake.street_name()}"
        latitude, longitude = _fake.local_latlng(country_code="US", coords_only=True)
        novo["latitude"] = f"{float(latitude):.2f}"
        novo["longitude"] = f"{float(longitude):.2f}"

    elif grupo == "renda":
        # Variação entre -10% e +20%, aplicada às duas rendas na mesma proporção
        fator = sorteador.uniform(0.90, 1.20)
        novo["yearly_income"] = _formatar_dinheiro(round(_ler_dinheiro(cliente["yearly_income"]) * fator))
        novo["per_capita_income"] = _formatar_dinheiro(round(_ler_dinheiro(cliente["per_capita_income"]) * fator))

    elif grupo == "divida":
        # Variação entre -30% e +30%; quem não tinha dívida passa a ter uma pequena
        atual = _ler_dinheiro(cliente["total_debt"])
        if atual == 0:
            nova = sorteador.randint(500, 5000)
        else:
            nova = max(0, round(atual * sorteador.uniform(0.70, 1.30)))
        novo["total_debt"] = _formatar_dinheiro(nova)

    elif grupo == "score":
        # Variação entre -40 e +40 pontos (nunca zero), dentro da faixa 300-850
        atual = int(cliente["credit_score"])
        deltas = [d for d in range(-40, 41) if d != 0 and 300 <= atual + d <= 850]
        novo["credit_score"] = str(atual + sorteador.choice(deltas))

    else:
        raise ValueError(f"Grupo de mudança desconhecido: {grupo}")

    return novo


# ---------------------------------------------------------------------------
# 3. Replay do D0 até a data pedida
# ---------------------------------------------------------------------------

def reconstruir_estado(data_simulada: date, clientes: dict) -> tuple:
    """Reaplica em memória as mudanças de cada dia, do D0 até `data_simulada`.

    Args:
        data_simulada: dia a gerar.
        clientes: estado inicial no D0, {id: registro completo}, com valores como texto.

    Returns:
        (estado, mudados_no_dia): o estado de todos os clientes em `data_simulada`
        e a lista ordenada dos ids que mudaram nesse próprio dia.
    """
    estado = copy.deepcopy(clientes)
    ids_ordenados = sorted(estado, key=int)
    mudados_no_dia = set()

    dia = D0
    while dia <= data_simulada:
        for id_cliente, grupo, semente in sortear_mudancas(dia, ids_ordenados):
            estado[id_cliente] = aplicar_mudanca(estado[id_cliente], grupo, semente)
            if dia == data_simulada:
                mudados_no_dia.add(id_cliente)
        dia += timedelta(days=1)

    return estado, sorted(mudados_no_dia, key=int)