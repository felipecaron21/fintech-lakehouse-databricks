"""Mudanças diárias de cartões com replay determinístico a partir do D0 (ADR-08).

Mesmo padrão do client_changes.py, aplicado aos cartões (SCD1 na Gold):
- Um dia tem mudanças com probabilidade PROB_DIA_COM_MUDANCAS; nesses dias,
  PROP_CARTOES_QUE_MUDAM dos cartões muda exatamente um grupo de campos.
- A semente do sorteio é a data + um deslocamento fixo, para que os dias com
  mudanças de cartões não coincidam com os dias com mudanças de clientes.
- Os valores novos saem no mesmo formato da fonte (ex.: "$12400", "03/2021").
"""

import copy
import random
from datetime import date, timedelta

D0 = date(2017, 1, 1)
PROB_DIA_COM_MUDANCAS = 0.30
PROP_CARTOES_QUE_MUDAM = 0.01
DESLOCAMENTO_SEMENTE = 1_000_000_000   # separa o sorteio dos cartões do sorteio dos clientes
ANOS_VALIDADE_REEMISSAO = 4

GRUPOS = ["limite", "dark_web", "senha", "reemissao"]
# Pesos do sorteio do grupo: vazamento na dark web é um evento raro.
# Com pesos iguais, ~56% dos cartões terminariam vazados até 2019-10.
PESOS_GRUPOS = [45, 3, 27, 25]


def _ler_dinheiro(valor: str) -> int:
    return int(valor.replace("$", ""))


def _formatar_dinheiro(valor: int) -> str:
    return f"${valor}"


# ---------------------------------------------------------------------------
# 1. Quais cartões mudam num dia
# ---------------------------------------------------------------------------

def sortear_mudancas(dia: date, ids_ordenados: list) -> list:
    """Retorna as mudanças de um dia: lista de (id_cartao, grupo, semente_da_mudanca)."""
    sorteador = random.Random(int(dia.strftime("%Y%m%d")) + DESLOCAMENTO_SEMENTE)

    if sorteador.random() >= PROB_DIA_COM_MUDANCAS:
        return []

    quantidade = max(1, round(len(ids_ordenados) * PROP_CARTOES_QUE_MUDAM))
    escolhidos = sorteador.sample(ids_ordenados, quantidade)

    return [(id_cartao, sorteador.choices(GRUPOS, weights=PESOS_GRUPOS)[0], sorteador.randrange(2**31))
            for id_cartao in escolhidos]


# ---------------------------------------------------------------------------
# 2. Como um cartão muda
# ---------------------------------------------------------------------------

def aplicar_mudanca(cartao: dict, grupo: str, semente: int, dia: date) -> dict:
    """Retorna uma cópia do cartão com um grupo de campos alterado."""
    sorteador = random.Random(semente)
    novo = dict(cartao)

    # Um cartão já vazado não "desvaza": sorteia outro grupo
    if grupo == "dark_web" and cartao["card_on_dark_web"] == "Yes":
        grupo = sorteador.choice([g for g in GRUPOS if g != "dark_web"])

    if grupo == "limite":
        # Variação entre -20% e +50%, arredondada para a centena
        atual = _ler_dinheiro(cartao["credit_limit"])
        novo_limite = round(atual * sorteador.uniform(0.80, 1.50) / 100) * 100
        if novo_limite == atual:
            novo_limite += 100
        novo["credit_limit"] = _formatar_dinheiro(max(100, novo_limite))

    elif grupo == "dark_web":
        novo["card_on_dark_web"] = "Yes"

    elif grupo == "senha":
        novo["year_pin_last_changed"] = str(dia.year)

    elif grupo == "reemissao":
        # Nova validade a partir do mês da reemissão, sempre posterior à validade atual
        mes_atual, ano_atual = (int(x) for x in cartao["expires"].split("/"))
        ano_novo = dia.year + ANOS_VALIDADE_REEMISSAO
        while (ano_novo, dia.month) <= (ano_atual, mes_atual):
            ano_novo += 1
        novo["expires"] = f"{dia.month:02d}/{ano_novo}"
        novo["num_cards_issued"] = str(int(cartao["num_cards_issued"]) + 1)

    else:
        raise ValueError(f"Grupo de mudança desconhecido: {grupo}")

    return novo


# ---------------------------------------------------------------------------
# 3. Replay do D0 até a data pedida
# ---------------------------------------------------------------------------

def reconstruir_estado(data_simulada: date, cartoes: dict) -> tuple:
    """Reaplica em memória as mudanças de cada dia, do D0 até `data_simulada`.

    Args:
        data_simulada: dia a gerar.
        cartoes: estado inicial no D0, {id: registro completo}, com valores como texto.

    Returns:
        (estado, mudados_no_dia): o estado de todos os cartões em `data_simulada`
        e a lista ordenada dos ids que mudaram nesse próprio dia.
    """
    estado = copy.deepcopy(cartoes)
    ids_ordenados = sorted(estado, key=int)
    mudados_no_dia = set()

    dia = D0
    while dia <= data_simulada:
        for id_cartao, grupo, semente in sortear_mudancas(dia, ids_ordenados):
            estado[id_cartao] = aplicar_mudanca(estado[id_cartao], grupo, semente, dia)
            if dia == data_simulada:
                mudados_no_dia.add(id_cartao)
        dia += timedelta(days=1)

    return estado, sorted(mudados_no_dia, key=int)