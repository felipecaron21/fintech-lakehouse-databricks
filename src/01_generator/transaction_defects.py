"""Defeitos de qualidade nas transações diárias, com gabarito (ADR-09).

Para gerar o arquivo do dia D, o módulo recebe as transações de origem de D e
dos 14 dias anteriores (cada dia ordenado por id) e aplica, de forma
determinística (semente = data + deslocamento por tipo de defeito):

1. Atrasadas (2%): saem do dia em que aconteceram e chegam de 1 a 7 dias depois.
2. Valor nulo (0,5%): amount = None.
3. Cartão inexistente (0,3%): card_id trocado por um id acima de 900000.
4. Duplicada no próprio arquivo (0,5%): cópia exata de uma transação do dia.
5. Duplicada reenviada (0,5%): cópia exata de uma transação enviada num dos
   7 dias anteriores.
6. Coluna nova: device_type a partir de DATA_COLUNA_NOVA, coerente com o use_chip.

Cada defeito injetado é registrado no gabarito, para validar depois as regras
de qualidade da Silver.
"""

import random
from datetime import date, timedelta

D0 = date(2017, 1, 1)
DATA_MAX = date(2019, 10, 31)
DATA_COLUNA_NOVA = date(2017, 3, 1)

TAXA_ATRASADAS = 0.02
TAXA_VALOR_NULO = 0.005
TAXA_CARTAO_INEXISTENTE = 0.003
TAXA_DUPLICADAS_MESMO_ARQUIVO = 0.005
TAXA_DUPLICADAS_REENVIADAS = 0.005
JANELA_DIAS = 7                      # atraso máximo e alcance das duplicadas reenviadas
ID_CARTAO_INEXISTENTE_MIN = 900000

# Um deslocamento por tipo de sorteio: cada defeito tem a sua própria sequência
DESL_ATRASO, DESL_QUALIDADE, DESL_DUP, DESL_REENVIO, DESL_DEVICE = (
    2_000_000_000, 3_000_000_000, 4_000_000_000, 5_000_000_000, 6_000_000_000)


def _semente(dia: date, deslocamento: int) -> int:
    return int(dia.strftime("%Y%m%d")) + deslocamento


def _dias_anteriores(dia: date, janela: int) -> list:
    """Dias de dia-janela até dia-1, sem passar do D0."""
    return [dia - timedelta(days=k) for k in range(janela, 0, -1)
            if dia - timedelta(days=k) >= D0]


class GeradorDeDefeitos:
    def __init__(self, transacoes_por_dia: dict):
        """`transacoes_por_dia`: {date: [linhas planas do Kaggle, ordenadas por id]}."""
        self._origem = transacoes_por_dia
        self._cache_atrasos, self._cache_entregues, self._cache_base = {}, {}, {}

    # --- 1. Atrasadas ------------------------------------------------------

    def _atrasos_do_dia(self, dia: date) -> dict:
        """{id: dias de atraso} das transações que saem do dia `dia`."""
        if dia not in self._cache_atrasos:
            linhas = self._origem[dia]
            atraso_max = min(JANELA_DIAS, (DATA_MAX - dia).days)
            atrasos = {}
            if atraso_max >= 1 and linhas:
                sorteador = random.Random(_semente(dia, DESL_ATRASO))
                quantidade = round(len(linhas) * TAXA_ATRASADAS)
                for linha in sorteador.sample(linhas, quantidade):
                    atrasos[linha["id"]] = sorteador.randint(1, atraso_max)
            self._cache_atrasos[dia] = atrasos
        return self._cache_atrasos[dia]

    def _entregues(self, dia: date) -> tuple:
        """Transações que chegam no arquivo do dia: as do dia que não atrasaram
        + as atrasadas de dias anteriores que chegam hoje."""
        if dia not in self._cache_entregues:
            atrasos = self._atrasos_do_dia(dia)
            linhas = [dict(l) for l in self._origem[dia] if l["id"] not in atrasos]
            gabarito = [("atrasada_saida", id_, str(dia + timedelta(days=k)))
                        for id_, k in sorted(atrasos.items(), key=lambda x: int(x[0]))]

            for origem in _dias_anteriores(dia, JANELA_DIAS):
                atrasos_origem = self._atrasos_do_dia(origem)
                for linha in self._origem[origem]:
                    k = atrasos_origem.get(linha["id"])
                    if k is not None and origem + timedelta(days=k) == dia:
                        linhas.append(dict(linha))
                        gabarito.append(("atrasada_chegada", linha["id"], str(origem)))

            self._cache_entregues[dia] = (linhas, gabarito)
        return self._cache_entregues[dia]

    # --- 2 e 3. Valor nulo e cartão inexistente ----------------------------

    def _base(self, dia: date) -> tuple:
        """Entregues do dia com valores nulos e cartões inexistentes aplicados."""
        if dia not in self._cache_base:
            linhas, gabarito = self._entregues(dia)
            linhas = [dict(l) for l in linhas]
            gabarito = list(gabarito)

            sorteador = random.Random(_semente(dia, DESL_QUALIDADE))
            n_nulo = round(len(linhas) * TAXA_VALOR_NULO)
            n_cartao = round(len(linhas) * TAXA_CARTAO_INEXISTENTE)
            indices = sorteador.sample(range(len(linhas)), n_nulo + n_cartao)

            for i in indices[:n_nulo]:
                gabarito.append(("valor_nulo", linhas[i]["id"], linhas[i]["amount"]))
                linhas[i]["amount"] = None

            for i in indices[n_nulo:]:
                cartao_falso = str(ID_CARTAO_INEXISTENTE_MIN + sorteador.randint(0, 99_999))
                gabarito.append(("cartao_inexistente", linhas[i]["id"], linhas[i]["card_id"]))
                linhas[i]["card_id"] = cartao_falso

            self._cache_base[dia] = (linhas, gabarito)
        return self._cache_base[dia]

    # --- 4, 5 e 6. Duplicadas e coluna nova --------------------------------

    def gerar_dia(self, dia: date) -> tuple:
        """Retorna (linhas_do_arquivo, gabarito) do dia, com o JSON já aninhado."""
        linhas, gabarito = self._base(dia)
        linhas = [dict(l) for l in linhas]
        gabarito = list(gabarito)

        # 4. Duplicadas no próprio arquivo
        sorteador = random.Random(_semente(dia, DESL_DUP))
        for linha in sorteador.sample(linhas, round(len(linhas) * TAXA_DUPLICADAS_MESMO_ARQUIVO)):
            linhas.append(dict(linha))
            gabarito.append(("duplicada_mesmo_arquivo", linha["id"], ""))

        # 5. Duplicadas reenviadas de dias anteriores (cópia exata do que foi enviado)
        candidatas = [(origem, linha)
                      for origem in _dias_anteriores(dia, JANELA_DIAS)
                      for linha in self._base(origem)[0]]
        if candidatas:
            sorteador = random.Random(_semente(dia, DESL_REENVIO))
            quantidade = min(len(candidatas), round(len(self._base(dia)[0]) * TAXA_DUPLICADAS_REENVIADAS))
            for origem, linha in sorteador.sample(candidatas, quantidade):
                linhas.append(dict(linha))
                gabarito.append(("duplicada_reenviada", linha["id"], str(origem)))

        # 6. JSON aninhado (+ device_type a partir da DATA_COLUNA_NOVA)
        arquivo = [_montar_json(linha, dia >= DATA_COLUNA_NOVA) for linha in linhas]

        gabarito = [{"date_partition": str(dia), "tipo_defeito": tipo,
                     "transaction_id": id_, "detalhe": "" if detalhe is None else str(detalhe)}
                    for tipo, id_, detalhe in gabarito]
        return arquivo, gabarito


def _montar_json(linha: dict, com_device_type: bool) -> dict:
    registro = {
        "id": linha["id"], "date": linha["date"], "client_id": linha["client_id"],
        "card_id": linha["card_id"], "amount": linha["amount"], "use_chip": linha["use_chip"],
        "errors": linha["errors"],
        "merchant": {
            "merchant_id": linha["merchant_id"], "merchant_city": linha["merchant_city"],
            "merchant_state": linha["merchant_state"], "zip": linha["zip"], "mcc": linha["mcc"],
        },
    }
    if com_device_type:
        # Sorteio por transação: a mesma transação recebe sempre o mesmo valor
        sorteador = random.Random(int(linha["id"]) + DESL_DEVICE)
        registro["device_type"] = (sorteador.choice(["mobile", "web"])
                                   if linha["use_chip"] == "Online Transaction" else "pos")
    return registro