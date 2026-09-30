"""Geração determinística de identidade sintética para clientes (ADR-05).

A mesma entrada (id do cliente + gênero) produz sempre a mesma identidade,
em qualquer execução e em qualquer notebook, desde que:
- a versão do Faker seja a fixada no projeto (faker==40.40.0);
- a ordem das chamadas ao Faker dentro da função não seja alterada.
"""

from faker import Faker

_fake = Faker("en_US")


def gerar_identidade(id_cliente: str, genero: str) -> dict:
    """Retorna SSN, nome e e-mail sintéticos para um cliente.

    Args:
        id_cliente: id do cliente, usado como semente.
        genero: "Female" ou "Male", usado para escolher o primeiro nome.

    Returns:
        Dicionário com as chaves id, ssn, name e email.
    """
    _fake.seed_instance(int(id_cliente))

    # A ordem abaixo NÃO pode mudar: ela define o resultado para cada semente.
    ssn = _fake.ssn()
    primeiro_nome = _fake.first_name_female() if genero == "Female" else _fake.first_name_male()
    sobrenome = _fake.last_name()
    dominio = _fake.free_email_domain()

    return {
        "id": id_cliente,
        "ssn": ssn,
        "name": f"{primeiro_nome} {sobrenome}",
        "email": f"{primeiro_nome}.{sobrenome}{id_cliente}@{dominio}".lower(),
    }