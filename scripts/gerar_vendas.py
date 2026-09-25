"""Gera o data/vendas.csv usado pelas DAGs dos desafios.

Roda uma vez; o CSV resultante e versionado no git.

    python scripts/gerar_vendas.py
"""

from __future__ import annotations

import csv
import random
from datetime import date, timedelta
from pathlib import Path

SEMENTE = 42
PRIMEIRA_SEGUNDA = date(2026, 7, 6)
SEMANAS = 12

CATALOGO = [
    ("Notebook", "Eletronicos", 2800.00, 4500.00),
    ("Monitor", "Eletronicos", 780.00, 1900.00),
    ("Smartphone", "Eletronicos", 1200.00, 3800.00),
    ("Mouse", "Perifericos", 35.00, 190.00),
    ("Teclado", "Perifericos", 89.00, 420.00),
    ("Headset", "Perifericos", 120.00, 650.00),
    ("Webcam", "Perifericos", 150.00, 520.00),
    ("Cadeira Gamer", "Mobiliario", 890.00, 2300.00),
    ("Mesa Ajustavel", "Mobiliario", 1100.00, 2900.00),
    ("Suporte Monitor", "Mobiliario", 95.00, 380.00),
    ("SSD 1TB", "Armazenamento", 320.00, 780.00),
    ("HD Externo 2TB", "Armazenamento", 380.00, 690.00),
    ("Pen Drive 128GB", "Armazenamento", 45.00, 130.00),
]

REGIOES = ["Sudeste", "Sul", "Nordeste", "Norte", "Centro-Oeste"]
PESOS_REGIAO = [40, 22, 20, 8, 10]

DESTINOS = [
    Path("airflow-local/data/vendas.csv"),
    Path("airflow-aws/data/vendas.csv"),
]


def gerar_linhas() -> list[dict[str, str]]:
    rng = random.Random(SEMENTE)
    linhas: list[dict[str, str]] = []

    for semana in range(SEMANAS):
        inicio = PRIMEIRA_SEGUNDA + timedelta(weeks=semana)
        # volume varia por semana para o branching do DAG 06 ter o que decidir
        vendas_na_semana = rng.randint(45, 85)

        for _ in range(vendas_na_semana):
            produto, categoria, preco_min, preco_max = rng.choice(CATALOGO)
            dia = inicio + timedelta(days=rng.randint(0, 6))
            linhas.append(
                {
                    "data": dia.isoformat(),
                    "produto": produto,
                    "categoria": categoria,
                    "quantidade": str(rng.randint(1, 15)),
                    "preco": f"{rng.uniform(preco_min, preco_max):.2f}",
                    "regiao": rng.choices(REGIOES, weights=PESOS_REGIAO)[0],
                }
            )

    linhas.sort(key=lambda linha: linha["data"])
    sujar(linhas, rng)
    return linhas


def sujar(linhas: list[dict[str, str]], rng: random.Random) -> None:
    """Estraga 15 linhas de proposito, para o ETL ter o que limpar."""
    alvos = rng.sample(range(len(linhas)), 15)
    for posicao, indice in enumerate(alvos):
        if posicao % 2 == 0:
            linhas[indice]["quantidade"] = ""
        else:
            linhas[indice]["preco"] = "0.00"


def main() -> None:
    raiz = Path(__file__).resolve().parent.parent
    linhas = gerar_linhas()

    for destino in DESTINOS:
        caminho = raiz / destino
        caminho.parent.mkdir(parents=True, exist_ok=True)
        with caminho.open("w", newline="", encoding="utf-8") as arquivo:
            escritor = csv.DictWriter(
                arquivo,
                fieldnames=["data", "produto", "categoria", "quantidade", "preco", "regiao"],
            )
            escritor.writeheader()
            escritor.writerows(linhas)
        print(f"{destino}: {len(linhas)} linhas")

    primeira = linhas[0]["data"]
    ultima = linhas[-1]["data"]
    print(f"periodo: {primeira} ate {ultima} ({SEMANAS} semanas)")


if __name__ == "__main__":
    main()
