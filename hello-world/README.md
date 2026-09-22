# exemplo2 — rodando o DAG `01_ola_airflow`

Airflow 3 mínimo, com um único DAG:
[`dags/01_ola_airflow.py`](dags/01_ola_airflow.py).

```
falar_oi ──▶ escolher_numero ──▶ dobrar ──▶ anunciar
```

---

## Pré-requisitos

Docker Desktop instalado e **aberto** (ícone da baleia com "Engine running"),
com pelo menos 4 GB de memória em *Settings → Resources*.

```bash
docker --version
docker compose version
```

---

## Execução

### 1. Entre na pasta

```bash
cd caminho/para/exemplo2
```

### 2. Prepare o banco e o usuário (só na primeira vez)

```bash
docker compose up airflow-init
```

Baixa a imagem (~1,5 GB na primeira vez) e termina com `exited with code 0`.

### 3. Suba o Airflow

```bash
docker compose up -d
docker compose ps    # postgres, api-server, scheduler, dag-processor
```

### 4. Abra a interface

**<http://localhost:8080>** — usuário `airflow`, senha `airflow`.

### 5. Rode o DAG

1. Ligue a chavinha (toggle) ao lado de `01_ola_airflow`.
2. Clique no botão ▶ (Trigger) e confirme.
3. Entre no DAG → aba **Graph**: os quatro quadradinhos ficam verdes da
   esquerda para a direita.
4. Clique na task `dobrar` → **Logs**. Deve aparecer:

   ```
   Recebi 7 pelo XCom e dobrei: 14
   ```

### Alternativa pelo terminal

```bash
# listar o DAG
docker compose exec airflow-scheduler airflow dags list

# disparar uma execução
docker compose exec airflow-scheduler airflow dags trigger 01_ola_airflow

# rodar na hora, com o log direto na tela
docker compose exec airflow-scheduler airflow dags test 01_ola_airflow
```

---

## Parar

```bash
docker compose down      # desliga (mantém o histórico)
docker compose down -v   # apaga também o banco de metadados
```

---

## Se der problema

| Sintoma | O que fazer |
|---|---|
| `port is already allocated` | troque `"8080:8080"` por `"8081:8080"` no `docker-compose.yaml` |
| A interface não abre | espere ~30s; depois `docker compose logs airflow-api-server \| tail -30` |
| O DAG não aparece | espere ~30s (o dag-processor relê a pasta) e atualize a página |
| Login recusado | rode `docker compose up airflow-init` de novo e confira o `exited with code 0` |
| Containers reiniciando | falta memória: Docker Desktop → *Settings → Resources* → 4 GB |

> Este compose não sobe o `triggerer`, que só é necessário para operadores
> "deferrable" — este DAG não usa nenhum.
