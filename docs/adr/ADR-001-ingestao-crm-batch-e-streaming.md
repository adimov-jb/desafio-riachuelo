# ADR-001 — Ingestão do CRM com dois caminhos: export diário (batch) e webhook (tempo real)

| | |
|---|---|
| **Status** | Proposto · revisão no fórum de arquitetura |
| **Data** | 2025-01-10 |
| **Decisores** | Coordenação de Engenharia de Dados (responsável), Arquitetura de TI |
| **Consultados** | Prevenção a Fraudes, Risco de Crédito, Segurança da Informação, DPO/Compliance, CRM/Marketing |
| **Informados** | Data Science, Financeiro, Operações (plantão) |
| **Revisar quando** | o CRM oferecer replay garantido do stream; o volume crítico passar de 20% do total; o custo do realtime passar de 15% do custo de dados do CRM; houver nova exigência regulatória de prazo |

---

## Resumo executivo (para quem não é técnico)

**O problema.** O CRM guarda o que os clientes fazem: compras, pagamentos, SMS, push e mudanças de cadastro. Ele entrega esses dados de duas formas:
- **Pacote diário:** completo, mas chega só no dia seguinte e precisa ser baixado em até 24h.
- **Aviso em tempo real:** chega em segundos, mas pode falhar e não tem como pedir de novo.

**A decisão.** Usamos as duas formas, cada uma no que faz melhor:
- **Pacote diário:** é a fonte oficial e completa, usada em relatórios, auditoria e modelos.
- **Tempo real:** usado só para o que não pode esperar, ou seja, bloquear fraude e respeitar na hora o cliente que pediu para não receber mensagens.
- **Verificação diária:** todo dia o sistema compara as duas e avisa se o tempo real deixou passar algo.

**Por que não uma só.**

| Se usarmos só... | Consequência |
|---|---|
| o pacote diário | uma fraude só seria vista no dia seguinte, depois do prejuízo |
| o tempo real | pagaríamos tempo real para 100% dos dados e não teríamos garantia de que nada se perdeu, o que é inaceitável para auditoria do Banco Central |

**Custo e risco.**
- **Custo:** o tempo real recebe só cerca de 10–20% dos eventos, então custa pouco perto de uma única fraude evitada.
- **Risco:** é mais um sistema para manter. Isso é mitigado com código e regras compartilhados, além da verificação diária automática.

**O que muda para cada área.**
- **Fraude:** passa a receber os sinais em segundos.
- **Marketing:** o descadastro vale em minutos.
- **Risco, Financeiro e Data Science:** continuam com os dados completos todo dia até as 12:00 UTC (09:00 em Brasília).

---

## Contexto

- O **Export Raw User Data** permite 1 requisição por dia (janela UTC), entrega links S3 que expiram em 24h, com até 2 TB, e avisa por webhook quando está pronto. É **completo e reprocessável**, mas tem latência de D+1.
- O **Webhook (Data Stream)** empurra eventos em tempo real. É **at-least-once**, **sem replay**: se o nosso endpoint estiver fora, o evento só é recuperado pelo export.
- **Exigências que dependem de latência:**
  - prevenção a fraudes em transações, cadastro e login (compartilhamento de indícios de fraude, Res. Conjunta BCB/CMN nº 6/2023);
  - monitoramento de operações para PLD/FT (Circular BCB nº 3.978/2020);
  - opt-out de comunicação (LGPD).
- **Exigências que dependem de completude e trilha auditável:**
  - política de segurança cibernética e de processamento em nuvem (Res. CMN nº 4.893/2021), com rastreabilidade de incidentes;
  - reconciliação contábil, PDD e modelos de risco.

## Decisão

1. **Batch (Export):**
   - O MWAA agenda o pedido às 00:15 UTC.
   - Uma Lambda recebe o aviso de "pronto".
   - Um job Glue copia os arquivos para o S3 da FinVarejo antes de o link expirar.
   - O Databricks carrega na Bronze (Delta) e faz o MERGE na Silver.
   - É a **fonte autoritativa**.
2. **Realtime (Webhook):**
   - Fluxo: API Gateway → Lambda (valida a assinatura e classifica) → **Kinesis Data Streams** (só eventos críticos) → antifraude (enhanced fan-out) e Databricks Structured Streaming → a mesma Silver.
   - Eventos não críticos vão pelo Firehose para o S3, sem custo por shard.
3. **Uma única Silver:** mesmo schema, mesma chave (`event_id`) e mesma função de MERGE nos dois caminhos. O export sobrescreve a versão provisória do webhook.
4. **Reconciliação diária:** mede a % dos eventos críticos do export que o webhook já tinha entregado. Abaixo de 99%, gera alerta.

## Alternativas consideradas

| Alternativa | Por que não |
|---|---|
| Só export diário | Fraude e opt-out com D+1: descumpre a expectativa regulatória de tempestividade e gera perda financeira direta |
| Só webhook | Sem replay nem garantia de completude, sem trilha reprocessável. Custo de tempo real sobre 100% do volume |
| Webhook direto para S3 (Firehose) e microbatch a cada 5 min | Latência de minutos; o antifraude precisa de segundos |
| Kinesis on-demand para tudo | Simples, mas paga por GB em todo o volume não crítico. Fica reservado para o início, até conhecermos o pico |

## Trade-offs explícitos

| Ganhamos | Pagamos |
|---|---|
| Fraude e opt-out em segundos | Mais um pipeline para operar (plantão, deploy, monitoramento) |
| Completude e auditoria pelo export | Consistência **eventual**: até D+1, um evento pode existir só na versão provisória do webhook |
| Custo de streaming limitado aos eventos críticos | Precisamos manter a lista de criticidade (governança de eventos novos) |
| Uma única Silver, sem "duas verdades" | Acoplamento: os dois caminhos dependem do mesmo schema e da mesma função de deduplicação |
| Reprocesso de qualquer dia (raw imutável, MERGE idempotente) | Duas cópias de dados pessoais (Bronze batch e Bronze stream), o que aumenta a superfície LGPD. Mitigado com retenção curta e acesso restrito |
| Stack AWS + Databricks já adotada | Dependência de dois fornecedores, com custo de saída se a estratégia de nuvem mudar |

## Consequências e plano de reversão

- **Novos SLOs:**
  - batch disponível às 12:00 UTC em 99% dos dias;
  - realtime com p95 abaixo de 2 min até a Silver e abaixo de 2 s até o antifraude;
  - cobertura do webhook de pelo menos 99% dos eventos críticos.
- **Novos custos mensais:** shards do Kinesis, cluster de streaming e Firehose. Estão sob orçamento com alerta em 80%.
- **Reversão:** desligar o streaming não perde dados, porque o export continua completo. O que se perde é a latência. A decisão é reversível com baixo custo.
