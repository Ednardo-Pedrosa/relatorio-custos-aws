# Relatório de Custos AWS — Logbit

Script Python para coleta automática de dados de infraestrutura e custos AWS, geração de relatório Excel e envio por e-mail (SendGrid) e Discord.

---

## Funcionalidades

- **EC2** — inventário de instâncias com custo de compute + EBS por região
- **RDS** — instâncias com custo de compute e storage (Single/Multi-AZ)
- **EBS Snapshots** — custo proporcional ao tempo de existência no mês
- **Data Transfer** — custos via Cost Explorer + volume de rede por instância (CloudWatch)
- **S3** — tamanho dos buckets (CloudWatch) + custos reais (Cost Explorer)
- **Gráficos** — 7 gráficos com matplotlib/seaborn
- **Resumo de Gastos** — Cost Explorer vs estimativa do script, comparativo linha a linha
- **Envio** — relatório Excel por e-mail via SendGrid e mensagem resumo no Discord

---

## Regiões monitoradas

| Região | Nome |
|--------|------|
| `sa-east-1` | South America (São Paulo) |
| `us-east-1` | US East (N. Virginia) |

---

## Estrutura do relatório Excel

| Aba | Conteúdo |
|-----|----------|
| Instancias EC2 | Inventário com custo compute + EBS estimado |
| Instancias RDS | Inventário com custo compute + storage estimado |
| Snapshots EBS | Lista de snapshots com custo proporcional |
| Data Transfer | Custos Cost Explorer + tráfego CloudWatch por instância |
| S3 | Buckets por região + custos Cost Explorer |
| Gráficos | 7 gráficos de distribuição e custo |
| Resumo de Gastos | Cost Explorer × estimativa do script com comparativo |

---

## Requisitos

- Python 3.8+
- Perfil AWS configurado com as permissões abaixo
- Conta SendGrid com API Key
- Webhook do Discord (opcional)

### Permissões IAM necessárias

```json
{
  "Effect": "Allow",
  "Action": [
    "ec2:DescribeInstances",
    "ec2:DescribeVolumes",
    "ec2:DescribeSnapshots",
    "rds:DescribeDBInstances",
    "s3:ListAllMyBuckets",
    "s3:GetBucketLocation",
    "cloudwatch:GetMetricStatistics",
    "ce:GetCostAndUsage",
    "route53:ListHostedZones",
    "route53:ListResourceRecordSets"
  ],
  "Resource": "*"
}
```

---

## Instalação

```bash
# Clonar o repositório
git clone https://github.com/Ednardo-Pedrosa/relatorio-custos-aws.git
cd relatorio-custos-aws

# Criar e ativar virtualenv
python3 -m venv venv
source venv/bin/activate

# Instalar dependências
pip install boto3 pandas matplotlib seaborn sendgrid xlsxwriter openpyxl python-dotenv requests
```

---

## Configuração

Copie o arquivo de exemplo e preencha com suas credenciais:

```bash
cp .env.example .env
```

Edite o `.env`:

```env
SENDGRID_API_KEY=SG.xxxxxx
SENDGRID_FROM_EMAIL=seu@email.com
AWS_PROFILE=nome-do-perfil-aws
DISCORD_WEBHOOK_URL=https://discord.com/api/webhooks/...
```

> `DISCORD_WEBHOOK_URL` é opcional. Se não preenchido, o envio ao Discord é ignorado.

### Configurar perfil AWS

```bash
aws configure --profile nome-do-perfil-aws
```

---

## Execução

```bash
python3 script-relatorio-ec2-sendgrid-aws.py
```

O script irá:
1. Coletar dados de EC2, RDS, Snapshots, Data Transfer e S3
2. Gerar gráficos
3. Exportar o relatório para `.xlsx`
4. Enviar por e-mail via SendGrid
5. Enviar mensagem resumo + arquivo para o canal Discord

---

## Agendamento (cron)

Para executar automaticamente todo dia 1 do mês às 08h:

```bash
crontab -e
```

```cron
0 8 1 * * /bin/bash /caminho/para/run_logbit_alert.sh >> /caminho/para/logbit_alert.log 2>&1
```

---

## Variáveis importantes no script

| Variável | Padrão | Descrição |
|----------|--------|-----------|
| `regioes` | `['sa-east-1', 'us-east-1']` | Regiões monitoradas |
| `TAXA_CAMBIO` | `5.18` | Taxa USD → BRL (atualizar mensalmente) |
| `HORAS_NO_MES` | `720` | Referência de horas mensais (30 dias) |
| `principais_gastos` | — | Valores do Cost Explorer — **atualizar mensalmente** |

> Os dados do `principais_gastos` (Seção Resumo) são preenchidos manualmente a partir do AWS Cost Explorer e devem ser atualizados antes de cada execução mensal.

---

## Observação sobre diferenças de estimativa

O script utiliza preços **On-Demand** para estimar custos. Diferenças em relação ao Cost Explorer são esperadas quando a conta possui:

- **Savings Plans** ou **Reserved Instances** (desconto sobre On-Demand)
- Instâncias criadas ou encerradas durante o período
- Créditos AWS aplicados

O comparativo na aba **Resumo de Gastos** exibe a diferença linha a linha (EC2, RDS) para facilitar a análise.
