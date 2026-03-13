#!/usr/bin/env python3

import boto3
import pandas as pd
from datetime import datetime, timezone, timedelta
import matplotlib.pyplot as plt
import seaborn as sns
import os
import base64
import requests
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail, Email, To, Content, Attachment, FileContent, FileName, FileType, Disposition
from pandas import ExcelWriter
from dotenv import load_dotenv

load_dotenv(override=True)

SENDGRID_API_KEY    = os.environ['SENDGRID_API_KEY']
SENDGRID_FROM_EMAIL = os.environ['SENDGRID_FROM_EMAIL']
AWS_PROFILE         = os.environ['AWS_PROFILE']
DISCORD_WEBHOOK_URL = os.environ.get('DISCORD_WEBHOOK_URL', '')

TAXA_CAMBIO = 5.15

# Sessão com perfil específico
session = boto3.Session(profile_name=AWS_PROFILE)
ec2_resource = session.resource('ec2')

# Regiões que você quer consultar
regioes = ['sa-east-1', 'us-east-1']

# Tabela de preços por hora EC2 (USD) - On-Demand, por região
# Valores extraídos da AWS Calculator (Março 2026)

precos_por_tipo_sa_east_1 = {
    # Família t3a (AMD)
    't3a.micro': 0.0152, 't3a.small': 0.0302, 't3a.medium': 0.0604, 't3a.large': 0.121,
    't3a.xlarge': 0.242, 't3a.2xlarge': 0.484,

    # Família t3 (Intel)
    't3.micro': 0.0146, 't3.small': 0.0336, 't3.medium': 0.0672, 't3.large': 0.162,
    't3.xlarge': 0.2688, 't3.2xlarge': 0.5376,

    # Gerações Anteriores
    't2.micro': 0.0186, 't2.small': 0.0322, 't2.medium': 0.0744, 't2.large': 0.1488,
    't2.2xlarge': 0.5152, 't1.micro': 0.027,

    # Famílias M5 e C5
    'm5.large': 0.134, 'm5.xlarge': 0.268, 'm5.2xlarge': 0.536,
    'c5.large': 0.119, 'c5.xlarge': 0.238, 'c5.2xlarge': 0.476,
}

precos_por_tipo_us_east_1 = {
    # Família t3a (AMD)
    't3a.medium': 0.0376, 't3a.xlarge': 0.1504,

    # Família t3 (Intel)
    't3.medium': 0.0416,

    # Gerações Anteriores
    't1.micro': 0.020, 't2.2xlarge': 0.3712,
}

precos_por_regiao = {
    'sa-east-1': precos_por_tipo_sa_east_1,
    'us-east-1': precos_por_tipo_us_east_1,
}

# Tabela RDS (USD) - On-Demand, por região e engine
# Valores extraídos da AWS Calculator (Março 2026)
# Engine values correspondem ao campo 'Engine' retornado pela API AWS:
#   postgres, mysql, mariadb, sqlserver-web, sqlserver-se, oracle-ee, etc.

# Preços específicos por (região, tipo, engine)
precos_rds_engine = {
    # South America (São Paulo)
    ('sa-east-1', 'db.t4g.micro', 'postgres'):      0.069,
    ('sa-east-1', 'db.t3.micro',  'postgres'):      0.076,
    ('sa-east-1', 'db.t3.small',  'postgres'):      0.151,
    ('sa-east-1', 'db.t3.small',  'sqlserver-web'): 0.155,
    # US East (N. Virginia)
    ('us-east-1', 'db.t3.micro',  'mysql'):         0.017,
}

# Preços base por região (sem diferenciação de engine — fallback)
precos_rds_base = {
    'sa-east-1': {
        'db.t3.medium': 0.084, 'db.t3.large': 0.168,
        'db.m5.large': 0.258, 'db.m5.xlarge': 0.516, 'db.m5.2xlarge': 1.032,
        'db.r5.large': 0.342, 'db.r5.xlarge': 0.684,
        'db.m6g.large': 0.231,
    },
    'us-east-1': {},
}

# Armazenamento EBS (EC2) — USD/GB-mês por região e tipo
# Fonte: AWS Pricing (Março 2026)
PRECO_EBS_VOLUME = {
    'sa-east-1': {'gp3': 0.152, 'gp2': 0.190, 'io1': 0.152, 'io2': 0.152, 'st1': 0.054, 'sc1': 0.027, 'standard': 0.114},
    'us-east-1': {'gp3': 0.080, 'gp2': 0.100, 'io1': 0.125, 'io2': 0.125, 'st1': 0.045, 'sc1': 0.025, 'standard': 0.050},
}

# Armazenamento RDS — USD/GB-mês por (região, storage_type, multi_az)
# Fonte: fatura fev/2026 + AWS Pricing (Março 2026)
PRECO_RDS_STORAGE = {
    'sa-east-1': {
        ('gp2', False): 0.219,   # Single-AZ gp2
        ('gp2', True):  0.437,   # Multi-AZ gp2
        ('gp3', False): 0.219,   # Single-AZ gp3
        ('gp3', True):  0.438,   # Multi-AZ gp3
        ('io1', False): 0.250,
        ('io1', True):  0.500,
    },
    'us-east-1': {
        ('gp2', False): 0.115,   # Single-AZ gp2
        ('gp2', True):  0.230,   # Multi-AZ gp2
        ('gp3', False): 0.092,
        ('gp3', True):  0.184,
        ('io1', False): 0.125,
        ('io1', True):  0.250,
    },
}

# Snapshots EBS — USD/GB-mês por região
# Fonte: fatura fev/2026
PRECO_EBS_SNAPSHOT_POR_REGIAO = {
    'sa-east-1': 0.068,
    'us-east-1': 0.050,
}

# Data Transfer OUT — USD/GB (saída para internet)
# NetworkIn é gratuito; NetworkOut cobrado por região
# Fonte: AWS Pricing (Março 2026)
PRECO_DT_OUT = {
    'sa-east-1': 0.138,  # primeiros 10 TB/mês
    'us-east-1': 0.090,  # primeiros 10 TB/mês
}

# Estimativa de memória por tipo (GB)
memoria_por_tipo = {
    't1.micro': 0.613,
    't2.micro': 1, 't2.small': 2, 't2.medium': 4, 't2.large': 8,'t2.xlarge': 16,
    't3.micro': 1, 't3.small': 2, 't3.medium': 4, 't3.large': 8,
    't3a.micro': 1, 't3a.small': 2, 't3a.medium': 4, 't3a.large': 8,
    't3.xlarge': 16, 't3.2xlarge': 32, 't3a.xlarge': 16, 't3a.2xlarge': 32,
    't2.2xlarge': 32, 'm5.large': 8, 'm5.xlarge': 16, 'm5.2xlarge': 32,
    'c5.large': 4, 'c5.xlarge': 8, 'c5.2xlarge': 16
}

# Mapeamento manual de domínios fixos por ID da instância
dominios_fixos = {
    'i-0e67f83e5d4c474f4': 'naoucde.logbit.com.br',
    'i-01c4ca6e99402d8e1': 'hmttlatam.logbit.com.br',
    'i-0de877e697985ec2b': 'www.logbit.com.br',
    'i-077a3180d100792ec': 'ourexcellencecup.coca-cola.com',
    'i-0336f03f4f7451092': 'smartcoolers.logbit.com.br',
    'i-026c6685e0945e46e': 'testewagtail.logbit.com.br',
    'i-0122a5ee1086ec5ba': 'e2ecoolers.logbit.com.br',
    'i-05d5bf916fbc01466': 'teste-master.logbit.com.br',
    'i-00576bef2ec1dd54c': 'naoucde.logbit.com.br',
    'i-0bef301b80d7c969f': 'simulador.logbit.com.br',
    'i-0a5943ded04200229': 'workload.logbit.com.br',
    'i-0dd8a669fae1abb15': 'sem acesso web',
    'i-0f05330f8db5081a5': 'sem acesso ssh',
    'i-082d4a0a6c0c73be1': 'sem acesso ssh',
    'i-04bf446a327e73219': 'sem acesso ssh',
}

# Mapeamento de domínios Route 53 por IP
# É necessário garantir que a sessão funcione antes de chamar o client Route 53
# Para evitar o erro ProfileNotFound, vamos inicializar o Route 53 DENTRO do bloco 'try/except'
# após confirmar que as credenciais estão OK. 
# Por enquanto, mantemos a inicialização aqui, mas o usuário deve resolver o erro ProfileNotFound
route53 = session.client('route53')
dominios_por_ip = {}
dominios_por_dns = {}

# OBS: Se o ProfileNotFound ocorrer, a linha abaixo falhará.
try:
    zonas = route53.list_hosted_zones()['HostedZones']
    for zona in zonas:
        zona_id = zona['Id'].split('/')[-1]
        registros = route53.list_resource_record_sets(HostedZoneId=zona_id)['ResourceRecordSets']
        for record in registros:
            if record['Type'] in ['A', 'CNAME']:
                for valor in record.get('ResourceRecords', []):
                    ip_ou_host = valor['Value']
                    dominios_por_ip[ip_ou_host] = record['Name'].rstrip('.')
                    dominios_por_dns[ip_ou_host.lower().rstrip('.')] = record['Name'].rstrip('.')
except Exception as e:
    print(f"Atenção: Não foi possível carregar as zonas do Route 53. Verifique as credenciais. Erro: {e}")


# Lista de dados
dados = []
dados_rds = []
dados_snapshots = []
HORAS_NO_MES = 24 * 30  # 720 horas por mês de 30 dias

# Obter dia atual e calcular proporção do mês
dia_atual = datetime.now().day
proporcao_mes = dia_atual / 30  # considerando mês com 30 dias
horas_referencia = dia_atual * 24  # horas acumuladas até hoje no mês

# ----------------------------------------------------
# 1. COLETAR DADOS EC2
# ----------------------------------------------------
for regiao in regioes:
    ec2 = session.client('ec2', region_name=regiao)
    response = ec2.describe_instances()

    for reservation in response['Reservations']:
        for instance in reservation['Instances']:
            estado = instance['State']['Name']
            instance_id = instance['InstanceId']
            tipo = instance['InstanceType']
            nome = next((tag['Value'] for tag in instance.get('Tags', []) if tag['Key'] == 'Name'), 'Sem Nome')
            host = next((tag['Value'] for tag in instance.get('Tags', []) if tag['Key'].lower() in ['host', 'dns']), '—')
            launch_time = instance.get('LaunchTime')
            zona = instance['Placement']['AvailabilityZone']
            ip = instance.get('PublicIpAddress') or instance.get('PrivateIpAddress') or '—'
            dns_publico = instance.get('PublicDnsName', '—') 
            dns_normalizado = dns_publico.lower().rstrip('.')
            
            # Determinar domínio
            if instance_id in dominios_fixos:
                dominio = dominios_fixos[instance_id]
            else:
                dominio = dominios_por_dns.get(dns_normalizado) or dominios_por_ip.get(ip) or '—'
            
            cpus = instance.get('CpuOptions', {}).get('CoreCount', '—')
            memoria = memoria_por_tipo.get(tipo, '—')

            # Armazenamento + custo EBS proporcional
            armazenamento = 0
            custo_ebs = 0.0
            for bd in instance.get('BlockDeviceMappings', []):
                volume_id = bd.get('Ebs', {}).get('VolumeId')
                if volume_id:
                    try:
                        volume = ec2.describe_volumes(VolumeIds=[volume_id])['Volumes'][0]
                        vol_size = volume['Size']
                        vol_type = volume.get('VolumeType', 'gp2')
                        armazenamento += vol_size
                        preco_ebs = PRECO_EBS_VOLUME.get(regiao, PRECO_EBS_VOLUME['us-east-1']).get(vol_type, 0.10)
                        custo_ebs += vol_size * preco_ebs * proporcao_mes
                    except Exception as e:
                        print(f'Erro ao buscar volume {volume_id}: {e}')
            custo_ebs = round(custo_ebs, 2)

            # Custo estimado proporcional ao dia do mês (compute + EBS)
            if estado == 'running':
                tabela_precos = precos_por_regiao.get(regiao, precos_por_tipo_sa_east_1)
                preco_hora = tabela_precos.get(tipo, precos_por_tipo_sa_east_1.get(tipo, 0.05))
                custo_compute = round(preco_hora * HORAS_NO_MES * proporcao_mes, 2)
                custo_mensal_estimado = round(custo_compute + custo_ebs, 2)
                uptime = datetime.now(timezone.utc) - launch_time
                uptime_str = str(uptime).split('.')[0]
            else:
                custo_compute = 0.0
                custo_mensal_estimado = custo_ebs  # volumes cobrados mesmo parados
                uptime_str = '—'

            dados.append({
                'ID DA INSTANCIA': instance_id,
                'NOME': nome,
                'ENDEREÇO IP': ip,
                'STATUS': 'Running' if estado == 'running' else 'Stopping',
                'TIPO': tipo,
                'Região': regiao,
                'ZONE': zona,
                'CPUs (cores)': cpus,
                'Memória (GB)': memoria,
                'Armazenamento (GB)': armazenamento,
                'Custo Compute Est. (USD)': custo_compute if estado == 'running' else 0.0,
                'Custo EBS Est. (USD)': custo_ebs,
                'Valor Mensal Estimado (USD)': custo_mensal_estimado,
                'Horas de Referência': horas_referencia if estado == 'running' else 0,
                'Uptime': uptime_str,
                'Domínio (Route 53)': dominio,
            })

# ----------------------------------------------------
# 2. COLETAR DADOS RDS (NOVA FUNCIONALIDADE)
# ----------------------------------------------------
for regiao in regioes:
    # Cria o cliente RDS para a região
    rds = session.client('rds', region_name=regiao)
    
    # Busca por todas as instâncias de DB na região
    try:
        response_rds = rds.describe_db_instances()
    except Exception as e:
        print(f"Erro ao buscar RDS na região {regiao}: {e}")
        continue

    for db_instance in response_rds.get('DBInstances', []):
        instance_id = db_instance['DBInstanceIdentifier']
        engine = db_instance['Engine']
        engine_version = db_instance['EngineVersion']
        tipo = db_instance['DBInstanceClass']
        status = db_instance['DBInstanceStatus']
        multi_az = 'Sim' if db_instance['MultiAZ'] else 'Não'
        storage_gb = db_instance['AllocatedStorage']
        storage_type = db_instance['StorageType']
        
        # Nome da instância (tenta buscar na Tag 'Name')
        nome = next((tag['Value'] for tag in db_instance.get('TagList', []) if tag['Key'] == 'Name'), 'Sem Nome')
        
        # Custo estimado do compute por hora
        preco_hora = (
            precos_rds_engine.get((regiao, tipo, engine)) or
            precos_rds_base.get(regiao, {}).get(tipo, 0.0)
        )

        # Custo Mensal Estimado de COMPUTE (proporcional ao dia do mês)
        if status == 'available':
            custo_compute_mensal = round(preco_hora * HORAS_NO_MES * proporcao_mes, 2)
        else:
            custo_compute_mensal = 0.0

        # Custo Mensal Estimado de STORAGE (por região, tipo e Multi-AZ, proporcional)
        _multi_az_bool = db_instance['MultiAZ']
        _storage_key   = (storage_type, _multi_az_bool)
        _preco_storage = PRECO_RDS_STORAGE.get(regiao, {}).get(_storage_key, 0.138)
        custo_storage_mensal = round(storage_gb * _preco_storage * proporcao_mes, 2)

        # Custo Total Mensal Estimado
        custo_total_mensal = round(custo_compute_mensal + custo_storage_mensal, 2)

        dados_rds.append({
            'ID DA INSTANCIA': instance_id,
            'NOME': nome,
            'STATUS': status,
            'TIPO': tipo,
            'ENGINE': engine,
            'VERSÃO': engine_version,
            'Região': regiao,
            'Multi-AZ': multi_az,
            'Armazenamento (GB)': storage_gb,
            'Tipo de Armazenamento': storage_type,
            'Custo Compute Est. (USD)': custo_compute_mensal,
            'Custo Storage Est. (USD)': custo_storage_mensal,
            'Valor Mensal Estimado (USD)': custo_total_mensal,
            'Horas de Referência': horas_referencia if status == 'available' else 0,
        })

# ----------------------------------------------------
# 2.5 COLETAR SNAPSHOTS EBS
# ----------------------------------------------------
_now = datetime.now()
_inicio_mes = datetime(_now.year, _now.month, 1)

for regiao in regioes:

    ec2 = session.client('ec2', region_name=regiao)

    try:
        paginator = ec2.get_paginator('describe_snapshots')
        pages = paginator.paginate(OwnerIds=['self'])
    except Exception as e:
        print(f"Erro ao buscar snapshots na região {regiao}: {e}")
        continue

    for page in pages:
        for snap in page.get('Snapshots', []):

            snapshot_id = snap['SnapshotId']
            volume_id = snap.get('VolumeId', '—')
            size_gb = snap.get('VolumeSize', 0)

            start_time = snap.get('StartTime')
            if start_time:
                start_time = start_time.replace(tzinfo=None)

            description = snap.get('Description', '—')

            # Horas que o snapshot existiu no mês atual
            if start_time:
                criacao_efetiva = max(start_time, _inicio_mes)
                horas_snap = int((_now - criacao_efetiva).total_seconds() / 3600)
            else:
                horas_snap = horas_referencia

            # Custo proporcional às horas de existência no mês (preço por região)
            preco_snap = PRECO_EBS_SNAPSHOT_POR_REGIAO.get(regiao, 0.05)
            custo_estimado = round(size_gb * preco_snap * (horas_snap / HORAS_NO_MES), 2)

            dados_snapshots.append({
                'Snapshot ID': snapshot_id,
                'Volume ID': volume_id,
                'Região': regiao,
                'Tamanho (GB)': size_gb,
                'Descrição': description,
                'Data Criação': start_time,
                'Custo Estimado (USD)': custo_estimado,
                'Horas de Referência': horas_snap,
            })

# ----------------------------------------------------
# 2.8 COLETAR DADOS DE DATA TRANSFER (Cost Explorer)
# ----------------------------------------------------
dados_dt = []
erro_dt = None

_prefixos_regiao = {
    'SAE1': 'sa-east-1', 'USE1': 'us-east-1', 'USE2': 'us-east-2',
    'USW1': 'us-west-1', 'USW2': 'us-west-2', 'APN1': 'ap-northeast-1',
    'APS1': 'ap-southeast-1', 'EUW1': 'eu-west-1', 'EUC1': 'eu-central-1',
}
_desc_uso = {
    'DataTransfer-Out-Bytes':          'Saida → Internet',
    'DataTransfer-In-Bytes':           'Entrada ← Internet',
    'DataTransfer-Regional-Bytes':     'Transferencia Inter-AZ',
    'AWS-Out-Bytes':                   'Saida → Servicos AWS',
    'DataTransfer-Out-AmazonSvc-Bytes':'Saida → Servicos AWS',
    'CloudFront-Out-Bytes':            'Saida → CloudFront',
    'DataTransfer-Out-AmazonCloudFront':'Saida → CloudFront',
}

def _parse_regiao(uso):
    prefixo = uso.split('-')[0]
    return _prefixos_regiao.get(prefixo, 'Global / Outro')

def _parse_tipo(uso):
    prefixo = uso.split('-')[0]
    chave = uso[len(prefixo) + 1:] if prefixo in _prefixos_regiao else uso
    return _desc_uso.get(chave, uso)

try:
    ce = session.client('ce', region_name='us-east-1')
    _start_dt = _inicio_mes.strftime('%Y-%m-%d')
    _end_dt   = (_now + timedelta(days=1)).strftime('%Y-%m-%d')

    _resp_dt = ce.get_cost_and_usage(
        TimePeriod={'Start': _start_dt, 'End': _end_dt},
        Granularity='MONTHLY',
        Filter={'Dimensions': {'Key': 'SERVICE', 'Values': ['AWS Data Transfer']}},
        GroupBy=[{'Type': 'DIMENSION', 'Key': 'USAGE_TYPE'}],
        Metrics=['BlendedCost', 'UsageQuantity'],
    )

    for group in _resp_dt['ResultsByTime'][0].get('Groups', []):
        uso      = group['Keys'][0]
        custo    = float(group['Metrics']['BlendedCost']['Amount'])
        qtd      = float(group['Metrics']['UsageQuantity']['Amount'])
        unidade  = group['Metrics']['UsageQuantity']['Unit']

        dados_dt.append({
            'Região':             _parse_regiao(uso),
            'Tipo de Transferência': _parse_tipo(uso),
            'Uso (Raw)':          uso,
            'Quantidade':         round(qtd, 4),
            'Unidade':            unidade,
            'Custo (USD)':        round(custo, 2),
            'Custo (BRL)':        round(custo * TAXA_CAMBIO, 2),
        })

except Exception as e:
    erro_dt = str(e)
    print(f'Aviso: Não foi possível coletar dados de Data Transfer via Cost Explorer: {e}')

df_dt = pd.DataFrame(dados_dt) if dados_dt else pd.DataFrame()

# ----------------------------------------------------
# 2.9 COLETAR MÉTRICAS DE REDE POR INSTÂNCIA (CloudWatch)
# ----------------------------------------------------
cw_lookup = {}  # {instance_id: {'in_gb': X, 'out_gb': X}}

for regiao in regioes:
    cw_client = session.client('cloudwatch', region_name=regiao)
    instancias_regiao = [d for d in dados if d['Região'] == regiao]

    for inst in instancias_regiao:
        instance_id = inst['ID DA INSTANCIA']
        _in_bytes  = 0
        _out_bytes = 0
        try:
            for metrica in ('NetworkIn', 'NetworkOut'):
                resp_cw = cw_client.get_metric_statistics(
                    Namespace='AWS/EC2',
                    MetricName=metrica,
                    Dimensions=[{'Name': 'InstanceId', 'Value': instance_id}],
                    StartTime=_inicio_mes,
                    EndTime=_now,
                    Period=86400,
                    Statistics=['Sum'],
                    Unit='Bytes',
                )
                total = sum(dp['Sum'] for dp in resp_cw['Datapoints'])
                if metrica == 'NetworkIn':
                    _in_bytes = total
                else:
                    _out_bytes = total
        except Exception as e:
            print(f'Erro CloudWatch {instance_id}: {e}')

        cw_lookup[instance_id] = {
            'in_gb':  round(_in_bytes  / (1024 ** 3), 4),
            'out_gb': round(_out_bytes / (1024 ** 3), 4),
        }

# ----------------------------------------------------
# 2.10 COLETAR DADOS S3 (CloudWatch + Cost Explorer)
# ----------------------------------------------------
_precos_s3 = {
    'sa-east-1': {
        'StandardStorage':             0.0208,
        'StandardIAStorage':           0.0138,
        'OneZoneIAStorage':            0.011,
        'GlacierStorage':              0.005,
        'DeepArchiveStorage':          0.002,
        'IntelligentTieringFAStorage': 0.0208,
    },
    'us-east-1': {
        'StandardStorage':             0.023,
        'StandardIAStorage':           0.0125,
        'OneZoneIAStorage':            0.01,
        'GlacierStorage':              0.004,
        'DeepArchiveStorage':          0.00099,
        'IntelligentTieringFAStorage': 0.023,
    },
}

_desc_s3_uso = {
    'TimedStorage-ByteHrs':             'Armazenamento Standard',
    'TimedStorage-INT-FA-ByteHrs':      'Intelligent-Tiering Frequent Access',
    'TimedStorage-INT-IA-ByteHrs':      'Intelligent-Tiering Infrequent Access',
    'TimedStorage-SIA-ByteHrs':         'Standard-IA Storage',
    'TimedStorage-ZIA-ByteHrs':         'One Zone-IA Storage',
    'TimedStorage-GlacierByteHrs':      'Glacier Flexible Retrieval',
    'TimedStorage-DeepArchive-ByteHrs': 'Glacier Deep Archive',
    'Requests-Tier1':                   'Requests PUT/COPY/POST/LIST',
    'Requests-Tier2':                   'Requests GET/SELECT',
    'Requests-Tier3':                   'Requests Lifecycle',
    'DataTransfer-Out-Bytes':           'Saida de Dados → Internet',
    'DataTransfer-In-Bytes':            'Entrada de Dados',
    'CloudFront-Out-Bytes':             'Saida → CloudFront',
}

def _desc_s3(uso):
    prefixo = uso.split('-')[0]
    chave = uso[len(prefixo) + 1:] if prefixo in _prefixos_regiao else uso
    return _desc_s3_uso.get(chave, uso)

dados_s3_buckets = []
dados_s3_ce      = []

# --- Buckets + CloudWatch ---
try:
    _s3_global   = session.client('s3', region_name='us-east-1')
    _todos_buckets = _s3_global.list_buckets().get('Buckets', [])
except Exception as e:
    _todos_buckets = []
    print(f'Erro ao listar buckets S3: {e}')

for _bucket in _todos_buckets:
    _bname    = _bucket['Name']
    _bcreated = _bucket['CreationDate'].replace(tzinfo=None)

    try:
        _loc      = _s3_global.get_bucket_location(Bucket=_bname)['LocationConstraint']
        _bregiao  = _loc if _loc else 'us-east-1'
    except Exception:
        continue

    if _bregiao not in regioes:
        continue

    _cw_s3   = session.client('cloudwatch', region_name=_bregiao)
    _size_gb  = 0.0
    _obj_count = 0

    try:
        _r_size = _cw_s3.get_metric_statistics(
            Namespace='AWS/S3', MetricName='BucketSizeBytes',
            Dimensions=[{'Name': 'BucketName', 'Value': _bname},
                        {'Name': 'StorageType', 'Value': 'AllStorageTypes'}],
            StartTime=_now - timedelta(days=3), EndTime=_now,
            Period=86400, Statistics=['Average'], Unit='Bytes',
        )
        if _r_size['Datapoints']:
            _latest  = max(_r_size['Datapoints'], key=lambda x: x['Timestamp'])
            _size_gb = round(_latest['Average'] / (1024 ** 3), 4)
    except Exception as e:
        print(f'Erro CloudWatch S3 size {_bname}: {e}')

    try:
        _r_count = _cw_s3.get_metric_statistics(
            Namespace='AWS/S3', MetricName='NumberOfObjects',
            Dimensions=[{'Name': 'BucketName', 'Value': _bname},
                        {'Name': 'StorageType', 'Value': 'AllStorageTypes'}],
            StartTime=_now - timedelta(days=3), EndTime=_now,
            Period=86400, Statistics=['Average'], Unit='Count',
        )
        if _r_count['Datapoints']:
            _latest    = max(_r_count['Datapoints'], key=lambda x: x['Timestamp'])
            _obj_count = int(_latest['Average'])
    except Exception as e:
        print(f'Erro CloudWatch S3 count {_bname}: {e}')

    _preco_gb  = _precos_s3.get(_bregiao, {}).get('StandardStorage', 0.023)
    _custo_est = round(_size_gb * _preco_gb, 2)

    dados_s3_buckets.append({
        'Bucket':                       _bname,
        'Região':                       _bregiao,
        'Criado em':                    _bcreated.strftime('%d/%m/%Y'),
        'Tamanho (GB)':                 _size_gb,
        'Objetos':                      _obj_count,
        'Custo Est. Storage (USD)':     _custo_est,
        'Custo Est. Storage (BRL)':     round(_custo_est * TAXA_CAMBIO, 2),
    })

# --- Cost Explorer S3 ---
try:
    _ce_s3     = session.client('ce', region_name='us-east-1')
    _resp_s3   = _ce_s3.get_cost_and_usage(
        TimePeriod={'Start': _inicio_mes.strftime('%Y-%m-%d'),
                    'End':   (_now + timedelta(days=1)).strftime('%Y-%m-%d')},
        Granularity='MONTHLY',
        Filter={'Dimensions': {'Key': 'SERVICE',
                               'Values': ['Amazon Simple Storage Service']}},
        GroupBy=[{'Type': 'DIMENSION', 'Key': 'USAGE_TYPE'}],
        Metrics=['BlendedCost', 'UsageQuantity'],
    )
    for _grp in _resp_s3['ResultsByTime'][0].get('Groups', []):
        _uso   = _grp['Keys'][0]
        _custo = float(_grp['Metrics']['BlendedCost']['Amount'])
        _qtd   = float(_grp['Metrics']['UsageQuantity']['Amount'])
        _unit  = _grp['Metrics']['UsageQuantity']['Unit']
        if _custo > 0 or _qtd > 0:
            dados_s3_ce.append({
                'Região':      _parse_regiao(_uso),
                'Tipo de Uso': _desc_s3(_uso),
                'Uso (Raw)':   _uso,
                'Quantidade':  round(_qtd, 4),
                'Unidade':     _unit,
                'Custo (USD)': round(_custo, 2),
                'Custo (BRL)': round(_custo * TAXA_CAMBIO, 2),
            })
except Exception as e:
    print(f'Erro ao coletar custos S3 do Cost Explorer: {e}')

df_s3_buckets = pd.DataFrame(dados_s3_buckets) if dados_s3_buckets else pd.DataFrame()
df_s3_ce      = pd.DataFrame(dados_s3_ce)      if dados_s3_ce      else pd.DataFrame()

# ----------------------------------------------------
# 3. TRATAMENTO DE DADOS E GERAÇÃO DE GRÁFICOS
# ----------------------------------------------------

# Criar DataFrame EC2
df = pd.DataFrame(dados)
df['NetworkIn (GB)']  = df['ID DA INSTANCIA'].map(lambda x: cw_lookup.get(x, {}).get('in_gb',  0))
df['NetworkOut (GB)'] = df['ID DA INSTANCIA'].map(lambda x: cw_lookup.get(x, {}).get('out_gb', 0))
df['STATUS'] = pd.Categorical(df['STATUS'], categories=['Running', 'Stopping'], ordered=True)
df = df.sort_values(by='STATUS')

# Criar DataFrame RDS
df_rds = pd.DataFrame(dados_rds)
df_rds['STATUS'] = df_rds['STATUS'].replace({'available': 'Available', 'stopped': 'Stopped', 'creating': 'Creating', 'modifying': 'Modifying'})
df_rds['STATUS'] = pd.Categorical(df_rds['STATUS'], categories=['Available', 'Stopped', 'Creating', 'Modifying'], ordered=True)
df_rds = df_rds.sort_values(by=['STATUS', 'Região'])

# Criar DataFrame Snapshots
df_snapshots = pd.DataFrame(dados_snapshots)

if not df_snapshots.empty:
    df_snapshots['Data Criação'] = pd.to_datetime(df_snapshots['Data Criação']).dt.tz_localize(None)

    total_snapshots_usd = df_snapshots['Custo Estimado (USD)'].sum()
    total_snapshots_brl = total_snapshots_usd * TAXA_CAMBIO

# Salvar CSV (apenas EC2)
data = datetime.now().strftime('%Y-%m-%d')
arquivo_csv = f'Relatório Infraestrutura AWS EC2 - Logbit-{data}.csv'
df.to_csv(arquivo_csv, index=False)

# Taxa de câmbio e totais
TAXA_CAMBIO = 5.18
total_ec2_usd = df['Valor Mensal Estimado (USD)'].sum()
total_rds_usd = df_rds['Valor Mensal Estimado (USD)'].sum()
total_snap_usd = df_snapshots['Custo Estimado (USD)'].sum() if not df_snapshots.empty else 0
total_usd = total_ec2_usd + total_rds_usd + total_snap_usd
total_brl = round(total_usd * TAXA_CAMBIO, 2)

# =====================================================
# GERAÇÃO DE GRÁFICOS
# =====================================================
PALETTE = ['#1F4E78', '#2E75B6', '#5BA3D9', '#9DC3E6', '#BDD7EE', '#70AD47', '#ED7D31', '#FFC000']
DPI = 100

sns.set_theme(style='whitegrid', font_scale=1.05)
plt.rcParams['axes.spines.top']   = False
plt.rcParams['axes.spines.right'] = False

def _salvar(nome):
    plt.tight_layout()
    plt.savefig(nome, dpi=DPI, bbox_inches='tight')
    plt.close()

# 1. Resumo geral de custos (EC2 / RDS / Snapshots)
g_resumo = 'g_resumo.png'
_labels_res = ['EC2', 'RDS', 'Snapshots EBS']
_vals_res   = [round(total_ec2_usd, 2), round(total_rds_usd, 2), round(total_snap_usd, 2)]
fig, ax = plt.subplots(figsize=(12, 3.5))
_bars = ax.barh(_labels_res, _vals_res, color=['#1F4E78', '#2E75B6', '#5BA3D9'], height=0.45, edgecolor='white')
for bar, val in zip(_bars, _vals_res):
    ax.text(bar.get_width() + max(_vals_res) * 0.01, bar.get_y() + bar.get_height() / 2,
            f'$ {val:,.2f}   |   R$ {val * TAXA_CAMBIO:,.2f}',
            va='center', fontsize=11, fontweight='bold', color='#1F4E78')
ax.set_xlim(0, max(_vals_res) * 1.45)
ax.set_title(f'Resumo de Custos Estimados — {data}', fontsize=14, fontweight='bold', pad=14)
ax.xaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f'$ {x:,.0f}'))
ax.set_xlabel('Custo Estimado (USD)', fontsize=11)
ax.grid(axis='x', linestyle='--', alpha=0.5)
ax.set_axisbelow(True)
_salvar(g_resumo)

# 2. EC2 por região e status
g_regiao_status = 'g_regiao_status.png'
fig, ax = plt.subplots(figsize=(10, 5))
sns.countplot(data=df, x='Região', hue='STATUS',
              palette={'Running': '#70AD47', 'Stopping': '#C00000'},
              ax=ax, edgecolor='white')
ax.set_title('Instâncias EC2 por Região e Status', fontsize=13, fontweight='bold', pad=12)
ax.set_xlabel('Região', fontsize=11)
ax.set_ylabel('Quantidade', fontsize=11)
for container in ax.containers:
    ax.bar_label(container, fontsize=10, padding=3)
ax.legend(title='Status', fontsize=10, title_fontsize=10)
_salvar(g_regiao_status)

# 3. Top 5 instâncias EC2 mais caras
g_top_ec2 = None
_df_running = df[df['STATUS'] == 'Running']
if not _df_running.empty:
    g_top_ec2 = 'g_top_ec2.png'
    _top = _df_running.nlargest(5, 'Valor Mensal Estimado (USD)').copy()
    _top['Label'] = _top['NOME'] + '\n(' + _top['TIPO'] + ')'
    _max_top = _top['Valor Mensal Estimado (USD)'].max()
    fig, ax = plt.subplots(figsize=(10, 5))
    _bars = ax.barh(_top['Label'], _top['Valor Mensal Estimado (USD)'],
                    color='#1F4E78', height=0.45, edgecolor='white')
    for bar, val in zip(_bars, _top['Valor Mensal Estimado (USD)']):
        ax.text(bar.get_width() + _max_top * 0.01, bar.get_y() + bar.get_height() / 2,
                f'$ {val:,.2f}', va='center', fontsize=10, color='#1F4E78')
    ax.set_xlim(0, _max_top * 1.35)
    ax.set_xlabel('Custo Estimado (USD)', fontsize=11)
    ax.set_title('Top 5 Instâncias EC2 por Custo', fontsize=13, fontweight='bold', pad=12)
    ax.xaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f'$ {x:,.0f}'))
    ax.invert_yaxis()
    ax.grid(axis='x', linestyle='--', alpha=0.5)
    ax.set_axisbelow(True)
    _salvar(g_top_ec2)

# 4. Custo EC2 por tipo (pizza)
g_custo_tipo_ec2 = None
_custo_tipo = df.groupby('TIPO')['Valor Mensal Estimado (USD)'].sum()
_custo_tipo = _custo_tipo[_custo_tipo > 0].sort_values(ascending=False)
if not _custo_tipo.empty:
    g_custo_tipo_ec2 = 'g_custo_tipo_ec2.png'
    _n = len(_custo_tipo)
    _cores = PALETTE[:_n] if _n <= len(PALETTE) else PALETTE * (_n // len(PALETTE) + 1)
    fig, ax = plt.subplots(figsize=(10, 5))
    wedges, texts, autotexts = ax.pie(
        _custo_tipo,
        labels=_custo_tipo.index,
        autopct=lambda p: f'{p:.1f}%\n($ {p * total_ec2_usd / 100:,.0f})',
        startangle=140,
        colors=_cores[:_n],
        pctdistance=0.78,
        wedgeprops={'edgecolor': 'white', 'linewidth': 2},
    )
    for t in autotexts:
        t.set_fontsize(8.5)
    ax.set_title('Distribuição de Custo EC2 por Tipo', fontsize=13, fontweight='bold', pad=12)
    _salvar(g_custo_tipo_ec2)

# 5. EC2 running por tipo (barras)
g_tipo_ligados = None
if not _df_running.empty:
    g_tipo_ligados = 'g_tipo_ligados.png'
    _order = _df_running['TIPO'].value_counts().index
    fig, ax = plt.subplots(figsize=(10, 5))
    sns.countplot(data=_df_running, x='TIPO', order=_order,
                  color='#2E75B6', ax=ax, edgecolor='white')
    ax.set_title('Instâncias EC2 Ligadas por Tipo', fontsize=13, fontweight='bold', pad=12)
    ax.set_xlabel('Tipo de Instância', fontsize=11)
    ax.set_ylabel('Quantidade', fontsize=11)
    ax.tick_params(axis='x', rotation=45)
    for container in ax.containers:
        ax.bar_label(container, fontsize=10, padding=3)
    _salvar(g_tipo_ligados)

# 6. Custo RDS por engine
g_rds_engine = None
_rds_avail = df_rds[df_rds['STATUS'] == 'Available']
if not _rds_avail.empty:
    g_rds_engine = 'g_rds_engine.png'
    _custo_eng = _rds_avail.groupby('ENGINE')['Valor Mensal Estimado (USD)'].sum().sort_values(ascending=False)
    _max_eng = _custo_eng.max()
    fig, ax = plt.subplots(figsize=(10, 5))
    _bars = ax.bar(_custo_eng.index, _custo_eng.values,
                   color=PALETTE[:len(_custo_eng)], width=0.45, edgecolor='white')
    for bar, val in zip(_bars, _custo_eng.values):
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + _max_eng * 0.02,
                f'$ {val:,.2f}', ha='center', fontsize=10, fontweight='bold', color='#1F4E78')
    ax.set_ylim(0, _max_eng * 1.25)
    ax.set_xlabel('Engine', fontsize=11)
    ax.set_ylabel('Custo Estimado (USD)', fontsize=11)
    ax.set_title('Custo RDS por Engine', fontsize=13, fontweight='bold', pad=12)
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f'$ {x:,.0f}'))
    ax.grid(axis='y', linestyle='--', alpha=0.5)
    ax.set_axisbelow(True)
    _salvar(g_rds_engine)

# 7. Custo estimado por região (EC2 vs RDS)
g_custo_regiao = 'g_custo_regiao.png'
_ec2_reg  = df.groupby('Região')['Valor Mensal Estimado (USD)'].sum()
_rds_reg  = df_rds.groupby('Região')['Valor Mensal Estimado (USD)'].sum()
_regioes_all = sorted(set(list(_ec2_reg.index) + list(_rds_reg.index)))
_ec2_vals = [_ec2_reg.get(r, 0) for r in _regioes_all]
_rds_vals = [_rds_reg.get(r, 0) for r in _regioes_all]
_x, _w = range(len(_regioes_all)), 0.35
fig, ax = plt.subplots(figsize=(10, 5))
_b1 = ax.bar([i - _w / 2 for i in _x], _ec2_vals, _w, label='EC2', color='#1F4E78', edgecolor='white')
_b2 = ax.bar([i + _w / 2 for i in _x], _rds_vals, _w, label='RDS', color='#2E75B6', edgecolor='white')
ax.bar_label(_b1, labels=[f'$ {v:,.0f}' for v in _ec2_vals], fontsize=9, padding=3)
ax.bar_label(_b2, labels=[f'$ {v:,.0f}' for v in _rds_vals], fontsize=9, padding=3)
ax.set_xticks(list(_x))
ax.set_xticklabels(_regioes_all, fontsize=11)
ax.set_ylabel('Custo Estimado (USD)', fontsize=11)
ax.set_title('Custo Estimado por Região (EC2 vs RDS)', fontsize=13, fontweight='bold', pad=12)
ax.legend(fontsize=10)
ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f'$ {x:,.0f}'))
ax.grid(axis='y', linestyle='--', alpha=0.5)
ax.set_axisbelow(True)
_salvar(g_custo_regiao)


# ----------------------------------------------------
# 3. COLETAR TODOS OS CUSTOS POR SERVIÇO (Cost Explorer)
# ----------------------------------------------------
_dados_gastos = []
_erro_gastos  = None

try:
    _ce_gastos = session.client('ce', region_name='us-east-1')
    _resp_gastos = _ce_gastos.get_cost_and_usage(
        TimePeriod={'Start': _inicio_mes.strftime('%Y-%m-%d'),
                    'End':   (_now + timedelta(days=1)).strftime('%Y-%m-%d')},
        Granularity='MONTHLY',
        GroupBy=[{'Type': 'DIMENSION', 'Key': 'SERVICE'}],
        Metrics=['BlendedCost'],
    )
    for group in _resp_gastos['ResultsByTime'][0].get('Groups', []):
        _servico = group['Keys'][0]
        _custo   = float(group['Metrics']['BlendedCost']['Amount'])
        if _custo > 0:
            _dados_gastos.append({
                'Serviço': _servico,
                'USD':     round(_custo, 2),
                'BRL':     round(_custo * TAXA_CAMBIO, 2),
            })
except Exception as e:
    _erro_gastos = str(e)
    print(f'Aviso: Nao foi possivel coletar custos por servico via Cost Explorer: {e}')

df_gastos = pd.DataFrame(_dados_gastos) if _dados_gastos else pd.DataFrame(
    columns=['Serviço', 'USD', 'BRL']
)

# ----------------------------------------------------
# 4. EXPORTAR PARA EXCEL
# ----------------------------------------------------
excel_path = f'Relatório Infraestrutura AWS EC2 e RDS - Logbit-{data}.xlsx'

with ExcelWriter(excel_path, engine='xlsxwriter') as writer:

    workbook = writer.book

    header_format = workbook.add_format({'bold': True, 'bg_color': '#1F4E78', 'font_color': 'white', 'align': 'center', 'border': 1})
    ligado_format = workbook.add_format({'bg_color': '#C6EFCE', 'font_color': '#006100', 'align': 'center', 'border': 1})
    desligado_format = workbook.add_format({'bg_color': '#FFC7CE', 'font_color': '#9C0006', 'align': 'center', 'border': 1})
    center_format = workbook.add_format({'align': 'center', 'border': 1})
    total_format = workbook.add_format({'bold': True, 'bg_color': '#D9E1F2', 'font_color': '#1F4E78', 'align': 'center', 'border': 1})

# ------------------------------------------------
# ABA EC2
# ------------------------------------------------
    df.to_excel(writer, sheet_name='Instancias EC2', index=False)
    instancias_ws = writer.sheets['Instancias EC2']

# Cabeçalho formatado
    for col_num, value in enumerate(df.columns.values):
        instancias_ws.write(0, col_num, value, header_format)

# Escrever linhas com formatação de STATUS
    for row_num, row_data in enumerate(df.itertuples(index=False), start=1):
        for col_num, value in enumerate(row_data):

            col_name = df.columns[col_num]

            if col_name == 'STATUS':
                if value == 'Running':
                    formato = ligado_format
                elif value in ['Stopped', 'Stopping']:
                    formato = desligado_format
                else:
                    formato = center_format
            else:
                 formato = center_format

            instancias_ws.write(row_num, col_num, value, formato)

# Auto ajuste de largura das colunas
    for i, col in enumerate(df.columns):
        max_len = max(
            df[col].astype(str).map(len).max(),
            len(col)
        ) + 2
        instancias_ws.set_column(i, i, max_len)

    instancias_ws.freeze_panes(1, 0)

# Totais
    linha_total = len(df) + 2

    instancias_ws.write(linha_total, 0, 'TOTAL MENSAL EC2 (USD)', total_format)
    instancias_ws.write(linha_total, 1, total_ec2_usd, center_format)

    instancias_ws.write(linha_total + 1, 0, 'TOTAL MENSAL EC2 (BRL)', total_format)
    instancias_ws.write(linha_total + 1, 1, round(total_ec2_usd * TAXA_CAMBIO, 2), center_format)

    # ------------------------------------------------
    # ABA RDS
    # ------------------------------------------------
    df_rds.to_excel(writer, sheet_name='Instancias RDS', index=False)
    rds_ws = writer.sheets['Instancias RDS']

    for col_num, value in enumerate(df_rds.columns.values):
        rds_ws.write(0, col_num, value, header_format)

    for row_num, row_data in enumerate(df_rds.itertuples(index=False), start=1):
        for col_num, value in enumerate(row_data):
            col_name = df_rds.columns[col_num]
            formato = ligado_format if col_name == 'STATUS' and value == 'Available' else center_format
            rds_ws.write(row_num, col_num, value, formato)

    for i, col in enumerate(df_rds.columns):
        max_len = max(df_rds[col].astype(str).map(len).max(), len(col))
        rds_ws.set_column(i, i, max_len + 2)

    rds_ws.freeze_panes(1, 0)

    linha_total_rds = len(df_rds) + 2
    rds_ws.write(linha_total_rds, 0, 'TOTAL MENSAL RDS (USD)', total_format)
    rds_ws.write(linha_total_rds, 1, total_rds_usd, center_format)

    rds_ws.write(linha_total_rds + 1, 0, 'TOTAL MENSAL RDS (BRL)', total_format)
    rds_ws.write(linha_total_rds + 1, 1, round(total_rds_usd * TAXA_CAMBIO, 2), center_format)

    # ------------------------------------------------
    # ABA SNAPSHOTS
    # ------------------------------------------------
    if not df_snapshots.empty:

        df_snapshots.to_excel(writer, sheet_name='Snapshots EBS', index=False)
        snap_ws = writer.sheets['Snapshots EBS']

        for col_num, value in enumerate(df_snapshots.columns.values):
            snap_ws.write(0, col_num, value, header_format)

        for i, col in enumerate(df_snapshots.columns):
            max_len = max(df_snapshots[col].astype(str).map(len).max(), len(col))
            snap_ws.set_column(i, i, max_len + 2)

        date_format = workbook.add_format({'num_format': 'yyyy-mm-dd hh:mm:ss'})
        snap_ws.set_column('F:F', 22, date_format)

        linha_total_snap = len(df_snapshots) + 2

        snap_ws.write(linha_total_snap, 0, 'TOTAL MENSAL SNAPSHOTS (USD)', total_format)
        snap_ws.write(linha_total_snap, 1, round(total_snapshots_usd, 2), center_format)

        snap_ws.write(linha_total_snap + 1, 0, 'TOTAL MENSAL SNAPSHOTS (BRL)', total_format)
        snap_ws.write(linha_total_snap + 1, 1, round(total_snapshots_brl, 2), center_format)

        snap_ws.freeze_panes(1, 0)

    # ------------------------------------------------
    # ABA DATA TRANSFER
    # ------------------------------------------------
    dt_ws = workbook.add_worksheet('Data Transfer')

    dt_titulo_fmt = workbook.add_format({
        'bold': True, 'font_size': 15, 'font_color': 'white',
        'bg_color': '#1F4E78', 'align': 'center', 'valign': 'vcenter',
    })
    dt_sub_fmt = workbook.add_format({
        'italic': True, 'font_size': 10, 'font_color': '#595959',
        'bg_color': '#D9E1F2', 'align': 'center',
    })
    dt_sec_fmt = workbook.add_format({
        'bold': True, 'font_size': 11, 'font_color': 'white',
        'bg_color': '#2E75B6', 'align': 'left', 'valign': 'vcenter', 'indent': 1,
    })
    dt_hdr_fmt = workbook.add_format({
        'bold': True, 'font_color': 'white', 'bg_color': '#1F4E78',
        'align': 'center', 'valign': 'vcenter', 'border': 1,
    })
    dt_row_a_txt = workbook.add_format({'align': 'left',   'border': 1, 'bg_color': '#EBF3FB', 'indent': 1})
    dt_row_b_txt = workbook.add_format({'align': 'left',   'border': 1, 'bg_color': '#FFFFFF',  'indent': 1})
    dt_row_a_num = workbook.add_format({'align': 'center', 'border': 1, 'bg_color': '#EBF3FB', 'num_format': '#,##0.0000'})
    dt_row_b_num = workbook.add_format({'align': 'center', 'border': 1, 'bg_color': '#FFFFFF',  'num_format': '#,##0.0000'})
    dt_row_a_usd = workbook.add_format({'align': 'center', 'border': 1, 'bg_color': '#EBF3FB', 'num_format': '"$ "#,##0.00'})
    dt_row_b_usd = workbook.add_format({'align': 'center', 'border': 1, 'bg_color': '#FFFFFF',  'num_format': '"$ "#,##0.00'})
    dt_row_a_brl = workbook.add_format({'align': 'center', 'border': 1, 'bg_color': '#EBF3FB', 'num_format': '"R$ "#,##0.00'})
    dt_row_b_brl = workbook.add_format({'align': 'center', 'border': 1, 'bg_color': '#FFFFFF',  'num_format': '"R$ "#,##0.00'})
    dt_tot_lbl   = workbook.add_format({'bold': True, 'font_color': 'white',   'bg_color': '#1F4E78', 'align': 'left',   'border': 1, 'indent': 1})
    dt_tot_usd   = workbook.add_format({'bold': True, 'font_color': '#1F4E78', 'bg_color': '#D9E1F2', 'align': 'center', 'border': 1, 'num_format': '"$ "#,##0.00'})
    dt_tot_brl   = workbook.add_format({'bold': True, 'font_color': 'white',   'bg_color': '#1F4E78', 'align': 'center', 'border': 1, 'num_format': '"R$ "#,##0.00'})
    dt_tot_gb    = workbook.add_format({'bold': True, 'font_color': '#1F4E78', 'bg_color': '#D9E1F2', 'align': 'center', 'border': 1, 'num_format': '#,##0.0000'})
    dt_note_fmt  = workbook.add_format({'italic': True, 'font_size': 9, 'font_color': '#595959'})
    dt_erro_fmt  = workbook.add_format({'bold': True, 'font_color': '#C00000', 'font_size': 11})

    dt_ws.set_column('A:A', 28)
    dt_ws.set_column('B:B', 36)
    dt_ws.set_column('C:C', 14)
    dt_ws.set_column('D:D', 10)
    dt_ws.set_column('E:E', 16)
    dt_ws.set_column('F:F', 18)
    dt_ws.set_column('G:G', 20)

    # Título
    dt_ws.set_row(0, 30)
    dt_ws.merge_range(0, 0, 0, 6, 'Data Transfer — Custos por Regiao e Tipo', dt_titulo_fmt)
    dt_ws.set_row(1, 16)
    dt_ws.merge_range(1, 0, 1, 6,
        f'Periodo: {_inicio_mes.strftime("%d/%m/%Y")} a {_now.strftime("%d/%m/%Y")}   |   Fonte: AWS Cost Explorer   |   Cambio: R$ {TAXA_CAMBIO:.2f}/USD',
        dt_sub_fmt)

    _linha_cw = 7  # row where CW section starts (default)

    if erro_dt and df_dt.empty:
        dt_ws.write(3, 0, f'Erro ao coletar dados: {erro_dt}', dt_erro_fmt)
        dt_ws.merge_range(4, 0, 4, 5,
            'Verifique se o usuario IAM possui permissao ce:GetCostAndUsage.', dt_note_fmt)
        _linha_cw = 7
    elif df_dt.empty:
        dt_ws.write(3, 0, 'Nenhum custo de Data Transfer encontrado para o periodo.', dt_note_fmt)
        _linha_cw = 6
    else:
        _df_dt_sorted = df_dt.sort_values(['Região', 'Custo (USD)'], ascending=[True, False]).reset_index(drop=True)
        _regioes_dt   = _df_dt_sorted['Região'].unique()
        _linha_atual  = 3

        for regiao in _regioes_dt:
            _grupo = _df_dt_sorted[_df_dt_sorted['Região'] == regiao].reset_index(drop=True)

            dt_ws.set_row(_linha_atual, 20)
            dt_ws.merge_range(_linha_atual, 0, _linha_atual, 5, f'REGIAO: {regiao.upper()}', dt_sec_fmt)
            _linha_atual += 1

            dt_ws.set_row(_linha_atual, 18)
            for c, h in enumerate(['Regiao', 'Tipo de Transferencia', 'Quantidade', 'Unidade', 'Custo (USD)', 'Custo (BRL)']):
                dt_ws.write(_linha_atual, c, h, dt_hdr_fmt)
            _linha_atual += 1

            for i, row in _grupo.iterrows():
                dt_ws.set_row(_linha_atual, 17)
                _alt = i % 2 == 0
                dt_ws.write(_linha_atual, 0, row['Região'],                dt_row_a_txt if _alt else dt_row_b_txt)
                dt_ws.write(_linha_atual, 1, row['Tipo de Transferência'], dt_row_a_txt if _alt else dt_row_b_txt)
                dt_ws.write(_linha_atual, 2, row['Quantidade'],            dt_row_a_num if _alt else dt_row_b_num)
                dt_ws.write(_linha_atual, 3, row['Unidade'],               dt_row_a_txt if _alt else dt_row_b_txt)
                dt_ws.write(_linha_atual, 4, row['Custo (USD)'],           dt_row_a_usd if _alt else dt_row_b_usd)
                dt_ws.write(_linha_atual, 5, row['Custo (BRL)'],           dt_row_a_brl if _alt else dt_row_b_brl)
                _linha_atual += 1

            _sub_usd = _grupo['Custo (USD)'].sum()
            _sub_brl = _grupo['Custo (BRL)'].sum()
            dt_ws.set_row(_linha_atual, 18)
            dt_ws.merge_range(_linha_atual, 0, _linha_atual, 3, f'Subtotal — {regiao}', dt_tot_lbl)
            dt_ws.write(_linha_atual, 4, round(_sub_usd, 2), dt_tot_usd)
            dt_ws.write(_linha_atual, 5, round(_sub_brl, 2), dt_tot_brl)
            _linha_atual += 2

        _total_dt_usd = df_dt['Custo (USD)'].sum()
        _total_dt_brl = df_dt['Custo (BRL)'].sum()
        dt_ws.set_row(_linha_atual, 20)
        dt_ws.merge_range(_linha_atual, 0, _linha_atual, 3, 'TOTAL GERAL Data Transfer', dt_tot_lbl)
        dt_ws.write(_linha_atual, 4, round(_total_dt_usd, 2), dt_tot_usd)
        dt_ws.write(_linha_atual, 5, round(_total_dt_brl, 2), dt_tot_brl)

        dt_ws.merge_range(_linha_atual + 2, 0, _linha_atual + 2, 6,
            '* Dados com latencia de 24-48h. Custos via Cost Explorer (acima). Volume por instancia na secao CloudWatch abaixo.',
            dt_note_fmt)

        _linha_cw = _linha_atual + 5

    # ---- Seção CloudWatch: tráfego por instância ----
    if cw_lookup:
        dt_ws.set_row(_linha_cw, 20)
        dt_ws.merge_range(_linha_cw, 0, _linha_cw, 6,
            'TRAFEGO DE REDE POR INSTANCIA — CloudWatch | Volume acumulado + Estimativa de custo (NetworkOut)',
            dt_sec_fmt)

        _cw_hdr = _linha_cw + 1
        dt_ws.set_row(_cw_hdr, 18)
        for c, h in enumerate(['Instancia', 'Tipo', 'Regiao', 'NetworkIn (GB)', 'NetworkOut (GB)', 'Total (GB)', 'Custo Est. Out (USD)']):
            dt_ws.write(_cw_hdr, c, h, dt_hdr_fmt)

        _cw_rows = []
        for item in dados:
            _iid = item['ID DA INSTANCIA']
            if _iid in cw_lookup:
                _cw  = cw_lookup[_iid]
                _reg = item['Região']
                _out = _cw['out_gb']
                _custo_out = round(_out * PRECO_DT_OUT.get(_reg, 0.09), 2)
                _cw_rows.append({
                    'nome':      item['NOME'],
                    'tipo':      item['TIPO'],
                    'regiao':    _reg,
                    'in_gb':     _cw['in_gb'],
                    'out_gb':    _out,
                    'total_gb':  round(_cw['in_gb'] + _out, 4),
                    'custo_out': _custo_out,
                })
        _cw_rows.sort(key=lambda x: x['total_gb'], reverse=True)

        for i, cw_item in enumerate(_cw_rows):
            _r = _cw_hdr + 1 + i
            dt_ws.set_row(_r, 17)
            _alt = i % 2 == 0
            dt_ws.write(_r, 0, cw_item['nome'],      dt_row_a_txt if _alt else dt_row_b_txt)
            dt_ws.write(_r, 1, cw_item['tipo'],      dt_row_a_txt if _alt else dt_row_b_txt)
            dt_ws.write(_r, 2, cw_item['regiao'],    dt_row_a_txt if _alt else dt_row_b_txt)
            dt_ws.write(_r, 3, cw_item['in_gb'],     dt_row_a_num if _alt else dt_row_b_num)
            dt_ws.write(_r, 4, cw_item['out_gb'],    dt_row_a_num if _alt else dt_row_b_num)
            dt_ws.write(_r, 5, cw_item['total_gb'],  dt_row_a_num if _alt else dt_row_b_num)
            dt_ws.write(_r, 6, cw_item['custo_out'], dt_row_a_usd if _alt else dt_row_b_usd)

        _r_tot_cw = _cw_hdr + 1 + len(_cw_rows)
        dt_ws.set_row(_r_tot_cw, 18)
        dt_ws.merge_range(_r_tot_cw, 0, _r_tot_cw, 2, 'TOTAL', dt_tot_lbl)
        dt_ws.write(_r_tot_cw, 3, round(sum(r['in_gb']     for r in _cw_rows), 4), dt_tot_gb)
        dt_ws.write(_r_tot_cw, 4, round(sum(r['out_gb']    for r in _cw_rows), 4), dt_tot_gb)
        dt_ws.write(_r_tot_cw, 5, round(sum(r['total_gb']  for r in _cw_rows), 4), dt_tot_gb)
        dt_ws.write(_r_tot_cw, 6, round(sum(r['custo_out'] for r in _cw_rows), 2), dt_tot_usd)

        dt_ws.merge_range(_r_tot_cw + 1, 0, _r_tot_cw + 1, 6,
            f'* NetworkIn = gratuito ($0.00/GB). NetworkOut cobrado por regiao: SA=$0.138/GB, US=$0.090/GB. '
            f'Acumulado de {_inicio_mes.strftime("%d/%m/%Y")} a {_now.strftime("%d/%m/%Y")}.',
            dt_note_fmt)

    dt_ws.freeze_panes(2, 0)

    # ------------------------------------------------
    # ABA S3
    # ------------------------------------------------
    s3_ws = workbook.add_worksheet('S3')

    s3_tit_fmt   = workbook.add_format({'bold': True, 'font_size': 15, 'font_color': 'white',   'bg_color': '#1F4E78', 'align': 'center', 'valign': 'vcenter'})
    s3_sub_fmt   = workbook.add_format({'italic': True, 'font_size': 10, 'font_color': '#595959', 'bg_color': '#D9E1F2', 'align': 'center'})
    s3_sec_fmt   = workbook.add_format({'bold': True, 'font_size': 11, 'font_color': 'white',   'bg_color': '#2E75B6', 'align': 'left', 'valign': 'vcenter', 'indent': 1})
    s3_hdr_fmt   = workbook.add_format({'bold': True, 'font_color': 'white', 'bg_color': '#1F4E78', 'align': 'center', 'valign': 'vcenter', 'border': 1})
    s3_ra_txt    = workbook.add_format({'align': 'left',   'border': 1, 'bg_color': '#EBF3FB', 'indent': 1})
    s3_rb_txt    = workbook.add_format({'align': 'left',   'border': 1, 'bg_color': '#FFFFFF',  'indent': 1})
    s3_ra_num    = workbook.add_format({'align': 'center', 'border': 1, 'bg_color': '#EBF3FB', 'num_format': '#,##0.0000'})
    s3_rb_num    = workbook.add_format({'align': 'center', 'border': 1, 'bg_color': '#FFFFFF',  'num_format': '#,##0.0000'})
    s3_ra_int    = workbook.add_format({'align': 'center', 'border': 1, 'bg_color': '#EBF3FB', 'num_format': '#,##0'})
    s3_rb_int    = workbook.add_format({'align': 'center', 'border': 1, 'bg_color': '#FFFFFF',  'num_format': '#,##0'})
    s3_ra_usd    = workbook.add_format({'align': 'center', 'border': 1, 'bg_color': '#EBF3FB', 'num_format': '"$ "#,##0.00'})
    s3_rb_usd    = workbook.add_format({'align': 'center', 'border': 1, 'bg_color': '#FFFFFF',  'num_format': '"$ "#,##0.00'})
    s3_ra_brl    = workbook.add_format({'align': 'center', 'border': 1, 'bg_color': '#EBF3FB', 'num_format': '"R$ "#,##0.00'})
    s3_rb_brl    = workbook.add_format({'align': 'center', 'border': 1, 'bg_color': '#FFFFFF',  'num_format': '"R$ "#,##0.00'})
    s3_tot_lbl   = workbook.add_format({'bold': True, 'font_color': 'white',   'bg_color': '#1F4E78', 'align': 'left',   'border': 1, 'indent': 1})
    s3_tot_usd   = workbook.add_format({'bold': True, 'font_color': '#1F4E78', 'bg_color': '#D9E1F2', 'align': 'center', 'border': 1, 'num_format': '"$ "#,##0.00'})
    s3_tot_brl   = workbook.add_format({'bold': True, 'font_color': 'white',   'bg_color': '#1F4E78', 'align': 'center', 'border': 1, 'num_format': '"R$ "#,##0.00'})
    s3_tot_gb    = workbook.add_format({'bold': True, 'font_color': '#1F4E78', 'bg_color': '#D9E1F2', 'align': 'center', 'border': 1, 'num_format': '#,##0.0000'})
    s3_tot_int   = workbook.add_format({'bold': True, 'font_color': '#1F4E78', 'bg_color': '#D9E1F2', 'align': 'center', 'border': 1, 'num_format': '#,##0'})
    s3_note_fmt  = workbook.add_format({'italic': True, 'font_size': 9, 'font_color': '#595959'})

    s3_ws.set_column('A:A', 40)
    s3_ws.set_column('B:B', 14)
    s3_ws.set_column('C:C', 14)
    s3_ws.set_column('D:D', 14)
    s3_ws.set_column('E:E', 14)
    s3_ws.set_column('F:F', 16)
    s3_ws.set_column('G:G', 16)
    _S3NC = 6  # índice da última coluna (0-based)

    s3_ws.set_row(0, 30)
    s3_ws.merge_range(0, 0, 0, _S3NC, 'Amazon S3 — Buckets e Custos', s3_tit_fmt)
    s3_ws.set_row(1, 16)
    s3_ws.merge_range(1, 0, 1, _S3NC,
        f'Periodo: {_inicio_mes.strftime("%d/%m/%Y")} a {_now.strftime("%d/%m/%Y")}   |   '
        f'Regioes: {", ".join(regioes)}   |   Cambio: R$ {TAXA_CAMBIO:.2f}/USD', s3_sub_fmt)

    _s3r = 3

    # ---- Seção 1: Buckets (CloudWatch) ----
    s3_ws.set_row(_s3r, 20)
    s3_ws.merge_range(_s3r, 0, _s3r, _S3NC,
        'BUCKETS POR REGIAO — Tamanho e Custo Estimado de Storage (CloudWatch)', s3_sec_fmt)
    _s3r += 1

    s3_ws.set_row(_s3r, 18)
    for c, h in enumerate(['Bucket', 'Regiao', 'Criado em', 'Tamanho (GB)', 'Objetos', 'Custo Est. (USD)', 'Custo Est. (BRL)']):
        s3_ws.write(_s3r, c, h, s3_hdr_fmt)
    _s3r += 1

    if df_s3_buckets.empty:
        s3_ws.merge_range(_s3r, 0, _s3r, _S3NC,
            'Nenhum bucket encontrado nas regioes monitoradas. Verifique permissao s3:ListAllMyBuckets.', s3_note_fmt)
        _s3r += 2
    else:
        _df_s3_ord = df_s3_buckets.sort_values(['Região', 'Tamanho (GB)'], ascending=[True, False]).reset_index(drop=True)
        for _reg_s3 in _df_s3_ord['Região'].unique():
            _grp_s3 = _df_s3_ord[_df_s3_ord['Região'] == _reg_s3].reset_index(drop=True)
            for i, row in _grp_s3.iterrows():
                s3_ws.set_row(_s3r, 17)
                _a = i % 2 == 0
                s3_ws.write(_s3r, 0, row['Bucket'],                   s3_ra_txt if _a else s3_rb_txt)
                s3_ws.write(_s3r, 1, row['Região'],                   s3_ra_txt if _a else s3_rb_txt)
                s3_ws.write(_s3r, 2, row['Criado em'],                s3_ra_txt if _a else s3_rb_txt)
                s3_ws.write(_s3r, 3, row['Tamanho (GB)'],             s3_ra_num if _a else s3_rb_num)
                s3_ws.write(_s3r, 4, row['Objetos'],                  s3_ra_int if _a else s3_rb_int)
                s3_ws.write(_s3r, 5, row['Custo Est. Storage (USD)'], s3_ra_usd if _a else s3_rb_usd)
                s3_ws.write(_s3r, 6, row['Custo Est. Storage (BRL)'], s3_ra_brl if _a else s3_rb_brl)
                _s3r += 1
            s3_ws.set_row(_s3r, 18)
            s3_ws.merge_range(_s3r, 0, _s3r, 2, f'Subtotal — {_reg_s3}', s3_tot_lbl)
            s3_ws.write(_s3r, 3, round(_grp_s3['Tamanho (GB)'].sum(), 4),              s3_tot_gb)
            s3_ws.write(_s3r, 4, int(_grp_s3['Objetos'].sum()),                        s3_tot_int)
            s3_ws.write(_s3r, 5, round(_grp_s3['Custo Est. Storage (USD)'].sum(), 2),  s3_tot_usd)
            s3_ws.write(_s3r, 6, round(_grp_s3['Custo Est. Storage (BRL)'].sum(), 2),  s3_tot_brl)
            _s3r += 2

        s3_ws.set_row(_s3r, 18)
        s3_ws.merge_range(_s3r, 0, _s3r, 2, 'TOTAL GERAL — Storage Estimado', s3_tot_lbl)
        s3_ws.write(_s3r, 3, round(df_s3_buckets['Tamanho (GB)'].sum(), 4),              s3_tot_gb)
        s3_ws.write(_s3r, 4, int(df_s3_buckets['Objetos'].sum()),                        s3_tot_int)
        s3_ws.write(_s3r, 5, round(df_s3_buckets['Custo Est. Storage (USD)'].sum(), 2),  s3_tot_usd)
        s3_ws.write(_s3r, 6, round(df_s3_buckets['Custo Est. Storage (BRL)'].sum(), 2),  s3_tot_brl)
        _s3r += 1
        s3_ws.merge_range(_s3r, 0, _s3r, _S3NC,
            '* Tamanho e contagem via CloudWatch (latencia de 24-48h). Custo estimado apenas para Standard Storage.', s3_note_fmt)
        _s3r += 2

    # ---- Seção 2: Cost Explorer S3 ----
    _s3r += 1
    s3_ws.set_row(_s3r, 20)
    s3_ws.merge_range(_s3r, 0, _s3r, _S3NC,
        'CUSTOS S3 — AWS Cost Explorer (mes atual, todos os tipos de uso)', s3_sec_fmt)
    _s3r += 1

    s3_ws.set_row(_s3r, 18)
    for c, h in enumerate(['Regiao', 'Tipo de Uso', 'Quantidade', 'Unidade', 'Custo (USD)', 'Custo (BRL)', '']):
        s3_ws.write(_s3r, c, h, s3_hdr_fmt)
    _s3r += 1

    if df_s3_ce.empty:
        s3_ws.merge_range(_s3r, 0, _s3r, _S3NC,
            'Nenhum custo S3 encontrado. Verifique permissao ce:GetCostAndUsage.', s3_note_fmt)
        _s3r += 2
    else:
        _df_ce_ord = df_s3_ce.sort_values(['Região', 'Custo (USD)'], ascending=[True, False]).reset_index(drop=True)
        for _reg_ce in _df_ce_ord['Região'].unique():
            _grp_ce = _df_ce_ord[_df_ce_ord['Região'] == _reg_ce].reset_index(drop=True)
            for i, row in _grp_ce.iterrows():
                s3_ws.set_row(_s3r, 17)
                _a = i % 2 == 0
                s3_ws.write(_s3r, 0, row['Região'],      s3_ra_txt if _a else s3_rb_txt)
                s3_ws.write(_s3r, 1, row['Tipo de Uso'], s3_ra_txt if _a else s3_rb_txt)
                s3_ws.write(_s3r, 2, row['Quantidade'],  s3_ra_num if _a else s3_rb_num)
                s3_ws.write(_s3r, 3, row['Unidade'],     s3_ra_txt if _a else s3_rb_txt)
                s3_ws.write(_s3r, 4, row['Custo (USD)'], s3_ra_usd if _a else s3_rb_usd)
                s3_ws.write(_s3r, 5, row['Custo (BRL)'], s3_ra_brl if _a else s3_rb_brl)
                _s3r += 1
            s3_ws.set_row(_s3r, 18)
            s3_ws.merge_range(_s3r, 0, _s3r, 3, f'Subtotal — {_reg_ce}', s3_tot_lbl)
            s3_ws.write(_s3r, 4, round(_grp_ce['Custo (USD)'].sum(), 2), s3_tot_usd)
            s3_ws.write(_s3r, 5, round(_grp_ce['Custo (BRL)'].sum(), 2), s3_tot_brl)
            _s3r += 2

        _tot_s3_usd = df_s3_ce['Custo (USD)'].sum()
        _tot_s3_brl = df_s3_ce['Custo (BRL)'].sum()
        s3_ws.set_row(_s3r, 20)
        s3_ws.merge_range(_s3r, 0, _s3r, 3, 'TOTAL GERAL S3 (Cost Explorer)', s3_tot_lbl)
        s3_ws.write(_s3r, 4, round(_tot_s3_usd, 2), s3_tot_usd)
        s3_ws.write(_s3r, 5, round(_tot_s3_brl, 2), s3_tot_brl)
        _s3r += 2
        s3_ws.merge_range(_s3r, 0, _s3r, _S3NC,
            '* Dados com latencia de 24-48h. Inclui storage, requests e data transfer do S3. '
            'O custo de storage estimado (secao 1) pode divergir pois considera apenas Standard Storage.',
            s3_note_fmt)

    s3_ws.freeze_panes(2, 0)

    # ------------------------------------------------
    # ABA GRAFICOS
    # ------------------------------------------------
    worksheet = workbook.add_worksheet('Graficos')

    # Formatos da aba
    titulo_fmt = workbook.add_format({
        'bold': True, 'font_size': 16, 'font_color': 'white',
        'bg_color': '#1F4E78', 'align': 'center', 'valign': 'vcenter',
    })
    subtitulo_fmt = workbook.add_format({
        'italic': True, 'font_size': 10, 'font_color': '#595959',
        'bg_color': '#D9E1F2', 'align': 'center', 'valign': 'vcenter',
    })
    secao_fmt = workbook.add_format({
        'bold': True, 'font_size': 12, 'font_color': 'white',
        'bg_color': '#2E75B6', 'align': 'left', 'valign': 'vcenter',
        'indent': 1,
    })
    lbl_fmt = workbook.add_format({
        'bold': True, 'font_size': 10, 'align': 'right', 'valign': 'vcenter',
        'bg_color': '#D9E1F2', 'font_color': '#1F4E78', 'border': 1,
    })
    val_int_fmt = workbook.add_format({
        'font_size': 10, 'align': 'center', 'valign': 'vcenter', 'border': 1,
    })
    val_usd_fmt = workbook.add_format({
        'font_size': 10, 'align': 'center', 'valign': 'vcenter', 'border': 1,
        'num_format': '"$ "#,##0.00',
    })
    val_brl_fmt = workbook.add_format({
        'font_size': 10, 'align': 'center', 'valign': 'vcenter', 'border': 1,
        'num_format': '"R$ "#,##0.00',
    })
    total_lbl_fmt = workbook.add_format({
        'bold': True, 'font_size': 11, 'align': 'right', 'valign': 'vcenter',
        'bg_color': '#1F4E78', 'font_color': 'white', 'border': 1,
    })
    total_usd_fmt = workbook.add_format({
        'bold': True, 'font_size': 11, 'align': 'center', 'valign': 'vcenter',
        'bg_color': '#D9E1F2', 'font_color': '#1F4E78', 'border': 1,
        'num_format': '"$ "#,##0.00',
    })
    total_brl_fmt = workbook.add_format({
        'bold': True, 'font_size': 11, 'align': 'center', 'valign': 'vcenter',
        'bg_color': '#1F4E78', 'font_color': 'white', 'border': 1,
        'num_format': '"R$ "#,##0.00',
    })

    # Larguras de coluna
    worksheet.set_column('A:A', 36)
    worksheet.set_column('B:C', 16)
    worksheet.set_column('D:T', 8)

    # ---- Título e subtítulo ----
    worksheet.set_row(0, 32)
    worksheet.merge_range(0, 0, 0, 18, 'Dashboard AWS — Infraestrutura Logbit', titulo_fmt)
    worksheet.set_row(1, 18)
    worksheet.merge_range(1, 0, 1, 18,
        f'Periodo: 01/{_now.strftime("%m/%Y")} a {data}   |   Cambio: R$ {TAXA_CAMBIO:.2f}/USD   |   Horas de referencia: {horas_referencia}h',
        subtitulo_fmt)

    # ---- Resumo Executivo ----
    worksheet.set_row(3, 22)
    worksheet.merge_range(3, 0, 3, 2, 'RESUMO EXECUTIVO', secao_fmt)

    _count_running = int((df['STATUS'] == 'Running').sum())
    _count_ec2     = len(df)
    _count_rds     = int((df_rds['STATUS'] == 'Available').sum())
    _count_snap    = len(df_snapshots) if not df_snapshots.empty else 0

    _metricas = [
        ('EC2 — Instancias Running',          _count_running,              val_int_fmt),
        ('EC2 — Total de Instancias',          _count_ec2,                  val_int_fmt),
        ('RDS — Instancias Available',         _count_rds,                  val_int_fmt),
        ('Snapshots EBS',                      _count_snap,                 val_int_fmt),
        ('Custo Estimado EC2 (USD)',            round(total_ec2_usd, 2),     val_usd_fmt),
        ('Custo Estimado RDS (USD)',            round(total_rds_usd, 2),     val_usd_fmt),
        ('Custo Estimado Snapshots (USD)',      round(total_snap_usd, 2),    val_usd_fmt),
    ]
    for i, (label, val, fmt) in enumerate(_metricas):
        worksheet.set_row(4 + i, 18)
        worksheet.write(4 + i, 0, label, lbl_fmt)
        worksheet.write(4 + i, 1, val,   fmt)

    worksheet.set_row(11, 20)
    worksheet.write(11, 0, 'CUSTO TOTAL ESTIMADO (USD)', total_lbl_fmt)
    worksheet.write(11, 1, round(total_usd, 2),           total_usd_fmt)
    worksheet.set_row(12, 20)
    worksheet.write(12, 0, 'CUSTO TOTAL ESTIMADO (BRL)', total_lbl_fmt)
    worksheet.write(12, 1, total_brl,                     total_brl_fmt)

    # ---- Opções de imagem ----
    _OPT_FULL  = {'x_scale': 1.0,  'y_scale': 1.0,  'x_offset': 5, 'y_offset': 5}
    _OPT_LEFT  = {'x_scale': 0.60, 'y_scale': 0.60, 'x_offset': 5, 'y_offset': 5}
    _OPT_RIGHT = {'x_scale': 0.60, 'y_scale': 0.60, 'x_offset': 615, 'y_offset': 5}

    # ---- Secao: Visao Geral ----
    worksheet.set_row(14, 22)
    worksheet.merge_range(14, 0, 14, 18, 'VISAO GERAL DE CUSTOS', secao_fmt)
    worksheet.insert_image(15, 0, g_resumo, _OPT_FULL)

    # ---- Secao: EC2 ----
    worksheet.set_row(34, 22)
    worksheet.merge_range(34, 0, 34, 18, 'ANALISE EC2', secao_fmt)
    worksheet.insert_image(35, 0, g_regiao_status, _OPT_LEFT)
    if g_top_ec2:
        worksheet.insert_image(35, 0, g_top_ec2, _OPT_RIGHT)
    if g_custo_tipo_ec2:
        worksheet.insert_image(52, 0, g_custo_tipo_ec2, _OPT_LEFT)
    if g_tipo_ligados:
        worksheet.insert_image(52, 0, g_tipo_ligados, _OPT_RIGHT)

    # ---- Secao: RDS ----
    worksheet.set_row(69, 22)
    worksheet.merge_range(69, 0, 69, 18, 'ANALISE RDS', secao_fmt)
    if g_rds_engine:
        worksheet.insert_image(70, 0, g_rds_engine, _OPT_LEFT)
    worksheet.insert_image(70, 0, g_custo_regiao, _OPT_RIGHT)

    # ------------------------------------------------
    # ABA RESUMO
    # ------------------------------------------------
    gastos_ws = workbook.add_worksheet('Resumo de Gastos')

    # --- Formatos ---
    g_titulo_fmt = workbook.add_format({
        'bold': True, 'font_size': 15, 'font_color': 'white',
        'bg_color': '#1F4E78', 'align': 'center', 'valign': 'vcenter',
    })
    g_sub_fmt = workbook.add_format({
        'italic': True, 'font_size': 10, 'font_color': '#595959',
        'bg_color': '#D9E1F2', 'align': 'center',
    })
    g_sec_fmt = workbook.add_format({
        'bold': True, 'font_size': 11, 'font_color': 'white',
        'bg_color': '#2E75B6', 'align': 'left', 'valign': 'vcenter', 'indent': 1,
    })
    g_hdr_fmt = workbook.add_format({
        'bold': True, 'font_color': 'white', 'bg_color': '#1F4E78',
        'align': 'center', 'valign': 'vcenter', 'border': 1,
    })
    g_row_a_txt = workbook.add_format({'align': 'left',   'border': 1, 'bg_color': '#EBF3FB', 'indent': 1})
    g_row_b_txt = workbook.add_format({'align': 'left',   'border': 1, 'bg_color': '#FFFFFF',  'indent': 1})
    g_row_a_usd = workbook.add_format({'align': 'center', 'border': 1, 'bg_color': '#EBF3FB', 'num_format': '"$ "#,##0.00'})
    g_row_b_usd = workbook.add_format({'align': 'center', 'border': 1, 'bg_color': '#FFFFFF',  'num_format': '"$ "#,##0.00'})
    g_row_a_brl = workbook.add_format({'align': 'center', 'border': 1, 'bg_color': '#EBF3FB', 'num_format': '"R$ "#,##0.00'})
    g_row_b_brl = workbook.add_format({'align': 'center', 'border': 1, 'bg_color': '#FFFFFF',  'num_format': '"R$ "#,##0.00'})
    g_row_a_pct = workbook.add_format({'align': 'center', 'border': 1, 'bg_color': '#EBF3FB', 'num_format': '0.0"%"'})
    g_row_b_pct = workbook.add_format({'align': 'center', 'border': 1, 'bg_color': '#FFFFFF',  'num_format': '0.0"%"'})
    g_tot_lbl   = workbook.add_format({'bold': True, 'font_color': 'white',   'bg_color': '#1F4E78', 'align': 'left',   'border': 1, 'indent': 1})
    g_tot_usd   = workbook.add_format({'bold': True, 'font_color': '#1F4E78', 'bg_color': '#D9E1F2', 'align': 'center', 'border': 1, 'num_format': '"$ "#,##0.00'})
    g_tot_brl   = workbook.add_format({'bold': True, 'font_color': 'white',   'bg_color': '#1F4E78', 'align': 'center', 'border': 1, 'num_format': '"R$ "#,##0.00'})
    g_tot_pct   = workbook.add_format({'bold': True, 'font_color': '#1F4E78', 'bg_color': '#D9E1F2', 'align': 'center', 'border': 1, 'num_format': '0.0"%"'})
    g_note_fmt  = workbook.add_format({'italic': True, 'font_size': 9, 'font_color': '#595959'})

    # --- Larguras ---
    gastos_ws.set_column('A:A', 46)
    gastos_ws.set_column('B:B', 16)
    gastos_ws.set_column('C:C', 18)
    gastos_ws.set_column('D:D', 14)

    # ---- Título ----
    gastos_ws.set_row(0, 30)
    gastos_ws.merge_range(0, 0, 0, 3, 'Resumo de Gastos AWS — Logbit', g_titulo_fmt)
    gastos_ws.set_row(1, 16)
    gastos_ws.merge_range(1, 0, 1, 3,
        f'Periodo: {data}   |   Cambio: R$ {TAXA_CAMBIO:.2f}/USD', g_sub_fmt)

    # ============================================================
    # SECAO 1 — Cost Explorer
    # ============================================================
    gastos_ws.set_row(3, 20)
    gastos_ws.merge_range(3, 0, 3, 3, 'GASTOS POR SERVICO — AWS Cost Explorer (mes atual)', g_sec_fmt)

    gastos_ws.set_row(4, 18)
    for c, h in enumerate(['Servico', 'USD', 'BRL', '% do Total']):
        gastos_ws.write(4, c, h, g_hdr_fmt)

    _df_ce = df_gastos.sort_values('USD', ascending=False).reset_index(drop=True)
    _total_ce = _df_ce['USD'].sum()

    for i, row in _df_ce.iterrows():
        _r = 5 + i
        gastos_ws.set_row(_r, 17)
        _alt = i % 2 == 0
        gastos_ws.write(_r, 0, row['Serviço'],                      g_row_a_txt if _alt else g_row_b_txt)
        gastos_ws.write(_r, 1, row['USD'],                          g_row_a_usd if _alt else g_row_b_usd)
        gastos_ws.write(_r, 2, row['BRL'],                          g_row_a_brl if _alt else g_row_b_brl)
        gastos_ws.write(_r, 3, row['USD'] / _total_ce * 100,        g_row_a_pct if _alt else g_row_b_pct)

    _r_tot_ce = 5 + len(_df_ce)
    gastos_ws.set_row(_r_tot_ce, 19)
    gastos_ws.write(_r_tot_ce, 0, 'TOTAL Cost Explorer',                g_tot_lbl)
    gastos_ws.write(_r_tot_ce, 1, round(_total_ce, 2),                  g_tot_usd)
    gastos_ws.write(_r_tot_ce, 2, round(_total_ce * TAXA_CAMBIO, 2),    g_tot_brl)
    gastos_ws.write(_r_tot_ce, 3, 100.0,                                g_tot_pct)

    if _erro_gastos:
        gastos_ws.merge_range(_r_tot_ce + 1, 0, _r_tot_ce + 1, 3,
            f'* Erro ao coletar dados do Cost Explorer: {_erro_gastos}',
            g_note_fmt)
    else:
        gastos_ws.merge_range(_r_tot_ce + 1, 0, _r_tot_ce + 1, 3,
            f'* Coletado automaticamente do AWS Cost Explorer. Periodo: {_inicio_mes.strftime("%d/%m/%Y")} a {_now.strftime("%d/%m/%Y")}.',
            g_note_fmt)

    # ============================================================
    # SECAO 2 — Custos Estimados pelo Script
    # ============================================================
    _r_sec2 = _r_tot_ce + 3
    gastos_ws.set_row(_r_sec2, 20)
    gastos_ws.merge_range(_r_sec2, 0, _r_sec2, 3, 'CUSTOS ESTIMADOS — Calculados pelo Script', g_sec_fmt)

    gastos_ws.set_row(_r_sec2 + 1, 18)
    for c, h in enumerate(['Componente', 'USD Estimado', 'BRL Estimado', '% do Total Est.']):
        gastos_ws.write(_r_sec2 + 1, c, h, g_hdr_fmt)

    _total_ec2_compute = round(df['Custo Compute Est. (USD)'].sum(), 2)
    _total_ec2_ebs     = round(df['Custo EBS Est. (USD)'].sum(), 2)
    _est_itens = [
        ('EC2 — Compute (instancias)',    _total_ec2_compute),
        ('EC2 — EBS (volumes anexados)',  _total_ec2_ebs),
        ('RDS (Compute + Storage)',       round(total_rds_usd,  2)),
        ('Snapshots EBS',                 round(total_snap_usd, 2)),
    ]
    for i, (label, val) in enumerate(_est_itens):
        _r = _r_sec2 + 2 + i
        gastos_ws.set_row(_r, 17)
        _alt = i % 2 == 0
        _pct = val / total_usd * 100 if total_usd > 0 else 0
        gastos_ws.write(_r, 0, label,                          g_row_a_txt if _alt else g_row_b_txt)
        gastos_ws.write(_r, 1, val,                            g_row_a_usd if _alt else g_row_b_usd)
        gastos_ws.write(_r, 2, round(val * TAXA_CAMBIO, 2),    g_row_a_brl if _alt else g_row_b_brl)
        gastos_ws.write(_r, 3, _pct,                           g_row_a_pct if _alt else g_row_b_pct)

    _r_tot_est = _r_sec2 + 2 + len(_est_itens)
    gastos_ws.set_row(_r_tot_est, 19)
    gastos_ws.write(_r_tot_est, 0, 'TOTAL Estimado (EC2 Compute + EBS + RDS + Snapshots)', g_tot_lbl)
    gastos_ws.write(_r_tot_est, 1, round(total_usd, 2),                       g_tot_usd)
    gastos_ws.write(_r_tot_est, 2, total_brl,                                  g_tot_brl)
    gastos_ws.write(_r_tot_est, 3, 100.0,                                      g_tot_pct)

    # ============================================================
    # SECAO 3 — Comparativo
    # ============================================================
    _r_sec3 = _r_tot_est + 3
    gastos_ws.set_row(_r_sec3, 20)
    gastos_ws.merge_range(_r_sec3, 0, _r_sec3, 3,
        'COMPARATIVO — Cost Explorer vs Estimado pelo Script (servicos cobertos)', g_sec_fmt)

    gastos_ws.set_row(_r_sec3 + 1, 18)
    for c, h in enumerate(['Servico', 'CE Real (USD)', 'Script Est. (USD)', 'Diferenca (USD)']):
        gastos_ws.write(_r_sec3 + 1, c, h, g_hdr_fmt)

    def _ce_val(nome):
        mask = _df_ce['Serviço'].str.contains(nome, case=False, na=False)
        return round(_df_ce.loc[mask, 'USD'].sum(), 2)

    _ce_ec2_compute = _ce_val('Compute Cloud - Compute')
    _ce_ec2_other   = _ce_val('EC2 - Other')
    _ce_rds         = _ce_val('Relational Database')

    _cmp_rows = [
        ('EC2 — Compute',        _ce_ec2_compute,                  _total_ec2_compute),
        ('EC2 — EBS + Snapshots', _ce_ec2_other,                   round(_total_ec2_ebs + total_snap_usd, 2)),
        ('RDS',                   _ce_rds,                         round(total_rds_usd, 2)),
        ('TOTAL coberto pelo script',
            round(_ce_ec2_compute + _ce_ec2_other + _ce_rds, 2),
            round(total_usd, 2)),
    ]

    _diff_pos_fmt = workbook.add_format({'bold': True, 'align': 'center', 'border': 1,
        'font_color': '#C00000', 'bg_color': '#FCE4D6', 'num_format': '"$ "#,##0.00'})
    _diff_neg_fmt = workbook.add_format({'bold': True, 'align': 'center', 'border': 1,
        'font_color': '#375623', 'bg_color': '#E2EFDA', 'num_format': '"$ "#,##0.00'})

    for i, (label, ce_val, script_val) in enumerate(_cmp_rows):
        _r = _r_sec3 + 2 + i
        gastos_ws.set_row(_r, 18 if i < len(_cmp_rows) - 1 else 20)
        _alt = i % 2 == 0
        _diff_val = round(script_val - ce_val, 2)
        _is_total = i == len(_cmp_rows) - 1
        _lbl_fmt  = g_tot_lbl   if _is_total else (g_row_a_txt if _alt else g_row_b_txt)
        _val_fmt  = g_tot_usd   if _is_total else (g_row_a_usd if _alt else g_row_b_usd)
        gastos_ws.write(_r, 0, label,       _lbl_fmt)
        gastos_ws.write(_r, 1, ce_val,      _val_fmt)
        gastos_ws.write(_r, 2, script_val,  _val_fmt)
        gastos_ws.write(_r, 3, _diff_val,   _diff_pos_fmt if _diff_val > 0 else _diff_neg_fmt)

    _r_notas = _r_sec3 + 2 + len(_cmp_rows) + 1
    gastos_ws.merge_range(_r_notas, 0, _r_notas, 3,
        '* Script usa precos On-Demand cheios. Diferenca positiva indica possivel Savings Plan, Reserved Instance ou instancias encerradas no periodo.',
        g_note_fmt)
    gastos_ws.merge_range(_r_notas + 1, 0, _r_notas + 1, 3,
        '* Lightsail, VPC, S3, Route 53, impostos e outros servicos nao entram na estimativa do script (ver Secao 1).',
        g_note_fmt)

    gastos_ws.freeze_panes(2, 0)

# Conteúdo do e-mail em formato HTML e texto simples
EMAIL_BODY_TEXT = """
Prezado(s),

Espero que estejam bem. Estamos iniciando uma rotina mensal de envio de um novo modelo de relatório de custos da AWS, ainda em formato de MVP. O objetivo é oferecer visibilidade rápida e prática dos principais indicadores de consumo para vocês. A rotina de envio acontecerá ao final de todo mês.

Anexo o primeiro relatório. Os dados apresentados refletem os custos acumulados até a data de extração, que coincide com a data deste envio.

Para facilitar a evolução do modelo, conto com seus comentários. Fiquem à vontade para compartilhar feedbacks e sugestões de melhoria, como campos adicionais, visualizações desejadas ou ajustes de granularidade.

Ficamos à disposição.
"""

EMAIL_BODY_HTML = """
<html>
    <body style="font-family: Arial, sans-serif;">
        <p>Prezado(s),</p>
        <p>Espero que estejam bem. Estamos iniciando uma rotina mensal de envio de um <b>novo modelo de relatório de custos da AWS</b>, ainda em formato de MVP. O objetivo é oferecer visibilidade rápida e prática dos principais indicadores de consumo para vocês. A rotina de envio acontecerá ao final de todo mês.</p>
        <p>Anexo o primeiro relatório. Os dados apresentados refletem os custos acumulados até a data de extração, que coincide com a data deste envio.</p>
        <p>Para facilitar a evolução do modelo, conto com seus comentários. Fiquem à vontade para compartilhar <b>feedbacks e sugestões de melhoria</b>, como campos adicionais, visualizações desejadas ou ajustes de granularidade.</p>
        <p>Ficamos à disposição.</p>
    </body>
</html>
"""


# Função para enviar e-mail via SendGrid
def enviar_email_com_anexo(destinatarios, caminho_arquivo, corpo_texto, corpo_html):
    with open(caminho_arquivo, 'rb') as f:
        dados = f.read()
        dados_base64 = base64.b64encode(dados).decode()    

    anexo = Attachment()
    anexo.file_content = FileContent(dados_base64)
    anexo.file_type = FileType('application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    anexo.file_name = FileName(os.path.basename(caminho_arquivo))
    anexo.disposition = Disposition('attachment')

    mensagem = Mail(
        from_email=Email(SENDGRID_FROM_EMAIL),
        to_emails=[To(email) for email in destinatarios],
        subject=f'Relatório Infraestrutura AWS EC2 e RDS - Logbit - {data}',
        plain_text_content=Content('text/plain', corpo_texto), # Usando o novo corpo
        html_content=Content('text/html', corpo_html)           # Adicionando o HTML
    )
    mensagem.attachment = anexo

    try:
        # A chave da API deve ser mantida segura. Idealmente, use variáveis de ambiente.
        sg = SendGridAPIClient(SENDGRID_API_KEY)
        resposta = sg.send(mensagem)
        print(f'E-mail enviado com status: {resposta.status_code}')
    except Exception as e:
        print(f'Erro ao enviar e-mail: {e}')

def enviar_discord(caminho_arquivo, webhook_url):
    if not webhook_url:
        print('Discord: DISCORD_WEBHOOK_URL não configurado, pulando envio.')
        return
    mensagem = (
        f'📊 **Relatório AWS Logbit — {data}**\n'
        f'Período: {_inicio_mes.strftime("%d/%m/%Y")} a {_now.strftime("%d/%m/%Y")}\n'
        f'EC2: **$ {round(total_ec2_usd, 2):,.2f}** | '
        f'RDS: **$ {round(total_rds_usd, 2):,.2f}** | '
        f'Snapshots: **$ {round(total_snap_usd, 2):,.2f}**\n'
        f'Total estimado: **$ {round(total_usd, 2):,.2f}** (R$ {round(total_brl, 2):,.2f})'
    )
    try:
        with open(caminho_arquivo, 'rb') as f:
            resp = requests.post(
                webhook_url,
                data={'content': mensagem},
                files={'file': (os.path.basename(caminho_arquivo), f,
                                'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')},
                timeout=30,
            )
        if resp.status_code in (200, 204):
            print(f'Discord: relatório enviado com sucesso.')
        else:
            print(f'Discord: erro ao enviar — status {resp.status_code}: {resp.text}')
    except Exception as e:
        print(f'Discord: erro ao enviar — {e}')


# Destinatários
destinatarios = [
    'ednardopedrosa@gmail.com',
]

# Envio do relatório
enviar_email_com_anexo(destinatarios, excel_path, EMAIL_BODY_TEXT, EMAIL_BODY_HTML)
enviar_discord(excel_path, DISCORD_WEBHOOK_URL)
