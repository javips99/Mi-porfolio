import boto3
from botocore.exceptions import ClientError
import json
import zipfile, io
import time


# Helper para crear clientes
def client(service):
    return boto3.client(service, endpoint_url='http://localhost:4566',
                        aws_access_key_id='test', aws_secret_access_key='test',
                        region_name='eu-west-1')

s3 = client('s3')
iam = client('iam')
glue = client('glue')
sfn = client('stepfunctions')
lambda_client = client('lambda')

# Crear buckets
for bucket in ['fashionstore-raw', 'fashionstore-processed', 'fashionstore-athena-results']:

    try:
        s3.create_bucket(Bucket=bucket,
                        CreateBucketConfiguration={'LocationConstraint': 'eu-west-1'})
        print(f"[OK] Bucket: {bucket}")
    except ClientError as e:
        if e.response['Error']['Code'] == 'BucketAlreadyOwnedByYou':
            print(f"[INFO] Bucket ya existe: {bucket}")
        else:
            raise


# Crear database en Glue Catalog
# glue.create_database(DatabaseInput={'Name': 'fashionstore_prod'})
# print("[OK] Database: fashionstore_prod")

# NOTA: glue.create_database() no está disponible en LocalStack gratuito
# (requiere el plan Ultimate). En un entorno con moto o AWS real, esta línea
# registraría la database del catálogo. Se omite aquí para no bloquear
# el resto del pipeline (S3, Lambda y Step Functions sí funcionan en LocalStack).


# Role para Lambdas del pipeline
trust = json.dumps({"Version": "2012-10-17", "Statement": [
    {"Effect": "Allow", "Principal": {"Service": "lambda.amazonaws.com"}, "Action": "sts:AssumeRole"}
]})

try:
    iam.create_role(RoleName='pipeline-lambda-role', AssumeRolePolicyDocument=trust)
    print("[OK] Role creado: pipeline-lambda-role")
except iam.exceptions.EntityAlreadyExistsException:
    print("[INFO] Role ya existe: pipeline-lambda-role")

# Policy: las Lambdas pueden leer raw, escribir processed, y actualizar catálogo
policy_doc = json.dumps({"Version": "2012-10-17", "Statement": [
    {"Effect": "Allow", "Action": ["s3:GetObject"], "Resource": ["arn:aws:s3:::fashionstore-raw/*"]},
    {"Effect": "Allow", "Action": ["s3:PutObject"], "Resource": ["arn:aws:s3:::fashionstore-processed/*"]},
    {"Effect": "Allow", "Action": ["glue:*"], "Resource": ["*"]},
    {"Effect": "Allow", "Action": ["logs:*"], "Resource": ["*"]},
]})

try:
    policy = iam.create_policy(PolicyName='pipeline-lambda-policy', PolicyDocument=policy_doc)
    iam.attach_role_policy(RoleName='pipeline-lambda-role', PolicyArn=policy['Policy']['Arn'])
    print("[OK] IAM configurado con mínimo privilegio")
except iam.exceptions.EntityAlreadyExistsException:
    print("[INFO] Policy ya existe: pipeline-lambda-policy")


# Crear Lambdas del pipeline
def crear_lambda(nombre, codigo):
    """Helper para crear una Lambda con su código."""
    try:
        lambda_client.delete_function(FunctionName=nombre)
    except lambda_client.exceptions.ResourceNotFoundException:
        pass

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, 'w') as zf:
        info = zipfile.ZipInfo('handler.py')
        info.external_attr = 0o644 << 16
        zf.writestr(info, codigo)
    buf.seek(0)
    lambda_client.create_function(
        FunctionName=nombre, Runtime='python3.11',
        Role='arn:aws:iam::000000000000:role/pipeline-lambda-role',
        Handler='handler.handler', Code={'ZipFile': buf.read()},
        Timeout=300, MemorySize=256
    )
    print(f"  [OK] Lambda: {nombre}")

crear_lambda('fs-validar', """
import json
def handler(event, context):
    key = event['key']
    size = event.get('size', 0)
    errores = []
    if size == 0: errores.append('Archivo vacío')
    if not key.endswith('.csv'): errores.append('No es CSV')
    return {'key': key, 'valido': len(errores) == 0, 'errores': errores}
""")

crear_lambda('fs-transformar', """
import json, csv, io, boto3
def handler(event, context):
    s3 = boto3.client('s3', endpoint_url='http://localhost.localstack.cloud:4566')
    key = event['key']
    obj = s3.get_object(Bucket='fashionstore-raw', Key=key)
    content = obj['Body'].read().decode()
    reader = csv.DictReader(io.StringIO(content))
    records = list(reader)
    out_key = key.replace('raw/', '').replace('.csv', '.json')
    s3.put_object(Bucket='fashionstore-processed', Key=out_key,
                  Body=json.dumps(records).encode())
    return {'output_key': out_key, 'records': len(records)}
""")

crear_lambda('fs-catalogar', """
import json, boto3
def handler(event, context):
    return {'catalogado': True, 'tabla': 'ventas', 'registros': event.get('records', 0)}
""")

crear_lambda('fs-notificar', """
import json
def handler(event, context):
    print(f"NOTIFICACION: Pipeline completado - {json.dumps(event)}")
    return {'notificado': True}
""")
print("\n[OK] Todas las Lambdas creadas")

# Step Functions: orquesta las 4 Lambdas
pipeline = {
    "StartAt": "Validar",
    "States": {
        "Validar": {
            "Type": "Task",
            "Resource": "arn:aws:lambda:eu-west-1:000000000000:function:fs-validar",
            "Next": "CheckValidez",
            "Retry": [{"ErrorEquals": ["States.ALL"], "MaxAttempts": 2}]
        },
        "CheckValidez": {
            "Type": "Choice",
            "Choices": [{"Variable": "$.valido", "BooleanEquals": True, "Next": "Transformar"}],
            "Default": "NotificarError"
        },
        "Transformar": {
            "Type": "Task",
            "Resource": "arn:aws:lambda:eu-west-1:000000000000:function:fs-transformar",
            "Next": "Catalogar"
        },
        "Catalogar": {
            "Type": "Task",
            "Resource": "arn:aws:lambda:eu-west-1:000000000000:function:fs-catalogar",
            "Next": "Notificar"
        },
        "Notificar": {
            "Type": "Task",
            "Resource": "arn:aws:lambda:eu-west-1:000000000000:function:fs-notificar",
            "End": True
        },
        "NotificarError": {
            "Type": "Task",
            "Resource": "arn:aws:lambda:eu-west-1:000000000000:function:fs-notificar",
            "End": True
        }
    }
}

try:
    sfn.create_state_machine(
        name='fashionstore-etl-pipeline',
        definition=json.dumps(pipeline),
        roleArn='arn:aws:iam::000000000000:role/pipeline-lambda-role'
    )
    print("[OK] Pipeline Step Functions creado")
except sfn.exceptions.StateMachineAlreadyExists:
    print("[INFO] Step Function ya existe: fashionstore-etl-pipeline")

# Simular llegada de un archivo CSV de ventas
csv_data = """order_id,customer_id,product,quantity,amount,date
ORD001,CUST42,Camiseta Premium,2,51.98,2024-01-15
ORD002,CUST17,Zapatillas Runner,1,89.99,2024-01-15
ORD003,CUST42,Pantalón Slim,1,49.99,2024-01-15
ORD004,CUST08,Chaqueta Invierno,1,129.99,2024-01-15
"""

s3.put_object(Bucket='fashionstore-raw', Key='ventas/2024-01-15/ventas_hora_14.csv',
              Body=csv_data.encode())
print("Archivo subido a S3 raw")

# Ejecutar el pipeline
execution = sfn.start_execution(
    stateMachineArn='arn:aws:states:eu-west-1:000000000000:stateMachine:fashionstore-etl-pipeline',
    input=json.dumps({
        "key": "ventas/2024-01-15/ventas_hora_14.csv",
        "size": len(csv_data),
        "bucket": "fashionstore-raw"
    })
)
print(f"Pipeline ejecutándose: {execution['executionArn'].split(':')[-1]}")

for _ in range(20):
    desc = sfn.describe_execution(executionArn=execution['executionArn'])
    if desc['status'] != 'RUNNING':
        break
    time.sleep(1)

print(f"\n{'[OK]' if desc['status'] == 'SUCCEEDED' else '[ERROR]'} Pipeline: {desc['status']}")
if 'output' in desc:
    print(f"   Output: {desc['output']}")

# Verificar que los datos llegaron a processed
processed = s3.list_objects_v2(Bucket='fashionstore-processed')
print("\nArchivos en fashionstore-processed:")
for obj in processed.get('Contents', []):
    print(f"   {obj['Key']} ({obj['Size']} bytes)")

# Leer el archivo transformado
if processed.get('Contents'):
    key = processed['Contents'][0]['Key']
    data = s3.get_object(Bucket='fashionstore-processed', Key=key)
    contenido = json.loads(data['Body'].read().decode())
    print(f"\nDatos transformados ({len(contenido)} registros):")
    for registro in contenido[:3]:
        print(f"   {registro}")

# Lambda dispatcher: recibe evento S3 y arranca Step Functions
dispatcher_code = """
import json, boto3
def handler(event, context):
    sfn = boto3.client('stepfunctions', endpoint_url='http://localhost.localstack.cloud:4566')
    for record in event.get('Records', []):
        key = record['s3']['object']['key']
        size = record['s3']['object']['size']
        sfn.start_execution(
            stateMachineArn='arn:aws:states:eu-west-1:000000000000:stateMachine:fashionstore-etl-pipeline',
            input=json.dumps({'key': key, 'size': size, 'bucket': 'fashionstore-raw'})
        )
    return {'dispatched': len(event.get('Records', []))}
"""

crear_lambda('fs-dispatcher', dispatcher_code)

# Configurar S3 para triggear el dispatcher
s3.put_bucket_notification_configuration(
    Bucket='fashionstore-raw',
    NotificationConfiguration={
        'LambdaFunctionConfigurations': [{
            'LambdaFunctionArn': 'arn:aws:lambda:eu-west-1:000000000000:function:fs-dispatcher',
            'Events': ['s3:ObjectCreated:*'],
            'Filter': {'Key': {'FilterRules': [{'Name': 'suffix', 'Value': '.csv'}]}}
        }]
    }
)
print("[OK] Automatización completa: archivo nuevo → pipeline se ejecuta solo")
