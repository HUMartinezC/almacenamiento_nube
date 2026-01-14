from curses import keyname
from select import kevent
import boto3
from dotenv import load_dotenv
import os

load_dotenv()

# Creamos una sesión de boto3
session = boto3.Session(
    aws_access_key_id=os.getenv('ACCESS_KEY'),
    aws_secret_access_key=os.getenv('SECRET_KEY'),
    aws_session_token=os.getenv('SESSION_TOKEN'),
    region_name=os.getenv('REGION')
)

# Probamos la conexión listando las instancias EC2 contando el número de instancias
ec2 = session.client('ec2')

response = ec2.describe_instances()

instance_count = sum(len(reservation['Instances']) for reservation in response['Reservations'])

print(f"Número de instancias EC2: {instance_count}")

# Crear una instancia EC2, ejecutarla, pararla y eliminarla
def administrar_instancia_ec2(eliminar=False):
    # Crear una instancia EC2
    # De forma obligatoria se debe especificar una AMI. 
    # Además de MinCount y MaxCount para crear múltiples instancias
    # De forma opcional se puede especificar el tipo de instancia con InstanceType, KeyName, SecurityGroups, etc.
    instance = ec2.run_instances(
        ImageId='ami-07ff62358b87c7116',  # Amazon Linux AMI
        InstanceType='t3.micro',
        MinCount=1,
        MaxCount=1,
        KeyName=os.getenv('PEM_FILE_PATH'),
    )

    instance_id = instance['Instances'][0]['InstanceId']
    print(f"\nInstancia creada con ID: {instance_id}")
    
    # obtener región de la instancia
    instance_description = ec2.describe_instances(InstanceIds=[instance_id])
    instance_region = instance_description['Reservations'][0]['Instances'][0]['Placement']['AvailabilityZone'][:-1]
    print(f"Región de la instancia: {instance_region}") 

    # Aplicar etiqueta 'Name' a la instancia (puede configurarse con la variable de entorno INSTANCE_NAME)
    ec2.create_tags(Resources=[instance_id], Tags=[{'Key': 'Name', 'Value': 'test-instance'}])
    print(f"Etiqueta 'Name' aplicada: test-instance")

    # Esperar a que la instancia esté en estado 'running'
    waiter = ec2.get_waiter('instance_running')
    waiter.wait(InstanceIds=[instance_id])
    print(f"Instancia {instance_id} está en estado 'running'.")

    if eliminar:
        # Parar la instancia
        ec2.stop_instances(InstanceIds=[instance_id])
        print(f"Instancia {instance_id} detenida.")

        # Esperar a que la instancia esté en estado 'stopped'
        waiter = ec2.get_waiter('instance_stopped')
        waiter.wait(InstanceIds=[instance_id])
        print(f"Instancia {instance_id} está en estado 'stopped'.")

        # Eliminar la instancia
        ec2.terminate_instances(InstanceIds=[instance_id])
        print(f"Instancia {instance_id} eliminada.")
        
        return
    return instance_id, instance_region
    
# Probar la creación, ejecución, parada y eliminación de la instancia
# administrar_instancia_ec2(eliminar=True)

# Crear un volumen EBS y asignarlo a la instancia creada
# instance_id = administrar_instancia_ec2()

def crear_y_asignar_volumen_ebs(instance_id, size_gb=1, zona_disponibilidad=None):
    # Crear un volumen EBS, debe pertenecer a la misma zona de disponibilidad que la instancia
    volume = ec2.create_volume(
        AvailabilityZone=os.getenv('REGION') + zona_disponibilidad, 
        Size=size_gb,
        VolumeType='gp3'
    )

    volume_id = volume['VolumeId']
    print(f"\nVolumen EBS creado con ID: {volume_id}")

    # Esperar a que el volumen esté disponible
    waiter = ec2.get_waiter('volume_available')
    waiter.wait(VolumeIds=[volume_id])
    print(f"Volumen {volume_id} está disponible.")

    # Asignar el volumen a la instancia
    ec2.attach_volume(
        VolumeId=volume_id,
        InstanceId=instance_id,
        Device='/dev/sdf'  # Dispositivo en la instancia
    )
    print(f"Volumen {volume_id} asignado a la instancia {instance_id}.")
    
    # Devolver el ID del volumen para uso posterior
    return volume_id
    
volumen_id = crear_y_asignar_volumen_ebs(instance_id='i-084f8e6fba656a379', zona_disponibilidad='c')
    
# Montar el volumen EBS en la instancia (esto requiere acceso SSH a la instancia, no se puede hacer solo con boto3)

def montar_volumen_ebs_en_instancia(instance_ip, key_file, device='/dev/xvdf', mount_point='/mnt/ebs_volume'):
    import paramiko

    # Configurar la conexión SSH
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(instance_ip, username='ec2-user', key_filename=key_file)

    # Comandos para formatear y montar el volumen
    commands = [
        f'sudo mkfs -t ext4 {device}',
        f'sudo mkdir -p {mount_point}',
        f'sudo mount {device} {mount_point}',
        f'sudo chmod 777 {mount_point}'  # Permisos para todos los usuarios
    ]

    for command in commands:
        stdin, stdout, stderr = ssh.exec_command(command)
        stdout.channel.recv_exit_status()  # Esperar a que el comando termine
        print(f"Ejecutado: {command}")

    ssh.close()
    print(f"Volumen montado en {mount_point} en la instancia {instance_ip}.")
