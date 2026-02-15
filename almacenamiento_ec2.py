import re
import boto3
from dotenv import load_dotenv
import os
import uuid
from datetime import datetime
import faker

load_dotenv()

session = boto3.Session(
    aws_access_key_id=os.getenv("ACCESS_KEY"),
    aws_secret_access_key=os.getenv("SECRET_KEY"),
    aws_session_token=os.getenv("SESSION_TOKEN"),
    region_name=os.getenv("REGION"),
)


ec2 = session.client("ec2")

response = ec2.describe_instances()

instance_count = sum(
    len(reservation["Instances"]) for reservation in response["Reservations"]
)

print(f"Número de instancias EC2 actual: {instance_count}")


# --------------------------------
# Gestión de instancias EC2: crear, ejecutar, parar y eliminar.
# --------------------------------

class EC2Manager:
    def __init__(
        self,
        ami_id,
        instance_type="t3.micro",
        key_name=None,
        instance_name="test-instance",
    ):
        self.ami_id = ami_id
        self.instance_type = instance_type
        self.key_name = key_name or os.getenv("PEM_NAME")
        self.instance_name = instance_name
        self.instance_id = None
        self.instance_region = None

    def crear_instancia(self):
        """Crear una instancia EC2"""
        print(self.key_name)
        response = ec2.run_instances(
            ImageId=self.ami_id,
            InstanceType=self.instance_type,
            MinCount=1,
            MaxCount=1,
            KeyName=self.key_name,
        )
        self.instance_id = response["Instances"][0]["InstanceId"]
        print(f"\nInstancia creada con ID: {self.instance_id}")
        return self.instance_id

    def _get_instance_id(self, instance_id):
        if not self.instance_id and not instance_id:
            raise ValueError(
                "Debes proporcionar un ID de instancia o crear una previamente."
            )
        return self.instance_id or instance_id

    def _find_free_device(self, instance_id):
        description = ec2.describe_instances(InstanceIds=[instance_id])
        instance_data = description["Reservations"][0]["Instances"][0]
        used_devices = {
            mapping.get("DeviceName")
            for mapping in instance_data.get("BlockDeviceMappings", [])
            if mapping.get("DeviceName")
        }
        for letter in "fghijklmnop":
            device_name = f"/dev/sd{letter}"
            if device_name not in used_devices:
                return device_name
        raise ValueError("No hay device libre disponible para adjuntar el volumen EBS.")

    def parar_instancia(self, instance_id=None):
        """Parar la instancia EC2"""
        self._get_instance_id(instance_id)
        instance_id = self.instance_id or instance_id
        ec2.stop_instances(InstanceIds=[instance_id])
        print(f"Instancia {instance_id} detenida.")
        self.esperar_estado("stopped")

    def obtener_region(self, instance_id=None):
        """Obtener la zona de disponibilidad de la instancia"""
        self._get_instance_id(instance_id)
        instance_id = self.instance_id or instance_id
        description = ec2.describe_instances(InstanceIds=[instance_id])
        self.instance_region = description["Reservations"][0]["Instances"][0][
            "Placement"
        ]["AvailabilityZone"]
        print(f"Zona de disponibilidad de la instancia: {self.instance_region}")
        return self.instance_region

    def aplicar_etiqueta(self, tag=None, instance_id=None):
        """Aplicar etiqueta 'Name' a la instancia"""
        self._get_instance_id(instance_id)
        instance_id = self.instance_id or instance_id
        ec2.create_tags(
            Resources=[instance_id],
            Tags=[{"Key": "Name", "Value": tag or self.instance_name}],
        )
        print(f"Etiqueta 'Name' aplicada: {tag or self.instance_name}")

    def esperar_estado(self, estado="running", instance_id=None):
        """Esperar a que la instancia llegue a un estado específico"""
        self._get_instance_id(instance_id)
        instance_id = self.instance_id or instance_id
        waiter = ec2.get_waiter(f"instance_{estado}")
        waiter.wait(InstanceIds=[instance_id])
        print(f"Instancia {instance_id} está en estado '{estado}'.")

    def eliminar_instancia(self, instance_id=None):
        """Detener y eliminar la instancia"""
        self._get_instance_id(instance_id)
        instance_id = self.instance_id or instance_id

        ec2.stop_instances(InstanceIds=[instance_id])
        print(f"Instancia {instance_id} detenida.")
        self.esperar_estado("stopped", instance_id=instance_id)

        ec2.terminate_instances(InstanceIds=[instance_id])
        print(f"Instancia {instance_id} eliminada.")

    def crear_volumen_ebs(
        self, size_gb=1, instance_region=None, zona_disponibilidad=None
    ):
        """Crear un volumen EBS en la misma zona de disponibilidad que la instancia"""
        if not self.instance_region and not instance_region and not zona_disponibilidad:
            raise ValueError(
                "Debes proporcionar la zona de disponibilidad (ej: 'us-east-1a')."
            )
        region = zona_disponibilidad or self.instance_region or instance_region
        volume = ec2.create_volume(
            AvailabilityZone=region,
            Size=size_gb,
            VolumeType="gp3",
        )
        volume_id = volume["VolumeId"]
        print(f"\nVolumen EBS creado con ID: {volume_id}")
        return volume_id
    
    def obtener_ip_publica(self, instance_id=None):
        self._get_instance_id(instance_id)
        instance_id = self.instance_id or instance_id
        description = ec2.describe_instances(InstanceIds=[instance_id])
        instance_data = description["Reservations"][0]["Instances"][0]
        public_ip = instance_data.get("PublicIpAddress") or os.getenv("INSTANCE_IP")
        print(f"IP pública de la instancia {instance_id}: {public_ip}")
        return public_ip

    def asignar_volumen_ebs(
        self, volume_id, instance_id=None, device=None
    ):
        """Asignar un volumen EBS a la instancia"""
        self._get_instance_id(instance_id)
        instance_id = self.instance_id or instance_id
        device = device or self._find_free_device(instance_id)
        ec2.attach_volume(
            VolumeId=volume_id,
            InstanceId=instance_id,
            Device=device,
        )
        print(f"Volumen {volume_id} asignado a la instancia {instance_id} en {device}.")
        return device

    def montar_volumen_ebs_en_instancia(
        self,
        instance_ip,
        device="/dev/xvdf",
        mount_point="/mnt/ebs_volume",
        username="ec2-user",
    ):
        """Montar el volumen EBS en la instancia (requiere acceso SSH)"""
        import paramiko

        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        key = paramiko.RSAKey.from_private_key_file(os.getenv("PEM_FILE"))

        ssh.connect(
            hostname=instance_ip, username=username, pkey=key, port=22, timeout=10
        )

        # Comandos para formatear y montar el volumen
        commands = [
            f"sudo mkfs -t ext4 {device}",
            f"sudo mkdir -p {mount_point}",
            f"sudo mount {device} {mount_point}",
            f"sudo chmod 777 {mount_point}",
        ]

        for command in commands:
            stdin, stdout, stderr = ssh.exec_command(command)
            stdout.channel.recv_exit_status()  # Esperar a que el comando termine
            print(f"Ejecutado: {command}")

        ssh.close()
        print(f"Volumen montado en {mount_point} en la instancia {instance_ip}.")
        
        # Agregar un archivo de prueba en el volumen montado
        ssh.connect(
            hostname=instance_ip, username=username, pkey=key, port=22, timeout=10
        )
        test_file_command = f'echo "Prueba de almacenamiento en EBS" | sudo tee {mount_point}/prueba_ebs.txt'
        stdin, stdout, stderr = ssh.exec_command(test_file_command)
        stdout.channel.recv_exit_status()
        print(f"Archivo de prueba creado en {mount_point}/prueba_ebs.txt")
        
        # Leer el archivo de prueba para verificar
        read_file_command = f"sudo cat {mount_point}/prueba_ebs.txt"
        stdin, stdout, stderr = ssh.exec_command(read_file_command)
        output = stdout.read().decode()
        print(f"Contenido del archivo de prueba: {output}")
        
        ssh.close()
        
    def crear_efs_y_montar_en_instancia(self, instance_ip, instance_id=None, username="ec2-user"):
        """Crear un sistema de archivos EFS, montarlo en la instancia y añadir un archivo de prueba"""
        import paramiko
        import time

        # Crear EFS
        efs = session.client("efs")
        response = efs.create_file_system(CreationToken=str(uuid.uuid4()))
        file_system_id = response["FileSystemId"]
        print(f"\nEFS creado con ID: {file_system_id}")

        # Esperar a que el EFS esté disponible (EFS no tiene waiter nativo, usamos polling)
        max_attempts = 30
        for attempt in range(max_attempts):
            try:
                fs_info = efs.describe_file_systems(FileSystemId=file_system_id)
                if fs_info["FileSystems"][0]["LifeCycleState"] == "available":
                    print(f"EFS {file_system_id} está disponible.")
                    break
            except Exception as e:
                print(f"Esperando EFS... ({attempt + 1}/{max_attempts})")
            time.sleep(2)
        
        # Obtener la zona de disponibilidad de la instancia
        self.obtener_region(instance_id=instance_id)
        
        # Obtener SubnetId y SecurityGroupId de la instancia
        self._get_instance_id(instance_id)
        instance_id = self.instance_id or instance_id
        description = ec2.describe_instances(InstanceIds=[instance_id])
        instance_data = description["Reservations"][0]["Instances"][0]
        subnet_id = instance_data["SubnetId"]
        security_group_id = instance_data["SecurityGroups"][0]["GroupId"]
        print(f"SubnetId: {subnet_id}, SecurityGroupId: {security_group_id}")

        # Crear punto de montaje
        mount_target = efs.create_mount_target(
            FileSystemId=file_system_id,
            SubnetId=subnet_id,
            SecurityGroups=[security_group_id],
        )
        print(f"Punto de montaje creado: {mount_target}")

        # Configurar la conexión SSH
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        # Cargar clave privada
        key = paramiko.RSAKey.from_private_key_file(os.getenv("PEM_FILE"))

        ssh.connect(
            hostname=instance_ip, username=username, pkey=key, port=22, timeout=10
        )

        # Comandos para montar EFS
        commands = [
            "sudo yum install -y amazon-efs-utils",
            f"sudo mkdir -p /mnt/efs",
            f"sudo mount -t efs {file_system_id}:/ /mnt/efs",
            "sudo chmod 777 /mnt/efs",
        ]

        for command in commands:
            stdin, stdout, stderr = ssh.exec_command(command)
            stdout.channel.recv_exit_status()  # Esperar a que el comando termine
            print(f"Ejecutado: {command}")

        # Agregar un archivo de prueba en el EFS montado
        test_file_command = f'echo "Prueba de almacenamiento en EFS" | sudo tee /mnt/efs/prueba_efs.txt'
        stdin, stdout, stderr = ssh.exec_command(test_file_command)
        stdout.channel.recv_exit_status()
        print(f"Archivo de prueba creado en /mnt/efs/prueba_efs.txt")
        
        # Leer el archivo de prueba para verificar
        read_file_command = f"sudo cat /mnt/efs/prueba_efs.txt"
        stdin, stdout, stderr = ssh.exec_command(read_file_command)
        output = stdout.read().decode()
        print(f"Contenido del archivo de prueba: {output}")
        
        ssh.close()


ec2_manager = EC2Manager(ami_id="ami-07ff62358b87c7116", instance_name="Test")


# 1. Probar la creación, ejecución, parada y eliminación de la instancia

# # Crear y ejecutar instancia
# ec2_manager.crear_instancia()

# # Asignar etiqueta
# ec2_manager.aplicar_etiqueta(tag="EC2-Test")

# # Esperar a que esté en ejecución
# ec2_manager.esperar_estado("running")

# # Parar ejecución
# ec2_manager.parar_instancia()

# # Esperar a que esté detenida
# ec2_manager.esperar_estado("stopped")

# # Eliminar instancia
# ec2_manager.eliminar_instancia()


# 2. Crear un volumen EBS y asignarlo a la instancia creada

# Crear una nueva instancia para pruebas
# ec2_manager.crear_instancia()
# ec2_manager.esperar_estado("running")

instance_id = ec2_manager.instance_id or os.getenv("INSTANCE_ID")
if instance_id:
    # Obtener la zona de disponibilidad de la instancia
    ec2_manager.obtener_region(instance_id=instance_id)
    
    # Crear volumen en la misma zona
    volumen_id = ec2_manager.crear_volumen_ebs(size_gb=1)
    
    # Esperar a que el volumen esté disponible
    waiter = ec2.get_waiter("volume_available")
    waiter.wait(VolumeIds=[volumen_id])
    print(f"Volumen {volumen_id} está disponible.")
    
    # Asignar volumen EBS a la instancia
    device_name = ec2_manager.asignar_volumen_ebs(
        volume_id=volumen_id, instance_id=instance_id
    )
else:
    print("INSTANCE_ID no configurado en variables de entorno")

# Montar el volumen EBS en la instancia (requiere acceso SSH)

instance_ip = ec2_manager.obtener_ip_publica(instance_id=instance_id)

mount_device = device_name.replace("/dev/sd", "/dev/xvd")
ec2_manager.montar_volumen_ebs_en_instancia(
    instance_ip=instance_ip,
    device=mount_device,
)

# Crear EFS, montar en la instancia y añadir un archivo de prueba
ec2_manager.crear_efs_y_montar_en_instancia(
    instance_ip=instance_ip,
    instance_id=instance_id
)
