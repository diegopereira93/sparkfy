import boto3
import configparser
import json
import time
import os
from dotenv import load_dotenv


def create_clients(aws_key=None, aws_secret=None, region=None, aws_session_token=None):
    """Create boto3 clients for IAM, EC2, S3, and Redshift."""
    # Load environment variables from .env file
    load_dotenv()
    
    # Use environment variables if parameters not provided
    aws_key = aws_key or os.getenv('AWS_ACCESS_KEY_ID')
    aws_secret = aws_secret or os.getenv('AWS_SECRET_ACCESS_KEY')
    region = region or os.getenv('AWS_REGION', 'us-west-2')
    aws_session_token = aws_session_token or os.getenv('AWS_SESSION_TOKEN')
    
    kwargs = {
        'region_name': region,
        'aws_access_key_id': aws_key,
        'aws_secret_access_key': aws_secret
    }
    
    if aws_session_token:
        kwargs['aws_session_token'] = aws_session_token
    
    ec2 = boto3.resource('ec2', **kwargs)
    iam = boto3.client('iam', **kwargs)
    redshift = boto3.client('redshift', **kwargs)
    
    return ec2, iam, redshift


def create_iam_role(iam, role_name='redshift-s3-access'):
    """Create IAM role for Redshift to access S3."""
    print("Creating IAM Role...")
    try:
        role = iam.create_role(
            Path='/',
            RoleName=role_name,
            Description="Allows Redshift to access S3",
            AssumeRolePolicyDocument=json.dumps({
                'Statement': [{
                    'Action': 'sts:AssumeRole',
                    'Effect': 'Allow',
                    'Principal': {'Service': 'redshift.amazonaws.com'}
                }],
                'Version': '2012-10-17'
            })
        )

        # Attach S3 read only access policy
        iam.attach_role_policy(
            RoleName=role_name,
            PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
        )

        return role['Role']['Arn']
    except iam.exceptions.EntityAlreadyExistsException:
        print(f"IAM role {role_name} already exists. Getting ARN...")
        return iam.get_role(RoleName=role_name)['Role']['Arn']


def create_redshift_cluster(redshift, iam_role_arn, cluster_params):
    """Create a Redshift cluster."""
    try:
        print("Creating Redshift cluster...")
        response = redshift.create_cluster(
            ClusterType=cluster_params['cluster_type'],
            NodeType=cluster_params['node_type'],
            NumberOfNodes=int(cluster_params['num_nodes']),
            DBName=cluster_params['db_name'],
            ClusterIdentifier=cluster_params['cluster_identifier'],
            MasterUsername=cluster_params['master_username'],
            MasterUserPassword=cluster_params['master_password'],
            IamRoles=[iam_role_arn],
            PubliclyAccessible=True
        )
        
        return response['Cluster']
    except redshift.exceptions.ClusterAlreadyExistsFault:
        print(f"Cluster {cluster_params['cluster_identifier']} already exists. Getting details...")
        return redshift.describe_clusters(
            ClusterIdentifier=cluster_params['cluster_identifier']
        )['Clusters'][0]


def open_tcp_port(ec2, redshift, cluster_identifier, port=5439):
    """Configure security group to allow access through redshift port."""
    cluster_props = redshift.describe_clusters(
        ClusterIdentifier=cluster_identifier
    )['Clusters'][0]
    
    vpc_id = cluster_props['VpcId']
    vpc_sg_id = cluster_props['VpcSecurityGroups'][0]['VpcSecurityGroupId']
    
    print(f"Opening TCP port {port} for Redshift access...")
    try:
        vpc = ec2.Vpc(id=vpc_id)
        security_group = list(vpc.security_groups.all())[0]
        
        security_group.authorize_ingress(
            GroupId=vpc_sg_id,
            CidrIp='0.0.0.0/0',
            IpProtocol='TCP',
            FromPort=port,
            ToPort=port
        )
        print(f"Port {port} opened successfully")
    except Exception as e:
        # If the rule already exists, this will catch the error
        print(f"Security group ingress rule error: {e}")


def wait_for_cluster_available(redshift, cluster_identifier, timeout=600):
    """Wait for the cluster to become available."""
    print(f"Waiting for cluster {cluster_identifier} to become available...")
    start_time = time.time()
    while time.time() - start_time < timeout:
        cluster_props = redshift.describe_clusters(
            ClusterIdentifier=cluster_identifier
        )['Clusters'][0]
        
        if cluster_props['ClusterStatus'] == 'available':
            print(f"Cluster {cluster_identifier} is now available!")
            return cluster_props
        
        print(f"Cluster status: {cluster_props['ClusterStatus']}. Waiting...")
        time.sleep(30)
    
    raise TimeoutError(f"Cluster did not become available within {timeout} seconds")


def update_config_file(config_file, section, key, value):
    """Update a value in the config file."""
    config = configparser.ConfigParser()
    config.read(config_file)
    
    if not config.has_section(section):
        config.add_section(section)
    
    config.set(section, key, value)
    
    with open(config_file, 'w') as f:
        config.write(f)


def setup_redshift_cluster():
    """Set up Redshift cluster and update config file."""
    # Load environment variables
    load_dotenv()
    
    # Read config file
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    # Get cluster parameters from .env or prompt if not found
    CLUSTER_IDENTIFIER = os.getenv('REDSHIFT_CLUSTER_ID') or input("Enter Redshift Cluster Identifier: ")
    DB_NAME = os.getenv('REDSHIFT_DB_NAME') or input("Enter Database Name (default: sparkify): ") or 'sparkify'
    DB_USER = os.getenv('REDSHIFT_DB_USER') or input("Enter Database User (default: admin): ") or 'admin'
    DB_PASSWORD = os.getenv('REDSHIFT_DB_PASSWORD') or input("Enter Database Password: ")
    DB_PORT = os.getenv('REDSHIFT_DB_PORT') or input("Enter Database Port (default: 5439): ") or '5439'
    NODE_TYPE = os.getenv('REDSHIFT_NODE_TYPE') or input("Enter Node Type (default: dc2.large): ") or 'dc2.large'
    CLUSTER_TYPE = os.getenv('REDSHIFT_CLUSTER_TYPE') or input("Enter Cluster Type (default: multi-node): ") or 'multi-node'
    NUM_NODES = os.getenv('REDSHIFT_NUM_NODES') or input("Enter Number of Nodes (default: 4): ") or '4'
    
    cluster_params = {
        'cluster_identifier': CLUSTER_IDENTIFIER,
        'db_name': DB_NAME,
        'master_username': DB_USER,
        'master_password': DB_PASSWORD,
        'node_type': NODE_TYPE,
        'cluster_type': CLUSTER_TYPE,
        'num_nodes': NUM_NODES
    }
    
    # Create clients using environment variables
    ec2, iam, redshift = create_clients()
    
    # Create IAM role and attach policy
    IAM_ROLE_NAME = os.getenv('IAM_ROLE_NAME') or 'redshift-s3-access'
    iam_role_arn = create_iam_role(iam, IAM_ROLE_NAME)
    print(f"IAM Role ARN: {iam_role_arn}")
    
    # Update IAM_ROLE in config file
    update_config_file('dwh.cfg', 'IAM_ROLE', 'ARN', iam_role_arn)
    
    # Create Redshift cluster
    cluster = create_redshift_cluster(redshift, iam_role_arn, cluster_params)
    
    # Wait for cluster to become available
    cluster_props = wait_for_cluster_available(redshift, CLUSTER_IDENTIFIER)
    
    # Get cluster endpoint
    endpoint = cluster_props['Endpoint']['Address']
    print(f"Redshift Cluster Endpoint: {endpoint}")
    
    # Open TCP port for Redshift
    open_tcp_port(ec2, redshift, CLUSTER_IDENTIFIER, int(DB_PORT))
    
    # Update CLUSTER section in config file
    update_config_file('dwh.cfg', 'CLUSTER', 'HOST', endpoint)
    update_config_file('dwh.cfg', 'CLUSTER', 'DB_NAME', DB_NAME)
    update_config_file('dwh.cfg', 'CLUSTER', 'DB_USER', DB_USER)
    update_config_file('dwh.cfg', 'CLUSTER', 'DB_PASSWORD', DB_PASSWORD)
    update_config_file('dwh.cfg', 'CLUSTER', 'DB_PORT', DB_PORT)
    
    print("Configuration file updated successfully!")
    print("\nRedshift cluster is ready to use. You can now run:")
    print("1. python create_tables.py")
    print("2. python etl.py")
    print("3. python run_analytics.py")


def delete_redshift_cluster():
    """Delete the Redshift cluster and clean up resources."""
    # Load environment variables
    load_dotenv()
    
    # Read config file
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    # Get cluster identifier from .env or prompt if not found
    CLUSTER_IDENTIFIER = os.getenv('REDSHIFT_CLUSTER_ID') or input("Enter Redshift Cluster Identifier to delete: ")
    
    # Create redshift client using environment variables
    _, iam, redshift = create_clients()
    
    # Delete cluster
    print(f"Deleting Redshift cluster {CLUSTER_IDENTIFIER}...")
    try:
        redshift.delete_cluster(
            ClusterIdentifier=CLUSTER_IDENTIFIER,
            SkipFinalClusterSnapshot=True
        )
        print(f"Cluster {CLUSTER_IDENTIFIER} deletion initiated.")
    except Exception as e:
        print(f"Error deleting cluster: {e}")
    
    # Clean up IAM role
    role_name = os.getenv('IAM_ROLE_NAME') or input("Enter IAM Role Name to delete (default: redshift-s3-access): ") or 'redshift-s3-access'
    try:
        print(f"Detaching policies from IAM role {role_name}...")
        iam.detach_role_policy(
            RoleName=role_name,
            PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
        )
        
        print(f"Deleting IAM role {role_name}...")
        iam.delete_role(RoleName=role_name)
        print(f"IAM role {role_name} deleted successfully.")
    except Exception as e:
        print(f"Error cleaning up IAM role: {e}")


if __name__ == "__main__":
    print("Redshift Cluster Management Tool")
    print("--------------------------------")
    print("1. Set Up Redshift Cluster")
    print("2. Delete Redshift Cluster")
    choice = input("Select an option (1/2): ")
    
    if choice == '1':
        setup_redshift_cluster()
    elif choice == '2':
        delete_redshift_cluster()
    else:
        print("Invalid option. Please choose 1 or 2.") 