"""
Setup Airflow Variables for MinIO and RabbitMQ
Usage: python setup_airflow_variables.py
"""

import subprocess
import sys

def set_variable(key, value):
    """Set an Airflow variable"""
    try:
        cmd = ['airflow', 'variables', 'set', key, str(value)]
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        if result.returncode == 0:
            print(f"✓ Set {key} = {value}")
            return True
        else:
            print(f"✗ Failed to set {key}: {result.stderr}")
            return False
    except FileNotFoundError:
        print("✗ Error: 'airflow' command not found. Is Airflow installed?")
        return False
    except Exception as e:
        print(f"✗ Error setting {key}: {str(e)}")
        return False

def setup_minio_variables():
    """Setup MinIO configuration variables"""
    
    print("\n" + "=" * 80)
    print("Setting up MinIO Variables")
    print("=" * 80)
    
    minio_vars = {
        'minio_endpoint': '192.168.100.17:9000',
        'minio_access_key': 'admin',
        'minio_secret_key': '12345678',
        'minio_bucket_name': 'demokcb-minhvd',
        'minio_xml_bucket_name': 'demokcb-minhvd',
        'minio_xml_folder_path': 'XmlFiles',
        'minio_use_ssl': 'false'
    }
    
    success_count = 0
    for key, value in minio_vars.items():
        if set_variable(key, value):
            success_count += 1
    
    print(f"\nMinIO: {success_count}/{len(minio_vars)} variables set successfully")
    return success_count == len(minio_vars)

def setup_rabbitmq_variables():
    """Setup RabbitMQ configuration variables"""
    
    print("\n" + "=" * 80)
    print("Setting up RabbitMQ Variables")
    print("=" * 80)
    
    rabbitmq_vars = {
        'rabbitmq_host': 'localhost',
        'rabbitmq_port': '5672',
        'rabbitmq_username': 'guest',
        'rabbitmq_password': 'guest',
        'rabbitmq_exchange': 'xml.exchange',
        'rabbitmq_queue': 'xml.queue',
        'rabbitmq_routing_key': 'xml.key'
    }
    
    success_count = 0
    for key, value in rabbitmq_vars.items():
        if set_variable(key, value):
            success_count += 1
    
    print(f"\nRabbitMQ: {success_count}/{len(rabbitmq_vars)} variables set successfully")
    return success_count == len(rabbitmq_vars)

def setup_api_variables():
    """Setup API configuration variables"""
    
    print("\n" + "=" * 80)
    print("Setting up API Variables")
    print("=" * 80)
    
    api_vars = {
        'bhyt_api_endpoint': 'https://localhost:44380/api/app/files/test',
        'bhyt_output_bucket': 'bhyt-processed-json'
    }
    
    success_count = 0
    for key, value in api_vars.items():
        if set_variable(key, value):
            success_count += 1
    
    print(f"\nAPI: {success_count}/{len(api_vars)} variables set successfully")
    return success_count == len(api_vars)

def list_variables():
    """List all configured variables"""
    
    print("\n" + "=" * 80)
    print("Current Airflow Variables")
    print("=" * 80)
    
    try:
        cmd = ['airflow', 'variables', 'list']
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        if result.returncode == 0:
            print(result.stdout)
        else:
            print(f"Failed to list variables: {result.stderr}")
    except Exception as e:
        print(f"Error listing variables: {str(e)}")

def main():
    """Main function"""
    
    print("\n")
    print("=" * 80)
    print("Airflow Variables Setup Script")
    print("BHYT ETL Pipeline - MinIO & RabbitMQ Configuration")
    print("=" * 80)
    
    # Check if we can run airflow commands
    try:
        result = subprocess.run(['airflow', 'version'], capture_output=True, text=True)
        if result.returncode == 0:
            print(f"\n✓ Airflow found: {result.stdout.strip()}")
        else:
            print("\n✗ Error: Could not run Airflow commands")
            print("Make sure Airflow is installed and in your PATH")
            sys.exit(1)
    except FileNotFoundError:
        print("\n✗ Error: Airflow is not installed or not in PATH")
        print("Install Airflow first: pip install apache-airflow")
        sys.exit(1)
    
    # Prompt user for what to setup
    print("\nWhat would you like to setup?")
    print("1. MinIO variables only")
    print("2. RabbitMQ variables only")
    print("3. API variables only")
    print("4. All variables (MinIO + RabbitMQ + API)")
    print("5. List current variables")
    print("6. Exit")
    
    choice = input("\nEnter your choice (1-6): ").strip()
    
    if choice == '1':
        setup_minio_variables()
    elif choice == '2':
        setup_rabbitmq_variables()
    elif choice == '3':
        setup_api_variables()
    elif choice == '4':
        minio_ok = setup_minio_variables()
        rabbitmq_ok = setup_rabbitmq_variables()
        api_ok = setup_api_variables()
        
        print("\n" + "=" * 80)
        if minio_ok and rabbitmq_ok and api_ok:
            print("✓ All variables configured successfully!")
        else:
            print("⚠ Some variables failed to configure. Check the output above.")
        print("=" * 80)
    elif choice == '5':
        list_variables()
    elif choice == '6':
        print("\nExiting...")
        sys.exit(0)
    else:
        print("\n✗ Invalid choice")
        sys.exit(1)
    
    # Offer to list variables
    print("\n")
    show_list = input("Would you like to see all configured variables? (y/n): ").strip().lower()
    if show_list == 'y':
        list_variables()
    
    print("\n✓ Setup complete!")
    print("\nNext steps:")
    print("1. Ensure MinIO is running at 192.168.100.17:9000")
    print("2. Ensure RabbitMQ is running at localhost:5672")
    print("3. Test MinIO connection: python test_minio_connection.py")
    print("4. Publish test message: python test_rabbitmq_publisher.py")
    print("5. Run the pipeline: airflow dags trigger bhyt_complete_etl_pipeline_v2")

if __name__ == "__main__":
    main()
