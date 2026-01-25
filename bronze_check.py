import subprocess
import sys

def run_hdfs_command(command):
    """Execute HDFS command safely with error handling"""
    try:
        result = subprocess.run(
            command,
            shell=False,
            check=True,
            capture_output=True,
            text=True
        )
        print(f"✓ Success: {' '.join(command)}")
        return True
    except Exception as e:
        print(f"✗ Error: {' '.join(command)}")
        print(f"  {e.stderr}")
        return False

def initialize_data_lake():
    """Initialize HDFS data lake with secure permissions"""
    
    # Define directories
    directories = [
        "/data_lake/bronze",
        "/data_lake/silver", 
        "/data_lake/gold"
    ]
    
    # Create directories
    for directory in directories:
        success = run_hdfs_command([
            "hdfs", "dfs", "-mkdir", "-p", directory
        ])
        if not success:
            print(f"Failed to create {directory}")
            sys.exit(1)
    
    # Set secure permissions: (owner: rwx, group: rx, others: none)
    # Adjust based on your specific requirements
    run_hdfs_command([
        "hdfs", "dfs", "-chmod", "-R", "775", "/data_lake"
    ])
    
    
    print("HDFS data lake initialized securely.")

if __name__ == "__main__":
    initialize_data_lake()