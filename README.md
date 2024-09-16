# OpenStack-DB-Usage-Exporter

`openstack_db_usage_json_exporter.py` is a Python tool designed to calculate compute resource usage from an OpenStack environment by directly querying the database.  
It generates a comprehensive JSON output detailing resource consumption per project, including compute, volume, image, snapshot, and optionally, load balancer usage.

#### TLDR

- Dependencies: Requires only Python and PyMySQL. Directly queries OpenStack databases without relying on OpenStack APIs or additional tools.
- Comprehensive Coverage: Calculates usage for compute (CPU/RAM), volumes, images, snapshots, and optionally load balancers.
- State Multipliers: Adjusts calculations according to instance states (e.g., active, stopped, deleted) using customizable multipliers.  
- JSON Output: Outputs usage data in JSON format, organized by project for easy integration with other tools.


## Limitations

- The script cannot query historical data for volumes and Octavia load balancers. It provides a snapshot of the __current state__  projected over the specified time range.
- Requires direct database access, which may not be available in all OpenStack deployments.
- Performance may be impacted when querying large datasets or long time ranges. 

## Prerequisites

- Access to OpenStack databases (Nova, Cinder, Glance, Keystone, and optionally Octavia)

### Dependencies
The script should be compatible with Python 3.6 and above.  
Ensure you have the pymysql library installed:

```bash
pip install pymysql
```
### Environment Variables  
The tool uses environment variables for database connection parameters.  
For setting up the necessary database permissions, refer to the Database User Setup  section #database-User-Setup 

```bash
export DB_HOST="host"
export DB_PORT="port"
export DB_USER="user"
export DB_PASSWORD="password"
```

### Usage
The script provides command-line arguments to specify the time range, project ID, and whether to enable debug output.

Command-Line Arguments
- `--start-time`: Start time in format YYYY-MM-DD HH:MM:SS (required).
- `--end-time`: End time in format YYYY-MM-DD HH:MM:SS (required).
- `--project-id`: Optional project ID to filter the usage calculation to a specific project.
- `--debug`: Enable debug output to include detailed information.
- `--octavia`: Include Octavia load balancer usage
- `--yes-i-know-that-i-can-not-query-historical-data`: Acknowledge that historical data cannot be queried for volumes and Octavia
- `--exclude-project`: Project ID or name to exclude from calculation (can be specified multiple times)

### Example Usage

Calculate usage for all projects within a time range
```bash
python openstack_db_usage_json_exporter.py --start-time "2024-01-01 00:00:00" --end-time "2024-02-01 00:00:00"
```
Calculate usage for a specific project
```bash
python openstack_db_usage_json_exporter.py --start-time "2024-01-01 00:00:00" --end-time "2024-02-01 00:00:00" --project-id "0fd375afec134f7681032db770c9a473"
```
Enable debug output for detailed information
```bash
python openstack_db_usage_json_exporter.py --start-time "2024-01-01 00:00:00" --end-time "2024-02-01 00:00:00" --debug
```
Calculate usage for all projects with volumes usage
```bash
python openstack_db_usage_json_exporter.py --start-time "2024-01-01 00:00:00" --end-time "2024-02-01 00:00:00" --yes-i-know-that-i-can-not-query-historical-data 
``` 

Calculate usage for all projects with volumes and LoadBalancer usage
```bash
python openstack_db_usage_json_exporter.py --start-time "2024-01-01 00:00:00" --end-time "2024-02-01 00:00:00" --octavia --yes-i-know-that-i-can-not-query-historical-data 
``` 

### Hacking and Testing

It is unreasonable to develop this in Production.  
You can quickly set up a local `MySQL` service and test your database backup locally using `nix` and `devenv`.


```bash
curl --proto '=https' --tlsv1.2 -sSf -L https://install.determinate.systems/nix | sh -s -- install
nix-shell -p devenv
```


###  devenv 

`devenv up`

This will create the `.devenv/state` folder where you can restore your production backup too.  
As soon as it is up, it can be stopped to `mariabackup --prepare` data in the place. 



### Populating your new development Database

Here's an example workflow to populate the MySQL database with production data:
```bash
nix-shell -p mariadb_106
FILENAME="mysqlbackup-00-00-2024-numnumnum.qp.xbc.xbs.gz" 
scp $OPENSTACK_BACKUP:/var/lib/docker/volumes/mariadb_backup/_data/${FILENAME} .

gunzip ${FILENAME}
mkdir -p ${PWD}/out
mbstream -x -C ${PWD}/out < "${FILENAME%%.gz}"
mariabackup --prepare --target-dir ${PWD}/out
# samples/out is now a /var/lib/mysql dir
mv ${PWD}/out .devenv/state/mysql 
```
Note: Be cautious with handling production data and ensure compliance with data security policies.
I like to use `DBeaver` dig around the tables. 

### Customization

**Adjust State Multipliers**: Modify the state_charge_multipliers dictionary in the script if your billing policies assign different multipliers to instance states.


### Database User Setup
For obvius reasons, it's recommended to use a dedicated database user with read-only access to the required tables. 


- Connect to your MySQL server as an administrative user:
```bash
  mysql -u root -p
```


As time of writing the needed permissions looks like this:   
> ! Remember to change the default password to a secure one!
```sql
START TRANSACTION;
CREATE USER 'openstack_usage_reader'@'localhost' IDENTIFIED BY 'YOUREALLYHAVETOCHANGEME';
GRANT SELECT ON nova.instances TO 'openstack_usage_reader'@'localhost';
GRANT SELECT ON nova.instance_actions TO 'openstack_usage_reader'@'localhost';
GRANT SELECT ON nova.migrations TO 'openstack_usage_reader'@'localhost';

-- Nova API database
GRANT SELECT ON nova_api.flavors TO 'openstack_usage_reader'@'localhost';

-- Cinder database
GRANT SELECT ON cinder.volumes TO 'openstack_usage_reader'@'localhost';
GRANT SELECT ON cinder.snapshots TO 'openstack_usage_reader'@'localhost';
GRANT SELECT ON cinder.reservations TO 'openstack_usage_reader'@'localhost';
GRANT SELECT ON cinder.volume_types TO 'openstack_usage_reader'@'localhost';

-- Glance database
GRANT SELECT ON glance.images TO 'openstack_usage_reader'@'localhost';

-- Keystone database
GRANT SELECT ON keystone.project TO 'openstack_usage_reader'@'localhost';

-- If you need to rollback the changes, use:
-- ROLLBACK;
-- else:
COMMIT;
FLUSH PRIVILEGES;

```

- Exit the MySQL client:
```sql
EXIT;
```
