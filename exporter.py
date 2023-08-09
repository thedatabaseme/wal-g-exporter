import logging
import os
import psycopg2
import time
import signal
import argparse
import subprocess
import json
import datetime
import signal

from prometheus_client import start_http_server, Gauge
from psycopg2.extras import DictCursor

# Argument configuration
parser = argparse.ArgumentParser()
parser.add_argument("--debug", help="enable debug log", action="store_true")
args = parser.parse_args()
if args.debug:
    logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.DEBUG)
else:
    logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)

terminate = False;

def signal_handler(sig, frame):
    global terminate
    logging.info('SIGTERM received, preparing to shut down...')
    terminate = True

# Class definition
class Exporter():
    def __init__(self):
        self.basebackup_count = Gauge('walg_basebackup_count', 'Remote Basebackups count')
        self.oldest_basebackup = Gauge('walg_oldest_basebackup', 'oldest full backup')
        self.newest_basebackup = Gauge('walg_newest_basebackup', 'newest full backup')
        self.last_basebackup_duration = Gauge('walg_last_basebackup_duration',
                                          'Duration of the last basebackup in seconds')
        self.last_basebackup_throughput = Gauge('walg_last_basebackup_throughput_bytes',
                                                'Show the throuhput in bytes per second for the last backup')
        self.wal_archive_count = Gauge('walg_wal_archive_count', 'Archived WAL count')
        self.wal_archive_missing_count = Gauge('walg_wal_archive_missing_count', 'Missing WAL count')
        self.wal_integrity_status = Gauge('walg_wal_integrity_status', 'Overall WAL archive integrity status',
                                          ['status'])
        self.last_upload = Gauge('walg_last_upload', 'Last upload of incremental or full backup',
                                 ['type'])
        self.s3_diskusage = Gauge('walg_s3_diskusage', 'Usage of S3 storage in bytes')

    # Fetch current basebackups located on S3
    def update_basebackup(self):

        logging.info('Updating basebackup metrics...')
        try:
            # Fetch remote backup list
            res = subprocess.run(["wal-g", "backup-list",
                                  "--detail", "--json"],
                                 capture_output=True, check=True)

        except subprocess.CalledProcessError as e:
            logging.error(str(e))

        if res.stdout.decode("utf-8") == "":
            basebackup_list = []
        else:
            basebackup_list = list(json.loads(res.stdout))
            basebackup_list.sort(key=lambda basebackup: basebackup['start_time'])

        # Update backup list and export metrics
        if (len(basebackup_list) > 0):
            logging.info("%s basebackups found (first: %s, last: %s)",
                    len(basebackup_list),
                     basebackup_list[0]['start_time'],
                     basebackup_list[len(basebackup_list) - 1]['start_time'])
            
            # We need to convert the start_time to a timestamp
            oldest_basebackup_timestamp = datetime.datetime.strptime(basebackup_list[0]['start_time'], "%Y-%m-%dT%H:%M:%S.%fZ").timestamp()
            newest_basebackup_timestamp = datetime.datetime.strptime(basebackup_list[len(basebackup_list) - 1]['start_time'], "%Y-%m-%dT%H:%M:%S.%fZ").timestamp()
            last_basebackup_duration = datetime.datetime.strptime(basebackup_list[len(basebackup_list) - 1]['finish_time'], "%Y-%m-%dT%H:%M:%S.%fZ").timestamp(
                ) - datetime.datetime.strptime(basebackup_list[len(basebackup_list) - 1]['start_time'], "%Y-%m-%dT%H:%M:%S.%fZ").timestamp()
            last_basebackup_throughput = basebackup_list[len(basebackup_list) - 1]['compressed_size'] / last_basebackup_duration

            logging.info("Last basebackup duration: %s", last_basebackup_duration)

            # Set basebackup count, oldest and newest timestamp
            self.basebackup_count.set(len(basebackup_list))
            self.oldest_basebackup.set(oldest_basebackup_timestamp)
            self.newest_basebackup.set(newest_basebackup_timestamp)
            self.last_upload.labels('basebackup').set(newest_basebackup_timestamp)
            self.last_basebackup_duration.set(last_basebackup_duration)
            self.last_basebackup_throughput.set(last_basebackup_throughput)

            logging.info('Finished updating basebackup metrics...')
        else:
            logging.info("No basebackups found")
            self.basebackup_count.set(0)
            self.oldest_basebackup.set(0)
            self.newest_basebackup.set(0)

    # Fetch WAL archives located on S3
    def update_wal_archive(self):

        logging.info('Updating WAL archive metrics...')
        try:
            # Fetch remote archive list
            res = subprocess.run(["wal-g", "wal-verify", "integrity", "--json"],
                                 capture_output=True, check=True)

        except subprocess.CalledProcessError as e:
            logging.error(str(e))

        # Check json output of wal-g for the integrity status
        if res.stdout.decode("utf-8") == "":
            wal_archive_list = []
            wal_archive_integrity_status = []
        else:
            wal_archive_list = list(json.loads(res.stdout)["integrity"]["details"])
            wal_archive_list.sort(key=lambda walarchive: walarchive['timeline_id'])
            wal_archive_integrity_status = json.loads(res.stdout)["integrity"]["status"]

        wal_archive_count = 0
        wal_archive_missing_count = 0

        if (len(wal_archive_list) > 0):
            # Update WAL archive list and export metrics
            # Count found and missing WAL archives
            for timelines in wal_archive_list:
                if timelines['status'] == 'FOUND':
                    wal_archive_count = wal_archive_count + timelines['segments_count']
                else:
                    wal_archive_missing_count = wal_archive_missing_count + timelines['segments_count']

            # Get archive status from database
            archive_status = self.get_archive_status()

            # Log WAL informations
            logging.info("WAL integrity status is: %s", wal_archive_integrity_status)
            logging.info("Found %s WAL archives in %s timelines, %s WAL archives missing",
                         wal_archive_count, len(wal_archive_list), wal_archive_missing_count)
            
            # Update all WAL related metrics
            # Check for the integrity status and set the metrics accordingly
            if wal_archive_integrity_status == 'OK':
                self.wal_integrity_status.labels('OK').set(1)
                self.wal_integrity_status.labels('FAILURE').set(0)
            else:
                self.wal_integrity_status.labels('OK').set(0)
                self.wal_integrity_status.labels('FAILURE').set(1)
            
            self.wal_archive_count.set(wal_archive_count)
            self.wal_archive_missing_count.set(wal_archive_missing_count)
            self.last_upload.labels('wal').set(archive_status['last_archived_time'].timestamp())

            logging.info('Finished updating WAL archive metrics...')
        else:
            logging.info("No WAL archives found")
            self.wal_archive_count.set(0)

    # Fetch S3 object list for disk usage calculation
    def update_s3_disk_usage(self):

        logging.info('Updating S3 disk usage...')
        try:
            # Fetch remote object list
            res = subprocess.run(["wal-g", "st", "ls", "-r"], capture_output=True, check=True)

        except subprocess.CalledProcessError as e:
            logging.error(str(e))

        # Check output of S3 ls command 
        if res.stdout.decode("utf-8") == "":
            s3_object_list = []
        else:
            s3_object_list = res.stdout.decode().split('\n')[1:]

        s3_diskusage=0

        # Loop through the list of all objects and count the size
        if (len(s3_object_list) > 0):
            for s3_object in s3_object_list:
                if s3_object.strip():
                    s3_object = s3_object.split(' ')
                    s3_diskusage = s3_diskusage + int(s3_object[2])

            logging.info("S3 diskusage in bytes: %s", s3_diskusage)

            self.s3_diskusage.set(s3_diskusage)

            logging.info('Finished updating S3 metrics...')
        else:
            logging.info("No S3 objects found")
            self.s3_diskusage.set(0)

    def get_archive_status(self):
        with db_connection.cursor(cursor_factory=DictCursor) as pg_archive_status_cursor:
            try:
                pg_archive_status_cursor.execute('SELECT archived_count, failed_count, '
                    'last_archived_wal, '
                    'last_archived_time, '
                    'last_failed_wal, '
                    'last_failed_time '
                    'FROM pg_stat_archiver')
                pg_archive_status = pg_archive_status_cursor.fetchone()
                if not bool(pg_archive_status) or not pg_archive_status[0]:
                    logging.warning("Cannot fetch archive status")
                else:
                    return pg_archive_status
            except Exception as e:
                logging.error(
                    "Unable to fetch archive status from pg_stat_archiver")
                raise Exception(
                    "Unable to fetch archive status from pg_stat_archiver" + str(e))

# Main loop
if __name__ == '__main__':
    logging.info("Startup...")
    logging.info('My PID is: %s', os.getpid())

    # Register the signal handler for SIGTERM
    signal.signal(signal.SIGTERM, signal_handler)

    logging.info("Reading environment configuration")

    # Read the configuration
    http_port = int(os.getenv('HTTP_PORT', 9351))
    pg_host = os.getenv('PGHOST', 'localhost')
    pg_port = os.getenv('PGPORT', '5432')
    pg_user = os.getenv('PGUSER', 'postgres')
    pg_database = os.getenv('PGDATABASE', 'postgres')
    pg_password = os.getenv('PGPASSWORD')
    pg_ssl_mode = os.getenv('PGSSLMODE', 'require')
    wal_g_scrape_interval = int(os.getenv('WAL_G_SCRAPE_INTERVAL', 60))
    first_start = True

    # Start up the server to expose the metrics.
    http_server = start_http_server(http_port)
    logging.info("Webserver started on port %s", http_port)
    logging.info("PGHOST %s", pg_host)
    logging.info("PGUSER %s", pg_user)
    logging.info("PGDATABASE %s", pg_database)
    logging.info("SSLMODE %s", pg_ssl_mode)

    logging.info("Starting exporter...")

    # Check if this is a primary instance
    # with while True and try catch this is how reconnect already should work.
    while True:

        if terminate:
            logging.info("Received SIGTERM, shutting down...")
            break

        try:
            with psycopg2.connect(
                host = pg_host,
                port = pg_port,
                user = pg_user,
                password = pg_password,
                dbname = pg_database,
                sslmode = pg_ssl_mode,
            ) as db_connection:

                db_connection.autocommit = True
                with db_connection.cursor() as pg_cursor:
                    try:
                        pg_cursor.execute("SELECT NOT pg_is_in_recovery()")
                        pg_is_primary = pg_cursor.fetchone()
                        logging.info("Is NOT in recovery mode? %s",
                                     pg_is_primary[0])

                        if bool(pg_is_primary) and pg_is_primary[0]:
                            logging.info("Connected to primary database")
                            logging.info("Evaluating wal-g backups...")

                            # To recognize a later failover, we set first_start = False now
                            if first_start:
                                exporter = Exporter()
                                first_start = False

                            exporter.update_basebackup()
                            exporter.update_wal_archive()
                            exporter.update_s3_disk_usage()

                            logging.info(
                                "All metrics collected. Waiting for next update cycle...")
                            time.sleep(wal_g_scrape_interval)
                        else:
                            # If the exporter had run before and run on a replica suddenly, there was
                            # potentially a failover. So we kill our own process and start from scratch
                            if not first_start:
                                logging.info(
                                    "Potential failover detected. Clearing old metrics. Stopping exporter.")
                                os.kill(os.getpid(), signal.SIGTERM)

                            logging.info(
                                "Running on replica, waiting for promotion...")
                            time.sleep(wal_g_scrape_interval)
                    except Exception as e:
                        logging.error(
                            "Unable to execute SELECT NOT pg_is_in_recovery()")
                        raise Exception(
                            "Unable to execute SELECT NOT pg_is_in_recovery()" + str(e))
        except Exception as e:
            if terminate:
                logging.info("Received SIGTERM during exception, shutting down...")
                break
            logging.error(
                "Error occured, retrying in 60sec..." + str(e))
            time.sleep(wal_g_scrape_interval)
