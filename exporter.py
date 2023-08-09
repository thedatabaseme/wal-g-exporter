#!/usr/bin/env python

"""
gitlab ci tool for generate Vault ACL policies
"""

import logging
import os
import sys

import psycopg2
import time
import argparse
import subprocess
import json
import datetime
import daemon
from prometheus_client import start_http_server, Gauge
from psycopg2.extras import DictCursor


class Exporter:
    def __init__(self, config):
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
        self.config = config

    # Fetch current basebackups located on S3
    def update_base_backup(self):

        logging.info('Updating backup metrics...')
        walg_data = ""
        try:
            # Fetch remote backup list
            res = subprocess.run(["wal-g", "backup-list",
                                  "--detail", "--json"],
                                 capture_output=True, check=True)
            walg_data = res.stdout.decode("utf-8")
        except subprocess.CalledProcessError as e:
            logging.error(str(e))

        if walg_data == "":
            base_backup_list = []
        else:
            base_backup_list = list(json.loads(walg_data))
            base_backup_list.sort(key=lambda base_backup: base_backup['start_time'])

        # Update backup list and export metrics
        if len(base_backup_list) > 0:
            logging.info("%s base backups found (first: %s, last: %s)",
                         len(base_backup_list),
                         base_backup_list[0]['start_time'],
                         base_backup_list[len(base_backup_list) - 1]['start_time'])

            # We need to convert the start_time to a timestamp
            oldest_base_backup_timestamp = datetime.datetime.strptime(base_backup_list[0]['start_time'],
                                                                      "%Y-%m-%dT%H:%M:%S.%fZ").timestamp()
            newest_base_backup_timestamp = datetime.datetime.strptime(
                base_backup_list[len(base_backup_list) - 1]['start_time'], "%Y-%m-%dT%H:%M:%S.%fZ").timestamp()
            last_base_backup_duration = datetime.datetime.strptime(
                base_backup_list[len(base_backup_list) - 1]['finish_time'], "%Y-%m-%dT%H:%M:%S.%fZ").timestamp(
            ) - datetime.datetime.strptime(base_backup_list[len(base_backup_list) - 1]['start_time'],
                                           "%Y-%m-%dT%H:%M:%S.%fZ").timestamp()
            last_base_backup_throughput = base_backup_list[len(base_backup_list) - 1][
                                              'compressed_size'] / last_base_backup_duration

            logging.info("Last backup duration: %s", last_base_backup_duration)

            # Set backup count, oldest and newest timestamp
            self.basebackup_count.set(len(base_backup_list))
            self.oldest_basebackup.set(oldest_base_backup_timestamp)
            self.newest_basebackup.set(newest_base_backup_timestamp)
            self.last_upload.labels('backup').set(newest_base_backup_timestamp)
            self.last_basebackup_duration.set(last_base_backup_duration)
            self.last_basebackup_throughput.set(last_base_backup_throughput)

            logging.info('Finished updating backup metrics...')
        else:
            logging.info("No base backups found")
            self.basebackup_count.set(0)
            self.oldest_basebackup.set(0)
            self.newest_basebackup.set(0)

    # Fetch WAL archives located on S3
    def update_wal_archive(self, db_connection):

        logging.info('Updating WAL archive metrics...')
        walg_data = ""
        try:
            # Fetch remote archive list
            res = subprocess.run(["wal-g", "wal-verify", "integrity", "--json"],
                                 capture_output=True, check=True)
            walg_data = res.stdout.decode("utf-8")

        except subprocess.CalledProcessError as e:
            logging.error(str(e))

        # Check json output of wal-g for the integrity status
        if walg_data == "":
            wal_archive_list = []
            wal_archive_integrity_status = []
        else:
            wal_archive_list = list(json.loads(walg_data)["integrity"]["details"])
            wal_archive_list.sort(key=lambda walarchive: walarchive['timeline_id'])
            wal_archive_integrity_status = json.loads(walg_data)["integrity"]["status"]

        wal_archive_count = 0
        wal_archive_missing_count = 0

        if len(wal_archive_list) > 0:
            # Update WAL archive list and export metrics
            # Count found and missing WAL archives
            for timelines in wal_archive_list:
                if timelines['status'] == 'FOUND':
                    wal_archive_count = wal_archive_count + timelines['segments_count']
                else:
                    wal_archive_missing_count = wal_archive_missing_count + timelines['segments_count']

            # Get archive status from database
            archive_status = self.get_archive_status(db_connection)

            # Log WAL information
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
        walg_data = ""
        try:
            # Fetch remote object list
            res = subprocess.run(["wal-g", "st", "ls", "-r"], capture_output=True, check=True)
            walg_data = res.stdout.decode("utf-8")

        except subprocess.CalledProcessError as e:
            logging.error(str(e))

        # Check output of S3 ls command
        if walg_data == "":
            s3_object_list = []
        else:
            s3_object_list = walg_data.split('\n')[1:]

        s3_diskusage = 0

        # Loop through the list of all objects and count the size
        if len(s3_object_list) > 0:
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

    @staticmethod
    def get_archive_status(db_connection):
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

    def run(self):
        logging.info("Startup...")
        logging.info('My PID is: %s', os.getpid())
        # Start up the server to expose the metrics.
        start_http_server(self.config.http_port)
        logging.info("Webserver started on port %s", self.config.http_port)
        logging.info("PGHOST %s", self.config.pg_host)
        logging.info("PGUSER %s", self.config.pg_user)
        logging.info("PGDATABASE %s", self.config.pg_database)
        logging.info("SSLMODE %s", self.config.pg_ssl_mode)
        logging.info("Starting exporter...")

        # Check if this is a primary instance
        # with while True and try catch this is how reconnect already should work.
        while True:
            try:
                with psycopg2.connect(
                        host=self.config.pg_host,
                        port=self.config.pg_port,
                        user=self.config.pg_user,
                        password=self.config.pg_password,
                        dbname=self.config.pg_database,
                        sslmode=self.config.pg_ssl_mode,
                ) as db_connection:

                    db_connection.autocommit = True
                    with db_connection.cursor() as pg_cursor:
                        try:
                            pg_cursor.execute("SELECT NOT pg_is_in_recovery()")
                            pg_is_primary = pg_cursor.fetchone()
                            logging.info("Is NOT in recovery mode? %s", pg_is_primary[0])

                            if bool(pg_is_primary) and pg_is_primary[0]:
                                logging.info("Connected to primary database")
                                logging.info("Evaluating wal-g backups...")

                                self.update_base_backup()
                                self.update_wal_archive(db_connection)
                                self.update_s3_disk_usage()

                                logging.info("All metrics collected. Waiting for next update cycle...")
                                time.sleep(self.config.scrape_interval)
                            else:
                                # If the exporter had run before and run on a replica suddenly, there was
                                # potentially a failover. So we kill our own process and start from scratch
                                logging.info("Running on replica, waiting for promotion...")
                                time.sleep(self.config.scrape_interval)
                        except Exception as e:
                            raise Exception("Unable to execute SELECT NOT pg_is_in_recovery()" + str(e))
            except Exception as e:
                logging.error(
                    "Error occurred, retrying in 60sec..." + str(e))
                time.sleep(60)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        '-l', '--log-level',
        default='info',
        choices=['critical', 'debug', 'error', 'info', 'warning'],
        help='Logging level. Default: %(default)s')
    parser.add_argument("--http-port", default=os.getenv('HTTP_PORT', 9351), type=int,
                        help="HTTP_PORT. Default: %(default)s")
    parser.add_argument("--pg-host", default=os.getenv('PGHOST', 'localhost'),
                        help="PGHOST. Default: %(default)s")
    parser.add_argument("--pg-port", default=os.getenv('PGPORT', '5432'),
                        help="PGPORT. Default: %(default)s")
    parser.add_argument("--pg-user", default=os.getenv('PGUSER', 'postgres'),
                        help="PGUSER. Default: %(default)s")
    parser.add_argument("--pg-database", default=os.getenv('PGDATABASE', 'postgres'),
                        help="PGDATABASE. Default: %(default)s")
    parser.add_argument("--pg-password", default=os.getenv('PGPASSWORD'),
                        help="PGPASSWORD. Default: %(default)s")
    parser.add_argument("--pg-ssl-mode", default=os.getenv('PGSSLMODE', 'require'),
                        help="PGSSLMODE. Default: %(default)s")
    parser.add_argument("--scrape-interval", default=os.getenv('SCRAPE_INTERVAL', 60), type=int,
                        help="SCRAPE_INTERVAL. Default: %(default)s")
    _args = parser.parse_args()
    logger = logging.getLogger()
    logging.basicConfig(level=getattr(logging, _args.log_level.upper()))
    print(_args.http_port)
    with daemon.DaemonContext(stdout=sys.stdout, stderr=sys.stdout, detach_process=False):
        exporter = Exporter(_args)
        exporter.run()
