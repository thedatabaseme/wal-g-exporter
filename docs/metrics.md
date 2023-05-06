# Metrics
---

The following metrics are exported by wal-g-exporter:

| Metric name                           | labels              | description                                                                                    |
| ------------------------------------- | ------------------- | ---------------------------------------------------------------------------------------------- |
| walg_basebackup_count                 |                     | Number of basebackups stored on S3                                                             |
| walg_oldest_basebackup                |                     | Timestamp of the oldest basebackup                                                             |
| walg_newest_basebackup                |                     | Timestamp of the newest basebackup                                                             |
| walg_last_basebackup_duration         |                     | Duration in seconds of the last basebackup                                                     |
| walg_last_basebackup_throughput_bytes |                     | Throughput in bytes of the last basebackup                                                     |
| walg_wal_archive_count                |                     | Number of WAL archives stored on S3                                                            |
| walg_wal_archive_missing_count        |                     | Amount of missing WAL archives, will only be > 0 when `walg_wal_integrity_status` is `FAILURE` |
| walg_wal_integrity_status             | `OK`, `FAILURE`     | Can be `1` or `0`, while `1` means that the integrity_status is true                           |
| walg_last_upload                      | `basebackup`, `wal` | Timestamp of the last upload to S3 of the respective label / file type                         |
| walg_s3_diskusage                     |                     | Disk usage on S3 in byte for all backup / archive objects related to this Postgres instance    |