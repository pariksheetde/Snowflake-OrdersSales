import logging
import re
import os
import signal
import sys
from pathlib import PurePosixPath
from email.utils import formatdate
from datetime import datetime, timezone
from collections import Counter

import boto3
import yaml
from boto3.s3.transfer import TransferConfig
from botocore.exceptions import ClientError

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

LOG = logging.getLogger("flywheel_ingestion")
TRANSFER = TransferConfig(
                          multipart_threshold=64 * 1024 * 1024,
                          multipart_chunksize=64 * 1024 * 1024
                          )

# Failure notification limits
MAX_FILE_FAILURES_PER_STUDY = 10    # stop a study after this many failed files (e.g. access denied)
MAX_FAILURES_IN_EMAIL = 50          # failure lines listed in the email body (the log has all of them)

# NAME_PREFIXES = ("0000-", "R", "VG")
NAME_PREFIXES = ("0000-", "VG")
NAME_R_DIGIT = re.compile(r"R\d")
NAME_CONTAINS = ("-", "_")


class Terminated(BaseException):
    """Raised on SIGTERM (Airflow task timeout / cancel) so a failure email can still be sent.
    It is a BaseException on purpose: 'except Exception' blocks must not swallow it."""


class StudyCopyError(Exception):
    """Raised when one or more files of a study were not transferred."""
    def __init__(self, study, failed_files):
        self.study = study
        self.failed_files = failed_files        # list of (source_key, reason)
        super().__init__("{} file(s) not transferred in study {}".format(len(failed_files), study))


def folder_matches(name):
    return (name.startswith(NAME_PREFIXES)
            or NAME_R_DIGIT.match(name) is not None
            or any(char in name for char in NAME_CONTAINS))

def join_key(*parts):
    return "/".join(str(part).strip("/") for part in parts if part is not None and str(part).strip("/"))


def normalize_prefix(value):
    return str(value).strip().lstrip("/").rstrip("/") + "/"


def validate_buckets(s3, config):
    for label, bucket in (
        ("RTS source", config["rts_bucket"]),
        ("SDL target", config["sdl_bucket"])
    ):
        try:
            s3.head_bucket(Bucket=bucket)
            LOG.info("%s bucket is accessible: %s", label, bucket)
        except ClientError as error:
            code = error.response.get("Error", {}).get("Code", "Unknown")
            raise RuntimeError("{} bucket unavailable: {}; error={}".format(label, bucket, code)) from error


def ensure_top_level_folders(s3, config):
    s3.put_object(
        Bucket = config["sdl_bucket"],
        Key = join_key(
            config["s3_root_prefix"],
            "Studies",
            ".keep"
        ),
        Body = b"",
        ServerSideEncryption = config.get(
            "encryption",
            "AES256"
        ),
    )

# ============ SCENARIO 3: incremental load (checks the destination bucket) ============
# No state file. Before copying a study, list what already exists under its destination
# folder (needs only s3:ListBucket) and skip every file that is already there with the
# same size. A study whose files are all present is skipped completely, and a study that
# failed halfway only copies the files that are still missing on the next run.
def list_destination(s3, config, destination_base):
    existing = {}
    paginator = s3.get_paginator("list_objects_v2")

    for page in paginator.paginate(Bucket = config["sdl_bucket"], Prefix = destination_base + "/"):
        for destination_object in page.get("Contents", []):
            existing[destination_object["Key"]] = destination_object["Size"]

    return existing


def list_studies(s3, bucket, source_prefix):
    studies = {}
    ignored = set()      # folder names skipped by the Scenario 1 name filter
    paginator = s3.get_paginator("list_objects_v2")

    for page in paginator.paginate(Bucket = bucket,Prefix = source_prefix):
        for source_object in page.get("Contents", []):
            source_key = source_object["Key"]
            if source_key.endswith("/"):
                continue
            relative_key = (source_key[len(source_prefix):] if source_key.startswith(source_prefix) else "")

            parts = PurePosixPath(relative_key).parts

            if len(parts) < 2:
                LOG.warning("Ignoring object outside a study folder: %s", source_key)

                continue

            study_name = parts[0]

            # SCENARIO 1: ignore folders whose name doesn't match the filter
            if not folder_matches(study_name):
                ignored.add(study_name)
                continue

            studies.setdefault(
                study_name,
                []
            ).append(source_object)

    if ignored:
        LOG.info("Ignored %d folder(s) not matching the name filter under %s: %s",
                 len(ignored), source_prefix, sorted(ignored))

    return studies

def copy_source_object(s3,config,source_key,destination_key):
    s3.copy({
        "Bucket" : config["rts_bucket"],
        "Key" : source_key
    },
    config["sdl_bucket"],
    destination_key,
    ExtraArgs = {
        "MetadataDirective" : "COPY",
        "ServerSideEncryption" : config.get(
            "encryption",
            "AES256"
        ),
        },
        Config = TRANSFER,
    )

def process_study(s3, config, source_name, mapping, study, objects, copied_files):
    copied = 0
    source_prefix = normalize_prefix(mapping["source_prefix"])


    destination_base = join_key(
        config["s3_root_prefix"],
        "Studies",
        study,
        "Zipped")


    # SCENARIO 3: what is already in the destination folder from earlier runs
    existing = list_destination(s3, config, destination_base)
    skipped = 0
    failed_files = []        # (source_key, reason) for every file that was NOT transferred
    new_files = []           # files copied in this run; verified below before they are counted

    study_prefix = source_prefix + study + "/"
    for source_object in objects:
        source_key = source_object["Key"]
        relative_path = source_key[len(study_prefix):]
        destination_key = join_key(destination_base, relative_path)

        # SCENARIO 3: already copied (same key and same size) -> do not copy again
        if existing.get(destination_key) == source_object["Size"]:
            skipped += 1
            continue

        LOG.info("Copying %s -> %s", source_key, destination_key)

        # FAILURE NOTIFICATION: a file that cannot be copied is recorded and the study
        # carries on with its other files; the failure is reported in the email.
        try:
            copy_source_object(s3, config, source_key, destination_key)
        except Exception as error:
            LOG.error("File not transferred: %s | %s: %s", source_key, type(error).__name__, error)
            failed_files.append((source_key, "{}: {}".format(type(error).__name__, error)))
            if len(failed_files) >= MAX_FILE_FAILURES_PER_STUDY:
                LOG.error("Stopping study %s after %d failed files; the rest is retried on the next run",
                          study, len(failed_files))
                break
            continue

        new_files.append({
            "study" : study,
            "source" : source_key,
            "target" : destination_key,
            "size" : source_object["Size"],
            "arrival_time" : source_object["LastModified"],
            "load_time" : datetime.now(timezone.utc)
        })

    # FAILURE NOTIFICATION: verify that every file copied in this run is really in the target
    # bucket with the same size (needs only s3:ListBucket). Anything else counts as not transferred.
    if new_files:
        in_target = list_destination(s3, config, destination_base)
        for item in new_files:
            if in_target.get(item["target"]) == item["size"]:
                copied_files.append(item)
                copied += 1
            else:
                LOG.error("File missing in target after copy: %s", item["source"])
                failed_files.append((item["source"], "not found in the target bucket after copy (or size differs)"))

    LOG.info("Study completed %s, files copied= %d, already present= %d, failed= %d",
             study, copied, skipped, len(failed_files))

    if failed_files:
        raise StudyCopyError(study, failed_files)

    return "SKIPPED" if copied == 0 else "PROCESSED"

def send_email(ses, sender, recipients, subject, body):
    ses.send_email(
        Source = sender,
        Destination = {
            "ToAddresses" : recipients
        },
        Message = {
            "Subject" : {
                "Data" : subject
            },
            "Body" : {
                "Text" : {
                    "Data" : body
                }
            }
        }
    )

    LOG.info("Notification email sent to %s", ",".join(recipients))

def notify(ses, config, subject, body):
    """Send an email without ever raising: a broken notification must not hide the real result."""
    sender = config.get("email_sender")
    recipients = config.get("email_recipients") or []
    if isinstance(recipients, str):
        recipients = [recipients]

    if not sender or not recipients:
        LOG.warning("email_sender / email_recipients not configured - email NOT sent: %s", subject)
        return

    try:
        send_email(ses, sender, recipients, subject, body)
    except Exception as error:
        LOG.exception("Failed to send notification email '%s': %s", subject, error)

def notify_success(ses, config, copied_files):
    email_body = []
    email_body.append(
        "Flywheel Ingestion Completed Successfully"
    )
    email_body.append("")
    email_body.append("Execution Timestamp: {}".format(formatdate(localtime=True)))
    email_body.append("Files Copied: {}".format(len(copied_files)))
    email_body.append("")
    email_body.append("Loaded Files: ")
    email_body.append(
        "---------------------------------------------------------")
    study_counts = Counter(item["study"] for item in copied_files)

    for item in copied_files:
        email_body.append("""
        Study : {}
        File Count : {}
        Source : {}
        Target : {}
        Size : {} bytes
        Source Arrival : {}
        Target Load Time : {}
        ------------------------------------------------------------------------------------------------------------------


        """.format(
            item["study"],
            study_counts[item["study"]],
            item["source"],
            item["target"],
            item["size"],
            item["arrival_time"].strftime("%Y-%m-%d %H:%M:%S UTC"),
            item["load_time"].strftime("%Y-%m-%d %H:%M:%S UTC")
        )
        )

    # one email for the whole run (not one per file)
    notify(ses, config, "Flywheel Ingestion Notification", "\n".join(email_body))

def notify_failure(ses, config, stats, failures, copied_files):
    failure_body = []
    failure_body.append(
        "Flywheel Ingestion Failed"
    )

    failure_body.append("")

    failure_body.append(
        "Execution Timestamp : {}".format(formatdate(localtime=True)))

    failure_body.append("")

    failure_body.append(
        "Processed Studies : {}".format(stats["processed"])
    )

    failure_body.append(
        "Skipped Studies : {}".format(stats["skipped"])
    )

    failure_body.append(
        "Failed Studies : {}".format(stats["failed"])
    )

    failure_body.append(
        "Files Copied Before Failure : {}".format(len(copied_files))
    )

    failure_body.append(
        "Failure Count : {}".format(len(failures))
    )

    failure_body.append("")
    failure_body.append("Failure Details: ")

    failure_body.append("-----------------------------------------------------------")
    for failure in failures[:MAX_FAILURES_IN_EMAIL]:
        failure_body.append(failure)

    if len(failures) > MAX_FAILURES_IN_EMAIL:
        failure_body.append("... and {} more (see the job log)".format(len(failures) - MAX_FAILURES_IN_EMAIL))

    notify(ses, config, "[FAILED] Flywheel Ingestion Notification", "\n".join(failure_body))

def run_ingestion(session, config, stats, failures, copied_files):
    LOG.info("AWS identity: %s", session.client("sts").get_caller_identity()["Arn"])

    s3 = session.client("s3")

    validate_buckets(s3, config)
    ensure_top_level_folders(s3, config)

    for source_name, mapping in config["sources"].items():
        source_prefix = normalize_prefix(mapping["source_prefix"])

        # FAILURE NOTIFICATION: if a source cannot be listed, record it and carry on with the others
        try:
            studies = list_studies(
                s3,
                config["rts_bucket"],
                source_prefix
            )
        except Exception as error:
            stats["failed"] += 1
            failures.append("Source = {} | Error = could not list {}: {}: {}".format(
                source_name, source_prefix, type(error).__name__, error))
            LOG.exception("Source failed: %s", source_name)
            continue

        LOG.info("Found %d non-empty studies under %s", len(studies), source_prefix)

        for study, objects in sorted(studies.items()):
            LOG.info("Discovered study: %s | objects=%d", study, len(objects))

            try:
                # SCENARIO 3: process_study returns "SKIPPED" when everything was already copied
                result = process_study(s3, config, source_name, mapping, study, objects, copied_files)
                if result == "SKIPPED":
                    stats["skipped"] += 1
                else:
                    stats["processed"] += 1
            except StudyCopyError as error:
                # FAILURE NOTIFICATION: files that were not transferred
                stats["failed"] += 1
                for source_key, reason in error.failed_files:
                    failures.append("Source = {} | Study = {} | File not transferred = {} | Error = {}".format(
                        source_name, study, source_key, reason))
                LOG.error("Study failed: %s (%d file(s) not transferred)", study, len(error.failed_files))
            except Exception as error:
                stats["failed"] += 1

                failures.append("Source = {} | Study = {} | Error = {}: {}".format(
                    source_name, study, type(error).__name__, error))

                LOG.exception("Study failed: %s", study)

def main(config):
    session = boto3.Session(
        region_name = config["aws_region"]
    )

    ses = session.client("ses")

    stats = {"processed" : 0, "skipped" : 0, "failed" : 0}
    failures = []
    copied_files = []

    # FAILURE NOTIFICATION: anything that stops the run (AWS / server issue, access denied,
    # bad configuration, the DAG task being terminated, ...) sends a failure email and then
    # re-raises, so the task still fails in Airflow.
    try:
        run_ingestion(session, config, stats, failures, copied_files)
    except BaseException as error:
        LOG.exception("Ingestion aborted")
        failures.append("Run aborted | Error = {}: {}".format(type(error).__name__, error))
        notify_failure(ses, config, stats, failures, copied_files)
        raise

    LOG.info("Run Summary : processed=%d, skipped=%d, failed=%d",
             stats["processed"], stats["skipped"], stats["failed"])

    # FAILURE NOTIFICATION: studies or files that were not transferred
    if failures:
        notify_failure(ses, config, stats, failures, copied_files)

        raise RuntimeError(
                "One or more studies failed ({} issue(s)): ".format(len(failures))
                + " | ".join(failures[:20])
                )

    if copied_files:
        notify_success(ses, config, copied_files)

def handle_sigterm(signum, frame):
    raise Terminated(
        "Process received signal {} (for example an Airflow task timeout or a manual cancel)".format(signum))

if __name__ == "__main__":
    args = sys.argv
    if len(args) < 4:
        raise SystemExit(
            "Usage: python3 flywheel_ingestion.py " "<DEV|PRD> <owner> <execution_date>")

    environment = args[1].upper()
    owner = args[2]
    execution_date = args[3]

    config_dir = (
        "/gd/home/{}/gds-sda/CDE/Config/".format(owner)
    )

    config_file = (
        config_dir + "flywheel_ingestion.yml"
    )

    LOG.info(
        "Environment:  %s", environment
    )

    LOG.info(
        "Owner: %s",
        owner
    )

    LOG.info(
        "Execution Date Received: %s",
        execution_date
    )

    LOG.info(
        "Loading Flywheel Configuration: %s",
        config_file
    )

    with open(config_file,"r",encoding = "utf-8") as stream:
        all_config = (
            yaml.safe_load(stream)
            or {}
        )

    if environment not in all_config:
        raise ValueError(
            "Environment not found in configuration: " + environment
        )

    # Airflow stops a timed-out / cancelled task with SIGTERM: turn it into an exception
    # so main() can send the failure email before the process exits.
    signal.signal(signal.SIGTERM, handle_sigterm)

    main(
        all_config[environment]
    )