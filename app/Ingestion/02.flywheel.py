import logging
import re
import os
import sys
from pathlib import PurePosixPath
from email.utils import formatdate
from datetime import datetime, timezone
from Collections import Counter

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

# NAME_PREFIXES = ("0000-", "R", "VG")
NAME_PREFIXES = ("0000-", "VG")
NAME_R_DIGIT = re.compile(r"R\d")
NAME_CONTAINS = ("-", "_")

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
        copy_source_object(s3, config, source_key, destination_key)
        copied_files.append({
            "study" : study,
            "source" : source_key,
            "target" : destination_key,
            "size" : source_object["Size"],
            "arrival_time" : source_object["LastModified"],
            "load_time" : datetime.now(timezone.utc)
        })
        copied += 1

    LOG.info("Study completed %s, files copied= %d, already present= %d", study, copied, skipped)
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

def main(config):
    session = boto3.Session(
        region_name = config["aws_region"]
    )

    LOG.info("AWS identity: %s", session.client("sts").get_caller_identity()["Arn"])

    s3 = session.client("s3")

    validate_buckets(s3, config)
    ensure_top_level_folders(s3, config)

    processed = 0
    skipped = 0
    failed = 0

    failures = []

    for source_name, mapping in config["sources"].items():
        source_prefix = normalize_prefix(mapping["source_prefix"])
        studies = list_studies(
            s3,
            config["rts_bucket"],
            source_prefix
        )

        LOG.info("Found %d non-empty studies under %s", len(studies), source_prefix)

        for study, objects in sorted(studies.items()):
            LOG.info("Discovered study: %s | objects=%d", study, len(objects))

            try:
                # SCENARIO 3: process_study returns "SKIPPED" when everything was already copied
                result = process_study(s3, config, source_name, mapping, study, objects)
                if result == "SKIPPED":
                    skipped += 1
                else:
                    processed += 1
            except Exception as error:
                failed += 1

                failures.append("{}/{}: {}".format(source_name, study, error)
                )

                LOG.exception("Study failed: %s", study)

    LOG.info("Run Summary : processed=%d, skipped=%d, failed=%d", processed, skipped, failed)

    if failures:
        raise RuntimeError(
                "One or more studies failed: " + " | ".join(failures)
                )

if __name__ == "__main__":
    args = sys.argv
    if len(args) < 4:
        raise SystemExit(
            "Usage: python3 flywheel_ingestion.py" "<DEV|PRD> <owner> <execution_date>")

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

    main(
        all_config[environment]
    )