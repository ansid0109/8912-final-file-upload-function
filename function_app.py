import cgi
import json
import logging
import os
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import azure.functions as func
from azure.core.exceptions import ResourceExistsError
from azure.storage.blob import BlobSasPermissions, BlobServiceClient, generate_blob_sas

app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)

CONTAINER_NAME = "customer-uploads"


def _json_response(payload: dict, status_code: int = 200) -> func.HttpResponse:
    return func.HttpResponse(
        body=json.dumps(payload),
        status_code=status_code,
        mimetype="application/json",
    )


def _get_connection_string() -> str:
    conn_str = os.getenv("AzureWebJobsStorage") or os.getenv("BLOB_CONNECTION_STRING")
    if not conn_str:
        raise ValueError("Storage connection string is not configured.")
    return conn_str


def _parse_connection_string(connection_string: str) -> dict:
    items = {}
    for part in connection_string.split(";"):
        if not part or "=" not in part:
            continue
        key, value = part.split("=", 1)
        items[key] = value
    return items


def _parse_first_uploaded_file(req: func.HttpRequest) -> tuple[str, bytes]:
    content_type = req.headers.get("Content-Type") or req.headers.get("content-type")
    if not content_type:
        raise ValueError("Missing Content-Type header.")

    content_type_main, params = cgi.parse_header(content_type)
    if content_type_main.lower() != "multipart/form-data" or "boundary" not in params:
        raise ValueError("Content-Type must be multipart/form-data with a boundary.")

    boundary = params["boundary"].encode("utf-8")
    raw_body = req.get_body()
    delimiter = b"--" + boundary

    for part in raw_body.split(delimiter):
        candidate = part.strip()
        if not candidate or candidate == b"--":
            continue

        if candidate.endswith(b"--"):
            candidate = candidate[:-2].rstrip()

        if b"\r\n\r\n" not in candidate:
            continue

        raw_headers, file_content = candidate.split(b"\r\n\r\n", 1)
        header_map = {}
        for line in raw_headers.split(b"\r\n"):
            if b":" not in line:
                continue
            key, value = line.split(b":", 1)
            header_map[key.decode("latin1").strip().lower()] = value.decode(
                "latin1"
            ).strip()

        disposition = header_map.get("content-disposition")
        if not disposition:
            continue

        _, disp_params = cgi.parse_header(disposition)
        filename = disp_params.get("filename")
        if not filename:
            continue

        if file_content.endswith(b"\r\n"):
            file_content = file_content[:-2]
        return filename, file_content

    raise ValueError("No file found in multipart form-data.")


def _build_unique_blob_name(original_filename: str) -> str:
    suffix = Path(original_filename).suffix
    return f"{uuid.uuid4().hex}{suffix}"


@app.route(route="upload", methods=["POST"])
def upload(req: func.HttpRequest) -> func.HttpResponse:
    try:
        original_filename, file_bytes = _parse_first_uploaded_file(req)
        blob_name = _build_unique_blob_name(original_filename)

        connection_string = _get_connection_string()
        service_client = BlobServiceClient.from_connection_string(connection_string)
        container_client = service_client.get_container_client(CONTAINER_NAME)
        try:
            container_client.create_container()
        except ResourceExistsError:
            pass

        blob_client = container_client.get_blob_client(blob_name)
        blob_client.upload_blob(file_bytes, overwrite=False)

        return _json_response(
            {
                "message": "File uploaded successfully.",
                "filename": blob_name,
                "originalFilename": original_filename,
                "container": CONTAINER_NAME,
            },
            status_code=201,
        )
    except ValueError as exc:
        return _json_response({"error": str(exc)}, status_code=400)
    except Exception as exc:
        logging.exception("Upload failed")
        return _json_response({"error": f"Upload failed: {exc}"}, status_code=500)


@app.route(route="download", methods=["GET"])
def download(req: func.HttpRequest) -> func.HttpResponse:
    filename = req.params.get("filename")
    if not filename:
        return _json_response(
            {"error": "Query parameter 'filename' is required."}, status_code=400
        )

    try:
        connection_string = _get_connection_string()
        service_client = BlobServiceClient.from_connection_string(connection_string)
        container_client = service_client.get_container_client(CONTAINER_NAME)
        blob_client = container_client.get_blob_client(filename)

        if not blob_client.exists():
            return _json_response({"error": "File not found."}, status_code=404)

        conn_parts = _parse_connection_string(connection_string)
        account_name = conn_parts.get("AccountName")
        account_key = conn_parts.get("AccountKey")
        if not account_name or not account_key:
            raise ValueError(
                "Connection string must include AccountName and AccountKey."
            )

        expires_on = datetime.now(timezone.utc) + timedelta(minutes=15)
        sas_token = generate_blob_sas(
            account_name=account_name,
            container_name=CONTAINER_NAME,
            blob_name=filename,
            account_key=account_key,
            permission=BlobSasPermissions(read=True),
            expiry=expires_on,
        )

        return _json_response(
            {
                "filename": filename,
                "downloadUrl": f"{blob_client.url}?{sas_token}",
                "expiresInMinutes": 15,
            }
        )
    except ValueError as exc:
        return _json_response({"error": str(exc)}, status_code=400)
    except Exception as exc:
        logging.exception("Download URL generation failed")
        return _json_response(
            {"error": f"Download URL generation failed: {exc}"}, status_code=500
        )
