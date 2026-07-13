from __future__ import annotations

import argparse
import base64
import json
import os
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen


DEFAULT_CLUSTER_NAME = "cl-toledano"
DEFAULT_LOCAL_JAR = Path("lib/sap/ngdbc-2.28.7.jar")
DEFAULT_DBFS_JAR = "dbfs:/FileStore/jars/sap/ngdbc-2.28.7.jar"


def request_json(host: str, token: str, method: str, path: str, payload: dict | None = None) -> dict:
    url = host.rstrip("/") + path
    body = None if payload is None else json.dumps(payload).encode("utf-8")
    request = Request(
        url,
        data=body,
        method=method,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
    )
    try:
        with urlopen(request, timeout=120) as response:
            raw = response.read()
            return json.loads(raw.decode("utf-8")) if raw else {}
    except HTTPError as exc:
        details = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"Databricks API error {exc.code} calling {path}: {details}") from exc


def find_cluster_id(host: str, token: str, cluster_name: str) -> str:
    response = request_json(host, token, "GET", "/api/2.0/clusters/list")
    clusters = response.get("clusters", [])
    for cluster in clusters:
        if cluster.get("cluster_name") == cluster_name:
            return cluster["cluster_id"]
    available = ", ".join(sorted(cluster.get("cluster_name", "") for cluster in clusters))
    raise RuntimeError(f"No se encontro cluster {cluster_name!r}. Clusters disponibles: {available}")


def upload_dbfs(host: str, token: str, local_path: Path, dbfs_path: str) -> None:
    if not local_path.exists():
        raise FileNotFoundError(f"No existe el JAR local: {local_path}")

    parent = dbfs_path.rsplit("/", 1)[0]
    request_json(host, token, "POST", "/api/2.0/dbfs/mkdirs", {"path": parent})
    handle = request_json(
        host,
        token,
        "POST",
        "/api/2.0/dbfs/create",
        {"path": dbfs_path, "overwrite": True},
    )["handle"]
    try:
        with local_path.open("rb") as file:
            while True:
                chunk = file.read(1024 * 1024)
                if not chunk:
                    break
                request_json(
                    host,
                    token,
                    "POST",
                    "/api/2.0/dbfs/add-block",
                    {"handle": handle, "data": base64.b64encode(chunk).decode("ascii")},
                )
    finally:
        request_json(host, token, "POST", "/api/2.0/dbfs/close", {"handle": handle})


def install_library(host: str, token: str, cluster_id: str, dbfs_path: str) -> None:
    request_json(
        host,
        token,
        "POST",
        "/api/2.0/libraries/install",
        {"cluster_id": cluster_id, "libraries": [{"jar": dbfs_path}]},
    )


def restart_cluster(host: str, token: str, cluster_id: str) -> None:
    request_json(host, token, "POST", "/api/2.0/clusters/restart", {"cluster_id": cluster_id})


def main() -> None:
    parser = argparse.ArgumentParser(description="Instala ngdbc.jar en un cluster Databricks.")
    parser.add_argument("--cluster-name", default=DEFAULT_CLUSTER_NAME)
    parser.add_argument("--local-jar", type=Path, default=DEFAULT_LOCAL_JAR)
    parser.add_argument("--dbfs-jar", default=DEFAULT_DBFS_JAR)
    parser.add_argument("--restart", action="store_true", help="Reinicia el cluster despues de instalar la libreria.")
    args = parser.parse_args()

    host = os.environ.get("DATABRICKS_HOST", "").strip()
    token = os.environ.get("DATABRICKS_TOKEN", "").strip()
    if not host or not token:
        raise RuntimeError("Configura DATABRICKS_HOST y DATABRICKS_TOKEN antes de ejecutar este script.")

    cluster_id = find_cluster_id(host, token, args.cluster_name)
    upload_dbfs(host, token, args.local_jar, args.dbfs_jar)
    install_library(host, token, cluster_id, args.dbfs_jar)
    if args.restart:
        restart_cluster(host, token, cluster_id)

    print(f"JAR instalado en cluster {args.cluster_name} ({cluster_id}): {args.dbfs_jar}")


if __name__ == "__main__":
    main()
