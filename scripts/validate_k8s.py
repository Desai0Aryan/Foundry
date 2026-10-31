#!/usr/bin/env python3
"""Render every kustomize overlay and validate each resource against the Kubernetes schema.

    python scripts/validate_k8s.py            # needs `kustomize` on PATH and `pip install kubernetes-validate`
CRD-backed extras (KEDA, prometheus-operator) are only checked for YAML syntax.
"""

import glob
import subprocess
import sys

import kubernetes_validate
import yaml

VERSION = "1.30"
CRD_GROUPS = ("keda.sh", "monitoring.coreos.com")
failed = checked = 0

docs = []
for overlay in ("base", "overlays/dev", "overlays/prod"):
    out = subprocess.run(
        ["kustomize", "build", f"deploy/k8s/{overlay}"], capture_output=True, text=True
    )
    if out.returncode:
        sys.exit(f"kustomize build {overlay} failed:\n{out.stderr}")
    docs += [(overlay, d) for d in yaml.safe_load_all(out.stdout) if d]
for f in glob.glob("deploy/k8s/extras/*.yaml"):
    with open(f) as fh:
        docs += [(f, d) for d in yaml.safe_load_all(fh) if d]

for origin, d in docs:
    if d["apiVersion"].split("/")[0] in CRD_GROUPS:
        continue
    checked += 1
    try:
        kubernetes_validate.validate(d, VERSION, strict=True)
    except Exception as exc:
        failed += 1
        print(f"INVALID {origin}: {d['kind']}/{d['metadata']['name']}: {str(exc)[:300]}")
print(f"{checked} resources validated against Kubernetes {VERSION}; {failed} invalid")
sys.exit(1 if failed else 0)
