"""Reproduce the vendored profile types from an official, hash-checked archive."""

import hashlib
import io
import json
from pathlib import Path
import re
import tarfile
from urllib.request import urlopen

VERSION = "4.6.0"
ROOT = Path(__file__).resolve().parents[1]
PREFIX = "impala_shell-4.6.0/impala_thrift_gen/"
DEST = ROOT / "src/impala_profile_parser/vendor"


def main():
    manifest_path = ROOT / "docs/reference/thrift-source.json"
    if manifest_path.exists():
        source = json.loads(manifest_path.read_text())
    else:
        with urlopen("https://pypi.org/pypi/impala-shell/4.6.0/json") as response:
            release = json.load(response)
        archive = next(item for item in release["urls"] if item["packagetype"] == "sdist")
        source = {"version": VERSION, "url": archive["url"], "sha256": archive["digests"]["sha256"]}
    with urlopen(source["url"]) as response:
        data = response.read()
    if hashlib.sha256(data).hexdigest() != source["sha256"]:
        raise ValueError("Official source archive checksum mismatch")
    modules = set()
    pending = ["RuntimeProfile"]
    DEST.mkdir(parents=True, exist_ok=True)
    (DEST / "__init__.py").write_text('"""Vendored Apache Impala Thrift types; see THIRD-PARTY-NOTICES.md."""\n')
    with tarfile.open(fileobj=io.BytesIO(data), mode="r:gz") as archive:
        while pending:
            module = pending.pop()
            if module in modules:
                continue
            modules.add(module)
            path = PREFIX + module + "/ttypes.py"
            content = archive.extractfile(path).read().decode("utf-8")
            pending.extend(re.findall(r"^import impala_thrift_gen\.(\w+)\.ttypes", content, re.M))
            destination = DEST / module
            destination.mkdir(exist_ok=True)
            (destination / "__init__.py").write_text("")
            (destination / "ttypes.py").write_text(content.replace("impala_thrift_gen.", "impala_profile_parser.vendor."))
        for name in ["LICENSE.txt", "NOTICE.txt"]:
            candidates = [m for m in archive.getmembers() if m.name.count("/") == 1 and m.name.split("/")[-1] in (name, name[:-4])]
            if candidates:
                (ROOT / ("APACHE-IMPALA-" + name)).write_bytes(archive.extractfile(candidates[0]).read())
    source["modules"] = sorted(modules)
    source["modification"] = "Only import namespace rewritten: impala_thrift_gen -> impala_profile_parser.vendor"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(json.dumps(source, indent=2) + "\n")
    (DEST / "source.json").write_text(json.dumps(source, indent=2) + "\n")
    print("Vendored modules:", ", ".join(sorted(modules)))


if __name__ == "__main__":
    main()
