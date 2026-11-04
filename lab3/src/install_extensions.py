"""One-time download into the workspace, excluded from benchmark timings."""
import duckdb
from common import LAB
dest = LAB / ".runtime" / "duckdb_extensions"
dest.mkdir(parents=True, exist_ok=True)
db = duckdb.connect(config={"extension_directory": str(dest)})
db.execute("INSTALL httpfs")
db.execute("LOAD httpfs")
print("httpfs installed and loaded in workspace")
