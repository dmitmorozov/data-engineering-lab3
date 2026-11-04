"""Reproduce the three complete validations, including hard mapper failure."""
import subprocess
import sys
from common import ROOT, LAB
def call(*args):
    subprocess.run([sys.executable,"-X","utf8",*map(str,args)],cwd=ROOT,check=True)
def main():
    base=LAB/"artifacts"/"validation-20261101"/"results"
    for k in ("5","10","all"):
        for method,script in (("mr","mapreduce.py"),("sql","duckdb_counts.py")):
            call(LAB/"src"/script,"--limit",k,"--output",base/f"{method}{k}")
        call(LAB/"src"/"verify.py",base/f"mr{k}"/"counts.csv",base/f"sql{k}"/"counts.csv",
             "--output",base/f"verify{k}")
    call(LAB/"src"/"mapreduce.py","--limit","all","--fail-chunk","2","--output",base/"mr_failure")
    call(LAB/"src"/"verify.py",base/"mrall"/"counts.csv",base/"mr_failure"/"counts.csv",
         "--output",base/"verify_failure")
if __name__=="__main__":
    main()
