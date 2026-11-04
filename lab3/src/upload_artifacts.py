"""Upload evidence, check remote metadata and re-read every full counts CSV."""
import argparse
import hashlib
from common import LAB, byte_chunks, config, s3_client, save_json, utc_now
def sha256_file(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream,"sha256").hexdigest()
def main():
    parser=argparse.ArgumentParser()
    parser.add_argument("run_id")
    args=parser.parse_args()
    directory=LAB/"artifacts"/args.run_id
    client,cfg=s3_client(),config()
    uploaded=[]
    for path in sorted(directory.rglob("*")):
        if not path.is_file() or path.name=="s3_upload.json":
            continue
        key=f"results/analysts/{args.run_id}/{path.relative_to(directory).as_posix()}"
        checksum=sha256_file(path)
        client.upload_file(str(path),cfg["bucket"],key,ExtraArgs={"Metadata":{"sha256":checksum}})
        head=client.head_object(Bucket=cfg["bucket"],Key=key)
        assert head["ContentLength"]==path.stat().st_size and head["Metadata"]["sha256"]==checksum
        record={"key":key,"bytes":path.stat().st_size,"sha256":checksum,"head_equal":True}
        if path.name=="counts.csv":
            response=client.get_object(Bucket=cfg["bucket"],Key=key)
            remote=hashlib.sha256()
            with response["Body"] as stream:
                for chunk in byte_chunks(stream,262144):
                    remote.update(chunk)
            assert remote.hexdigest()==checksum
            record["remote_content_equal"]=True
        uploaded.append(record)
    save_json(directory/"s3_upload.json",{"at_utc":utc_now(),"bucket":cfg["bucket"],"objects":uploaded})
    print(f"Uploaded and verified {len(uploaded)} objects to s3://{cfg['bucket']}/results/analysts/{args.run_id}/")
if __name__=="__main__":
    main()
