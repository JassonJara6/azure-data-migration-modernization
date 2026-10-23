"""CI/local dependency helper; use HTTPS with inherited proxy/CA, no Ivy bypass."""
import sys
from pathlib import Path
from urllib.request import urlopen
root=Path(sys.argv[1]);root.mkdir(parents=True,exist_ok=True)
for artifact in ('delta-spark_2.12','delta-storage'):
    name=artifact+'-3.2.1.jar'
    with urlopen('https://repo.maven.apache.org/maven2/io/delta/'+artifact+'/3.2.1/'+name) as response:
        (root/name).write_bytes(response.read())
print(','.join(str(root/(a+'-3.2.1.jar')) for a in ('delta-spark_2.12','delta-storage')))
