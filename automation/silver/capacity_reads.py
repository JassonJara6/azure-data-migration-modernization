"""Bounded ARM SKU pagination; only GET, no partial catalog accepted."""
import time
from urllib.parse import urlsplit,parse_qs,urlencode
from diagnose import SUBSCRIPTION
from plan_guard import require,Blocked

SKU_PATH='/subscriptions/'+SUBSCRIPTION+'/providers/Microsoft.Compute/skus'
SKU_URL='https://management.azure.com'+SKU_PATH+'?'+urlencode({'api-version':'2021-07-01','$filter':"location eq 'eastus'"})


def read_sku_catalog(read,*,progress=None,clock=time.monotonic,budget=180,max_pages=100):
    deadline=clock()+budget;url=SKU_URL;seen=set();catalog=[]
    for page in range(1,max_pages+1):
        parts=urlsplit(url)
        require(parts.scheme=='https' and parts.netloc=='management.azure.com' and
            parts.path.lower()==SKU_PATH.lower() and not parts.fragment and
            parse_qs(parts.query).get('api-version')==['2021-07-01'],'INVALID_AZURE_SKU_NEXT_LINK')
        require(url not in seen,'REPEATED_AZURE_SKU_NEXT_LINK');seen.add(url)
        for attempt in range(2):
            remaining=deadline-clock()
            require(remaining>0,'READ_TIMEOUT_AT_AZURE_SKUS')
            if progress:progress({'stage':'AZURE_SKUS','page':page,'attempt':attempt+1,'status':'READING'})
            try:
                value=read(['az','rest','--method','get','--url',url,'--output','json'],
                    'AZURE_SKUS',min(60,remaining))
                break
            except Blocked as exc:
                retryable=str(exc) in ('READ_TIMEOUT_AT_AZURE_SKUS','CONNECTIVITY_FAILED_AT_AZURE_SKUS','DNS_FAILED_AT_AZURE_SKUS')
                if not retryable or attempt==1:raise
        require(clock()<=deadline,'READ_TIMEOUT_AT_AZURE_SKUS')
        require(isinstance(value,dict) and isinstance(value.get('value'),list) and
            all(isinstance(s,dict) for s in value['value']),'INVALID_AZURE_SKU_PAGE')
        catalog.extend(value['value'])
        next_link=value.get('nextLink')
        require(next_link is None or isinstance(next_link,str),'INVALID_AZURE_SKU_NEXT_LINK')
        if progress:progress({'stage':'AZURE_SKUS','page':page,'status':'COMPLETE','items':len(catalog)})
        if not next_link:return catalog
        url=next_link
    raise Blocked('AZURE_SKU_PAGE_LIMIT_EXCEEDED')
