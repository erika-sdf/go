#import jsondiff
from jycm.helper import make_ignore_order_func
from jycm.jycm import YouchamaJsonDiffer
import requests
import urllib.parse
import pprint

WANT = 'https://horizon.stellar.org/'
GOT = 'http://localhost:8000'
ENDPOINTS = [
    '/ledgers/39137121',
    #'/ledgers/39137121/transactions?limit=10',
    #'/transactions/168101034237247488',
    #'/transactions/6f3797e136d88f30ebd0df28d3021989f46a20e0ca8ede5fda231fa0b431add1',

    #'/accounts/?signer=GACA3J4AFKC7EAY5JISZFT2NIV34GWVVFJCHB55JICPWFKEFGIOT7ZG6'
]
def query(host, endpoint):
    addr = urllib.parse.urljoin(host, endpoint)
    print(addr)
    r = requests.get(addr)
    return r.status_code, r.json()
    
def do_compare():
    for e in ENDPOINTS:
        w_code, w_json = query(WANT, e)
        g_code, g_json = query(GOT, e)
        if w_code != 200 or  g_code != 200:
            print('failed query on {} (return codes {}, {})'.format(e, w_code, g_code))
            continue

        if "_embedded" in w_json:
            w_json = w_json["_embedded"]
        ycm = YouchamaJsonDiffer(w_json, g_json)
        ycm.diff()
        pprint.pprint(ycm.to_dict(no_pairs=True))

        #d = jsondiff.diff(w_json, g_json)
        #if d:
        #    print 'diff found for endpoint {}'.format(e)
        #    print d
        

if __name__ == "__main__":
    do_compare()
