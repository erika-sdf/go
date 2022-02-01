from datetime import datetime
import requests
import urllib.parse
#import pprint


HOST = 'http://localhost:8000'
LEDGER_ENDPOINT = '/ledgers/{}'
LEDGER_TXN_ENDPOINT = '/ledgers/{}/transactions'
TXNS_ENDPOINT = '/transactions/{}'

def query(host, endpoint):
    addr = urllib.parse.urljoin(host, endpoint)
    #print(addr)
    r = requests.get(addr)
    return r.status_code, r.json()
    
def do_ledgers():
    total = 0
    for i in range(39137121, 39142121):
        endpoint = LEDGER_ENDPOINT.format(i)
        code, res = query(HOST, endpoint)
        if code != 200:
            print('FAILED query on ', endpoint)
            return
        total += 1
        if total % 1000 == 0:
            print(total, 'ledger requests finished')
        
def do_ledger_transactions():
    total = 0
    for i in range(39137121, 39142121):
        endpoint = LEDGER_TXN_ENDPOINT.format(i)
        code, res = query(HOST, endpoint)
        if code != 200:
            print('FAILED query on ', endpoint)
            return
        total += 1
        if total % 1000 == 0:
            print(total, 'ledger txn requests finished')

        #for t in res["records"]:
        #    endpoint = TXNS_ENDPOINT.format(t["hash"])
        #    code, res = query(HOST, endpoint)
        #    if code != 200:
        #        print('FAILED query on ', endpoint)
        #        return

        

if __name__ == "__main__":
    print('compression lzw')
    start = datetime.now()
    do_ledgers()
    chkpt1 = datetime.now()
    do_ledger_transactions()
    #chkpt2 = datetime.now()
    #do_transactions()
    end = datetime.now()
    print('start:', start)
    print('chkpt1:', chkpt1, 'duration:', chkpt1-start)
    #print('chkpt2:', chkpt2, 'duration:', chkpt2-chkpt1)
    print('end:', end, 'duration:', end-chkpt1)
    print('total duration:', end-start)
