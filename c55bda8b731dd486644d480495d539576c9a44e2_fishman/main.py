from og_solver import OGSolver
from account import Account
from kafka import *
import json
import _thread
import random

KAFKA_TOPIC = 'hack-final-test'
KAFKA_SERVER = '47.100.222.11:30000'
API_URL = "http://47.100.222.11:30020"
TOKEN = "kpA9pHqyHoewN5wL"
private_key = "b6275270494f1a9ab1458485d7e54df65117b25dc292e3fcf98945e4abb59c15"
pubic_key = "049365208f9118fa95872cf33df93b60d2d51c1022ee68e19c6d83dac766817efc7f6d46ce4207143dd5ddb6a46802284eb51ccb7ece3b63b206a33d01272acfcc"
address = "c55bda8b731dd486644d480495d539576c9a44e2"

def get_genertee(guarantee,treasure):
    '''
    0-250    100
    250-500  250
    500-1000  500
    1000-2500  800
    2500-4000  1000
    4000-      1500
    '''
    if guarantee <= 200:
        return 150 + random.randint(0, 50), random.randint(0, 100) <= 28
    elif guarantee < 500:
        return 250 + random.randint(0, 100), random.randint(0, 100) <= 60
    elif guarantee < 1000:
        return 500 + random.randint(0, 200), random.randint(0, 100) <= 80
    elif guarantee < 2500:
        return 1000 + random.randint(0, 350), random.randint(0, 100) <= 100
    elif guarantee < 4000:
        return 1500 + random.randint(0, 400), random.randint(0, 100) <= 100
    elif guarantee < 6000:
        return 2000 + random.randint(0, 600), random.randint(0, 100) <= 100
    elif guarantee < 5 * treasure:
        return 2500 + random.randint(0, 800), random.randint(0, 100) <= 100
    else:
        return treasure // 2, random.randint(0, 100) <= 100

parent = None
guarantee = 2600
treasure = 10000000000
def on_new_tx(tx):
    global parent  
    global guarantee
    global treasure
    if tx['type'] == 0:  
        if (tx['data']['from'] == ('0x' + address)):
            return
        # print('Received Tx: ', tx)
        og = OGSolver(API_URL, TOKEN)
        account = Account(private_key)

        # if account.nonce == 0:
        account.nonce = og.query_nonce(account.address)['data']

        guarantee, flag = get_genertee(int(tx['data']['guarantee']), treasure)
        
        if not flag:
            return

        if not parent:
            guarantee = 2600
            height = og.query_next_sequencer_info()['data']['height'] - 1
            seq_json = og.query_sequencer_by_height(height)['data']
            parent = seq_json['hash']
            treasure = int(seq_json['treasure'])

        print(guarantee)

        parent_hashes = []
        parent_hashes.append(parent)
        parent_hashes.append(tx['data']['hash'])
        resp = og.send_tx(account, parent_hashes, account.nonce + 1, account.address, guarantee, account.public_key, None,
                    0)
        print('sent tx with nonce %d' % (account.nonce + 1))
        account.nonce += 1
        # print(resp)
        if resp['data'] is not None:
            parent = resp['data']
        # Maybe we need to follow it?
    elif tx['type'] == 1:  
        print('Received Seq: ', tx)
        treasure = int(tx['data']['treasure'])
        # Maybe we need to follow it?
        og = OGSolver(API_URL, TOKEN)

        account = Account(private_key)
        if account.nonce == 0:
            account.nonce = og.query_nonce(account.address)['data']
        
        guarantee = 2600
        
        print(guarantee)
        parent_hashes = []
        parent_hashes.append(tx['data']['hash'])
        resp = og.send_tx(account, parent_hashes, account.nonce + 1, account.address, guarantee, account.public_key, None,
                    0)
        print('sent tx with nonce %d' % (account.nonce + 1))
        account.nonce += 1
        # print(resp)
        if resp['data'] is not None:
            parent = resp['data']
        else:
            parent = tx['data']['hash']

consumer = KafkaConsumer(KAFKA_TOPIC, bootstrap_servers=KAFKA_SERVER)
for message in consumer:
    tx = json.loads(message.value.decode('utf-8'))
    # _thread.start_new_thread(on_new_tx, (tx,))
    on_new_tx(tx)