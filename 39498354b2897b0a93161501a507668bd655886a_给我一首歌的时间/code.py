import json
import traceback

from kafka import KafkaConsumer

# modify according to the specific
from account import Account
from og_solver import OGSolver

KAFKA_TOPIC = 'hack-final-test'
KAFKA_SERVER = ['47.100.222.11:30000']
API_URL = "http://47.100.222.11:30020"

private_key = "8ca0d1f89ca5f241d58ca91035b731e3eacd1a63e21514fbd9d1134195b636e5"
TOKEN = 'vKgrOgFZsjVX3fmt'

og = OGSolver(API_URL, TOKEN)
my_account = Account(private_key)

from_self = "0x39498354b2897b0a93161501a507668bd655886a"
aim = "0xc55bda8b731dd486644d480495d539576c9a44e2"
#aim= '0x415cb4b14b9663c267b98f29fffe32f83fd0b966'

# def catch_self(tx):
#     if tx["data"]["from"] == from_self:
#         return tx["data"]["hash"]
#
# def catch_other(tx):
#     if tx["data"]["from"] == aim:
#         return tx["data"]["hash"]

def on_new_tx(tx, hash):
    if tx['type'] == 0:
        # the type is transaction
        print('Received Tx: ', tx)

        if tx["data"]["from"] == from_self:
            hash.append(tx["data"]["hash"])
        elif tx["data"]["from"] == aim:
            hash.append(tx["data"]["hash"])
        else:
            pass
            # resp = og.query_nonce(my_account.address)
            # nonce = resp['data']
            # og.send_tx(my_account, [tx['data']['hash']], nonce + 1, my_account.address, 100, my_account.public_key)
    elif tx['type'] == 1:
        # the type is sequencer
        print('Received Seq: ', tx)
        hash.append(tx["data"]["hash"])

        resp = og.query_nonce(my_account.address)
        nonce = resp['data']
        og.send_tx(my_account, [tx['data']['hash']], nonce + 1, my_account.address, 4000, my_account.public_key)


if __name__ == '__main__':
    # this is the callback for receiving a new tx:
    consumer = KafkaConsumer(KAFKA_TOPIC, bootstrap_servers=KAFKA_SERVER)
    hash = []
    for message in consumer:
        tx = json.loads(message.value.decode('utf-8'))
        print(tx)
        try:
            on_new_tx(tx, hash)
            if len(hash) == 2:
                resp = og.query_nonce(my_account.address)
                nonce = resp['data']
                og.send_tx(my_account, hash, nonce + 1, my_account.address, 175, my_account.public_key)
                hash = []
        except:
            traceback.print_exc()
