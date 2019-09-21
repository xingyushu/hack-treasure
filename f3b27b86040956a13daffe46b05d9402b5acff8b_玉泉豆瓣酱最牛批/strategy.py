import json
import time
import traceback
import threading
import random

from kafka import KafkaConsumer

# modify according to the specific
from account import Account
from og_solver import OGSolver

KAFKA_TOPIC = 'hack-final-test'
KAFKA_SERVER = ['47.100.222.11:30000']
API_URL = "http://47.100.222.11:30020"

private_key = "7cdad86181b1d298ff1329be1b665cfe1bfc701c33282fad38656c33016ca130"
TOKEN = 'C2dgVTAOZ4Cuq6RP'

og = OGSolver(API_URL, TOKEN)
my_account = Account(private_key)

startHeight = 0
startHash = 0
startTreasure = 0
tempState = 0
tempHash = 0
hashPoolMe = []
hashPoolOther = []
valueMul = 1
timeBefore = time.time()
hashBack = 0x0
txNum = 0

def on_new_tx(tx):
    global startHeight
    global startHash
    global startTreasure
    global tempHash
    global valueMul
    global hashPoolMe
    global hashPoolOther
    global timeBefore
    global nonceNow
    global hashBack
    global txNum
    if tx['type'] == 0:
        print('\nReceived Tx: ', tx,"\n")
        timeLeft = og.query_next_sequencer_info()['data']['time_left']
        if(txNum>0):
            if(True):
                if(tx['data']['from'] != '0xf3b27b86040956a13daffe46b05d9402b5acff8b'):
                    hashPoolOther.append(tx['data']['hash'])
                    mePoolSize = len(hashPoolMe)
                    if((True)):
                        if(hashPoolMe[-1] == startHash):
                            valueMul = 50
                            resp = og.query_nonce(my_account.address)
                            nonce = resp['data']
                            nonceNow = nonce
                            error_message = og.send_tx(my_account, [hashPoolMe[-1],tx['data']['hash']], nonce + 1, my_account.address, valueMul*100, my_account.public_key)
                            hashBack = error_message['data']
                            hashPoolMe.append(hashBack)
                            print(error_message)
                            timeBefore = time.time()
                            print(timeBefore)
                            valueMul = 6
                        else:# 
                            if((tx['data']['from'] == '0x8426d1f54a02da69dacb27d0b989767588216d13' or tx['data']['from']=='0x7e94b0dbfc889144f36e5287c8db1bf5f55bdf0f' or tx['data']['from']=='0x9ff71b29bb0aabfa77c018e7ec16d9d4cc8484b6' ) and (time.time()-timeBefore>1)):
                                resp = og.query_nonce(my_account.address)
                                nonce = resp['data']
                                nonceNow = nonceNow+1
                                print(hashBack)
                                valueMul = valueMul-1
                                if(valueMul>0):
                                    error_message = og.send_tx(my_account, [hashPoolMe[-1],tx['data']['hash']], nonce + 1, my_account.address, random.randint(10,30)*100, my_account.public_key)
                                else:
                                    error_message = og.send_tx(my_account, [hashPoolMe[-1],tx['data']['hash']], nonce + 1, my_account.address, random.randint(1,2)*100, my_account.public_key)
                                try:
                                    hashBack = error_message['data']
                                    if(hashBack!=None):
                                        hashPoolMe.append(hashBack)
                                        txNum = txNum - 1
                                except:
                                    pass
                                print(error_message)
                                timeBefore = time.time()

                # else:
                #     hashPoolMe.append(tx['data']['hash'])
                timeBefore = timeLeft

    #hashBack = error_message['data']


        # if(len(hashPool)>=2):
        #     resp = og.query_nonce(my_account.address)
        #     nonce = resp['data']

        #     error_message = og.send_tx(my_account, [hashPool.pop(0),hashPool.pop(0)], nonce + 1, my_account.address, valueMul*102, my_account.public_key)
        #     print(error_message)
        #     valueMul = 1





        # this is the very naive policy
        # just follow the sequencer once a sequencer is generated.

    elif tx['type'] == 1:
        print('Received Seq: ', tx)

        valueMul = 20
        #time.sleep(30)
        # this is the very naive policy
        # just follow the sequencer once a sequencer is generated.
        startHeight = tx['data']['height']
        startTreasure = tx['data']['treasure']
        startHash = tx['data']['hash']
        tempHash = tx['data']['hash']
        resp = og.query_nonce(my_account.address)
        nonce = resp['data']

        hashPoolMe = [startHash]
        hashPoolOther = []
        timeLeft = 1000000
        txNum = 9999999999
        #error_message = og.send_tx(my_account, [tx['data']['hash']], nonce + 1, my_account.address, 100, my_account.public_key)
        #print(error_message)


if __name__ == '__main__':
    consumer = KafkaConsumer(KAFKA_TOPIC, bootstrap_servers=KAFKA_SERVER)
    for message in consumer:
        tx = json.loads(message.value.decode('utf-8'))
        try:
            on_new_tx(tx)
        except:
            traceback.print_exc()
