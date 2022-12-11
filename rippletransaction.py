import json
from urllib.request import urlopen
import time
import sys

def getcount():
    cons_gateway_errors = 10
    MAX_CONSECUTIVE_ERRORS = 10
    file=open("transaction2022_1_week1_3.txt","w")
    time.sleep(3)
    response=urlopen("https://data.ripple.com/v2/transactions/?marker=20220106102102|000068829690|00024&limit=1000000&type=Payment&result=tesSUCCESS")
    time.sleep(3)
    data=response.read()
    print(data)
    responseJson=json.loads(data)
    marker=responseJson.get("marker")
    counter=0
    for i in responseJson["transactions"]:
        try:
            Hash_tX = i["hash"]
            #Account=i["Account"]
            #value=i["tx"]["TakerGets"]["value"]
            TransactionType=i["tx"]["TransactionType"]
            senderAccount = i["tx"]["Account"]
            DerstionAccount = i["tx"]["Destination"]
            Value = i["tx"]["Amount"]["value"]
            currency = i["tx"]["Amount"]["currency"]

            inception=i["date"]
            counter=counter+1
            file.writelines(senderAccount+"       "+DerstionAccount+"        "+Hash_tX+"        "+  TransactionType+"          "+Value+"       "+currency+"       "+inception+"        "+marker+"\n")
        except:
            continue
    while True:
        time.sleep(3)
        response = urlopen(
            "https://data.ripple.com/v2/transactions/?marker=" + marker + "&limit=1000000&type=Payment&result=tesSUCCESS")
        time.sleep(3)
        data = response.read()
        responseJson = json.loads(data)
        marker = responseJson.get("marker")
        counter=0
        for i in responseJson["transactions"]:
            try:
                Hash_tX = i["hash"]
                # Account=i["Account"]
                #value = i["tx"]["TakerGets"]["value"]
                TransactionType = i["tx"]["TransactionType"]
                senderAccount=i["tx"]["Account"]
                DerstionAccount=i["tx"]["Destination"]
                Value=i["tx"]["Amount"]["value"]
                currency=i["tx"]["Amount"]["currency"]
                # print(TransactionType)

                inception = i["date"]
                counter = counter + 1
                file.writelines(senderAccount+"       "+DerstionAccount+"        "+Hash_tX+"        "+  TransactionType+"          "+Value+"       "+currency+"       "+inception+"        "+marker+"\n")
                if(counter==500000):
                    break
            except:
                # e = str(traceback.print_exc())

                e = str(sys.exc_info()[0]) + ' ' + str(sys.exc_info()[1])

                print
                "Error is:", e

                sys.stdout.flush()

                if "HTTP Error 5" in e or "Name or service not known" in e or "HTTP Error 4" in e:  # a gateway error starting with 5 (501, 502, 504)

                    cons_gateway_errors += 1

                    if cons_gateway_errors > MAX_CONSECUTIVE_ERRORS: break  # if these many consecutive errors, then just move on

                    sleep_time = cons_gateway_errors * 60

                    print
                    "%d consecutive gateway errors. So sleeping for %d" % (cons_gateway_errors, sleep_time)

                    time.sleep(sleep_time)


                continue

getcount()
