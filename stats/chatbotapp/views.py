
from rest_framework.response import Response
from rest_framework.views import APIView

from django.http import HttpResponse
import json
from chatbotapp.mathoperations import math_op
import gridfs
from pymongo import MongoClient
from bson.json_util import dumps
import ast
from chatbotapp.db import databaseOperations as db
import numpy as np

class chatbot(APIView):

    # # client = MongoClient()
    # MdbURI = "mongodb://prasad:Qtx%4012345@172.16.1.38:27017/?authSource=web_app"
    # client = MongoClient(MdbURI)
    # # client.admin.authenticate("allcargo",)
    # print(client)
    # print(client['web_app'])
    #
    # db = client['web_app']
    # collection = db.meters
    #
    # appdata = collection.find({"medidor_id" : 2077688})
    # if (appdata != None):
    #     appdata = dumps(appdata)
    #     appdata = ast.literal_eval(appdata)
    # else:
    #     pass
    # db.find_meter('meters', 2077688)


    def post(self, request):
        print(request.data)
        json1 = str(request.data['queryResult']['parameters'])
        print(json1)

        var1 = str(request.data['queryResult']['action'])

        if var1 == 'active_consumption_all_years':
            meter_id = request.data['queryResult']['parameters']['meter_id']
            appdata = db.find_meter('meters',meter_id)
            if (appdata != None):
                appdata = dumps(appdata)
                appdata = ast.literal_eval(appdata)
                data = appdata[0]['data']
                years = list(data.keys())
                active_consumption_for_a_meter = [data[str(i)][str(months)]['active_consum'] for i in years for months in list(data[str(i)].keys()) for acti in months ]
                npa = np.asarray(active_consumption_for_a_meter, dtype=np.double)
                out =  str(np.mean(npa))

            else:
                out =  "Nothing Found"


        # if var1 == 'addition':
        #     number = request.data['queryResult']['parameters']['number']
        #     number1 = request.data['queryResult']['parameters']['number1']
        #     out = math_op.addition(number,number1)
        # elif var1 == 'subtraction':
        #     number = request.data['queryResult']['parameters']['number']
        #     number1 = request.data['queryResult']['parameters']['number1']
        #     out = math_op.subtraction(number,number1)
        # elif var1 == 'division':
        #     number = request.data['queryResult']['parameters']['number']
        #     number1 = request.data['queryResult']['parameters']['number1']
        #     out = math_op.division(number,number1)
        # elif var1 == 'multiplication':
        #     number = request.data['queryResult']['parameters']['number']
        #     number1 = request.data['queryResult']['parameters']['number1']
        #     out = math_op.multiplication(number,number1)

        response = {'fulfillmentText': out}
        response = json.dumps(response)
        return HttpResponse(response)
