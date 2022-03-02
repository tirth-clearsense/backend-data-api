import json
from flask import Flask, request, jsonify
import os
from postgresdb import *
from sqlalchemy import select
from flask_marshmallow import Marshmallow

app = Flask(__name__)
ma = Marshmallow(app) 
session = loadSession()

# for object serialization and deserialization
class HeartrateSchema(ma.Schema):
    class Meta:
        fields = ('individual_id','timestamp','source','value','unit','confidence')

heartrate_schema = HeartrateSchema()
heartrates_schema = HeartrateSchema(many=True)
table_to_model = {'heartrate': Heartrate, 'heart_intensity_minutes': '', 'active_minutes': '', 'resting_calories': '','active_calories':'',
'total_calories': '', 'distance': '', 'weight': '', 'body_fat': '', 'height':'', 'location': '', 'speed': '', 'blood_glucose': '',
'body_temperature':''
}

# api endpoint to get request given an user_id, datatype, source and date range
@app.route('/request', methods=['GET'])
def get_data():
    try:
        individual_id = request.args['user_id']
        source = request.args['source']
        datatype = request.args['datatype']
        query = select(table_to_model[datatype]).where((Heartrate.individual_id == individual_id) & (Heartrate.source == source) & 
        (Heartrate.timestamp.between('2022-02-28T16:50:11.226854','2022-02-28T16:50:11.226990')))
        result = session.execute(query)

        return heartrates_schema.jsonify(result.scalars().all())

    except Exception as e:
        print(e)
        return "Invalid request",400

    
if __name__ == '__main__':
    app.run(debug=True)