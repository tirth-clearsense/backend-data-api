import json
from flask import Flask, request, jsonify
import os
from postgresdb import *
from sqlalchemy import select
from flask_marshmallow import Marshmallow

app = Flask(__name__)
ma = Marshmallow(app) 
session = loadSession()

script_dir = os.path.dirname(__file__)
data_dict_path = os.path.join(script_dir,"data_dictionary/personicle_data_types.json")

with open(data_dict_path, 'r') as fi:
            personicle_data_types_json = json.load(fi)
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
def get_table_name(data_type):
    personicle_data_type = data_type.split(".")
    return personicle_data_types_json["com.personicle"]["individual"]["datastreams"][personicle_data_type[-1]]["TableName"] 

# api endpoint to get request given an user_id, datatype, source and date range
@app.route('/request', methods=['GET'])
def get_data():
    try:
        individual_id = request.args['user_id']
        source = request.args['source'] if 'source' in request.args else None
        datatype = get_table_name(request.args['datatype'])
        start_time = request.args['startTime']
        end_time = request.args['endTime']

        query = (select(table_to_model[datatype]).where((Heartrate.individual_id == individual_id) & (Heartrate.source == source) & 
        (Heartrate.timestamp.between(start_time,end_time)))) if source else (select(table_to_model[datatype]).where((Heartrate.individual_id == individual_id) & 
        (Heartrate.timestamp.between(start_time,end_time)))) 

        result = session.execute(query)

        return heartrates_schema.jsonify(result.scalars().all())

    except Exception as e:
        print(e)
        return "Invalid request",400

    
if __name__ == '__main__':
    app.run(debug=True)