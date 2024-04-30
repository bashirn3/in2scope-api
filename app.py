from flask import Flask, request, jsonify, send_file
import os
import psycopg2
from psycopg2 import extras  # Import DictCursor
import requests
import csv
from io import StringIO
import concurrent.futures
from flask_cors import CORS


from dotenv import load_dotenv



app = Flask(__name__)
CORS(app)


# Load environment variables from .env file
load_dotenv()

# Google Maps API key
GOOGLE_MAPS_API_KEY = os.getenv('GOOGLE_MAPS_API_KEY')


# Establish a connection to the PostgreSQL database
# "dpg-cm0k1ei1hbls73dbibjg-a.oregon-postgres.render.com
conn = psycopg2.connect(
    host="postgres://in2scopeuser:1dUd7javDFVD6QJ6YhMRsqRihgnzvvhR@dpg-conr3u21hbls73fp2h10-a.oregon-postgres.render.com/in2scope2_itk7?ssl=true",
    database="in2scope2_itk7",
    user=os.getenv('DB_USERNAME'),
    password=os.getenv('DB_PASSWORD')
)

# Open a cursor to perform database operations
cur = conn.cursor()
cur = conn.cursor(cursor_factory=extras.DictCursor)


@app.route('/health')
def health_check():
    return "The app is working correctly"


@app.route('/find_schools', methods=['POST'])
def find_schools():
    try:
        # Get data from the request
        data = request.get_json()

        # Validate required fields
        required_fields = ['postcode', 'mode_of_transport', 'max_travel_time']
        for field in required_fields:
            if field not in data:
                raise ValueError(f"Missing required field: {field}")

        validate_postcode(data['postcode'])
        validate_mode_of_transport(data['mode_of_transport'])
        validate_max_travel_time(data['max_travel_time'])

        # Extract candidate's information
        candidate_postcode = data['postcode']
        max_travel_time = data['max_travel_time']
        mode_of_transport = data['mode_of_transport']

        # Retrieve schools from the database
        cur.execute('SELECT * FROM schools;')

        schools = cur.fetchall()

        # Filter schools based on the smart algorithm using Google Maps API
        filtered_schools = filter_schools_parallel(schools, candidate_postcode, max_travel_time, mode_of_transport)

        return jsonify({'schools': filtered_schools})

    except ValueError as ve:
        return jsonify({'error': str(ve)}), 400  # Bad Request
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    




@app.route('/filter_csv', methods=['POST'])
def filter_csv():
    try:
        # Get the CSV file from the request
        csv_file = request.files.get('csv_file')

        # csv_data = csv_file.read().decode('utf-8')
        # csv_reader = csv.DictReader(StringIO(csv_data))

        csv_data = csv_file.read()
        csv_file_object = StringIO(csv_data.decode('utf-8'))
        csv_reader = csv.DictReader(csv_file_object)

          # Validate CSV columns
        required_columns = ['postcode', 'First Name', 'Last Name', 'Email', 'Name',]
        for column in required_columns:
         if column not in csv_reader.fieldnames:
            raise ValueError(f"Missing required column: {column}")

        schools = []
        for row in csv_reader:
          # Your processing logic here
          print(row)

          schools.append(row)


# Use StringIO to create a file-like object

     
          # Extract JSON data from the form
          
    
        candidate_postcode = request.form.get('postcode')
   
        max_travel_time = request.form.get('max_travel_time')
        mode_of_transport = request.form.get('mode_of_transport')


    
       
    
        #     # Validate required fields
        # required_fields = ['postcode', 'mode_of_transport', 'max_travel_time']
        # for field in required_fields:
        #         if field not in data:
        #             raise ValueError(f"Missing required field: {field}")

        # validate_postcode(data['postcode'])
        # validate_mode_of_transport(data['mode_of_transport'])
        # validate_max_travel_time(data['max_travel_time'])

        #     # Extract candidate's information
        #     candidate_postcode = data['postcode']
        #     max_travel_time = data['max_travel_time']
        #     mode_of_transport = data['mode_of_transport']
        # else:
        #  raise ValueError("Missing JSON data in the request.")

        filtered_schools = filter_schools2(schools, candidate_postcode, max_travel_time, mode_of_transport)
        return jsonify(filtered_schools)


        # return jsonify(schools)

        
        # Filter schools based on the CSV data
       

      

        # # Create a new CSV in memory
        # output_csv = StringIO()
        # csv_writer = csv.DictWriter(output_csv, fieldnames=['postcode', 'First Name', 'LastName', 'Email', 'Name' 'travel_time'])
        # csv_writer.writeheader()
        # csv_writer.writerows(filtered_schools)

        # # Return the new CSV as a downloadable file
        # output_csv.seek(0)
        # return send_file(BytesIO(output_csv.read()),
        #                  mimetype='text/csv',
        #                  download_name='filtered_results.csv',
        #                  as_attachment=True)

    except ValueError as ve:
        return jsonify({'error': str(ve)}), 400  # Bad Request
    except Exception as e:
        return jsonify({'error': str(e)}), 500

        










# helper functions

def validate_postcode(postcode):
    # Add your postcode validation logic here
    # For example, check if the postcode format is valid
    if not postcode or not isinstance(postcode, str) or len(postcode) < 5:
        raise ValueError("Invalid postcode")

def validate_mode_of_transport(mode_of_transport):
    # Add your mode of transport validation logic here
    valid_modes = ['transit', 'driving', 'bicycling']
    if mode_of_transport not in valid_modes:
        raise ValueError("Invalid mode_of_transport")

def validate_max_travel_time(max_travel_time):
    # Add your max travel time validation logic here
    if not isinstance(max_travel_time, (int, float, str)) or float(max_travel_time) <= 0:
        raise ValueError("Invalid max_travel_time")



def get_travel_time(origin_postcode, destination_postcode, mode_of_transport):
    # Make a request to Google Maps Distance Matrix API
    url = f'https://maps.googleapis.com/maps/api/distancematrix/json?origins={origin_postcode}&destinations={destination_postcode}&mode={mode_of_transport}&key={GOOGLE_MAPS_API_KEY}'
    response = requests.get(url)
    data = response.json()

    # Extract travel time from the API response
    try:
        travel_time_seconds = data['rows'][0]['elements'][0]['duration']['value']
        travel_time_minutes = travel_time_seconds / 60
        return travel_time_minutes
    except (KeyError, IndexError):
        # Handle invalid response or missing data
        return float('inf')  # Return infinity for an invalid travel time

# def get_travel_time(origin, destinations, mode_of_transport):
#     # Check cache first
#     cache_key = (origin, tuple(destinations), mode_of_transport)
  
 
#     if cache_key in travel_time_cache:
#         return travel_time_cache[cache_key]
    


#     # Call Google Maps Distance Matrix API for batch processing
#     matrix = gmaps.distance_matrix(origin, destinations, mode=mode_of_transport)


#     # Parse results and extract travel time
#     travel_times = []
#     for element in matrix['rows'][0]['elements']:
#         if 'duration' in element:
#             travel_times.append(element['duration']['value'])
#         else:
#             # Handle cases where travel time couldn't be calculated
#             travel_times.append(float('inf'))

#     # Update cache
#     travel_time_cache[cache_key] = travel_times

   
#     return travel_times

# def get_travel_time(origin, destinations, mode_of_transport):
#     # Call Google Maps Distance Matrix API for batch processing
#     matrix = gmaps.distance_matrix(origin, destinations, mode=mode_of_transport)

#     # Parse results and extract travel time
#     travel_times = []
#     for element in matrix['rows'][0]['elements']:
#         if 'duration' in element:
#             travel_times.append(element['duration']['value'])
#         else:
#             # Handle cases where travel time couldn't be calculated
#             travel_times.append(float('inf'))

#     return travel_times






def filter_schools_parallel(schools, candidate_postcode, max_travel_time, mode_of_transport):
    filtered_schools = []

    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = []
        for school in schools:
            futures.append(
                executor.submit(
                    get_travel_time, candidate_postcode, school["postcode"], mode_of_transport
                )
            )

        for school, future in zip(schools, futures):
            travel_time = future.result()
            max_travel_time_to_use = float(max_travel_time)
            if travel_time <=  max_travel_time_to_use:
                filtered_schools.append({
                    'id': school["id"],
                    'schoolname': school["schoolname"],
                    'postcode': school["postcode"],
                    'address': school["address"],
                    'latitude': school["latitude"],
                    'longitude': school["longitude"],
                    'website': school["website"],
                    'borough': school["borough"],
                    'travel_time': int(travel_time)
                })

    # Sort schools based on travel time
    filtered_schools.sort(key=lambda x: x['travel_time'])

    return filtered_schools




def filter_schools2(schools, candidate_postcode, max_travel_time, mode_of_transport):
    filtered_schools=[]
    for school in schools:
        # Get travel time from Google Maps Distance Matrix API
        travel_time = get_travel_time(candidate_postcode, school["postcode"], mode_of_transport)

        max_travel_time = float(max_travel_time)

        # Check if the school is within the specified max travel time
        # postcode', 'First Name', 'Last Name', 'Email', 'Name' "School Name"
        if travel_time <= max_travel_time:
            filtered_schools.append({
                'schoolname': school["Name"],
                'postcode': school["postcode"],
                'borough': school["Borough"],
                'email': school["Email"],
                'first name': school["First Name"],
                'last name': school["Last Name"],
                'position': school["Postion"],
                'travel_time': int(travel_time)
            })
    


    # Sort schools based on travel time
    filtered_schools.sort(key=lambda x: x['travel_time'])

    return filtered_schools


if __name__ == '__main__':
    app.run(debug=True,port=os.getenv('PORT'))



