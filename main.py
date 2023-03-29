from flask import Flask, jsonify, request
import uuid
import asyncio
import pandas as pd
import datetime 
import threading
from dateutil import parser
from reportGenerator import generate_final_report
import requests
app = Flask(__name__)



# Async function to generate report
def generate_report(report_id):
    # Simulate report generation process
    # Hardcoded time and date based on max data in current static data set
    print('report id',report_id)
    data = generate_final_report(datetime.date(2023, 1, 25), parser.parse('2023-01-25 19:00:00'))
    # Generate report file
    data.to_csv(f'{report_id}.csv', index=False)
    return 



# Endpoint to trigger report generation
@app.route('/trigger_report', methods=['GET'])
def trigger_report():
    # Generate report ID
    report_id = str(uuid.uuid4())
    # Start async task to generate report
    # requests.get("http://127.0.0.1:5000/trigger_report_internal?report_id="+report_id)
    thread = threading.Thread(target=generate_report, args=(report_id,))
    thread.start()
    # Return report ID as JSON
    return jsonify(report_id=report_id)


# Endpoint to get report status or CSV file
@app.route('/get_report', methods=['GET'])
def get_report():
    # Get report ID from request
    report_id = request.args.get('report_id')
    if not report_id:
        return jsonify(error='Missing report ID')
    try:
        # Try to open report file
        with open(f'{report_id}.csv', 'r') as file:
            # Return CSV file if report has been generated
            return file.read(), 200, {'Content-Type': 'text/csv'}
    except FileNotFoundError:
        # Return status if report has not been generated
        return 'Running' 

if __name__ == '__main__':
    
    app.run(debug=True)