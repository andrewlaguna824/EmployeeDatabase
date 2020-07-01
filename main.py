import pandas as pd
from flask import jsonify
from datetime import datetime
from google.cloud import datastore

def parse_csv_to_db(request):
    """
    HTTP Cloud Function
    :param request: Flask.request
    :return: Flask.response
    """

    # Load CSV into pandas dataframe from given URL
    try:
        # TODO: Allow csv path to be in HTTP request
        df = pd.read_csv("https://raw.githubusercontent.com/andrewlaguna824/EmployeeDatabase/master/takehome.csv")

        for index, row in df.iterrows():
            # Create Employee object and save to DB
            ds = datastore.Client()
            key = ds.key("Employee")
            print("Creating new Employee with key: {}".format(key))
            employee = datastore.Entity(key)
            employee.update({
                "Name": "{} {}".format(row["First"], row["Last"]),
                "Salary": int(row["Salary"]),
                "Email": row["Email"],
                "Phone": row["Phone"],
                "Language": row["Language"],
                "Hire Date": datetime.strptime(row["Hire Date"], "%B %d, %Y")
            })

            print("Writing Entity to DB: {}".format(employee))
            ds.put(employee)

        return jsonify({"message": "Success"}), 200

    except Exception as e:
        print("Exception when fetching and/or loading CSV into pandas dataframe: {}".format(e))
        return jsonify({"error": "{}".format(e)}), 500
