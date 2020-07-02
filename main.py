import pandas as pd
from flask import jsonify
from datetime import datetime
from dateutil.relativedelta import relativedelta

def fetch_employees(request):
    """ HTTP Cloud Function
    :param request: Flask.request
    :return: Flask.response
    """
    if not request.get_json() or "filters" not in request.get_json():
        return jsonify({"error": "Missing json body with 'filters' parameter"}), 400

    filtering_params = request.get_json()["filters"]
    print("Querying employees with filters: {}".format(request.get_json()["filters"]))

    try:
        # read csv into pandas dataframe
        df = pd.read_csv("https://raw.githubusercontent.com/andrewlaguna824/EmployeeDatabase/master/takehome.csv")

        # filter dataframe as needed
        # Name
        if "First" in filtering_params:
            df = df.loc[df['First'] == filtering_params["First"]]
        if "Last" in filtering_params:
            df = df.loc[df['Last'] == filtering_params["Last"]]

        # Email
        if "Email" in filtering_params:
            df = df.loc[df['Email'] == filtering_params["Email"]]

        # Phone
        if "Phone" in filtering_params:
            df = df.loc[df['Phone'] == filtering_params["Phone"]]

        # Language
        if "Languages" in filtering_params:
            df = df.loc[df['Language'].isin(filtering_params["Languages"])]

        # Salary
        if "Salary" in filtering_params:
            if "min" in filtering_params["Salary"]:
                df = df.loc[df['Salary'] >= filtering_params["Salary"]["min"]]
            if "max" in filtering_params["Salary"]:
                df = df.loc[df['Salary'] <= filtering_params["Salary"]["max"]]

        # Hire Date
        if "Date" in filtering_params:
            today = datetime.now()
            # Convert all string dates to datetime object for comparisons
            df["Hire Date"] = pd.to_datetime(df["Hire Date"])
            if "min" in filtering_params["Date"]:
                # Generate comparison date
                min_date = today - relativedelta(years=filtering_params["Date"]["min"])
                df = df.loc[df["Hire Date"] <= min_date]
            if "max" in filtering_params["Date"]:
                # Generate comparison date
                max_date = today - relativedelta(years=filtering_params["Date"]["max"])
                df = df.loc[df["Hire Date"] >= max_date]

        # Convert resulting data into desired output format
        results = []
        for index, row in df.iterrows():
            employee = {
                "name": "{} {}".format(row["First"], row["Last"]),
                "email": row["Email"],
                "phone": row["Phone"]
            }
            results.append(employee)

        return jsonify({"employees": results}), 200
    except Exception as e:
        return jsonify({"error": e}), 400

