import pandas as pd
from flask import jsonify
from datetime import datetime
from dateutil.relativedelta import relativedelta
# from google.cloud import datastore
import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore

def fetch_employees(request):
    """ HTTP Cloud Function
    :param request: Flask.request
    :return: Flask.response
    """
    print(request.get_json())
    if not request.get_json():
        return jsonify({"error": "No json body with 'filters' parameter"}), 400
    filtering_params = request.get_json()["filters"]

    try:
        # read csv into pandas dataframe
        df = pd.read_csv("https://raw.githubusercontent.com/andrewlaguna824/EmployeeDatabase/master/takehome.csv")

        results = []

        # filter dataframe as needed

        return jsonify({"employees": results}), 200
    except Exception as e:
        return jsonify({"error": e}), 400

def fetch_employees_firestore(request):
    """ HTTP Cloud Function
    :param request: Flask.request
    :return: Flask.response
    """
    print(request.get_json())
    if not request.get_json():
        return jsonify({"error": "No json body with 'filters' parameter"}), 400
    filtering_params = request.get_json()["filters"]

    try:
        # Init firestore
        cred = credentials.ApplicationDefault()
        if not firebase_admin._apps:
            firebase_admin.initialize_app(cred, {
                'projectId': 'employeedatabasefirestore'
            })

        db = firestore.client()
        query = db.collection("Employee")

        # Name
        if "First" in filtering_params:
            print("First included")
            query.where("First", "==", filtering_params["First"])
        if "Last" in filtering_params:
            print("Last included")
            query.where("Last", "==", filtering_params["Last"])

        # Email
        if "Email" in filtering_params:
            print("Email included")
            query.where("Email", '==', filtering_params["Email"])

        # Phone
        if "Phone" in filtering_params:
            print("Phone included")
            query.where("Phone", '==', filtering_params["Phone"])

        # Language
        if "Language" in filtering_params:
            print("Language included")
            query.where("Language", 'in', filtering_params["Language"])

        # Salary
        # if "Salary" in filtering_params:
        #     print("Salary included")
        #     # TODO: Make sure salaries are in number format
        #     if "min" in filtering_params["Salary"]:
        #         print("Min Salary included")
        #         query.add_filter("Salary", ">=", filtering_params["Salary"]["min"])
        #     if "max" in filtering_params["Salary"]:
        #         print("Max Salary included")
        #         query.add_filter("Salary", ">=", filtering_params["Salary"]["min"])

        # # Hire Date
        # today = datetime.now()
        # if "Date" in filtering_params:
        #     print("Date included")
        #     if "min" in filtering_params["Date"]:
        #         print("Min Date included")
        #         # TODO: Make sure min is integer
        #         # Turn integer into a time delta
        #         min_date = today - relativedelta(years=filtering_params["Date"]["min"])
        #         query.add_filter("Hire Date", "<=", min_date)
        #     if "max" in filtering_params["Date"]:
        #         print("Max Date included")
        #         # TODO: Make sure min is integer
        #         # Turn integer into a time delta
        #         max_date = today + relativedelta(years=filtering_params["Date"]["max"])
        #         query.add_filter("Hire Date", "<=", max_date)

        # Query
        # print("Querying Employee DB with filters: {}".format(query.filters))
        results = list(query.get())

        return jsonify({"employees": results}), 200
    except Exception as e:
        return jsonify({"message": "Request error: {}".format(e)}), 500

# def fetch_employees_datastore(request):
#     """ HTTP Cloud Function
#     :param request: Flask.request
#     :return: Flask.response
#     """
#     print(request.get_json())
#     if not request.get_json():
#         return jsonify({"error": "No json body with 'filters' parameter"}), 400
#     filtering_params = request.get_json()["filters"]
#
#     # Construct datastore query
#     ds = datastore.Client()
#     query = ds.query(kind="Employee")
#
#     try:
#         # Name
#         if "First" in filtering_params:
#             print("First included")
#             query.add_filter("First", "=", filtering_params["First"])
#         if "Last" in filtering_params:
#             print("Last included")
#             query.add_filter("Last", "=", filtering_params["Last"])
#
#         # Email
#         if "Email" in filtering_params:
#             print("Email included")
#             query.add_filter("Email", '=', filtering_params["Email"])
#
#         # Phone
#         if "Phone" in filtering_params:
#             print("Phone included")
#             # TODO: Make sure phone number is in number format
#             query.add_filter("Phone", '=', filtering_params["Phone"])
#
#         # Language
#         if "Language" in filtering_params:
#             print("Language included")
#             for i in filtering_params["Language"]:
#                 query.add_filter("Language", '=', i)
#
#         # Salary
#         if "Salary" in filtering_params:
#             print("Salary included")
#             # TODO: Make sure salaries are in number format
#             if "min" in filtering_params["Salary"]:
#                 print("Min Salary included")
#                 query.add_filter("Salary", ">=", filtering_params["Salary"]["min"])
#             if "max" in filtering_params["Salary"]:
#                 print("Max Salary included")
#                 query.add_filter("Salary", ">=", filtering_params["Salary"]["min"])
#
#         # Hire Date
#         today = datetime.now()
#         if "Date" in filtering_params:
#             print("Date included")
#             if "min" in filtering_params["Date"]:
#                 print("Min Date included")
#                 # TODO: Make sure min is integer
#                 # Turn integer into a time delta
#                 min_date = today - relativedelta(years=filtering_params["Date"]["min"])
#                 query.add_filter("Hire Date", "<=", min_date)
#             if "max" in filtering_params["Date"]:
#                 print("Max Date included")
#                 # TODO: Make sure min is integer
#                 # Turn integer into a time delta
#                 max_date = today + relativedelta(years=filtering_params["Date"]["max"])
#                 query.add_filter("Hire Date", "<=", max_date)
#
#         # Query
#         print("Querying Employee DB with filters: {}".format(query.filters))
#         results = list(query.fetch())
#
#         return jsonify({"employees": results}), 200
#     except Exception as e:
#         return jsonify({"message": "Request error: {}".format(e)}), 500

def parse_csv_to_firestore(csv_path):
    """
    Quick function to populate the DB from a given CSV
    """
    try:
        # read csv into pandas dataframe
        df = pd.read_csv(csv_path)

        # Init firestore
        cred = credentials.Certificate("employeedatabasefirestore.json")
        firebase_admin.initialize_app(cred)
        db = firestore.client()

        # Batch up DB call with up to 500 entities
        employees = []
        # batch = db.batch()
        for index, row in df.iterrows():
            # Create Employee object and save to DB
            employee = db.collection("Employee").document("{} {}".format(row["First"], row["Last"])).set({
                "First": row["First"],
                "Last": row["Last"],
                "Salary": int(row["Salary"]),
                "Email": row["Email"],
                "Phone": int(row["Phone"]),
                "Language": row["Language"],
                "Hire Date": datetime.strptime(row["Hire Date"], "%B %d, %Y")
            })

    except Exception as e:
        print("Exception populating DB from given CSV: {}".format(e))


if __name__ == "__main__":
    print("Running Main")
    parse_csv_to_firestore("https://raw.githubusercontent.com/andrewlaguna824/EmployeeDatabase/master/takehome.csv")
