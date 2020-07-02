# EmployeeDatabase

## Usage
Endpoint is live at https://us-central1-employeedatabase-282002.cloudfunctions.net/fetch_employees

Feel free to use your favorite HTTP client to make a post request and retrive the desired employee data.

Here's a simple curl command to get started with:

    curl --header "Content-Type: application/json" \  
    --request POST \  
    --data  '{"filters": {"Languages": ["spanish", "italian"], "Salary": {"min": 100000, "max": 300000}, "Date": {"min": 2, "max": 3}}}' \  
    https://us-central1-employeedatabase-282002.cloudfunctions.net/fetch_employees