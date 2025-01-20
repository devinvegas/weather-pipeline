Write a Python script that consumes data from a HTTP Rest API and writes the result to a suitable store of that data. For this example, we will use the OpenMeteo API to obtain weather forecast data at a particular point in time.

https://open-meteo.com/en/docs

Feel free to use the parameters of your choice.

# Notes

- The script will be called with some frequency frequently to ingest the latest data from the API. E.g. hourly, daily
- How would you orchestrate this process in a production environment?
- The state of the data at the time the API was called might be useful.
- It might be a nice feature if the script could be genericised in some way to work for any API
- Some API’s might produce a lot of data, what are good ways of storing this data in ways that will scale well over time
- What things can you do to make my job of evaluating your solution easier?
- Decide how much time you want to spend on this solution, and please add a comment/note somewhere to let me know how much time you did spend on it.

# Submitting your solution

Fork this repo, commit your changes, and create a PR back to the main repo, adding **rkarim-nnk** as a reviewer. Your solution should consist of a Python file, a notes.txt with any additional comments/responses, and any additional files needed to execute/demonstrate your solution.
