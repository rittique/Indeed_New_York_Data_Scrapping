import facebook
import pandas as pd
import pymongo
import time
from pymongo import MongoClient
from transformers import pipeline
import gspread

gc = gspread.service_account(filename = "jobapp-eb06c-270a71c74d88.json")
KEY = "1nCT9zi1zic2SYKHsEMp9rm4rWA0bDTZcknXGWlZhrDc"

workSheet = gc.open_by_key(KEY)

current_sheet = workSheet.sheet1

connection = MongoClient("mongodb://localhost:27017")
db = connection.IndeedJobPostDB
df = pd.DataFrame(list(db.Job_Posts.find()))
df = df.sort_values(by=['Current_Date'], ascending=False)
df = df.reset_index(drop=True)

print("Connected to the database successfully.")

#df = df[df.Schedule.notnull()]
#df = df[df.Salary.notnull()]

def exceptionWait(exception, min):
	if exception.strip() == "(#4) Application request limit reached":
		print("-----------------------------------------------")
		print(f"| Sleeping for {min} mintutes                    |\n")
		print(f"| Will start the automation after {min} mintutes.|")
		print("-----------------------------------------------")
		time.sleep(min*60)

def message(df, i, summary):
    return (
        f'**Test Post: {i}**\n'
        f'Post ID: {df.JobID[i]}\n'
        f'Job Opening: {df.Title[i]}\n'
        f'\n'
        f'{df.Location[i]}\n'
        f'\n'
        f'{df.Job_Type[i]}\n'
        f'\n'
        f'👉Comment "Interested" If You want to apply.\n'
        f'\n'
        f'Work Summary: \n'
        f"{summary}\n"
        f'\n'
        f'#houstonjobs #houston #ergool #houstonjob #jobsearch #jobseekers #search #empower #career #nowhiring #jobopportunities #jobplacement #employmentassistance #jobupdates #restaurantjobs #retailjobs #jobopening #jobalert'
    )

groups = ["674232410710265", "658314902622656"] #NY, TX
access_token = "EAAJHKZAZAZBPbsBO8niM1nsPQz23X70xBOofkcKhc7hMwHSLrE7UvTZBY29ZAfc608FBIYSpVZAZCe7tH6TZAkExuC1ZCcg2psobZBnjxQcqyZCk6ZAEK6mxbiUPKuUOIhSwidTu1b1x0rEyNTFeSaTpWHwa92pAT61oHDZBDgU4xsXd8AoyaykpnUlIRPCExul0sa9xjXne4PZB0f3AVNg6dsPFET1VAAqhTI6CUKZAklypC1OVgNR87g2uTxU39CZCxysZD"
graph = facebook.GraphAPI(access_token=access_token)

for row in range(len(df)):
    print(f'Post No. {row}')
    try:
        summary = df.Details[row]
        
        if "NY" in df.Location[row]:
            try:
                x = graph.put_object(groups[0], 'feed', message=message(df, row, summary))
                print(x)
                print("Posted Successfully in NY Group!")
                data_to_send = [datetime.now().date(), datetime.now().time(), df.JobID[row], df.Title[row], df.Company[row], df.Location[row], df.Apply_link[row]]  # Replace with your data
                current_sheet.insert_row(data_to_send, 2)
            except Exception as error:
                print("An exception occurred:", error)
                print(f"Passed the error and posted {df.JobID[row]}")
		data_to_send = [datetime.now().date(), datetime.now().time(), df.JobID[row], df.Title[row], df.Company[row], df.Location[row], df.Apply_link[row]]  # Replace with your data
                current_sheet.insert_row(data_to_send, 2)
        elif "TX" in df.Location[row]:
            try:
                x = graph.put_object(groups[1], 'feed', message=message(df, row, summary))
                print(x)
                print("Posted Successfully in the TX Group!")
                data_to_send = [datetime.now().date(), datetime.now().time(), df.JobID[row], df.Title[row], df.Company[row], df.Location[row], df.Apply_link[row]]  # Replace with your data
                current_sheet.insert_row(data_to_send, 2)
            except Exception as error:
                print("An exception occurred:", error)
                print(f"Passed the error and posted {df.JobID[row]}")
    except Exception as error:
        print("An exception occurred:", error)
     
    
    time.sleep(10)

print(f"Posted {count} posts.")
