import facebook
import pandas as pd
import pymongo
import time
from pymongo import MongoClient
from transformers import pipeline

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
access_token = "EAAJHKZAZAZBPbsBO5t4D0jmbuVZBM41QJlvw8OB7ZASADxZA5F1RsSPBmbzzvcZBPyVBrE7T5qhJeasJ2kkkWwJ6H9Dro96ZCbpuhBGT2ZB9urGe0M2wscFemWtZB6VyEot2ZBSihFX1jQ5ZAoK26m65eQ3TicbZBoENSsiwuxweTnRZBJKTehz9J3ZAqeAg0vZA"
graph = facebook.GraphAPI(access_token=access_token)
print("Starting the post automation")
count = 0
for row in range(len(df)):
    print(f'Trying to post instance {row}')
    print(f"Will try to post in {df.Location[row]} group")
    try:
        summary = df.Details[row]
        
        if "NY" in df.Location[row]:
            try:
                x = graph.put_object(groups[0], 'feed', message=message(df, row, summary))
                print(x)
                print("Posted Successfully in NY Group!")
                count+=1
            except Exception as error:
                print("An exception occurred:", error)
                #print(f"Passed the error and posted {df.JobID[row]}")
        elif "TX" in df.Location[row]:
            try:
                x = graph.put_object(groups[1], 'feed', message=message(df, row, summary))
                print(x)
                print("Posted Successfully in the TX Group!")
                count+=1
            except Exception as error:
                print("An exception occurred:", error)
                #print(f"Passed the error and posted {df.JobID[row]}")
    except Exception as error:
        print("An exception occurred:", error)
     
    
    time.sleep(100)

print(f"Posted {count} posts.")
