**1. How would your describe your ideal ETL setup?**


As part of this task, we would like to hear your opinions on assembling a ETL system in a company, from scratch. 
We are interested to hear which tools & services would you employ to build it and why. What would you optimise for and why? How would you go about making debugging data pipelines easier? Would you collect any monitoring data for pipelines & for what purpose? Any security considerations to be aware of? How would you imagine this ETL scales as company grows? What else would you say is important when architecting an ETL system from scratch?
Please, outline of the ideal ETL setup. When preparing an overview of your ideal ETL, feel free to stay at a high level overview, or deep dive into specifics & low level details. Be ready to receive questions from us regarding the implementation details & design decisions you’ve made while in a chat on Slack.

**2. Home assignment**

In Contractbook you will be working a lot with containers, in a cloud environment, building and maintaining data pipelines by writing and maintaining scripts. You will also help preparing, maintaining and validating report data containing valuable insights for internal clients, such as various department like marketing, sales, customer success, and others.

In this exercise, we would like to understand your skills in basic scripting, basic knowledge about Linux containers, as well as in SQL.
Using your favourite interpreted programming language and data obtained from OWID Covid19 dataset (https://github.com/owid/covid-19-data), we would like you to write a program that will:
download a .csv file containing necessary data on the disk,
connect to a database, create a table and upload the contents of .csv in the table,
query the table to produce the reports below, then output reports on the screen.


The 3 reports are:
1. validate, whether data from the most-recent available day is reasonably clean, valid and reliable; we expect that you suggest a criteria upon which you believe the data can be considered clean, valid and reliable.

2. answer two questions:
    ``` 
    what are the top five countries with the highest vaccination rate?
    what is their vaccination rate?
    ```

3. find out a trend for new covid-19 cases for current week comparing to previous 3 weeks (in table format, by weeks).

We would like to validate the code of your program as well as the results. After completing the script, pack it into a container image & push it to a public docker registry. Finally, provide us with a docker-compose.yaml file that references:
``` 
a container with your program,
a container with a database.
```

We should be able to run your program & verify the output by merely receiving a docker-compose.yaml file from you and running the following command:
docker-compose run exercise
Note, that in case you have any question regarding the task, please do not hesitate to contact us for elaboration.