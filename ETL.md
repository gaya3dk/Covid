
As part of this task, we would like to hear your opinions on assembling a ETL system in a company, from scratch. 
We are interested to hear which tools & services would you employ to build it and why. What would you optimise for and why? 
How would you go about making debugging data pipelines easier? Would you collect any monitoring data for pipelines & for what purpose? 
Any security considerations to be aware of? How would you imagine this ETL scales as company grows? What else would you say is important when architecting an ETL system from scratch?
Please, outline of the ideal ETL setup. When preparing an overview of your ideal ETL, 
feel free to stay at a high level overview, or deep dive into specifics & low level details. Be ready to receive questions from us regarding the implementation details 
& design decisions you’ve made while in a chat on Slack.


##ETL system from scratch (based on past experience, by no means definitive)
### Design considerations
Some of the decisions depend on the user group and type of usage of data in the company. 
Some questions to ask first
- Are there many SQL users, services or developers?
- What can be an MVP? 
 
### Batch ETLs
**Processing** 
- Apache Spark with fast and resilient distributed data processing for growing data (ETL), SQL friendly
- Open-source framework that supports Python, Scala and SQL APIs. 
- Compute can be on K8S or any implementation of Spark out there (like EMR etc)

**Storage**  
- Data Lake Storage for storage during processing. Open-Source Parquet/Delta file formats  
- Cheap and has pretty much same capabilities as a database/warehouse. 
- Datawarehouse systems need to be looked at when need arises 


**Expose**
  
When clean data is exposed, how do teams or services use it?

- How data is exposed? 
    - So far I have used Hive to expose "views" of the data from data lake and services could use Spark to read it
    - Data can be exposed as dashboards
    - Data can be exposed as plain csvs in some storage
    - APIs (not much experience in this) 

**Orchestration**  
- Any declarative orchestration tooling for scheduling ETL workflows with versioning, unit testing capabilities (example choice : Airflow)
- Maintaining home-baked or buying out-of-box depends on company/team preferences

 
**Monitoring**
- My knowledge here is Work in Progress 

- Any ETL workflow would need monitoring to keep informed about when pipeline started, finished, failure/success, number of records created/modified etc
- Monitor data infrastructure if any (example orchestration tool itself)
- Several ways to monitor if all is good in an ETL instance
    - Pipeline monitoring - I currently collect metrics using telegraf from my Airflow infrastructure on every pipeline and push to azure monitoring (could be Datadog)
    - Data quality monitoring-  From code, one can log non-critical quality checks to separate storage and analyze it later (slow process)
    - Data correctness monitoring - Query the data produced everyday and run quality checks to raise alarms  (quick and easy to do)
- Combined with alerting or monitoring dashboards  

  
#### Nature of ETL pipeline:
- business transformations should be defined together with user during the development of the pipeline (basically one who knows about the data)
- schedule based or event-based (file arriving, data updated in table). Preferably event-based by design- if stack allows
- incremental data processing/micro batching (no full loads)
- Capability to be idempotent (ETL instance can be run multiple times with same result)



#### Security Considerations
 Data processing could follow a layered approach in storage ->  raw, processed, clean

   - apart from infrastructure security, one way of achieving security is to have data processed in several layers (also other benefits)
   - most teams/services have access to clean layer (some exceptions) 
   - raw layer has all data as raw as possible - it can contain sensitive data
   - processed could contain data with business meaning, joined from several sources, sensitive columns removed/anonymized   


#### Infrastructure Considerations

Setting up a good ETL product requires infrastructure best practices, access controls to data.

- Need for infrastructure as code support for data related infrastructure (collaboration with DevOps teams)
- role-based access control and personas are defined together with the Security part of the organization (if one exists)

#### Scaling considerations

Infrastructure needs to be self-healing as number of pipelines grow
Latency can be a problem when data grows. 
Team needs to look at event-streaming platforms to reduce latency of data. Example - Kafka. 
But this naturally means source systems need to change as well. 
