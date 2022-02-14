# Introduction

Covid  is a spark application that reads and writes data from Postgres table and outputs 3 reports to console.

- Which are the top 5 vaccinated countries and their vaccination rates  - returns %age of vaccination by population in one view
- Week on week trend of new cases between current and last 3 weeks - returns % age increase of decrease in new cases 


## Repository structure


```bash
.
├── README.md
├── requirements.txt
├── reporting
    |--- src/
    |------/utils/
    |--- test/
|-- conftest.py
```


#### Requirements
- Docker


## PART 1 - How to run docker container

``` 
export PG_PWD="see secret link in chat"
```

```
docker-compose run covidexercise
```

## PART 2 - How to run unit tests locally
#### Requirements
- Python (3.7.3)
 
#### Setup env
```bash
python3 -m venv venv
source venv/bin/activate
pip install --upgrade pip
pip --version
pip install setuptools wheel
pip install -r requirements.txt
```
#### Run tests
``` 
pytest -m reporting
```


