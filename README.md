a very simplified streaming ml pipeline to serve predictions

three services:

1. etl - ingests data, does some light feature engineering
2. model - makes predictions
3. app - consumes data from etl and model service  


to run you'll need kafka installed locally, and mongodb running. then start the services
```
python etl.py     
python model.py  
python app.py    
```