import requests
from pandas.io.json import json_normalize
import pandas as pd
import pyodbc

def connect_db():
    try:
        conn = pyodbc.connect("Driver={SQL Server Native Client 11.0};"
                              "Server=localhost;"
                              "Database=SingaporeHealthcare;"
                              "Trusted_Connection=yes;")
    except pyodbc.Error as ex:
        sqlstate = ex.args[1]
        print(sqlstate)

    return conn


def update_for_death_life_expectancy(overall):
    # Connect to DB and clear current data
    conn = connect_db()
    try:
        cursor = conn.cursor()
        cursor.execute('DELETE FROM Death_Life_Expectancy')

        # Insert new records
        for item in overall:

            sql = 'INSERT INTO Death_Life_Expectancy (Country_Code, Year, Crude_Death, Infant_Mortality_Rate, Neonatal_Mortality_Rate, ' \
                  'Perinatal_Mortality_Rate, Maternal_Mortality_Rate, Under5_Mortality_Rate, Death_Rate, Total_Death, Life_Expectancy_Birth,' \
                  'Life_Expectancy_Age65) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'
            parameters = ('SG', item['year'], item['crude_death'], item['infant_mortality_rate'], item['neonatal_mortality_rate'],
                          item['perinatal_mortality_rate'], item['maternal_mortality_rate'], item['under5_mortality_rate'],
                          item['death_rate'], item['total_death'], item['life_expectancy_birth'], item['life_expectancy_age65'])
            cursor.execute(sql, parameters)
    except Exception as e:
        print(e)
    finally:
        conn.commit()
        cursor.close()
        conn.close()


url = 'http://www.tablebuilder.singstat.gov.sg/publicfacing/api/json/title/13276.json'
url_2 = 'http://www.tablebuilder.singstat.gov.sg/publicfacing/api/json/title/13274.json'

data = requests.get(url).json()

z = json_normalize(data['Level1'])

level1 = json_normalize(data['Level1'])
level1[['value','year']] = level1[['value','year']].apply(pd.to_numeric)
z = level1[(level1['level_1'] == 'Total Life Expectancy At Birth (Residents)') & (level1['year'] >= 1990)]

life_expectancy_birth = z[['value']]
life_expectancy_birth.columns = ['life_expectancy_birth']
life_expectancy_birth.reset_index(drop = True, inplace=True)

x = level1[(level1['level_1'] == 'Total Life Expectancy At Age 65 Years (Residents)') & (level1['year'] >= 1990)]

life_expectancy_age65 = x[['value']]
life_expectancy_age65.columns = ['life_expectancy_age65']
life_expectancy_age65.reset_index(drop = True, inplace=True)

data_2 = requests.get(url_2).json()

level1e = json_normalize(data_2['Level1e'])
level1e[['value','year']] = level1e[['value','year']].apply(pd.to_numeric)
z = level1e[ (level1e['year'] >= 1990)]
total_death = z[['value']]
total_death.columns = ['total_death']
total_death.reset_index(drop = True, inplace=True)

level1a = json_normalize(data_2['Level1a'])

level1a[['value','year']] = level1a[['value','year']].apply(pd.to_numeric)
z = level1a[(level1a['level_1'] == 'Age-standardised Death Rate') &  (level1a['year'] >= 1990)]
death_rate = z[['value']]
death_rate.columns = ['death_rate']
death_rate.reset_index(drop = True, inplace=True)

z = level1a[(level1a['level_1'] == 'Crude Death Rate') &  (level1a['year'] >= 1990)]
crude_death = z[['value']]
crude_death.columns = ['crude_death']
crude_death.reset_index(drop = True, inplace=True)

level1b = json_normalize(data_2['Level1b'])
level1b.loc[((level1b['value']) =='na'), 'value'] = 0

level1b[['value','year']] = level1b[['value','year']].apply(pd.to_numeric)
z = level1b[(level1b['level_1'] == 'Under-5 Mortality Rate') &   (level1b['year'] >= 1990)]
under5_mortality_rate = z[['value']]
under5_mortality_rate.columns = ['under5_mortality_rate']
under5_mortality_rate.reset_index(drop = True, inplace=True)

z = level1b[(level1b['level_1'] == 'Neonatal Mortality Rate') &   (level1b['year'] >= 1990)]
neonatal_mortality_rate = z[['value']]
neonatal_mortality_rate.columns = ['neonatal_mortality_rate']
neonatal_mortality_rate.reset_index(drop = True, inplace=True)

z = level1b[(level1b['level_1'] == 'Infant Mortality Rate') &   (level1b['year'] >= 1990)]
infant_mortality_rate = z[['value']]
infant_mortality_rate.columns = ['infant_mortality_rate']
infant_mortality_rate.reset_index(drop = True, inplace=True)

level1c = json_normalize(data_2['Level1c'])
level1c[['value','year']] = level1c[['value','year']].apply(pd.to_numeric)
z = level1c[(level1c['level_1'] == 'Perinatal Mortality Rate') &   (level1c['year'] >= 1990)]
perinatal_mortality_rate = z[['value']]
perinatal_mortality_rate.columns = ['perinatal_mortality_rate']
perinatal_mortality_rate.reset_index(drop = True, inplace=True)

level1d = json_normalize(data_2['Level1d'])

level1d[['value','year']] = level1d[['value','year']].apply(pd.to_numeric)
z = level1d[(level1d['level_1'] == 'Maternal Mortality Rate') &   (level1d['year'] >= 1990)]
maternal_mortality_rate = z[['value']]
maternal_mortality_rate.columns = ['maternal_mortality_rate']
maternal_mortality_rate.reset_index(drop = True, inplace=True)

level1d = json_normalize(data_2['Level1d'])

level1d[['value','year']] = level1d[['value','year']].apply(pd.to_numeric)
z = level1d[(level1d['year'] >= 1990)]
year = z[['year']]
year.columns = ['year']
year.reset_index(drop = True, inplace=True)

country_code = pd.DataFrame()

overall = pd.concat([year,crude_death,infant_mortality_rate,neonatal_mortality_rate,
                     perinatal_mortality_rate,maternal_mortality_rate,under5_mortality_rate
                    ,death_rate,total_death,life_expectancy_birth,life_expectancy_age65], axis = 1)
print(overall)

# Update table
update_for_death_life_expectancy(overall)