import requests
import pyodbc


# Read data from data.gov.sg through api
def retrieve_data_via_api(url):
    data = requests.get(url).json()
    data = data['result']['records']
    return data


def convert_data_for_hospital_beds(jsonobj):
    hospital_beds = []
    for obj in jsonobj:
        if obj['sector'] == 'Public Sector':
            item = {
                'Country_Code': 'SG',
                'Year': obj['year'],
                'Public_Sector': obj['no_of_hospital_beds'],
                'Private_Sector': 0
            }
            hospital_beds.append(item)

        if obj['sector'] == 'Private Sector':
            exist = False
            for item in hospital_beds:
                if item['Year'] == obj['year']:
                    exist = True
                    item['Private_Sector'] = obj['no_of_hospital_beds']

            if not exist:
                item = {
                    'Country_Code': 'SG',
                    'Year': obj['year'],
                    'Private_Sector': obj['no_of_hospital_beds'],
                    'Public_Sector': 0
                }
                hospital_beds.append(item)

    return hospital_beds


def convert_data_for_healthcare_graduate(jsonobj):
    # Get graduate types
    types = []
    infos = {}

    for obj in jsonobj:
        types.append(obj['graduate_type'])

    types = list(set(types))
    types.sort()

    for i in range(1, len(types) + 1):
        infos[types[i - 1]] = i

    # Convert data for table Healthcare_Graduate
    healthcare_graduate = []

    for obj in jsonobj:
        if obj['no_of_graduates'] == 'na':
            obj['no_of_graduates'] = 0

        item = {
            'Country_Code': 'SG',
            'Year': obj['year'],
            'Graduate_Type_ID': infos[obj['graduate_type']],
            'Graduate_Number': obj['no_of_graduates']
        }
        healthcare_graduate.append(item)

    # Convert data for table Graduate_Type
    graduate_type = []

    for val in infos:
        item = {
            'ID': infos[val],
            'Graduate_Type': val
        }

        graduate_type.append(item)

    return healthcare_graduate, graduate_type


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


def update_for_hospital_beds(hospital_beds):
    # Connect to DB and clear current data
    conn = connect_db()
    try:
        cursor = conn.cursor()
        cursor.execute('DELETE FROM Hospital_Bed')

        # Insert new records
        for item in hospital_beds:

            sql = 'INSERT INTO Hospital_Bed (Country_Code, Year, Public_Sector, Private_Sector) VALUES (?, ?, ?, ?)'
            parameters = (item['Country_Code'], item['Year'], item['Public_Sector'], item['Private_Sector'])
            cursor.execute(sql, parameters)
    except Exception as e:
        print(e)
    finally:
        conn.commit()
        cursor.close()
        conn.close()


def update_for_healthcare_graduate(healthcare_graduate, graduate_type):
    # Connect to DB and clear current data
    conn = connect_db()
    try:
        cursor = conn.cursor()
        cursor.execute('DELETE FROM Healthcare_Graduate')
        cursor.execute('DELETE FROM Graduate_Type')

        # Insert new records
        for item in graduate_type:

            sql = 'INSERT INTO Graduate_Type (ID, Graduate_Type) VALUES (?, ?)'
            parameters = (item['ID'], item['Graduate_Type'])
            cursor.execute(sql, parameters)

        for item in healthcare_graduate:

            sql = 'INSERT INTO Healthcare_Graduate (Country_Code, Year, Graduate_Type_ID, Graduate_Number) VALUES (?, ?, ?, ?)'
            parameters = (item['Country_Code'], item['Year'], item['Graduate_Type_ID'], item['Graduate_Number'])
            cursor.execute(sql, parameters)
    except Exception as e:
        print(e)
    finally:
        conn.commit()
        cursor.close()
        conn.close()


## Main
if __name__ == '__main__':
    # Table: Hospital_Bed
    url = 'https://data.gov.sg/api/action/datastore_search?resource_id=9df79e72-a7ed-4df3-9a60-5fe434f38fe7&limit=200'
    jsonobj = retrieve_data_via_api(url)

    # Convert JSON file to table format
    hospital_beds = convert_data_for_hospital_beds(jsonobj)

    # Update table
    update_for_hospital_beds(hospital_beds)

    # Table: Healthcare_Graduare & Graduate_Type
    url = 'https://data.gov.sg/api/action/datastore_search?resource_id=85183e12-cf96-437e-b048-d06f6b84f228&limit=60'
    jsonobj = retrieve_data_via_api(url)

    # Convert JSON file to table format
    healthcare_graduate, graduate_type = convert_data_for_healthcare_graduate(jsonobj)

    # Update table
    update_for_healthcare_graduate(healthcare_graduate, graduate_type)

