import json
import requests
import shutil 
import os
import datetime
import subprocess
from zipfile import ZipFile

def deleteTemp():
    if(not(os.path.isdir(f'{PATH}tmp/'))):
       os.mkdir(f'{PATH}/tmp')
    else:
       shutil.rmtree(f'{PATH}tmp/')
       os.mkdir(f'{PATH}/tmp')




def loadConfig(location):
    f = open(location)
    data = json.load(f)
    f.close()

    return data

def loadFiles(filesLocation):
    #Ver si el archivo existe
    f = open(filesLocation)
    data = json.load(f)
    f.close()
    return data


#Main Path
PATH = __file__.replace('main.py','')
#Load Config File
CONFIG = loadConfig(PATH+"/config.json")
#File names
FILES = loadFiles(CONFIG['dir_json'])


def handleFiles(s):
    tmp_files = os.listdir(PATH+'tmp/')
    files = {}
    for file in tmp_files:
        files[file] = open(PATH+'tmp/'+file, 'rb')
    response = s.post(CONFIG['url_upload'],files=files)
    print(response.json())

def clearName(name):
    replace = [' ',':','-','.','/']
    for item in replace:
        name = name.replace(item, "_")
    return name


def login():
    s = requests.Session()
    result = s.post(CONFIG['url_auth'],
    json={"username":CONFIG['username'],"password":CONFIG['password']}
    )

    return s

    

def getFilename():
    filename = str(datetime.datetime.now())
    filename = clearName(filename)
    return (CONFIG["start_file"]+filename)


def createZip(filename):
    if(CONFIG['single_file']):
        if(FILES):
            fileLine = f"zip -r {PATH}tmp/{filename} "
            for file in FILES:
                fileLine += file+" "
            os.system(fileLine)
    else:
        for file in FILES:
            name = clearName(file)
            fileLine = f"zip -r {PATH}tmp/{filename}_{name} {file}"
            os.system(fileLine)     

    


def checkDefaultTables(tableName):
    default_tables = ['information_schema',
    'mysql','performance_schema','sys']
    for table in default_tables:
        if(tableName == table):
            return False
    return True


def main():
    deleteTemp()
    


    
    if(CONFIG['db_backup']):
        import pandas
        import mysql.connector
        if(not(os.path.isdir(f'{PATH}db'))):
            os.mkdir(f'{PATH}db/')
        else:
            shutil.rmtree(f'{PATH}db')
            os.mkdir(f'{PATH}db/')


        db = mysql.connector.connect(
        host=CONFIG['db_url'],
        user=CONFIG['db_user'],
        password=CONFIG['db_pass'],
        port=CONFIG['db_port']
        )

        cursor = db.cursor(dictionary=True)

        #Borra los antiguos sql
        for dir in os.listdir(f'{PATH}db'):
            shutil.rmtree(f'{PATH}db/{dir}')


        cursor.execute("SHOW DATABASES")
        dbs = cursor.fetchall();
        for bd in dbs:

            bd = bd['Database']
            if checkDefaultTables(bd) :
                os.mkdir(f'{PATH}db/{bd}')
                #Export to .sql file
                username = CONFIG['db_user']
                password = CONFIG['db_pass']
                host = CONFIG['db_pass']
                port = CONFIG['db_pass']

                
                subprocess.Popen(f'mysqldump -h "{host}" -u "{username}" -p"{password}"  {bd}  --port {port} > {PATH}db/{bd}/{bd}.sql', shell=True)
                db.database = bd

                #Export to .csv file
                cursor.execute(f"""SELECT table_name FROM information_schema.tables WHERE table_type='BASE TABLE' AND table_schema = '{bd}'""")
                tablas = cursor.fetchall();
                for table in tablas:
                    table = table['table_name']
                    cursor.execute(f'SELECT * FROM `{table}` ')
                    data = cursor.fetchall()
                    df = pandas.DataFrame(data)
                    
                    df.to_csv(f'{PATH}/db/{bd}/{table}.csv', index=False)
        shutil.make_archive(f'tmp/db_{getFilename()}', 'zip', f'{PATH}db')

    
    #Main zip name
    filename = getFilename()
    for item in FILES:
        if(not(os.path.exists(item))):
            print('ERROR: No se encontro el archivo '+item)
            exit()

    createZip(filename)
    handleFiles(login())
    




    
    



    


main()