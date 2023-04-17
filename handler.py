import os
import json
import time


def read_database(file_path):
    with open(file_path, 'r') as file:
        content = file.read()
    return json.loads(content)


def create_record(database, row_key, column_family, column, timestamp, value):
    if row_key not in database:
        database[row_key] = {}
    if column_family not in database[row_key]:
        database[row_key][column_family] = {}
    if column not in database[row_key][column_family]:
        database[row_key][column_family][column] = {}
    if timestamp in database[row_key][column_family][column]:
        print(
            f"Warning: a record with the same key {row_key}, column family {column_family}, column {column} and timestamp {timestamp} already exists")
    database[row_key][column_family][column][timestamp] = value


def createTable(tableName, columnFamily):
    # tableName = nombre del json
    # columnFamily = array con todos los nombres de columnas a ingresar en el json

    data = {
        'status': 'ENABLED',
        'columnfamilies': columnFamily,
        'regions': {}
    }

    path = './data/{}.json'.format(tableName)

    with open(path, 'w') as file:
        json.dump(data, file)


def listTables():
    tables = os.listdir('./data')

    # Imprimir los nombres de archivo
    print(' >> Tablas en HBase:')
    for name in tables:
        print('    -' + name.replace('.json', ''))


def disableTable(tableName):

    path = './data/{}.json'.format(tableName)
    with open(path, 'r') as file:
        content = file.read()
        content = json.loads(content)
        print(content['status'])
        if content['status'] == 'DISABLED':
            # ya estaba bloqueada
            print(' >> ERROR: la lista que desea deshabilitar ya se encuentra bloqueada.')
        else:
            content['status'] = 'DISABLED'
        print(content['status'])

    with open(path, 'w') as file:
        json.dump(content, file)


def is_enabled(tableName):
    path = './data/{}.json'.format(tableName)
    with open(path, 'r') as file:
        content = file.read()
        content = json.loads(content)
        print(content['status'])
        if content['status'] == 'ENABLED':
            # ya estaba bloqueada
            print(True)
        else:
            print(False)


def alterTable(tableName, tipo, kwargs=[]):
    path = './data/{}.json'.format(tableName)
    with open(path, 'r') as file:
        content = file.read()
        content = json.loads(content)
        print(type(content['columnfamilies']))
        if tipo == 'ADD':
            families = kwargs['families']
            for i in families:
                content['columnfamilies'].append(i)
        elif tipo == 'DROP':
            pass
        elif tipo == 'RENAME TO':
            new_name = kwargs['newName']
            os.rename(path, new_name)


def dropTable(tableName):
    path = './data/{}.json'.format(tableName)
    if os.path.exists(path):
        os.remove(path)
        print(f"El archivo {tableName} ha sido eliminado correctamente.")
    else:
        print(f"El archivo {tableName} no existe.")


def dropAll():

    for archivo in os.listdir('./data'):
        if archivo.endswith(".json"):
            os.remove(os.path.join('./data', archivo))
            print(f"El archivo {archivo} ha sido eliminado correctamente.")

    print("Todos los archivos JSON han sido eliminados de la carpeta 'data'.")


'''
Table ventas is ENABLED
ventas
COLUMN FAMILIES DESCRIPTION
{NAME => 'detalles', BLOOMFILTER => 'ROW', VERSIONS => '1', IN_MEMORY => 'false', KEEP_DELETED_CELLS => 'FALSE', DATA_BLOCK_ENCODING => 'NONE', TTL => 'FOREVER', COMPRESSION => 'NONE', MIN_VERSIONS => '0', BLOCKCACHE => 'true', BLOCKSIZE => '65536', REPLICATION_SCOPE => '0'}
{NAME => 'resumen', BLOOMFILTER => 'ROW', VERSIONS => '1', IN_MEMORY => 'false', KEEP_DELETED_CELLS => 'FALSE', DATA_BLOCK_ENCODING => 'NONE', TTL => 'FOREVER', COMPRESSION => 'NONE', MIN_VERSIONS => '0', BLOCKCACHE => 'true', BLOCKSIZE => '65536', REPLICATION_SCOPE => '0'}
2 row(s)
Took 0.0449 seconds

'''


def describeTable(tableName):
    ti = time.time()
    path = './data/{}.json'.format(tableName)
    with open(path, 'r') as file:
        content = file.read()
        content = json.loads(content)
        # table status
        print('\nTable {} is {}'.format(tableName, content['status']))
        print(tableName)
        print('COLUMN FAMILIES DESCRIPTION')
        
        for column in content['columnfamilies']:
            print(f"{{NAME => '{column}', BLOOMFILTER => 'ROW', VERSIONS => '1', IN_MEMORY => 'false', KEEP_DELETED_CELLS => 'FALSE', DATA_BLOCK_ENCODING => 'NONE', TTL => 'FOREVER', COMPRESSION => 'NONE', MIN_VERSIONS => '0', BLOCKCACHE => 'false', BLOCKSIZE => '65536', REPLICATION_SCOPE => '0'}}")

        print('{} row(s)'.format(len(content['columnfamilies'])))

    tf = time.time()  # registra el tiempo de fin
    totaltime = tf - ti
    print('Took {} seconds'.format(round(totaltime, 4)))
    #   llave del valor
# database[rowkey][columnfamily][columnqualifier][timestamp]
