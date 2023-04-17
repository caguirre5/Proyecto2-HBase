import os
import json
import time
import datetime


def read_database(file_path):
    with open(file_path, 'r') as file:
        content = file.read()
    return json.loads(content)


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

    print(f' >> La tabla {tableName} sido creada con exito.')


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


def alterTable(tableName, tipo, columF=None, newName=None):
    path = './data/{}.json'.format(tableName)
    with open(path, 'r') as file:
        content = file.read()
        content = json.loads(content)
        if tipo == 'ADD':
            content['columnfamilies'].extend(columF)
            with open(path, 'w') as file:
                json.dump(content, file)
        elif tipo == 'DROP':
            # Eliminar column family del arreglo
            for item in columF:
                if item in content['columnfamilies']:
                    del content['columnfamilies'][content['columnfamilies'].index(
                        item)]
                    for regionkey in content['regions'].keys():
                        for rowkey in content['regions'][regionkey].keys():
                            del content['regions'][regionkey][rowkey][item]

                    print(
                        f" >> La column family {item} ha sido eliminada correctamente.")

                # for i in columF:
                #     content['columnfamilies'] = content['columnfamilies'].remove(columF)
                # Eliminar column familie de cada row key

            with open(path, 'w') as file:
                json.dump(content, file)
        elif tipo == 'RENAME TO':
            newPath = './data/{}.json'.format(newName)
            if os.path.exists(newPath):
                print(
                    " >> ERROR: no se puede cambiar el nombre de tu lista pues ese nombre ya existe.")
            else:
                file.close()
                os.rename(path, newPath)
            # os.remove(path)


def dropTable(tableName):
    path = './data/{}.json'.format(tableName)
    if os.path.exists(path):
        os.remove(path)
        print(f" >> El archivo {tableName} ha sido eliminado correctamente.")
    else:
        print(f" >> El archivo {tableName} no existe.")


def dropAll():

    for archivo in os.listdir('./data'):
        if archivo.endswith(".json"):
            os.remove(os.path.join('./data', archivo))
            print(f" >> El archivo {archivo} ha sido eliminado correctamente.")

    print(" >> Todos los archivos JSON han sido eliminados de la carpeta 'data'.")


# -------------- DML -------------------------

def Put(tableName, rowKey, columFamily, columnName, value):
    content = None
    path = './data/{}.json'.format(tableName)
    if os.path.exists(path):
        with open(path, 'r') as file:
            content = file.read()
            content = json.loads(content)

        timestamp = datetime.datetime.now().timestamp()
        regionsCountControl = content['regions'].keys()

        # Si el column family no existe
        if columFamily not in content['columnfamilies']:
            # Se agrega al arreglo de columnFamilies
            content['columnfamilies'].append(columFamily)
            # Agregamos el columnFamily a todo el resto de rowkeys
            for regionkey in content['regions'].keys():
                for rowkey in content['regions'][regionkey].keys():
                    content['regions'][regionkey][rowkey][columFamily] = {}

        for rK in content['regions'].copy().keys():
            regionCount = len(content['regions'][rK].copy().keys())
            if regionCount < 5:

                # Comprobar si existen las claves principales
                if rK not in content['regions']:
                    content['regions'][rK] = {}
                if rowKey not in content['regions'][rK]:
                    content['regions'][rK][rowKey] = {}
                if columFamily not in content['regions'][rK][rowKey]:
                    content['regions'][rK][rowKey][columFamily] = {}

                # Comprobar si existe el diccionario de la columna
                if columnName not in content['regions'][rK][rowKey][columFamily]:
                    content['regions'][rK][rowKey][columFamily][columnName] = {}

                # Agregar o actualizar el valor con su timestamp
                content['regions'][rK][rowKey][columFamily][columnName][timestamp] = value

                print(f' >> Se ha agreado exitosamente a la tabla {tableName}')
            else:
                # se crea una nueva region pues todas las otras regions ya estan llenas (balanceo de regions)
                regionsCount = len(content['regions'].keys()) + 1
                temp = 'region' + str(regionsCount)

                # Comprobar si existen las claves principales
                if 'regions' not in content:
                    content['regions'] = {}
                if temp not in content['regions']:
                    content['regions'][temp] = {}
                if rowKey not in content['regions'][temp]:
                    content['regions'][temp][rowKey] = {}
                if columFamily not in content['regions'][temp][rowKey]:
                    content['regions'][temp][rowKey][columFamily] = {}

                # Comprobar si existe el diccionario de la columna
                if columnName not in content['regions'][temp][rowKey][columFamily]:
                    content['regions'][temp][rowKey][columFamily][columnName] = {}

                # Agregar o actualizar el valor con su timestamp
                content['regions'][temp][rowKey][columFamily][columnName][timestamp] = value

                #content['regions'][temp][rowKey][columFamily][columnName][timestamp] = value
                print(f' >> Se ha agreado exitosamente a la tabla {tableName}')

    else:
        print(' >> ERROR: No existe la tabla a la que deseas agregar.')

    with open(path, 'w') as file:
        json.dump(content, file)


def Get():
    pass


def Scan():
    pass


def Delete():
    pass


def DeleteAll():
    pass


def Count():
    pass


def Truncate():
    pass


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
