import os
import json
import time
import datetime


def is_replace(contentJSON, rowKey):
    region = None
    # Verifica si el
    for region in contentJSON['regions'].keys():
        if rowKey in contentJSON['regions'][region].keys():
            return True, region
    return False, region


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

    return(f' >> La tabla {tableName} sido creada con exito.')


def listTables():
    tables = os.listdir('./data')

    # Imprimir los nombres de archivo
    table_names = ''
    for name in tables:
        table_names += ('    -' + name.replace('.json', '') + '\n')

    return ' >> Tablas en HBase:\n' + table_names


def disableTable(tableName):
    retorno = ''
    path = './data/{}.json'.format(tableName)
    if os.path.exists(path):
        with open(path, 'r') as file:
            content = file.read()
            content = json.loads(content)
            if content['status'] == 'DISABLED':
                # ya estaba bloqueada
                retorno += \
                    (' >> La lista que desea deshabilitar ya se encuentra bloqueada.\n')
            else:
                content['status'] = 'DISABLED'
                retorno += (f' >> La tabla <{tableName}> ha sido deshabilitada.\n')

        with open(path, 'w') as file:
            json.dump(content, file)

        return retorno
    else:
        return(f" >> La tabla <{tableName}> no existe.\n")


def is_enabled(tableName):
    path = './data/{}.json'.format(tableName)
    with open(path, 'r') as file:
        content = file.read()
        content = json.loads(content)
        if content['status'] == 'ENABLED':
            # ya estaba bloqueada
            return (True)
        else:
            return (False)


def alterTable(tableName, tipo, columF=None, newName=None):
    retorno = ''
    path = './data/{}.json'.format(tableName)
    with open(path, 'r') as file:
        content = file.read()
        content = json.loads(content)
        if tipo == 'ADD':
            content['columnfamilies'].extend(columF)
            retorno = + (
                f" >> Se han agregado con exito datos a la tabla {item}\n")
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

                    retorno = +(
                        f" >> La column family {item} ha sido eliminada correctamente.\n")

                # for i in columF:
                #     content['columnfamilies'] = content['columnfamilies'].remove(columF)
                # Eliminar column familie de cada row key

            with open(path, 'w') as file:
                json.dump(content, file)
        elif tipo == 'RENAME TO':
            newPath = './data/{}.json'.format(newName)
            if os.path.exists(newPath):
                retorno += (
                    " >> ERROR: no se puede cambiar el nombre de tu lista pues ese nombre ya existe.\n")
            else:
                file.close()
                os.rename(path, newPath)
    return retorno


def dropTable(tableName):
    path = './data/{}.json'.format(tableName)
    if os.path.exists(path):
        if is_enabled(tableName):
            return(f" >> La tabla <{tableName}> no se puede eliminar mientras esté enabled.\n")
        else:
            os.remove(path)
            return(f" >> La tabla <{tableName}> ha sido eliminada correctamente.\n")
    else:
        return(f" >> La tabla <{tableName}> no existe.\n")


def dropAll():
    retorno = ''
    for archivo in os.listdir('./data'):
        if archivo.endswith(".json"):
            os.remove(os.path.join('./data', archivo))
            retorno += (f" >> El archivo {archivo} ha sido eliminado correctamente.\n")

    retorno += (" >> Todos los archivos JSON han sido eliminados de la carpeta 'data'.\n")
    return retorno


def describeTable(tableName):
    retorno = ''
    ti = time.time()
    path = './data/{}.json'.format(tableName)
    with open(path, 'r') as file:
        content = file.read()
        content = json.loads(content)
        # table status
        retorno += ('\nTable {} is {}\n'.format(tableName, content['status']))
        retorno += (str(tableName) + '\n')
        retorno += ('COLUMN FAMILIES DESCRIPTION\n')

        for column in content['columnfamilies']:
            retorno += (f"{{NAME => '{column}', BLOOMFILTER => 'ROW', VERSIONS => '1', IN_MEMORY => 'false', KEEP_DELETED_CELLS => 'FALSE', DATA_BLOCK_ENCODING => 'NONE', TTL => 'FOREVER', COMPRESSION => 'NONE', MIN_VERSIONS => '0', BLOCKCACHE => 'false', BLOCKSIZE => '65536', REPLICATION_SCOPE => '0'}}\n")

        retorno += ('{} row(s)\n'.format(len(content['columnfamilies'])))

    tf = time.time()  # registra el tiempo de fin
    totaltime = tf - ti
    retorno += ('Took {} seconds\n'.format(round(totaltime, 4)))
    return retorno

# -------------- DML -------------------------


def Put(tableName, rowKey, columFamily, columnName, value):
    retorno = ''

    # if columFamily in contentJSON['regions'][region][rowKey]:
    #     if columnName in contentJSON['regions'][region][rowKey][columFamily]:
    #         return True

    content = None
    path = './data/{}.json'.format(tableName)
    if os.path.exists(path):
        with open(path, 'r') as file:
            content = file.read()
            content = json.loads(content)

        timestamp = datetime.datetime.now().timestamp()

        if columFamily not in content['columnfamilies']:
            # Se agrega al arreglo de columnFamilies
            content['columnfamilies'].append(columFamily)
            # Agregamos el columnFamily a todo el resto de rowkeys
            for regionkey in content['regions'].keys():
                for rowkey in content['regions'][regionkey].keys():
                    content['regions'][regionkey][rowkey][columFamily] = {}

        result, regionFound = is_replace(content, rowKey)
        # Si el row key ya existe
        if result:
            # Vamos a hacer una actualizacion
            content['regions'][regionFound][rowKey][columFamily][columnName] = {}
            content['regions'][regionFound][rowKey][columFamily][columnName][timestamp] = value
            retorno += (f' >> El rowKey {rowKey} Se ha actualizado correctamente\n')
        # Si no
        else:
            # Vamos a crear uno nuevo
            # validamos cantidad de rowkeys en el ultimo region
            ultima_region = '1'
            RegionCount = 0
            if len(content['regions'].keys()) > 0:
                ultima_region = sorted(content['regions'].keys())[-1]
                RegionCount = len(content['regions'][ultima_region])
            else:
                content['regions'][ultima_region] = {}
            # Si el region tiene menos de 5 rowkeys, agregamos el rowkey en este region
            if RegionCount < 5:
                content['regions'][ultima_region][rowKey] = {}
                content['regions'][ultima_region][rowKey][columFamily] = {}
                content['regions'][ultima_region][rowKey][columFamily][columnName] = {}
                content['regions'][ultima_region][rowKey][columFamily][columnName][timestamp] = value
            # si no, creamos un nuevo region
            else:
                regionsCount = len(content['regions'].keys()) + 1
                temp = str(regionsCount)
                content['regions'][temp] = {}
                content['regions'][temp][rowKey] = {}
                content['regions'][temp][rowKey][columFamily] = {}
                content['regions'][temp][rowKey][columFamily][columnName] = {}
                content['regions'][temp][rowKey][columFamily][columnName][timestamp] = value
            retorno += (
                f' >> El rowKey {rowKey} Se ha agreado exitosamente a la tabla {tableName}\n')
        with open(path, 'w') as file:
            json.dump(content, file)
    else:
        retorno += (' >> ERROR: No existe la tabla a la que deseas agregar.\n')

    return retorno


def Get(tableName, rowKey, columnsFam=None, columnsName=None):
    retorno = ''
    ti = time.time()
    content = None
    path = './data/{}.json'.format(tableName)
    temp = 0
    if os.path.exists(path):
        with open(path, 'r') as file:
            content = file.read()
            content = json.loads(content)

        if columnsFam and columnsName:
            # quiere columnas en especifico
            for regK in content['regions'].copy().keys():
                if rowKey in content['regions'][regK]:
                    retorno += ('\n    COLUMN                       CELL\n')
                    # si encuentra el rowkey mencionado
                    for i in columnsFam:
                        for x in columnsName:
                            for ts, val in content['regions'][regK][rowKey][i][x].items():
                                temp += 1
                                retorno += (
                                    f'{i} : {x} timestamp={ts}, value={val}\n\n')

            pass
        else:
            # quiere todas las columnas del row
            for regK in content['regions'].copy().keys():
                if rowKey in content['regions'][regK]:
                    retorno += ('\n    COLUMN                       CELL\n')
                    # si encuentra el rowkey mencionado
                    for colFKey, colFValue in content['regions'][regK][rowKey].items():
                        for colContentKey, colContentValue in content['regions'][regK][rowKey][colFKey].items():
                            for ts, val in content['regions'][regK][rowKey][colFKey][colContentKey].items():
                                temp += 1
                                retorno += (
                                    f'{colFKey} : {colContentKey} timestamp={ts}, value={val}\n\n')

    tf = time.time()  # registra el tiempo de fin
    totaltime = tf - ti
    retorno += (f'{temp} row(s) in {round(totaltime, 4)} seconds\n')
    return retorno


def Scan(tableName, startrow=None, stoprow=None, columns=None):
    content = None
    path = './data/{}.json'.format(tableName)
    with open(path, 'r') as file:
        content = file.read()
        content = json.loads(content)
        file.close()

    if startrow and stoprow:
        print('ROW\t\tCOLUMN\t\t\t\TIMESTAMP\t\t\tVALUE')
        for region in content['regions'].keys():
            for rowkey in content['regions'][region]:
                for columnfamily in content['regions'][region][rowkey]:
                    for columnName in content['regions'][region][rowkey][columnfamily]:
                        for timestamp in content['regions'][region][rowkey][columnfamily][columnName]:
                            value = content['regions'][region][rowkey][columnfamily][columnName][timestamp]
                            print(
                                str(rowkey) + '\t' + str(columnfamily) + '\t' + str(columnName) + '\t' +
                                str(timestamp) + '\t' + str(value)
                            )
    elif startrow and not stoprow:
        return '>> ERROR: Has ingresado startrow pero no stoprow.\n'
    elif not startrow and stoprow:
        return '>> ERROR: Has ingresado stoprow pero no startrow.\n'
    # Mostrar con el filtro de columns
    elif not startrow and not stoprow and columns:
        pass
    # Mostrar todas las filas - Este ya funciona!
    else:
        print('ROW\t\tCOLUMN\t\t\t\TIMESTAMP\t\t\tVALUE')
        for region in content['regions'].keys():
            for rowkey in content['regions'][region]:
                for columnfamily in content['regions'][region][rowkey]:
                    for columnName in content['regions'][region][rowkey][columnfamily]:
                        for timestamp in content['regions'][region][rowkey][columnfamily][columnName]:
                            value = content['regions'][region][rowkey][columnfamily][columnName][timestamp]
                            print(
                                str(rowkey) + '\t' + str(columnfamily) + '\t' + str(columnName) + '\t' +
                                str(timestamp) + '\t' + str(value)
                            )


def Delete(tableName, rowKey, ColFam=None, ColName=None, timeStam=None):
    retorno = ''
    ti = time.time()
    content = None
    path = './data/{}.json'.format(tableName)
    temp = 0
    if os.path.exists(path):
        with open(path, 'r') as file:
            content = file.read()
            content = json.loads(content)

        if ColFam and ColName:

            # content['regions'][regionFound][rowKey][columFamily][columnName] = {}
            result, region = is_replace(content, rowKey)

            if result:
                if ColFam in content['regions'][region][rowKey]:
                    if ColName in content['regions'][region][rowKey][ColFam]:
                        if timeStam:
                            # ingreso timestamp
                            if timeStam in content['regions'][region][rowKey][ColFam][ColName]:
                                del content['regions'][region][rowKey][ColFam][ColName][timeStam]
                                retorno += (
                                    ' >> Se ha borrado con exito.\n')
                            else:
                                retorno += (
                                    ' >> ERROR: No existe el timestamp que se ingreso.\n')
                        else:
                            # no ingreso timestamp
                            content['regions'][region][rowKey][ColFam][ColName] = {
                            }
                            retorno += (
                                ' >> Se ha borrado con exito.\n')

                    else:
                        retorno += (
                            ' >> ERROR: No existe la column name que se ingreso.\n')
                else:
                    retorno += (
                        ' >> ERROR: No existe la column family que se ingreso.\n')
            else:
                retorno += (' >> ERROR: No exist el row key \n')
        else:
            # delete all
            retorno += (' >> ERROR: la funcion delete no soporta los parametros ingresado, prueba utilizando el comando DELETE ALL\n')

    with open(path, 'w') as file:
        json.dump(content, file)

    tf = time.time()  # registra el tiempo de fin
    totaltime = tf - ti
    retorno += (f'{temp} row(s) in {round(totaltime, 4)} seconds\n')
    return retorno


def DeleteAll(tableName, rowKey, columnFamily=None):
    retorno = ''
    ti = time.time()
    content = None
    path = './data/{}.json'.format(tableName)
    with open(path, 'r') as file:
        content = file.read()
        content = json.loads(content)
        file.close()

    # si hay columnFamily
    if columnFamily:
        result, region = is_replace(content, rowKey)

        if result:
            if columnFamily in content['regions'][region][rowKey]:
                content['regions'][region][rowKey][columnFamily] = {
                }
                retorno += (
                    ' >> Se ha borrado con exito.\n')
            else:
                retorno += (' >> ERROR: No se ha encontrado la column family solicitada\n')

        else:
            retorno += (' >> ERROR: No se ha encontrado el row key solicitado\n')
    # No ingresaron column family
    else:
        result, region = is_replace(content, rowKey)
        if result:
            content['regions'][region][rowKey] = {
            }
            retorno += (' >> Se ha borrado con exito.\n')
        else:
            retorno += (' >> ERROR: No se ha encontrado el row key\n')
    with open(path, 'w') as file:
        json.dump(content, file)

    tf = time.time()  # registra el tiempo de fin
    totaltime = tf - ti
    retorno += (f'0 row(s) in {round(totaltime, 4)} seconds\n')
    return retorno


def Count(tableName):
    content = None
    path = './data/{}.json'.format(tableName)
    with open(path, 'r') as file:
        content = file.read()
        content = json.loads(content)
        file.close()
    counter = 0
    for region in content['regions']:
        for rowkey in content['regions'][region]:
            counter += 1
    return (counter)


def Truncate(tableName):
    retorno = ''
    content = None
    path = './data/{}.json'.format(tableName)
    with open(path, 'r') as file:
        content = file.read()
        content = json.loads(content)
    colFam = content['columnfamilies']
    disableTable(tableName)
    dropTable(tableName)
    createTable(tableName, colFam)
    retorno += (f' >> Truncate exitoso con la tabla {tableName}\n')
    return retorno
