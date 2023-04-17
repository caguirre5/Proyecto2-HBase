from databaseHandler import *


def Init(string):
    lista = [palabra.strip(',') for palabra in string.split()]
    print(lista)

    if lista[0].upper() == 'CREATE':
        return createTable(lista[1], lista[2:])
        pass

    elif lista[0].upper() == 'LIST':
        return listTables()

    elif lista[0].upper() == 'DISABLE':
        return disableTable(lista[1])

    elif lista[0].upper() == 'IS_ENABLED':
        return is_enabled(lista[1])

    elif lista[0].upper() == 'ALTER':
        pass

    elif lista[0].upper() == 'DROP':
        return dropTable(lista[1])

    elif lista[0].upper() == 'DROP_ALL':
        return dropAll()

    elif lista[0].upper() == 'DESCRIBE':
        return describeTable(lista[1])

# DML
    elif lista[0].upper() == 'PUT':
        return Put(lista[1], lista[2], lista[3], lista[4], lista[5])

    elif lista[0].upper() == 'GET':
        if len(lista) > 3:
            return Get(lista[1], lista[2], [lista[3]], [lista[4]])
        else:
            return Get(lista[1], lista[2])

    elif lista[0].upper() == 'SCAN':  # TO DO
        pass

    elif lista[0].upper() == 'DELETE':
        if len(lista) > 3:
            return Delete(lista[1], lista[2], lista[3], lista[4], lista[5])
        else:
            return Delete(lista[1], lista[2])

    elif lista[0].upper() == 'DELETE_ALL':
        if len(lista) > 3:
            return DeleteAll(lista[1], lista[2], lista[3])
        else:
            return DeleteAll(lista[1], lista[2])

    elif lista[0].upper() == 'COUNT':
        print(Count(lista[1]))

    elif lista[0].upper() == 'TRUNCATE':
        return Truncate(lista[1])
