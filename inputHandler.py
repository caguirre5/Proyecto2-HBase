from databaseHandler import *


def Init(string):
    lista = [palabra.strip(';') for palabra in string.split()]

    # CREATE <table_name>, <column_family>
    if lista[0].upper() == 'CREATE':
        return createTable(lista[1], lista[2:])
        pass

    elif lista[0].upper() == 'LIST':
        return listTables()

    # DISABLE <table_name>
    elif lista[0].upper() == 'DISABLE':
        return disableTable(lista[1])

    # IS_ENABLED <table_name>
    elif lista[0].upper() == 'IS_ENABLED':
        return is_enabled(lista[1])

    # **********************
    # ALTER <Table_name>, <ADD>, <[column_families]>
    # ALTER <Table_name>, <DROP>, <[column_families]>
    # ALTER <Table_name>, <RENAME_TO>, <New_name>
    elif lista[0].upper() == 'ALTER':
        if lista[2].upper() == 'ADD':
            my_list = eval(lista[3])
            return alterTable(lista[1], 'ADD', columnF=my_list)
        elif lista[2].upper() == 'DROP':
            my_list = eval(lista[3])
            return alterTable(lista[1], 'DROP', columnF=my_list)
        elif lista[2].upper() == 'RENAME_TO':
            return alterTable(lista[1], 'RENAME_TO', newName=lista[3])

    # DROP <table_name>
    elif lista[0].upper() == 'DROP':
        return dropTable(lista[1])

    # DROP
    elif lista[0].upper() == 'DROP_ALL':
        return dropAll()

    # DESCRIBE <table_name>
    elif lista[0].upper() == 'DESCRIBE':
        return describeTable(lista[1])

# DML

    # put 'nombre_tabla', 'row_key', 'column_family:column', 'valor', timestamp
    elif lista[0].upper() == 'PUT':
        return Put(lista[1], lista[2], lista[3], lista[4], lista[5])

    # GET <table_name>, <rowKey>
    # GET <table_name>, <rowKey>, <columnFam>, <columnsName>
    elif lista[0].upper() == 'GET':
        if len(lista) > 3:
            return Get(lista[1], lista[2], [lista[3]], [lista[4]])
        else:
            return Get(lista[1], lista[2])

    # SCAN <table_name>
    # SCAN <table_name>, <columns>
    # SCAN <table_name>, <startrow>, <stoprow>
    elif lista[0].upper() == 'SCAN':  # TO DO
        if len(lista) == 2:
            return Scan(lista[1])
        elif len(lista) == 3:
            my_list = eval(lista[2])
            return Scan(lista[1], columns=my_list)
        else:
            return Scan(lista[1], startrow=lista[2], stoprow=lista[3])

    # DELETE <table_name>, <rowKey>, <[colFam]>, <[colName]>, <timestamp>
    # DELETE <table_name>, <rowKey>, <[colFam]>, <[colName]>
    # DELETE <table_name>, <rowKey>
    elif lista[0].upper() == 'DELETE':
        if len(lista) == 6:
            return Delete(lista[1], lista[2], lista[3], lista[4], lista[5])
        elif len(lista) == 5:
            return Delete(lista[1], lista[2], lista[3], lista[4])
        else:
            return Delete(lista[1], lista[2])

    # DELETE_ALL <table_name>, <rowKey>, <[colFam]>
    # DELETE_ALL <table_name>, <rowKey>
    elif lista[0].upper() == 'DELETE_ALL':
        if len(lista) > 3:
            return DeleteAll(lista[1], lista[2], lista[3])
        else:
            return DeleteAll(lista[1], lista[2])

    # COUNT <tablename>
    elif lista[0].upper() == 'COUNT':
        return Count(lista[1])

    # TRUNCATE <tablename>
    elif lista[0].upper() == 'TRUNCATE':
        return Truncate(lista[1])
