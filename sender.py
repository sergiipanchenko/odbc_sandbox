import pyodbc
import logging
import requests


format_log = '%(asctime)s; %(levelname)s; %(message)s'
logging.basicConfig(filename='send_log', level=logging.INFO, format=format_log)


def send():
    # host = 'http://78.109.28.77/'
    host = 'http://192.168.0.117:5000/'
    api = 'fields/api/v01/train_weight'
    storage = 'Фастів ХПП'
    data = wagons()

    try:
        response = requests.post(url=host+api, json={'storage': storage, 'wagons': data})
    except Exception as e:
        logging.error('sending failed (error: ' + str(e) + ')')
    else:
        status = str(response.status_code)
        if status == '200':
            # записать в файл последний отправленный id вагона
            if data:
                with open('last_id_vagon', 'w') as file:
                    file.write(str(data[-1][0]))
            logging.info('success send ' + str(len(data)) + ' ids (status_code: ' + status + ')')
        else:
            logging.error('sending failed (status_code: ' + status + ')')


def wagons():
    # соединение с локальной БД
    conn_str = (
        r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};'
        # r'DBQ=C:\Users\HPP\AppData\Local\VirtualStore\Program Files\TechnowagyLTD\VagonScale\VagonVaga\Database\vagonvaga.mdb;'
        r'DBQ=\vagonvaga.mdb;'
        r'PWD=2708351'
        )
    cnxn = pyodbc.connect(conn_str)
    crsr = cnxn.cursor()

    # чтение из файла последнего отправленного id
    with open("last_id_vagon") as file:
        last_id = int(file.read())

    # запрос свежих записей в БД
    crsr.execute("""SELECT  id_vagon, brutto, tara, netto, nom_vagon,
                            dateb, timeb, rezhym, nom_poizd, kil_vagon,
                            name_oder,
                            name_gruz,
                            name_perev,
                            name_vidpr
                    FROM    vagon,
                            main_vagon,
                            oder,
                            gruz,
                            perev,
                            vidpr
                    WHERE   id_vagon > ? AND id_vagon <= ? AND
                            cod_main_vagon = id_main_vagon AND
                            cod_oder = id_oder AND
                            cod_gruz = id_gruz AND
                            cod_perev = id_perev AND
                            cod_vidpr = id_vidpr
                    order by id_vagon ASC;
                    """, (last_id, last_id+1000))

    request = crsr.fetchall()
    cnxn.close()

    # упаковка для json
    data = []
    for each in request:
        wagon = []
        for e in each:
            wagon.append(str(e))
            # {
            # 'id_vagon': each[0],
            # 'brutto': each[1],
            # 'tara': each[2],
            # 'netto': each[3],
            # 'nom_vagon': each[4],
            #
            # 'dateb': str(each[5])[:10],
            # 'timeb': str(each[6])[11:],
            #
            # 'rezhym': each[7],
            # 'nom_poizd': each[8],
            # 'kil_vagon': each[9],
            #
            # 'name_oder': each[10],
            # 'name_gruz': each[11],
            # 'name_perev': each[12].replace('"', ''),
            # 'name_vidpr': each[13].replace('"', '')
            # }
        data.append(wagon)

    # записать в файл последний отправленный id вагона
    # if data:
    #     with open('last_id_vagon', 'w') as file:
    #         file.write(str(data[-1][0]))

    return data


if __name__ == '__main__':
    send()
