from __future__ import print_function
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from oauth2client import file, client, tools
import time
import pprint as pp
import MySQLdb
from apiclient.discovery import build
from httplib2 import Http
import settings


# Функция добавления в БД
def db_query_set(sql):
    cursor.execute(sql)
    db.commit()

# Функция выборки из БД
def db_query_get(sql):
    cursor.execute(sql)
    return cursor.fetchone()


# Setup the Drive v2 API
SCOPES = 'https://www.googleapis.com/auth/drive.metadata.readonly'
store = file.Storage(settings.credentials_path)
creds = store.get()
if not creds or creds.invalid:
    flow = client.flow_from_clientsecrets(settings.client_secret_path, SCOPES)
    creds = tools.run_flow(flow, store)
service = build('drive', 'v2', http=creds.authorize(Http()))

# Подключаемся к google sheets
scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
creds = ServiceAccountCredentials.from_json_keyfile_name(settings.client_json_path, scope)
client = gspread.authorize(creds)

# Подключаемся к БД
db = MySQLdb.connect(settings.db_connection['host'],
					settings.db_connection['user'],
					settings.db_connection['password'],
					settings.db_connection['db'],
					use_unicode=settings.db_connection['use_unicode'],
					charset=settings.db_connection['charset'],)
cursor = db.cursor()

# Очищаем таблицы
sql = "TRUNCATE vehicle_main"
db_query_set(sql)
sql = "TRUNCATE vehicle_available"
db_query_set(sql)
sql = "TRUNCATE vehicle_tesla"
db_query_set(sql)
sql = "TRUNCATE vehicle_comming"
db_query_set(sql)
sql = "TRUNCATE vehicle_photos"
db_query_set(sql)


                                        # VEHICLE AVAILABLE


# Получаем записи из листа Наличия
sheet1 = client.open_by_key('1nzwOMZIHoHFhU-VneJUWMPFjI03b-lXVPoUP9QzF1io').worksheet('Наявність')
sheet_records = sheet1.get_all_records()
print(type(sheet_records))
for record in sheet_records:
# Словарь данных о машине
    vehicle = dict()
# Проверяем доступна ли машина для продажи(Акция, и отстутствие брони)
    if(str(record['Марка авто'])) == '':
        break
    if(str(record['Бронювання']) != '' and str(record['Бронювання']) != 'Акція'):
        continue
    vehicle['veh_title'] = record['Марка авто']
    vehicle['veh_comp'] = record['Комплектація']
# veh_type
    if('*' in str(record['Комплектація'])):
        vehicle['veh_type'] = 'hit'
    else:
        vehicle['veh_type'] = 'fine'
    vehicle['veh_photo'] = 'later'
    vehicle['veh_year'] = str(record['Мод рік'])
    vehicle['veh_mileage'] = str(record['пробіг(км)']).replace('\xa0','')
    vehicle['veh_color'] = str(record['колір'])
    vehicle['veh_color_in'] = str(record['салон'])
    vehicle['veh_price'] = str(record['Ціна в салоні']).replace('\xa0','')
    vehicle['veh_location'] = 'not here'
    vehicle['veh_state'] = 0
    vehicle['veh_battery'] = '24'
# vehicle hyperlink and photos directory
##    hplink = sheet1.find(record['VIN'])
##    hplink_dir = sheet1.acell('D%s'%str(hplink.row),'FORMULA').value
##    if 'google' in hplink_dir:
##        if 'folders' in hplink_dir:
##            hplink_dir = hplink_dir.replace('=HYPERLINK("https://drive.google.com/drive/folders/','')
##        else:
##            hplink_dir = hplink_dir.replace('=HYPERLINK("https://drive.google.com/open?id=',"")
##        hplink_dir = hplink_dir.replace('";"%s")'%record['VIN'],'')
##    else:
##        continue
##    vehicle['veh_folder'] = str(hplink_dir)
    vehicle['veh_folder'] = 'separatly'
    vehicle['battery_state'] = str(record['SOH'])
    vehicle['VIN'] = str(record['VIN'])

# Готовим запрос создания записи машины
    sql = "INSERT INTO hrichard_main.vehicle_main(veh_name, veh_vin, veh_type, veh_status, veh_folder) " \
          "VALUES ('%s', '%s', '%s', '%d', '%s')" % (
           vehicle['veh_title'] + ' ' + vehicle['veh_comp'] + ' ' + vehicle['veh_year'], vehicle['VIN'], 'available', 0, vehicle['veh_folder'])
    db_query_set(sql)

    sql = "SELECT  MAX(id) FROM hrichard_main.vehicle_main"
    sql_res = db_query_get(sql)
    veh_id = sql_res[0]

# Создаем запись доп. информации о машине
    sql = "INSERT INTO hrichard_main.vehicle_available(veh_id, veh_title, veh_comp, veh_year, veh_mileage, veh_color, veh_color_in, veh_state_type, veh_battery_state, veh_price)" \
          "VALUES ('%s', '%s','%s', '%s','%s', '%s','%s', '%s','%s', '%s')" % (
           veh_id, vehicle['veh_title'], vehicle['veh_comp'], vehicle['veh_year'], vehicle['veh_mileage'], vehicle['veh_color'], vehicle['veh_color_in'], vehicle['veh_type'], vehicle['battery_state'], vehicle['veh_price'])
    db_query_set(sql)

# Получаем доступ к фолдеру машины, выгребаем оттуда лопаткой фотки
    # try:
    #     children = service.children().list(folderId=vehicle['veh_folder'], q="mimeType='image/jpeg'").execute()
    #     for child in children.get('items', []):
    #         sql = "INSERT INTO hrichard_main.vehicle_photos(veh_id, photo_id)" \
    #               "VALUES ('%d', '%s')" % ( veh_id, str(child['id']))
    #         db_query_set(sql)
    # except:
    #     print('неуспех блеять')

    print('Finished with %s'%record['VIN'])
    veh_id = 0
    vehicle.clear()


                                        # VEHICLE COMMING


# Получаем записи из листа Приход
sheet1 = client.open_by_key('1nzwOMZIHoHFhU-VneJUWMPFjI03b-lXVPoUP9QzF1io').worksheet('Прихід')
sheet_records = sheet1.get_all_records()

for record in sheet_records:
# Словарь данных о машине
    vehicle = dict()
# Проверяем доступна ли машина нам
    if(str(record['Марка авто'])) == '':
        break
    if(str(record['Прибуття в порт']) == '#N/A' or str(record['Бронювання']) != ''):
        continue
    vehicle['veh_comp'] = str(record['Комплектація'])
    vehicle['veh_title'] = record['Марка авто']
    vehicle['veh_year'] = str(record['Модельний рік'])
    vehicle['veh_mileage'] = str(record['пробіг(км)'])
    vehicle['veh_color'] = str(record['колір'])
    vehicle['veh_color_in'] = str(record['салон'])
    vehicle['veh_price_1'] = str(record['ціни в дорозі\n1000$ бронь'])
    vehicle['veh_price_100'] = str(record['ціни в дорозі\n100% оплата'])
    vehicle['veh_date'] = str(record['Прибуття в порт'])
##    hplink = sheet1.find(record['VIN'])
### vehicle hyperlink and photos directory
##    hplink_dir = sheet1.acell('D%s'%str(hplink.row),'FORMULA').value
##    if 'google' in hplink_dir:
##        if 'folders' in hplink_dir:
##            hplink_dir = hplink_dir.replace('=HYPERLINK("https://drive.google.com/drive/folders/','')
##        else:
##            hplink_dir = hplink_dir.replace('=HYPERLINK("https://drive.google.com/open?id=',"")
##        hplink_dir = hplink_dir.replace('";"%s")'%record['VIN'],'')
##    else:
##        continue
##    vehicle['veh_folder'] = str(hplink_dir)
    vehicle['veh_folder'] = 'separatly'
    vehicle['veh_vin'] = str(record['VIN'])

# Готовим запрос создания записи машины
    sql = "INSERT INTO hrichard_main.vehicle_main(veh_name, veh_vin, veh_type, veh_status, veh_folder)" \
          "VALUES ('%s', '%s', '%s', '%d', '%s')" %(
           vehicle['veh_title'] + ' ' + vehicle['veh_year'] + ' ' + vehicle['veh_comp'],
           vehicle['veh_vin'], 'comming', 0, vehicle['veh_folder'])
    db_query_set(sql)

# Получаем id последней добавленной машины
    sql = "SELECT MAX(id) FROM hrichard_main.vehicle_main"
    sql_res = db_query_get(sql)
    veh_id = sql_res[0]

# Готовим запрос создания записи доп данных машины
    sql = "INSERT INTO hrichard_main.vehicle_comming(veh_id, veh_title, veh_comp, veh_year, veh_mileage, veh_color, veh_color_in, veh_price_1, veh_price_100, veh_date)" \
          "VALUES ('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')" %(
           veh_id, vehicle['veh_title'], vehicle['veh_comp'], vehicle['veh_year'], vehicle['veh_mileage'],
           vehicle['veh_color'], vehicle['veh_color_in'], vehicle['veh_price_1'], vehicle['veh_price_100'], vehicle['veh_date'])
    db_query_set(sql)

# Получаем доступ к фолдеру машины, выгребаем оттуда лопаткой фотки
    # try:
    #     children = service.children().list(folderId=vehicle['veh_folder'], q="mimeType='image/jpeg'").execute()
    #     for child in children.get('items', []):
    #         sql = "INSERT INTO hrichard_main.vehicle_photos(veh_id, photo_id)" \
    #               "VALUES ('%d', '%s')" % (veh_id, str(child['id']))
    #         db_query_set(sql)
    # except:
    #     print('неуспех блеять')

    print('Finished with %s'%record['VIN'])
    veh_id = 0
    vehicle.clear()


print('We are done here!')
db.close()
