import datetime
import json

from app.helper import save_image, myLogger
from config.database import db_connection_live, db_connection

column_industrial = [
    'kdcn_id', 'kdcn_id_gach', 'kdcn_ngaynop', 'kdcn_nguoinop_provcode', 'kdcn_nguoinop_nationality',
    'kdcn_ten', 'kdcn_mota', 'kdcn_tacgia', 'kdcn_pllocarno', 'kdcn_plquocgia', 'kdcn_bang_id_gach',
    'kdcn_bang_ngaycap', 'kdcn_daidienshtt', 'kdcn_chubang_info_name', 'kdcn_chubang_info_adress',
    'kdcn_chubang_info_provcode', 'kdcn_chubang_info_countrycode', 'kdcn_nguoinop', 'kdcn_nguoinop_address',
    'kdcn_socongbao_a', 'kdcn_ngaycongbao_a', 'kdcn_socongbao_b', 'kdcn_ngaycongbao_b', 'kdcn_socongbo',
    'kdcn_image', 'kdcn_sodonuutien', 'kdcn_ngayuutien', 'kdcn_manuocuutien', 'kdcn_ngaycongbo',
    'kdcn_nguoinop_name', 'kdcn_ttpl', 'kdcn_tltg', 'kdcn_donquocte', 'kdcn_donquocte_ngay', 'created_at',
    'updated_at', 'deleted_at', 'kdcn_tacgia_diachi', 'kdcn_shtt_diachi', 'kdcn_trangthai', 'kdcn_chapnhan_ngay',
    'kdcn_tientrinh', 'kdcn_banchat'
]

def config(data):
    data_insert = {
        'kdcn_id': None,
        'kdcn_id_gach': None,
        'kdcn_ngaynop': None,
        'kdcn_nguoinop_provcode': None,
        'kdcn_nguoinop_nationality': None,
        'kdcn_ten': None,
        'kdcn_mota': None,
        'kdcn_tacgia': None,
        'kdcn_pllocarno': None,
        'kdcn_plquocgia': None,
        'kdcn_bang_id_gach': None,
        'kdcn_bang_ngaycap': None,
        'kdcn_daidienshtt': None,
        'kdcn_chubang_info_name': None,
        'kdcn_chubang_info_adress': None,
        'kdcn_chubang_info_provcode': None,
        'kdcn_chubang_info_countrycode': None,
        'kdcn_nguoinop': None,
        'kdcn_nguoinop_address': None,
        'kdcn_socongbao_a': None,
        'kdcn_ngaycongbao_a': None,
        'kdcn_socongbao_b': None,
        'kdcn_ngaycongbao_b': None,
        'kdcn_socongbo': None,
        'kdcn_image': None,
        'kdcn_sodonuutien': None,
        'kdcn_ngayuutien': None,
        'kdcn_manuocuutien': None,
        'kdcn_ngaycongbo': None,
        'kdcn_nguoinop_name': None,
        'kdcn_ttpl': None,
        'kdcn_tltg': None,
        'kdcn_donquocte': None,
        'kdcn_donquocte_ngay': None,
        'created_at': datetime.datetime.now(),
        'updated_at': None,
        'deleted_at': None,
        'kdcn_tacgia_diachi': None,
        'kdcn_shtt_diachi': None,
        'kdcn_trangthai': None,
        'kdcn_chapnhan_ngay': None,
        'kdcn_tientrinh': None,
        'kdcn_banchat': None,
    }
    if '(20) Số đơn và Ngày nộp đơn' in data and data['(20) Số đơn và Ngày nộp đơn']:
        kdcn_id_gach, kdcn_ngaynop = data['(20) Số đơn và Ngày nộp đơn'].split()[1:3]
        data_insert['kdcn_id_gach'] = kdcn_id_gach
        data_insert['kdcn_ngaynop'] = datetime.datetime.strptime(kdcn_ngaynop, "%d.%m.%Y").strftime("%Y-%m-%d")
    if '(54) Tên kiểu dáng' in data and data['(54) Tên kiểu dáng']:
        data_insert['kdcn_ten'] = data['(54) Tên kiểu dáng'].split('(VI)')[1].strip()
    if 'Tóm tắt' in data and data['Tóm tắt']:
        data_insert['kdcn_mota'] = data['Tóm tắt']
    if '(72) Tác giả kiểu dáng' in data and data['(72) Tác giả kiểu dáng']:
        tacgia = [x.strip() for x in data['(72) Tác giả kiểu dáng'].replace('(VI)', '').split(':')]
        data_insert['kdcn_tacgia'] = tacgia[0].strip() if tacgia else ''
        data_insert['kdcn_tacgia_diachi'] = tacgia[1].strip() if tacgia else ''
    if '(51/52) Phân loại Locarno' in data and data['(51/52) Phân loại Locarno']:
        data_insert['kdcn_pllocarno'] = data['(51/52) Phân loại Locarno'].split(':')[1].strip()
    if '(10) Số bằng và ngày cấp' in data and data['(10) Số bằng và ngày cấp']:
        bang_info = data['(10) Số bằng và ngày cấp'].split()
        data_insert['kdcn_bang_id_gach'] = bang_info[0]
        data_insert['kdcn_bang_ngaycap'] = datetime.datetime.strptime(bang_info[1], "%d.%m.%Y").strftime("%Y-%m-%d")
    if '(74) Đại diện SHCN' in data and data['(74) Đại diện SHCN']:
        if '(VI)' in data['(74) Đại diện SHCN']:
            dai_dien_shtt = data['(74) Đại diện SHCN'].split('(VI)')[1].strip()
        else:
            dai_dien_shtt = ""
        if ':' in data['(74) Đại diện SHCN']:
            dia_chi_shtt = data['(74) Đại diện SHCN'].split(':', 1)[1].strip()
        else:
            dia_chi_shtt = ""
        data_insert['kdcn_daidienshtt'] = dai_dien_shtt
        data_insert['kdcn_shtt_diachi'] = dia_chi_shtt
    if '(71/73) Chủ đơn/Chủ bằng' in data and data['(71/73) Chủ đơn/Chủ bằng']:
        chubang = [x.strip() for x in data['(71/73) Chủ đơn/Chủ bằng'].replace('(VI)', '').split(':')]
        data_insert['kdcn_chubang_info_name'] = chubang[0].strip() if chubang else ''
        data_insert['kdcn_chubang_info_adress'] = chubang[1].strip() if chubang else ''
        data_insert['kdcn_nguoinop'] = chubang[0].strip() if chubang else ''
        data_insert['kdcn_nguoinop_address'] = chubang[1].strip() if chubang else ''
        data_insert['kdcn_nguoinop_name'] = chubang[0].strip() if chubang else ''
    if '(40) Số công bố và ngày công bố' in data and data['(40) Số công bố và ngày công bố']:
        so_cong_bo = data['(40) Số công bố và ngày công bố'].split("\r\n")
        so_cong_bo_a = so_cong_bo[0]
        ngay_cong_bo_a = so_cong_bo[1]
        if so_cong_bo_a and ngay_cong_bo_a:
            data_insert['kdcn_socongbao_a'] = so_cong_bo[0]
            data_insert['kdcn_ngaycongbao_a'] = datetime.datetime.strptime(so_cong_bo[1], "%d.%m.%Y").strftime("%Y-%m-%d")
            data_insert['kdcn_socongbo'] = so_cong_bo[0]
            data_insert['kdcn_ngaycongbo'] = datetime.datetime.strptime(so_cong_bo[1], "%d.%m.%Y").strftime("%Y-%m-%d")
        if len(so_cong_bo) > 2:
            so_cong_bo_b = so_cong_bo[2]
            ngay_cong_bo_b = so_cong_bo[3]
            if so_cong_bo_b and ngay_cong_bo_b:
                data_insert['kdcn_socongbao_b'] = so_cong_bo[2]
                data_insert['kdcn_ngaycongbao_b'] = datetime.datetime.strptime(so_cong_bo[3], "%d.%m.%Y").strftime("%Y-%m-%d")
    if 'Tiến trình' in data and data['Tiến trình']:
        if '(ID) QĐ chấp nhận đơn HT' in data['Tiến trình']:
            data_insert['kdcn_chapnhan_ngay'] = datetime.datetime.strptime(
                data['Tiến trình']['(ID) QĐ chấp nhận đơn HT'], "%d.%m.%Y").strftime("%Y-%m-%d")
    if '(30) Chi tiết về dữ liệu ưu tiên' in data and data['(30) Chi tiết về dữ liệu ưu tiên']:
        date_string = data.get('(30) Chi tiết về dữ liệu ưu tiên', '')
        if date_string:
            dates = date_string.split("\r\n")[1::2]
            formatted_dates = [datetime.datetime.strptime(date, "%d.%m.%Y").strftime("%Y-%m-%d") for date in dates]
            data_insert['kdcn_sodonuutien'] = date_string.split()[1]
            data_insert['kdcn_ngayuutien'] = formatted_dates[0]
            data_insert['kdcn_manuocuutien'] = date_string[:2]
    if 'Trạng thái' in data and data['Trạng thái']:
        data_insert['kdcn_trangthai'] = data['Trạng thái']
    if '(180) Ngày hết hạn' in data and data['(180) Ngày hết hạn']:
        data_insert['kdcn_ttpl'] = datetime.datetime.strptime(data['(180) Ngày hết hạn'], "%d.%m.%Y").strftime("%Y-%m-%d")
    if 'Tiến trình' in data and data['Tiến trình']:
        data_insert['kdcn_tientrinh'] = json.dumps(data['Tiến trình'])
    return data_insert


def insertOrUpdate(data, table="industrial"):
    try:
        cursor = db_connection.cursor()
        data_insert = config(data)
        check_query = f"SELECT * FROM {table} WHERE `kdcn_id_gach` = '{data_insert['kdcn_id_gach']}' ORDER BY `id` DESC LIMIT 1"
        cursor.execute(check_query)
        result = cursor.fetchone()
        if 'images' in data and data['images'] != '':
            if result:
                old_image = json.loads(result[24]) if result[24] else []
                # data_insert['kdcn_image'] = update_image('industrial', data['images'], old_image)
                data_insert['kdcn_image'] = old_image
            else:
                data_insert['kdcn_image'] = save_image('industrial', data['images'])
        if result:
            data_insert['kdcn_image'] = json.loads(result[24]) if result[24] else []
        if result:
            column_update = [
                'kdcn_ngaynop', 'kdcn_ten', 'kdcn_mota', 'kdcn_tacgia', 'kdcn_pllocarno', 'kdcn_plquocgia',
                'kdcn_bang_id_gach',
                'kdcn_bang_ngaycap', 'kdcn_daidienshtt', 'kdcn_chubang_info_name', 'kdcn_chubang_info_adress',
                'kdcn_nguoinop',
                'kdcn_nguoinop_address', 'kdcn_socongbao_a', 'kdcn_ngaycongbao_a', 'kdcn_image', 'kdcn_sodonuutien',
                'kdcn_ngayuutien',
                'kdcn_manuocuutien', 'kdcn_socongbo', 'kdcn_ngaycongbo', 'kdcn_nguoinop_name', 'kdcn_tacgia_diachi',
                'kdcn_shtt_diachi',
                'kdcn_trangthai', 'kdcn_chapnhan_ngay', 'kdcn_ttpl', 'kdcn_tientrinh'
            ]
            column_where = 'kdcn_id_gach'
            set_clause = ', '.join([f'{col} = %s' for col in column_update])
            update_query = f"UPDATE {table} SET {set_clause} WHERE {column_where} = %s AND `deleted_at` IS NULL"
            data_tuple_update = (
                data_insert['kdcn_ngaynop'], data_insert['kdcn_ten'], data_insert['kdcn_mota'],
                data_insert['kdcn_tacgia'],
                data_insert['kdcn_pllocarno'], data_insert['kdcn_plquocgia'], data_insert['kdcn_bang_id_gach'],
                data_insert['kdcn_bang_ngaycap'], data_insert['kdcn_daidienshtt'],
                data_insert['kdcn_chubang_info_name'],
                data_insert['kdcn_chubang_info_adress'], data_insert['kdcn_nguoinop'],
                data_insert['kdcn_nguoinop_address'],
                data_insert['kdcn_socongbao_a'], data_insert['kdcn_ngaycongbao_a'],
                json.dumps(data_insert['kdcn_image']),
                data_insert['kdcn_sodonuutien'], data_insert['kdcn_ngayuutien'], data_insert['kdcn_manuocuutien'],
                data_insert['kdcn_socongbo'], data_insert['kdcn_ngaycongbo'], data_insert['kdcn_nguoinop_name'],
                data_insert['kdcn_tacgia_diachi'], data_insert['kdcn_shtt_diachi'],
                data_insert['kdcn_trangthai'], data_insert['kdcn_chapnhan_ngay'], data_insert['kdcn_ttpl'], data_insert['kdcn_tientrinh'],
                data_insert['kdcn_id_gach']
            )
            try:
                cursor.execute(update_query, data_tuple_update)
                db_connection.commit()
                return True
            except Exception as e:
                myLogger(str(e), 'exception')
                return False
        else:
            columns = ', '.join(data_insert.keys())
            placeholders = ', '.join(['%s'] * len(data_insert))
            insert_query = f""" INSERT INTO {table} ({columns}) VALUES ({placeholders}) """
            values = list(data_insert.values())
            try:
                cursor.execute(insert_query, values)
                db_connection.commit()
                return True
            except Exception as e:
                db_connection.rollback()
                myLogger(str(e), 'exception')
                return False
    except Exception as e:
        myLogger(str(e), 'exception')
        return False


def updateImage(batch_size=100):
    try:
        # Truy vấn tất cả các bản ghi từ cơ sở dữ liệu local
        industrials_local_query = "SELECT kdcn_id_gach, kdcn_image FROM industrial_v2 WHERE kdcn_image IS NOT NULL"
        with db_connection.cursor() as cursor_local:
            cursor_local.execute(industrials_local_query)
            all_local_rows = cursor_local.fetchall()
        data_update = []
        for i in range(0, len(all_local_rows), batch_size):
            batch = all_local_rows[i:i + batch_size]
            for row in batch:
                data_update.append({
                    'kdcn_id_gach': row[0],
                    'images': json.loads(row[1])
                })
        with db_connection_live.cursor() as cursor_live:
            for data in data_update:
                update_query = "UPDATE industrial SET kdcn_image = %s WHERE kdcn_id_gach = %s"
                cursor_live.execute(update_query, (json.dumps(data['images']), data['kdcn_id_gach']))
                db_connection_live.commit()
        return True
    except Exception as e:
        db_connection_live.rollback()
        myLogger(str(e), 'exception')
        return False
