import datetime
import json
import re
from config.database import db_connection

from app.helper import save_image, update_image, myLogger


def config(data):
    data_insert = {
        'sangche_id_gach': None,
        'sangche_ngaynop': None,
        'sangche_ten': None,
        'sangche_image': None,
        'sangche_nguoinop_full': None,
        'sangche_nguoinop_name': None,
        'sangche_nguoinop_provcode': None,
        'sangche_nguoinop_address': None,
        'sangche_nguoinop_nationality': None,
        'sangche_bang_id_gach': None,
        'sangche_nguoinop_diachi': None,
        'sangche_bang_id': None,
        'sangche_nguoinop_quocgia': None,
        'sangche_bang_ngaycap': None,
        'sangche_bang_socongbo': None,
        'sangche_bang_ngaycongbo': None,
        'sangche_nguoinop': None,
        'sangche_chubang_info': None,
        'sangche_chubang_info_name': None,
        'sangche_chubang_info_adress': None,
        'sangche_chubang_info_provcode': None,
        'sangche_chubang_info_countrycode': None,
        'sangche_bang_tacgia': None,
        'sangche_bang_diachi': None,
        'sangche_bang_tinh': None,
        'sangche_sodonuutien_basic': None,
        'sangche_ngayuutien': None,
        'sangche_manuocuutien': None,
        'sangche_bang_quocgia': None,
        'sangche_socongbao_a': None,
        'sangche_daidienshtt': None,
        'sangche_ngaycongbao_a': None,
        'sangche_socongbao_b': None,
        'sangche_ngaycongbao_b': None,
        'sangche_ttpl': None,
        'sangche_tltrunggian': None,
        'sangche_tltg': None,
        'sangche_lixangchuyennhuong': None,
        'sangche_tldoichung': None,
        'sangche_tomtat': None,
        'sangche_socongbo': None,
        'sangche_ipc_gach': None,
        'sangche_donquocte': None,
        'sangche_donquocte_ngay': None,
        'sangche_donquocte_wo': None,
        'sangche_donquocte_wo_ngay': None,
        'sangche_tacgia': None,
        'sangche_xnnd_ngay': None,
        'sangche_pct': None,
        'sangche_relate': None,
        'sangche_anh': None,
        'sangche_anh_new': None,
        'sangche_tacgia_diachi': None,
        'sangche_trangthai': None,
        'sangche_tientrinh': None,
        'sangche_ngayvaophaqg': None,
        'created_at': datetime.datetime.now(),
        'updated_at': None,
        'deleted_at': None,
    }

    if '(20) Số đơn và Ngày nộp đơn' in data and data['(20) Số đơn và Ngày nộp đơn']:
        sangche_id_gach, sangche_ngaynop = data['(20) Số đơn và Ngày nộp đơn'].split()[1:3]
        data_insert['sangche_id_gach'] = sangche_id_gach
        data_insert['sangche_ngaynop'] = datetime.datetime.strptime(sangche_ngaynop, "%d.%m.%Y").strftime("%Y-%m-%d")

    if '(54) Tên' in data and data['(54) Tên']:
        data_insert['sangche_ten'] = data['(54) Tên'].split('(VI)')[1].strip()

    if '(57) Tóm tắt"' in data and data['(57) Tóm tắt"']:
        data_insert['sangche_mota'] = data['(57) Tóm tắt"']

    if '(58) Các tài liệu đối chứng"' in data and data['(58) Các tài liệu đối chứng"']:
        data_insert['sangche_tldoichung'] = data['(58) Các tài liệu đối chứng"']

    if '(72) Tác giả sáng chế' in data and data['(72) Tác giả sáng chế']:
        authors = []
        addresses = []
        pattern = r"\(VI\) ([^:]+) : ([^\r\n]+)"
        matches = re.findall(pattern, data['(72) Tác giả sáng chế'])
        for match in matches:
            authors.append(match[0].strip())
            addresses.append(match[1].strip())
        data_insert['sangche_tacgia'] = " ".join(authors)
        data_insert['sangche_tacgia_diachi'] = " ".join(addresses)

    if '(51) Phân loại IPC"' in data and data['(51) Phân loại IPC"']:
        ipc_data = data['(51) Phân loại IPC"'].replace('\r\n', ' ')
        data_insert['sangche_ipc_gach'] = ipc_data

    if '(86) Số đơn và ngày nộp đơn PCT' in data and data['(86) Số đơn và ngày nộp đơn PCT']:
        pct_data = data['(86) Số đơn và ngày nộp đơn PCT'].split()
        if len(pct_data) == 3:
            data_insert['sangche_donquocte'] = pct_data[1]
            data_insert['sangche_donquocte_ngay'] = datetime.datetime.strptime(pct_data[2], "%d.%m.%Y").strftime(
                "%Y-%m-%d")
        elif len(pct_data) == 4:
            data_insert['sangche_donquocte'] = ''.join(pct_data[1:3])
            data_insert['sangche_donquocte_ngay'] = datetime.datetime.strptime(pct_data[3], "%d.%m.%Y").strftime(
                "%Y-%m-%d")
        else:
            data_insert['sangche_donquocte'] = pct_data[0]
            data_insert['sangche_donquocte_ngay'] = datetime.datetime.strptime(pct_data[1], "%d.%m.%Y").strftime(
                "%Y-%m-%d")

    if '(87) Số công bố và ngày công bố đơn PCT' in data and data['(87) Số công bố và ngày công bố đơn PCT']:
        pct_data_2 = data['(87) Số công bố và ngày công bố đơn PCT'].split()
        data_insert['sangche_donquocte_wo'] = pct_data_2[1]
        data_insert['sangche_donquocte_wo_ngay'] = datetime.datetime.strptime(pct_data_2[-1], "%d.%m.%Y").strftime(
            "%Y-%m-%d")

    if '(10) Số bằng và ngày cấp' in data and data['(10) Số bằng và ngày cấp']:
        bang_info = data['(10) Số bằng và ngày cấp'].split()
        data_insert['sangche_bang_id_gach'] = bang_info[0]
        data_insert['sangche_bang_ngaycap'] = datetime.datetime.strptime(bang_info[1], "%d.%m.%Y").strftime("%Y-%m-%d")

    if '(74) Đại diện SHCN' in data and data['(74) Đại diện SHCN']:
        shcn_value = data['(74) Đại diện SHCN']
        if '\r\n' in shcn_value:
            dai_dien_shtt = shcn_value.split('\r\n')[-1].strip()
        elif '(VI)' in shcn_value:
            dai_dien_shtt = shcn_value.split('(VI)')[1].split(':')[0].strip()
        else:
            dai_dien_shtt = ""
        if ':' in shcn_value:
            dia_chi_shtt = shcn_value.split(':', 1)[1].split('\r\n')[0].strip()
        else:
            dia_chi_shtt = ""
        data_insert['sangche_daidienshtt'] = dai_dien_shtt
        # data_insert['sangche_shtt_diachi'] = dia_chi_shtt

    if '(71/73) Chủ đơn/Chủ bằng' in data and data['(71/73) Chủ đơn/Chủ bằng']:
        chubang = [x.strip() for x in data['(71/73) Chủ đơn/Chủ bằng'].replace('(VI)', '').split(':')]
        data_insert['sangche_nguoinop_full'] = data['(71/73) Chủ đơn/Chủ bằng']
        data_insert['sangche_chubang_info_name'] = chubang[0].strip() if chubang else ''
        data_insert['sangche_chubang_info_adress'] = chubang[1].strip() if chubang else ''
        data_insert['sangche_nguoinop'] = chubang[0].strip() if chubang else ''
        data_insert['sangche_nguoinop_address'] = chubang[1].strip() if chubang else ''
        data_insert['sangche_nguoinop_name'] = chubang[0].strip() if chubang else ''

    if '(40) Số công bố và ngày công bố' in data and data['(40) Số công bố và ngày công bố']:
        vn_data = data['(40) Số công bố và ngày công bố'].split("\r\n")
        so_cong_bo = vn_data[0]
        ngay_cong_bo = vn_data[2]
        data_insert['sangche_socongbao_a'] = so_cong_bo
        data_insert['sangche_ngaycongbao_a'] = datetime.datetime.strptime(ngay_cong_bo, "%d.%m.%Y").strftime("%Y-%m-%d")
        data_insert['sangche_socongbo'] = so_cong_bo[0]
        data_insert['sangche_ngaycongbo'] = datetime.datetime.strptime(ngay_cong_bo, "%d.%m.%Y").strftime("%Y-%m-%d")

    if '(85) Ngày vào pha quốc gia' in data and data['(85) Ngày vào pha quốc gia']:
        data_insert['sangche_ngayvaophaqg'] = '(85) Ngày vào pha quốc gia'

    if '(30) Chi tiết về dữ liệu ưu tiên' in data and data['(30) Chi tiết về dữ liệu ưu tiên']:
        date_string = data.get('(30) Chi tiết về dữ liệu ưu tiên', '')
        if date_string:
            dates = date_string.split("\r\n")[1::2]
            formatted_dates = [datetime.datetime.strptime(date, "%d.%m.%Y").strftime("%Y-%m-%d") for date in dates]
            data_insert['sangche_sodonuutien_basic'] = date_string.split()[1]
            data_insert['sangche_ngayuutien'] = formatted_dates[0]
            data_insert['sangche_manuocuutien'] = date_string[:2]

    if '(180) Ngày hết hạn' in data and data['(180) Ngày hết hạn']:
        data_insert['sangche_ttpl'] = datetime.datetime.strptime(data['(180) Ngày hết hạn'], "%d.%m.%Y").strftime(
            "%Y-%m-%d")

    if 'Trạng thái' in data and data['Trạng thái']:
        data_insert['sangche_trangthai'] = data['Trạng thái']

    if 'Tiến trình' in data and data['Tiến trình']:
        data_insert['sangche_tientrinh'] = json.dumps(data['Tiến trình'])

    return data_insert


def insertOrUpdate(data, table="invent"):
    try:
        cursor = db_connection.cursor()
        data_insert = config(data)
        check_query = f"SELECT * FROM `{table}` WHERE `sangche_id_gach` = '{data_insert['sangche_id_gach']}' ORDER BY `id` DESC LIMIT 1"
        cursor.execute(check_query)
        result = cursor.fetchone()
        if 'images' in data and data['images'] != '':
            if result:
                old_image = json.loads(result[4]) if result[4] else []
                # data_insert['sangche_image'] = update_image('invent', data['images'], old_image)
                data_insert['sangche_image'] = old_image
            else:
                data_insert['sangche_image'] = save_image('invent', data['images'])
        if result:
            data_insert['sangche_image'] = json.loads(result[4]) if result[4] else []
            column_update = [
                'sangche_ngaynop', 'sangche_ten', 'sangche_image', 'sangche_nguoinop_full', 'sangche_nguoinop_name',
                'sangche_nguoinop_provcode', 'sangche_nguoinop_address', 'sangche_nguoinop_name',
                'sangche_bang_id_gach',
                'sangche_nguoinop_diachi', 'sangche_bang_id', 'sangche_nguoinop_quocgia', 'sangche_bang_ngaycap',
                'sangche_bang_socongbo', 'sangche_bang_ngaycongbo', 'sangche_nguoinop', 'sangche_chubang_info',
                'sangche_chubang_info_name', 'sangche_chubang_info_adress', 'sangche_chubang_info_provcode',
                'sangche_chubang_info_countrycode', 'sangche_bang_tacgia', 'sangche_bang_diachi', 'sangche_bang_tinh',
                'sangche_sodonuutien_basic', 'sangche_ngayuutien', 'sangche_manuocuutien', 'sangche_bang_quocgia',
                'sangche_socongbao_a', 'sangche_daidienshtt', 'sangche_ngaycongbao_a', 'sangche_socongbao_b',
                'sangche_ngaycongbao_b', 'sangche_ttpl', 'sangche_tltrunggian', 'sangche_tltg',
                'sangche_lixangchuyennhuong',
                'sangche_tldoichung', 'sangche_tomtat', 'sangche_socongbo', 'sangche_ipc_gach', 'sangche_donquocte',
                'sangche_donquocte_ngay', 'sangche_donquocte_wo', 'sangche_donquocte_wo_ngay', 'sangche_tacgia',
                'sangche_xnnd_ngay', 'sangche_pct', 'sangche_relate', 'sangche_anh', 'sangche_anh_new',
                'sangche_tacgia_diachi', 'sangche_trangthai', 'sangche_tientrinh', 'updated_at'
            ]
            column_where = 'sangche_id_gach'
            set_clause = ', '.join([f'{col} = %s' for col in column_update])
            update_query = f"UPDATE {table} SET {set_clause} WHERE {column_where} = %s AND `deleted_at` IS NULL"
            data_tuple_update = (
                data_insert['sangche_ngaynop'], data_insert['sangche_ten'], json.dumps(data_insert['sangche_image']),
                data_insert['sangche_nguoinop_full'], data_insert['sangche_nguoinop_name'],
                data_insert['sangche_nguoinop_provcode'],
                data_insert['sangche_nguoinop_address'], data_insert['sangche_nguoinop_name'],
                data_insert['sangche_bang_id_gach'],
                data_insert['sangche_nguoinop_diachi'], data_insert['sangche_bang_id'],
                data_insert['sangche_nguoinop_quocgia'],
                data_insert['sangche_bang_ngaycap'], data_insert['sangche_bang_socongbo'],
                data_insert['sangche_bang_ngaycongbo'],
                data_insert['sangche_nguoinop'], data_insert['sangche_chubang_info'],
                data_insert['sangche_chubang_info_name'],
                data_insert['sangche_chubang_info_adress'], data_insert['sangche_chubang_info_provcode'],
                data_insert['sangche_chubang_info_countrycode'],
                data_insert['sangche_bang_tacgia'], data_insert['sangche_bang_diachi'],
                data_insert['sangche_bang_tinh'],
                data_insert['sangche_sodonuutien_basic'], data_insert['sangche_ngayuutien'],
                data_insert['sangche_manuocuutien'],
                data_insert['sangche_bang_quocgia'], data_insert['sangche_socongbao_a'],
                data_insert['sangche_daidienshtt'],
                data_insert['sangche_ngaycongbao_a'], data_insert['sangche_socongbao_b'],
                data_insert['sangche_ngaycongbao_b'],
                data_insert['sangche_ttpl'], data_insert['sangche_tltrunggian'], data_insert['sangche_tltg'],
                data_insert['sangche_lixangchuyennhuong'], data_insert['sangche_tldoichung'],
                data_insert['sangche_tomtat'],
                data_insert['sangche_socongbo'], data_insert['sangche_ipc_gach'], data_insert['sangche_donquocte'],
                data_insert['sangche_donquocte_ngay'], data_insert['sangche_donquocte_wo'],
                data_insert['sangche_donquocte_wo_ngay'],
                data_insert['sangche_tacgia'], data_insert['sangche_xnnd_ngay'], data_insert['sangche_pct'],
                data_insert['sangche_relate'], json.dumps(data_insert['sangche_image']), data_insert['sangche_anh_new'],
                data_insert['sangche_tacgia_diachi'], data_insert['sangche_trangthai'],
                data_insert['sangche_tientrinh'],
                datetime.datetime.now(),
                data_insert['sangche_id_gach']
            )
            try:
                cursor.execute(update_query, data_tuple_update)
                db_connection.commit()
                return True
            except Exception as e:
                myLogger(str(e), 'exception')
                db_connection.rollback()
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
                myLogger(str(e), 'exception')
                db_connection.rollback()
                return False
    except Exception as e:
        myLogger(str(e), 'exception')
        db_connection.rollback()
        return False
