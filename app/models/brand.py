import datetime
import json
import requests

from app.helper import save_image, update_image, myLogger
from config.database import db_connection_live, db_connection

import os
from dotenv import load_dotenv
load_dotenv()


def config(data):
    data_insert = {
        'nhanhieu_id_gach': None,
        'nhanhieu_ngaynop': None,
        'nhanhieu_ten': None,
        'nhanhieu_image': None,
        'nhanhieu_loai': None,
        'nhanhieu_colorname': None,
        'nhanhieu_pl_nice': None,
        'nhanhieu_pl_vienna': None,
        'nhanhieu_noidungkhac': None,
        'nhanhieu_bang_id_gach': None,
        'nhanhieu_bang_ngaycap': None,
        'nhanhieu_bang_ngaycongbo': None,
        'nhanhieu_lan_giahan': None,
        'nhanhieu_ttpl': None,
        'nhanhieu_chubang_info_name': None,
        'nhanhieu_chubang_info_adress': None,
        'nhanhieu_bang_quocgia': None,
        'nhanhieu_nguoinop': None,
        'nhanhieu_nguoinop_address': None,
        'nhanhieu_nguoinop_nationality': None,
        'nhanhieu_daidienshtt': None,
        'nhanhieu_socongbao_a': None,
        'nhanhieu_ngaycongbao_a': None,
        'nhanhieu_socongbao_b': None,
        'nhanhieu_ngaycongbao_b': None,
        'nhanhieu_tltg': None,
        'nhanhieu_sodonuutien': None,
        'nhanhieu_pl_niceclass': None,
        'nhanhieu_bang_id': None,
        'nhanhieu_ngayuutien': None,
        'nhanhieu_manuocuutien': None,
        'nhanhieu_pl_vienna_detail': None,
        'nhanhieu_id': None,
        'nhanhieu_chubang_info_provcode': None,
        'nhanhieu_chubang_info_countrycode': None,
        'nhanhieu_nguoinop_name': None,
        'nhanhieu_donquocte': None,
        'nhanhieu_donquocte_ngay': None,
        'nhanhieu_nguoinop_provcode': None,
        'nhanhieu_trangthai': None,
        'nhanhieu_dichthuat': None,
        'nhanhieu_kieucuamau': None,
        'nhanhieu_tientrinh': None,
        'nhanhieu_sanpham_dichvu': None,
        'deleted_at': None,
        'created_at': datetime.datetime.now(),
        'updated_at': None,
    }

    # nhanhieu_id

    if '(200) Số đơn và Ngày nộp đơn' in data and data['(200) Số đơn và Ngày nộp đơn']:
        nhanhieu_id_gach, nhanhieu_ngaynop = data['(200) Số đơn và Ngày nộp đơn'].split()[1:3]
        data_insert['nhanhieu_id_gach'] = nhanhieu_id_gach[1:]
        data_insert['nhanhieu_ngaynop'] = datetime.datetime.strptime(nhanhieu_ngaynop, "%d.%m.%Y").strftime("%Y-%m-%d")

    if '(541) Nhãn hiệu' in data and data['(541) Nhãn hiệu']:
        data_insert['nhanhieu_ten'] = data['(541) Nhãn hiệu'].split('(VI)')[1].strip()

    data_insert['nhanhieu_image'] = []
    if '(540) Mẫu nhãn' in data and data['(540) Mẫu nhãn']:
        data_insert['nhanhieu_image'] = data['(540) Mẫu nhãn']

    if 'Loại đơn' in data and data['Loại đơn']:
        data_insert['nhanhieu_loai'] = data['Loại đơn']

    if '(591) Màu sắc nhãn hiệu' in data and data['(591) Màu sắc nhãn hiệu']:
        data_insert['nhanhieu_colorname'] = data['(591) Màu sắc nhãn hiệu']

    if '(511) Phân loại Nice' in data and data['(511) Phân loại Nice']:
        entries = data['(511) Nhóm sản phẩm/dịch vụ'].split('\r\n')
        result = {}
        current_key = None
        for entry in entries:
            if entry.isdigit():
                current_key = int(entry)
                result[current_key] = ""
            else:
                if current_key is not None:
                    result[current_key] += entry.strip() + " "
        for key in result:
            result[key] = result[key].strip()
        data_insert['nhanhieu_pl_nice'] = " ".join([f"{key}: {value}" for key, value in result.items()])

    if '(531) Phân loại hình' in data and data['(531) Phân loại hình']:
        data_insert['nhanhieu_pl_vienna'] = ", ".join(data['(531) Phân loại hình'].split("\r\n"))

    # nhanhieu_noidungkhac

    if '(100) Số bằng và ngày cấp' in data and data['(100) Số bằng và ngày cấp']:
        bang_info = data['(100) Số bằng và ngày cấp'].split()
        data_insert['nhanhieu_bang_id_gach'] = bang_info[0]
        data_insert['nhanhieu_bang_ngaycap'] = datetime.datetime.strptime(bang_info[1], "%d.%m.%Y").strftime("%Y-%m-%d")
        # nhanhieu_bang_ngaycongbo

    # nhanhieu_lan_giahan

    if '(180) Ngày hết hạn' in data and data['(180) Ngày hết hạn']:
        data_insert['nhanhieu_ttpl'] = datetime.datetime.strptime(data['(180) Ngày hết hạn'], "%d.%m.%Y").strftime("%Y-%m-%d")

    if '(730) Chủ đơn/Chủ bằng' in data and data['(730) Chủ đơn/Chủ bằng']:
        chubang = [x.strip() for x in data['(730) Chủ đơn/Chủ bằng'].replace('(VI)', '').split(':')]
        data_insert['nhanhieu_chubang_info_name'] = chubang[0].strip() if chubang else ''
        data_insert['nhanhieu_chubang_info_adress'] = chubang[1].strip() if chubang else ''
        # nhanhieu_bang_quocgia
        # nhanhieu_chubang_info_provcode
        # nhanhieu_chubang_info_countrycode
        data_insert['nhanhieu_nguoinop'] = chubang[0].strip() if chubang else ''
        data_insert['nhanhieu_nguoinop_name'] = chubang[0].strip() if chubang else ''
        data_insert['nhanhieu_nguoinop_address'] = chubang[1].strip() if chubang else ''
        # nhanhieu_nguoinop_provcode
        # nhanhieu_nguoinop_nationality

    if '(740) Đại diện SHCN' in data and data['(740) Đại diện SHCN']:
        if '(VI)' in data['(740) Đại diện SHCN']:
            dai_dien_shtt = data['(740) Đại diện SHCN'].split('(VI)')[1].strip()
        else:
            dai_dien_shtt = ""
        if ':' in data['(740) Đại diện SHCN']:
            dia_chi_shtt = data['(740) Đại diện SHCN'].split(':', 1)[1].strip()
        else:
            dia_chi_shtt = ""
        data_insert['nhanhieu_daidienshtt'] = dai_dien_shtt
        # data_insert['kdcn_shtt_diachi'] = dia_chi_shtt

    if '(400) Số công bố và ngày công bố' in data and data['(400) Số công bố và ngày công bố']:
        vn_data = data['(400) Số công bố và ngày công bố'].split("\r\n")
        so_cong_bo = vn_data[2] if len(vn_data) > 2 else ''
        ngay_cong_bo = vn_data[1] if len(vn_data) > 1 else ''
        if so_cong_bo and ngay_cong_bo:
            data_insert['nhanhieu_socongbao_a'] = so_cong_bo
            data_insert['nhanhieu_ngaycongbao_a'] = datetime.datetime.strptime(ngay_cong_bo, "%d.%m.%Y").strftime("%Y-%m-%d")
            
        so_cong_bo_b = vn_data[5] if len(vn_data) > 5 else ''
        ngay_cong_bo_b = vn_data[4] if len(vn_data) > 4 else ''
        if so_cong_bo_b and ngay_cong_bo_b:
            data_insert['nhanhieu_socongbao_b'] = so_cong_bo_b
            data_insert['nhanhieu_ngaycongbao_b'] = datetime.datetime.strptime(ngay_cong_bo_b, "%d.%m.%Y").strftime("%Y-%m-%d")

    # nhanhieu_donquocte
    # nhanhieu_donquocte_ngay

    if '(30) Chi tiết về dữ liệu ưu tiên' in data and data['(30) Chi tiết về dữ liệu ưu tiên']:
        date_string = data.get('(30) Chi tiết về dữ liệu ưu tiên', '')
        if date_string:
            dates = date_string.split("\r\n")[1::2]
            formatted_dates = [datetime.datetime.strptime(date, "%d.%m.%Y").strftime("%Y-%m-%d") for date in dates]
            data_insert['nhanhieu_sodonuutien'] = date_string.split()[1]
            data_insert['nhanhieu_ngayuutien'] = formatted_dates[0]
            data_insert['nhanhieu_manuocuutien'] = date_string[:2]

    # nhanhieu_tltg
    # nhanhieu_pl_vienna_detail
    # nhanhieu_pl_niceclass

    if 'Trạng thái' in data and data['Trạng thái']:
        data_insert['nhanhieu_trangthai'] = data['Trạng thái']

    if '(566) Nhãn hiệu dịch thuật' in data and data['(566) Nhãn hiệu dịch thuật']:
        data_insert['nhanhieu_dichthuat'] = data['(566) Nhãn hiệu dịch thuật']

    if '(550) Kiểu của mẫu nhãn(hình/chữ/kết hợp)' in data and data['(550) Kiểu của mẫu nhãn(hình/chữ/kết hợp)']:
        data_insert['nhanhieu_kieucuamau'] = data['(550) Kiểu của mẫu nhãn(hình/chữ/kết hợp)']

    if 'Tiến trình' in data and data['Tiến trình']:
        data_insert['nhanhieu_tientrinh'] = json.dumps(data['Tiến trình'])

    if '(511) Nhóm sản phẩm/dịch vụ' in data and data['(511) Nhóm sản phẩm/dịch vụ']:
        data_insert['nhanhieu_sanpham_dichvu'] = data['(511) Nhóm sản phẩm/dịch vụ'].replace('\r\n', ':\n')

    return data_insert

def insertOrUpdate(data, table="brand"):
    if '(541) Nhãn hiệu' not in data:
        return False
    try:
        cursor = db_connection.cursor()
        data_insert = config(data)
        check_query = f"SELECT * FROM `{table}` WHERE `nhanhieu_id_gach` = '{data_insert['nhanhieu_id_gach']}' ORDER BY `id` DESC LIMIT 1"
        cursor.execute(check_query)
        result = cursor.fetchone()
        if 'nhanhieu_image' in data and data['nhanhieu_image'] != '':
            if result:
                if not isinstance(result[4], list):
                    old_image = json.dumps(result[4])
                old_image = json.loads(result[4]) if result[4] else []
                # data_insert['nhanhieu_image'] = update_image('brand', data['nhanhieu_image'], old_image)
                data_insert['nhanhieu_image'] = old_image
            else:
                data_insert['nhanhieu_image'] = save_image('brand', data['nhanhieu_image'])
        if result:
            if not isinstance(result[4], list):
                old_image = json.dumps([result[4]])
            data_insert['nhanhieu_image'] = json.loads(old_image) if old_image else []
        if result:
            column_update = [
                'nhanhieu_ngaynop', 'nhanhieu_ten', 'nhanhieu_image', 'nhanhieu_loai',
                'nhanhieu_colorname', 'nhanhieu_pl_nice', 'nhanhieu_pl_vienna', 'nhanhieu_noidungkhac',
                'nhanhieu_bang_id_gach', 'nhanhieu_bang_ngaycap', 'nhanhieu_bang_ngaycongbo',
                'nhanhieu_lan_giahan', 'nhanhieu_ttpl', 'nhanhieu_chubang_info_name',
                'nhanhieu_chubang_info_adress', 'nhanhieu_bang_quocgia', 'nhanhieu_nguoinop',
                'nhanhieu_nguoinop_address', 'nhanhieu_nguoinop_nationality', 'nhanhieu_daidienshtt',
                'nhanhieu_socongbao_a', 'nhanhieu_ngaycongbao_a', 'nhanhieu_socongbao_b',
                'nhanhieu_ngaycongbao_b', 'nhanhieu_tltg', 'nhanhieu_sodonuutien', 'nhanhieu_pl_niceclass',
                'nhanhieu_bang_id', 'nhanhieu_ngayuutien', 'nhanhieu_manuocuutien',
                'nhanhieu_pl_vienna_detail', 'nhanhieu_id', 'nhanhieu_chubang_info_provcode',
                'nhanhieu_chubang_info_countrycode', 'nhanhieu_nguoinop_name', 'nhanhieu_donquocte',
                'nhanhieu_donquocte_ngay', 'nhanhieu_nguoinop_provcode', 'updated_at',
                'nhanhieu_trangthai', 'nhanhieu_dichthuat', 'nhanhieu_kieucuamau', 
                'nhanhieu_tientrinh', 'nhanhieu_sanpham_dichvu'
            ]
            column_where = 'nhanhieu_id_gach'
            set_clause = ', '.join([f'{col} = %s' for col in column_update])
            update_query = f"UPDATE {table} SET {set_clause} WHERE {column_where} = %s AND `deleted_at` IS NULL"
            values_update = (
                data_insert['nhanhieu_ngaynop'], data_insert['nhanhieu_ten'], json.dumps(data_insert['nhanhieu_image']), data_insert['nhanhieu_loai'], 
                data_insert['nhanhieu_colorname'], data_insert['nhanhieu_pl_nice'], data_insert['nhanhieu_pl_vienna'], data_insert['nhanhieu_noidungkhac'],
                data_insert['nhanhieu_bang_id_gach'], data_insert['nhanhieu_bang_ngaycap'], data_insert['nhanhieu_bang_ngaycongbo'],
                data_insert['nhanhieu_lan_giahan'], data_insert['nhanhieu_ttpl'], data_insert['nhanhieu_chubang_info_name'],
                data_insert['nhanhieu_chubang_info_adress'], data_insert['nhanhieu_bang_quocgia'], data_insert['nhanhieu_nguoinop'],
                data_insert['nhanhieu_nguoinop_address'], data_insert['nhanhieu_nguoinop_nationality'], data_insert['nhanhieu_daidienshtt'],
                data_insert['nhanhieu_socongbao_a'], data_insert['nhanhieu_ngaycongbao_a'], data_insert['nhanhieu_socongbao_b'],
                data_insert['nhanhieu_ngaycongbao_b'], data_insert['nhanhieu_tltg'], data_insert['nhanhieu_sodonuutien'], data_insert['nhanhieu_pl_niceclass'],
                data_insert['nhanhieu_bang_id'], data_insert['nhanhieu_ngayuutien'], data_insert['nhanhieu_manuocuutien'],
                data_insert['nhanhieu_pl_vienna_detail'], data_insert['nhanhieu_id'], data_insert['nhanhieu_chubang_info_provcode'],
                data_insert['nhanhieu_chubang_info_countrycode'], data_insert['nhanhieu_nguoinop_name'], data_insert['nhanhieu_donquocte'], 
                data_insert['nhanhieu_donquocte_ngay'], data_insert['nhanhieu_nguoinop_provcode'], datetime.datetime.now(), 
                data_insert['nhanhieu_trangthai'], data_insert['nhanhieu_dichthuat'], data_insert['nhanhieu_kieucuamau'],
                data_insert['nhanhieu_tientrinh'], data_insert['nhanhieu_sanpham_dichvu'],
                data_insert['nhanhieu_id_gach']
            )
            try:
                cursor.execute(update_query, values_update)
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
        db_connection.rollback()
        myLogger(str(e), 'exception')
        return False
    
def updateImage(data, table="brand"):
    if '(541) Nhãn hiệu' not in data:
        return False
    try:
        data_update = config(data)
        cursor = db_connection.cursor()

        # Tối ưu hóa truy vấn SQL với SELECT có giới hạn cột
        check_query = f"""
            SELECT 1 
            FROM {table} 
            WHERE nhanhieu_id_gach = %s 
            ORDER BY id DESC 
            LIMIT 1
        """
        cursor.execute(check_query, (data_update['nhanhieu_id_gach'],))
        result = cursor.fetchone()

        if result:
            data_update['nhanhieu_image'] = save_image('brand', data_update['images'])
            update_query = f"""
                UPDATE {table} 
                SET nhanhieu_image = %s 
                WHERE nhanhieu_id_gach = %s AND deleted_at IS NULL
            """
            data_tuple_update = (
                data_update['nhanhieu_image'],
                data_update['nhanhieu_id_gach']
            )
            try:
                cursor.execute(update_query, data_tuple_update)
                db_connection.commit()
                return True
            except Exception as e:
                myLogger(str(e), 'exception')
                return False
        else:
            return False
    except Exception as e:
        myLogger(str(e), 'exception')
        return False

def updateProgressProfile():
    try:
        cursor = db_connection.cursor()
        url = os.getenv('SLM_URL') + '/getByProfileType/brand'
        response = requests.get(url)
        results = response.json()
        for result in results:
            profile_quantity = result.get('profile_quantity')
            progress_length = result.get('progress_length')
            check_query = f"SELECT * FROM `brand` WHERE `nhanhieu_id_gach` = '{profile_quantity}' ORDER BY `id` DESC LIMIT 1"
            cursor.execute(check_query)
            result = cursor.fetchone()
            new_length = len(json.loads(result[46]))
            if(progress_length != new_length):
                url = os.getenv('SLM_URL') + '/api/updateProgessProfile'
                response = requests.post(url, data={'profile_quantity': profile_quantity, 'progress': result[46]})
        return True
    except Exception as e:
        myLogger(str(e), 'exception')
        return False