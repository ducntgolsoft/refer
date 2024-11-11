import json, os, time, string, random, requests, logging, platform
from dotenv import load_dotenv
from config.database import db_connection

load_dotenv()

daily = time.strftime("%Y%m%d")
log_file_name = f"storage/logs/{daily}.log"
if not os.path.exists('storage/logs'):
    os.makedirs('storage/logs')
    if not os.path.exists(log_file_name):
        with open(log_file_name, 'w') as file:
            file.write('')
server_type = os.getenv('APP_ENV')
logging.basicConfig(filename=log_file_name, format='[%(asctime)s] {}.%(levelname)s: \n %(message)s'.format(server_type))
folder_image = 'storage/images/'


def get_txt_files_info(directory, filename):
    all_files = os.listdir(directory)
    txt_files = [file for file in all_files if file.endswith('.txt')]
    txt_files.sort()
    total_txt_files = len(txt_files)
    if filename in txt_files:
        file_index = txt_files.index(filename) + 1
    else:
        file_index = None
    return total_txt_files, file_index


def generate_random_filename(length=20):
    letters = string.ascii_letters
    return ''.join(random.choice(letters) for i in range(length))

def save_image(folder, images):
    urls = images
    url_list = urls.split("##")
    # save_dir = folder_image + folder
    # os.makedirs(save_dir, exist_ok=True)
    # new_images = []
    # error_images = []
    # for url in url_list:
    #     response = requests.get(url)
    #     if response.status_code == 200:
    #         image_data = response.content
    #         new_image = cv2.imdecode(np.frombuffer(image_data, np.uint8), cv2.IMREAD_COLOR)
    #         if new_image is None:
    #             error_images.append(url)
    #             continue
    #         random_filename = generate_random_filename()
    #         file_path = os.path.join(save_dir, f"{random_filename}.png")
    #         with open(file_path, "wb") as file:
    #             file.write(image_data)
    #         new_images.append(file_path)
    #     else:
    #         error_images.append(url)
    # if len(error_images) > 0:
    #     for error_url in error_images:
    #         print(error_url)
    # new_image_names = [os.path.basename(img).replace("\\", "/") for img in new_images if os.path.isfile(img)]
    # url_image_prefix = '/storage/' + folder + '/'
    # newImages = [url_image_prefix + image for image in new_image_names]
    return url_list


def myLogger(msg, log_level='critical'):
    if log_level == 'warning':
        logging.warning(msg)
    elif log_level == 'error':
        logging.error(msg, exc_info=True)
    elif log_level == 'critical':
        logging.critical(msg)
    elif log_level == 'exception':
        logging.exception(msg)
    else:
        logging.debug(msg)


def send_msg_tele(message):
    if message is None:
        return
    url = os.getenv('TELEGRAM_API_URL')
    if url is None:
        return
    response = requests.get(url + message)
    if response.status_code != 200:
        myLogger(f"Failed to send message: {response.text}", 'error')
    else:
        myLogger(f"Message sent: {message}")


def countdown(seconds):
    for i in range(seconds, 0, -1):
        print(f"Tắt máy tính sau {i} giây...", end='\r')
        time.sleep(1)
    print("Tắt máy tính ngay bây giờ!")


def shutdown():
    if platform.system() == "Windows":
        os.system("shutdown /s /t 1")
    elif platform.system() == "Linux" or platform.system() == "Darwin":
        os.system("sudo shutdown now")
    else:
        print("Hệ điều hành không được hỗ trợ cho việc tắt máy tự động.")


def shutdown_device(is_shutdown=False):
    while True:
        cursor = db_connection.cursor()
        cursor.execute("SELECT COUNT(*) FROM jobs")
        count = cursor.fetchone()[0]
        if count == 0:
            send_msg_tele("Hoàn thành refer data to live. Tắt máy tính sau 1 phút.")
            if is_shutdown:
                countdown(60)
                shutdown()
            break
        else:
            time.sleep(60)
        cursor.close()
        db_connection.close()
