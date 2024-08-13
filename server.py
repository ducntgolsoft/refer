import datetime
import json
import os
import time
import sys
from dotenv import load_dotenv
import tkinter as tk
from tkinter import ttk, messagebox
import threading
from app.helper import get_txt_files_info, myLogger, send_msg_tele
from app.models.brand import insertOrUpdate as BrandInsertOrUpdate, updateImage as BrandUpdateImage
from app.models.industrial import insertOrUpdate as IndustrialInsertOrUpdate, updateImage as IndustrialUpdateImage
from app.models.invent import insertOrUpdate as InventInsertOrUpdate, updateImage as InventUpdateImage
load_dotenv()
cancel_flag = False
task_running = False
storage_data = os.getenv('BASE_DIRECTORY')
folder_brand = os.getenv('DIRECTORY_BRAND')
folder_invent = os.getenv('DIRECTORY_INVENT')
folder_industrial = os.getenv('DIRECTORY_INDUSTRIAL')


def getFileError(file_type, year, month):
    global storage_data, folder_brand, folder_invent, folder_industrial
    global status, task_running, cancel_flag
    global cancel_flag
    global storage_data, folder_brand, folder_invent, folder_industrial
    folder_data_brand = folder_brand
    folder_data_invent = folder_invent
    folder_data_industrial = folder_industrial
    if(year != '' and month != ''):
        folder_data_brand = f"{folder_data_brand}\\{year}\\{month}"
        folder_data_invent = f"{folder_data_invent}\\{year}\\{month}"
        folder_data_industrial = f"{folder_data_industrial}\\{year}\\{month}"
    if(year != '' and month == ''):
        folder_data_brand = f"{folder_data_brand}\\{year}"
        folder_data_invent = f"{folder_data_invent}\\{year}"
        folder_data_industrial = f"{folder_data_industrial}\\{year}"
    if file_type == 'industrial':
        folder_data = folder_data_industrial
    elif file_type == 'invent':
        folder_data = folder_data_invent
    elif file_type == 'brand':
        folder_data = folder_data_brand
    else:
        root.after(0, lambda: messagebox.showerror("Lỗi", "Loại hồ sơ không hợp lệ."))
        return
    print("Lấy file lỗi")
    error_files = []
    try:
        error_file_path = 'storage/error.json'
        if os.path.exists(error_file_path):
            with open(error_file_path, 'r') as f:
                error_records = json.load(f)
                for error in error_records:
                    file_path = os.path.join(folder_data, error['file'])
                    if os.path.exists(file_path):
                        error_files.append(file_path)
                    else:
                        print(f"File {error['file']} does not exist.")
        else:
            print("Error file does not exist.")
    except Exception as e:
        print("Error: ", str(e))
    finally:
        error_folder = storage_data + '\\Error'
        if not os.path.exists(error_folder):
            os.makedirs(error_folder)
        for error_file in error_files:
            try:
                os.rename(error_file, os.path.join(error_folder, os.path.basename(error_file)))
            except Exception as e:
                print(f"Error: {str(e)}")
        print("Hoàn thành lấy file lỗi.")


def getFileUrl(file_type, year, month):
    global storage_data, folder_brand, folder_invent, folder_industrial
    global status, task_running, cancel_flag
    global cancel_flag
    global storage_data, folder_brand, folder_invent, folder_industrial
    folder_data_brand = folder_brand
    folder_data_invent = folder_invent
    folder_data_industrial = folder_industrial
    if(year != '' and month != ''):
        folder_data_brand = f"{folder_data_brand}\\{year}\\{month}"
        folder_data_invent = f"{folder_data_invent}\\{year}\\{month}"
        folder_data_industrial = f"{folder_data_industrial}\\{year}\\{month}"
    if(year != '' and month == ''):
        folder_data_brand = f"{folder_data_brand}\\{year}"
        folder_data_invent = f"{folder_data_invent}\\{year}"
        folder_data_industrial = f"{folder_data_industrial}\\{year}"
    if file_type == 'industrial':
        folder_data = folder_data_industrial
    elif file_type == 'invent':
        folder_data = folder_data_invent
    elif file_type == 'brand':
        folder_data = folder_data_brand
    else:
        root.after(0, lambda: messagebox.showerror("Lỗi", "Loại hồ sơ không hợp lệ."))
        return
    print("Lấy url lỗi")
    error_urls = []
    try:
        error_file_path = 'storage/error.json'
        if os.path.exists(error_file_path):
            with open(error_file_path, 'r') as f:
                error_records = json.load(f)
                for error in error_records:
                    file_path = os.path.join(folder_data, error['file'])
                    if os.path.exists(file_path):
                        with open(file_path, 'r', encoding='utf-8-sig') as file:
                            content = file.read()
                        data = json.loads(content)
                        error_urls.append(data['url'] + '\n')
                    else:
                        print(f"File {error['file']} does not exist.")
        else:
            print("Error file does not exist.")
    except Exception as e:
        print("Error: ", str(e))
    finally:
        error_url_folder = storage_data + '\\ErrorUrl'
        # Ensure the base error URL folder exists
        if not os.path.exists(error_url_folder):
            os.makedirs(error_url_folder)
        
        # Construct the base error URL file path
        error_url_file = f'{error_url_folder}/{file_type}_url.txt'
        
        # Modify the file path if year and/or month is provided
        if year and month:
            error_url_file = f'{error_url_folder}/{file_type}/{year}/{month}_url.txt'
        elif year:
            error_url_file = f'{error_url_folder}/{file_type}/{year}_url.txt'
        
        # Ensure the directory structure for the error URL file exists
        os.makedirs(os.path.dirname(error_url_file), exist_ok=True)
        
        # Log the error URLs to the file
        if not os.path.exists(error_url_file):
            with open(error_url_file, 'w', encoding='utf-8') as f:
                f.writelines(error_urls)
        
        print("Hoàn thành lấy url lỗi.")

def simulate_jobs(type, year, month, action = 'add'):
    global status, task_running, cancel_flag
    global cancel_flag
    global storage_data, folder_brand, folder_invent, folder_industrial
    folder_data_brand = folder_brand
    folder_data_invent = folder_invent
    folder_data_industrial = folder_industrial
    category = 'SHTT'
    if(year != '' and month != ''):
        folder_data_brand = f"{folder_data_brand}\\{year}\\{month}"
        folder_data_invent = f"{folder_data_invent}\\{year}\\{month}"
        folder_data_industrial = f"{folder_data_industrial}\\{year}\\{month}"
    if(year != '' and month == ''):
        folder_data_brand = f"{folder_data_brand}\\{year}"
        folder_data_invent = f"{folder_data_invent}\\{year}"
        folder_data_industrial = f"{folder_data_industrial}\\{year}"
    if type == 'industrial':
        folder_data = folder_data_industrial
        category = 'Kiểu dáng công nghiệp'
    elif type == 'invent':
        folder_data = folder_data_invent
        category = 'Sáng chế'
    elif type == 'brand':
        folder_data = folder_data_brand
        category = 'Nhãn hiệu'
    else:
        root.after(0, lambda: messagebox.showerror("Lỗi", "Loại hồ sơ không hợp lệ."))
        return
    if not os.path.exists(folder_data):
        root.after(0, lambda: messagebox.showerror("Lỗi", "Không tìm thấy thư mục dữ liệu."))
        task_running = False
        return
    error_records = []
    duration_process_total = 0
    try:
        limit = 0
        all_files = os.listdir(folder_data)
        if len(all_files) == 0:
            root.after(0, lambda: messagebox.showerror("Lỗi", "Không có file dữ liệu."))
            task_running = False
            return
        if limit > 0:
            if limit > len(all_files):
                limit = len(all_files)
            txt_files = [file for file in all_files if file.endswith('.txt')][:limit]
        else:
            txt_files = [file for file in all_files if file.endswith('.txt')]
            
        if year != '' and month == '':
            message = f"Bắt đầu xử lý dữ liệu {category} năm {year}: {len(txt_files)} file - {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - " + os.getenv('ULTRA_ID')
        elif year != '' and month != '':
            message = f"Bắt đầu xử lý dữ liệu {category} tháng {month}/{year}: {len(txt_files)} file - {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - " + os.getenv('ULTRA_ID')
        else:
            message = f"Bắt đầu xử lý dữ liệu {category}: {len(txt_files)} file - {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - " + os.getenv('ULTRA_ID')
        root.after(0, lambda: messagebox.showinfo("Thông báo", message))
        send_msg_tele(message)
        for file_name in txt_files:
            if cancel_flag:
                return
            start_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            start_time_sec = time.time()
            total_files, position = get_txt_files_info(folder_data, file_name)
            running_status = "\033[93mRUNNING\033[0m"
            log_start = f"{start_time} Processing file: {file_name} ({position}/{total_files}) .......... {running_status}"
            print(log_start)
            try:
                with open(os.path.join(folder_data, file_name), 'r', encoding='utf-8-sig') as file:
                    content = file.read()
                data = json.loads(content)
                result = None
                if action == 'update_image':
                    if type == 'industrial':
                        result = IndustrialUpdateImage(data)
                    elif type == 'invent':
                        result = InventUpdateImage(data)
                    elif type == 'brand':
                        result = BrandUpdateImage(data)
                else:
                    if type == 'industrial':
                        result = IndustrialInsertOrUpdate(data)
                    elif type == 'invent':
                        result = InventInsertOrUpdate(data)
                    elif type == 'brand':
                        result = BrandInsertOrUpdate(data)
                if result:
                    status = "\033[92mDONE\033[0m"
                else:
                    status = "\033[91mFAIL\033[0m"
                    error_records.append({
                        "file": file_name,
                        "status": "error",
                        "date": start_time,
                    })
            except Exception as e:
                status = "\033[91mFAIL\033[0m"
                error_records.append({
                    "file": file_name,
                    "status": "error",
                    "date": start_time,
                    "message": str(e),
                })
                myLogger(f"Error: {file_name}", 'exception')
            finally:
                end_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                end_time_sec = time.time()
                duration = (end_time_sec - start_time_sec) * 1000
                log_line = f"{end_time} {file_name} {'':.<{len(log_start) - len(end_time) - len(file_name) - len(str(duration)) - 7}} {duration:.2f}ms {status}"
                duration_process_total += duration
                print(log_line.ljust(100))
                print()
    except Exception as e:
        print("\033[91mError\033[0m")
        myLogger(f"Error: {str(e)}", 'exception')
    finally:
        print("Hoàn thành xử lý dữ liệu: " + category)
        root.after(0, lambda: messagebox.showinfo("Thông báo", f"Kết thúc xử lý dữ liệu {category}, thời gian xử lý: {round(duration_process_total/60000, 2)} phút"))
        send_msg_tele(f"Kết thúc xử lý dữ liệu {category}, thời gian xử lý: {round(duration_process_total/60000, 2)} phút - " + os.getenv('ULTRA_ID'))
        with open('storage/error.json', 'w') as f:
            json.dump(error_records, f, indent=4, ensure_ascii=False)
        if len(error_records) > 0:
            getFileUrl(type, year, month)
            getFileError(type, year, month)
            send_msg_tele(f"Xử lý dữ liệu {category} có lỗi. {len(error_records)} file - " + os.getenv('ULTRA_ID'))

def submit_action():
    global cancel_flag, task_running
    if task_running:
        return
    cancel_flag = False
    task_running = True
    def long_running_task():
        global cancel_flag, task_running
        type_data = type_var.get()
        action = action_var.get()
        year = year_var.get()
        month = month_var.get()
        if(year != '' and len(year) != 4):
            root.after(0, lambda: messagebox.showerror("Lỗi", "Năm không hợp lệ."))
            task_running = False
            return
        if(month != '' and len(month) != 2):
            root.after(0, lambda: messagebox.showerror("Lỗi", "Tháng không hợp lệ."))
            task_running = False
            return
        if(month != '' and (int(month) < 1 or int(month) > 12)):
            root.after(0, lambda: messagebox.showerror("Lỗi", "Tháng không hợp lệ."))
            task_running = False
            return
        if type_data in ['industrial', 'invent', 'brand']:
            submit_button.config(state="disabled")
            cancel_button.config(state="normal")
            restart_button.config(state="disabled")
            if action == "add":
                simulate_jobs(type_data, year, month, action)
            if action == "update_image":
                simulate_jobs(type_data, year, month, action)
            elif action == 'get_error':
                getFileUrl(type_data, year, month)
            elif action == 'get_url':
                getFileError(type_data, year, month)
            else:
                root.after(0, lambda: messagebox.showerror("Lỗi", "Hành động không hợp lệ. Vui lòng chọn lại."))
                task_running = False
                submit_button.config(state="normal")
                cancel_button.config(state="disabled")
                restart_button.config(state="normal")
                return
        else:
            root.after(0, lambda: messagebox.showerror("Lỗi", "Vui lòng chọn loại hồ sơ hợp lệ: industrial, invent, brand"))
            task_running = False
            return
        root.after(0, on_task_complete)
    def on_task_complete():
        global task_running
        task_running = False
        submit_button.config(state="normal")
        cancel_button.config(state="disabled")
        restart_button.config(state="normal")
        root.protocol("WM_DELETE_WINDOW", on_closing)
    task_thread = threading.Thread(target=long_running_task)
    task_thread.start()
    root.protocol("WM_DELETE_WINDOW", on_closing_disabled)

def cancel_action():
    global cancel_flag
    cancel_flag = True

def on_closing_disabled():
    if task_running:
        messagebox.showwarning("Cảnh báo", "Công việc đang chạy, không thể đóng cửa sổ.")

def on_closing():
    root.destroy()

def restart_program():
    python = sys.executable
    os.execl(python, python, *sys.argv)
root = tk.Tk()
root.title("Chọn hành động cho hồ sơ")
root.geometry("300x250")
type_var = tk.StringVar(value="brand")
action_var = tk.StringVar(value="add")
year_var = tk.StringVar(value=datetime.datetime.now().year)
month_current = datetime.datetime.now().month
if(month_current < 10):
    month_var = tk.StringVar(value=f"0{month_current}")
else:
    month_var = tk.StringVar(value=month_current)
style = ttk.Style()
style.configure("TButton", padding=6, relief="flat", background="#ccc")
main_frame = ttk.Frame(root, padding="10 10 10 10")
main_frame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
root.columnconfigure(0, weight=1)
root.rowconfigure(0, weight=1)
main_frame.columnconfigure(0, weight=1)
main_frame.columnconfigure(1, weight=1)
ttk.Label(main_frame, text="Chọn loại hồ sơ:", width=20).grid(row=0, column=0, padx=5, pady=5, sticky=tk.W)
type_menu = ttk.OptionMenu(main_frame, type_var, "brand", "industrial", "invent", "brand")
type_menu.grid(row=0, column=1, padx=5, pady=5, sticky=(tk.W, tk.E))
ttk.Label(main_frame, text="Chọn hành động:", width=20).grid(row=1, column=0, padx=5, pady=5, sticky=tk.W)
action_menu = ttk.OptionMenu(main_frame, action_var, "add", "add", "update_image", "get_error", "get_url")
action_menu.grid(row=1, column=1, padx=5, pady=5, sticky=(tk.W, tk.E))
ttk.Label(main_frame, text="Nhập năm:", width=10).grid(row=2, column=0, padx=5, pady=5, sticky=tk.W)
year_entry = ttk.Entry(main_frame, textvariable=year_var, width=25)
year_entry.grid(row=2, column=1, padx=5, pady=5, sticky=(tk.W, tk.E))
ttk.Label(main_frame, text="Nhập tháng:", width=10).grid(row=3, column=0, padx=5, pady=5, sticky=tk.W)
month_entry = ttk.Entry(main_frame, textvariable=month_var, width=25)
month_entry.grid(row=3, column=1, padx=5, pady=5, sticky=(tk.W, tk.E))
button_frame = ttk.Frame(main_frame, padding="5 5 5 5")
button_frame.grid(row=4, column=0, columnspan=2, pady=10)
button_frame.columnconfigure(0, weight=1)
button_frame.columnconfigure(1, weight=1)
button_frame.columnconfigure(2, weight=1)
submit_button = ttk.Button(button_frame, text="Submit", command=submit_action, width=10)
submit_button.grid(row=0, column=0, padx=5)
cancel_button = ttk.Button(button_frame, text="Cancel", command=cancel_action, width=10)
cancel_button.grid(row=0, column=1, padx=5)
cancel_button.config(state="disabled")
restart_button = ttk.Button(button_frame, text="Restart", command=restart_program, width=10)
restart_button.grid(row=0, column=2, padx=5)
root.protocol("WM_DELETE_WINDOW", on_closing)
root.mainloop()

