import pandas as pd
import os
import re

from requests import get

# ==========================================
# STEP 1: FUNGSI VALIDASI MODULAR (UNIT)
# ==========================================

def validate_not_blank(value):
    val_str = str(value).strip()
    return val_str != "" and val_str.lower() != 'nan'

def validate_is_numeric(value):
    return str(value).strip().isdigit()

def validate_not_only_numeric(value):
    return not str(value).strip().isdigit()

def validate_not_two_digits(value):
    return len(str(value).strip()) != 2

def validate_no_special_chars(value):
    pattern = r"[!@#$%^&*()+?/><}{\[\]\-_=]"
    return not bool(re.search(pattern, str(value).strip()))

def validate_is_exactly_4_digits(value):
    return len(str(value).strip()) == 4 and str(value).strip().isdigit()

def validate_is_exactly_5_digits(value):
    return len(str(value).strip()) == 5 and str(value).strip().isdigit()

def validate_is_exactly_16_digits(value): 
    return len(str(value).strip()) == 16 and str(value).strip().isdigit()

def validate_sex(value):
    return str(value).strip().upper() in ['F', 'M']

def validate_shareholder_type(value):
    return str(value).strip().upper() in ['P', 'C']

def get_cell_value(value):
    if pd.isna(value):
        return None
    return value
# ==========================================
# STEP 2, 3, & 4: PROSES DAN PENYIMPANAN
# ==========================================

def run_validation():
    current_dir = os.path.dirname(os.path.abspath(__file__))
    
    file_a = next((f for f in os.listdir(current_dir) if ('coraccount' in f.lower() or 'coreaccount' in f.lower()) and f.endswith('.txt')), None)
    file_b = next((f for f in os.listdir(current_dir) if f.lower().startswith('custcorpmanagement_') and f.endswith('.txt')), None)

    if not file_a or not file_b:
        print("Error: File .txt tidak ditemukan di folder.")
        return

    print(f"Membaca data...")
    df_a = pd.read_csv(os.path.join(current_dir, file_a), sep='|', dtype=str)
    df_b = pd.read_csv(os.path.join(current_dir, file_b), sep='|', dtype=str)

    # Step 3: Kelengkapan (Filter B yang ada di A)
    df_b1 = df_b[df_b['CUST_NO'].isin(df_a['CUST_NO'])].copy()
    
    # Gabungkan dengan info Partner dari File A
    df_merged = pd.merge(
        df_b1, 
        df_a[['CUST_NO', 'PARTNER_NAME', 'PARTNER_AGRMNT_NO', 'AGRMNT_NO']].drop_duplicates('CUST_NO'), 
        on='CUST_NO', 
        how='left'
    )

    # Dictionary untuk menampung data per sheet
    sheets_data = {
        'INVALID_NPWP': [],
        'INVALID_SHAREHOLDER_TYPE': [],
        'INVALID_SEX': [],
        'INVALID_MNGMNT_ADDR': [],
        'INVALID_MNGMNT_RT': [],
        'INVALID_MNGMNT_RW': [],
        'INVALID_MNGMNT_KEL': [],
        'INVALID_MNGMNT_KEC': [],
        'INVALID_MNGMNT_CITY': [],
        'INVALID_MNGMNT_ZIPCODE': [],
        'INVALID_MNGMNT_ID_TYPE': [],
        'INVALID_MNGMNT_ID_NO': [],
        'INVALID_MNGMNT_BIRTH_DT': [],
        'INVALID_MNGMNT_BIRTH_PLACE': [],
        'INVALID_SHARE_PORTION': [],
        'INVALID_JABATAN': [],
        'INVALID_PROVINSI': [],
    }

    def add_to_error(sheet_name, row, col_name, message):
        sheets_data[sheet_name].append({
            'PARTNER_NAME': row.get('PARTNER_NAME'),
            'PARTNER_AGRMNT_NO': row.get('PARTNER_AGRMNT_NO'),
            'AGRMNT_NO': row.get('AGRMNT_NO'),
            'CUST_NO': row['CUST_NO'],
            'CUST_NAME': row['CUST_NAME'],
            'DATA_ORIGINAL': row.get(col_name), # Mengambil data asli yang salah
            'KETERANGAN_ERROR': message
        })

    print("Sedang melakukan validasi per baris...")
    for _, row in df_merged.iterrows():
        # validasi share holder type
        val_shareholder_type = str(get_cell_value(row['SHAREHOLDER_TYPE']) or "").strip()
        if validate_not_blank(val_shareholder_type) and not validate_shareholder_type(val_shareholder_type):
            add_to_error('INVALID_SHAREHOLDER_TYPE', row, 'SHAREHOLDER_TYPE', "Tidak terdiri dari P / C digit")
        # validasi sex  
        val_sex = str(row['SEX']).strip()
        if validate_not_blank(val_sex) and not validate_sex(val_sex):
            add_to_error('INVALID_SEX', row, 'SEX', "Tidak terdiri dari F / M digit")
        # validasi alamat manajemen
        val_mngmnt_addr = str(row['MNGMNT_ADDR']).strip()
        if validate_not_blank(val_mngmnt_addr) and not validate_no_special_chars(val_mngmnt_addr):
            add_to_error('INVALID_MNGMNT_ADDR', row, 'MNGMNT_ADDR', "Ada karakter khusus")
        # validasi rt
        val_mngmnt_rt = str(row['MNGMNT_RT']).strip()
        if validate_not_blank(val_mngmnt_rt) and not validate_is_numeric(val_mngmnt_rt):
            add_to_error('INVALID_MNGMNT_RT', row, 'MNGMNT_RT', "Tidak terdiri dari 3 digit angka")
        # validasi rw
        val_mngmnt_rw = str(row['MNGMNT_RW']).strip()
        if validate_not_blank(val_mngmnt_rw) and not validate_is_numeric(val_mngmnt_rw):
            add_to_error('INVALID_MNGMNT_RW', row, 'MNGMNT_RW', "Tidak terdiri dari 3 digit angka")
        # validasi kelurahan
        val_mngmnt_kel = str(row['MNGMNT_KEL']).strip()
        if validate_not_blank(val_mngmnt_kel) and not validate_no_special_chars(val_mngmnt_kel):
            add_to_error('INVALID_MNGMNT_KEL', row, 'MNGMNT_KEL', "Ada karakter khusus")
        # validasi kecamatan
        val_mngmnt_kec = str(row['MNGMNT_KEC']).strip()
        if validate_not_blank(val_mngmnt_kec) and not validate_no_special_chars(val_mngmnt_kec):
            add_to_error('INVALID_MNGMNT_KEC', row, 'MNGMNT_KEC', "Ada karakter khusus")
        # validasi kota
        val_mngmnt_city = str(row['MNGMNT_CITY']).strip()
        if validate_not_blank(val_mngmnt_city) and not validate_no_special_chars(val_mngmnt_city):
            add_to_error('INVALID_MNGMNT_CITY', row, 'MNGMNT_CITY', "Ada karakter khusus")
        # validasi kode pos
        val_mngmnt_zipcode = str(row['MNGMNT_ZIPCODE']).strip()
        if validate_not_blank(val_mngmnt_zipcode) and not validate_is_exactly_5_digits(val_mngmnt_zipcode):
            add_to_error('INVALID_MNGMNT_ZIPCODE', row, 'MNGMNT_ZIPCODE', "Tidak terdiri dari 5 digit angka")
        # validasi jenis id manajemen
        val_mngmnt_id_type = str(row['MNGMNT_ID_TYPE']).strip()
        if validate_not_blank(val_mngmnt_id_type) and validate_not_two_digits(val_mngmnt_id_type):
            add_to_error('INVALID_MNGMNT_ID_TYPE', row, 'MNGMNT_ID_TYPE', "Tidak terdiri dari 2 digit")
        # validasi no id manajemen
        val_mngmnt_id_no = str(row['MNGMNT_ID_NO']).strip()
        if validate_not_blank(val_mngmnt_id_no) and not validate_no_special_chars(val_mngmnt_id_no):
            add_to_error('INVALID_MNGMNT_ID_NO', row, 'MNGMNT_ID_NO', "Ada karakter khusus")
        # validasi tempat lahir
        val_mngmnt_birth_place = str(row['MNGMNT_BIRTH_PLACE']).strip()
        if validate_not_blank(val_mngmnt_birth_place) and not validate_no_special_chars(val_mngmnt_birth_place):
            add_to_error('INVALID_MNGMNT_BIRTH_PLACE', row, 'MNGMNT_BIRTH_PLACE', "Ada karakter khusus")
        # validasi tanggal lahir        
        val_mngmnt_birth_date = str(row['MNGMNT_BIRTH_DATE']).strip()    
        if validate_not_blank(val_mngmnt_birth_date):   
            add_to_error('INVALID_MNGMNT_BIRTH_DT', row, 'MNGMNT_BIRTH_DATE', "Format tanggal tidak valid (DD-MM-YYYY)")        
        # tahun pendirian
        val_est_year = str(row['ESTABLISHMENT_YEAR']).strip() 
        if validate_not_blank(val_est_year) and not validate_is_numeric(val_est_year):
            add_to_error('INVALID_ESTABLISHMENT_YEAR', row, 'ESTABLISHMENT_YEAR', "Tidak terdiri dari 2 digit") 
        #validasi npwp
        val_npwp = str(row['NPWP']).strip() 
        if validate_not_blank(val_npwp):
            add_to_error('INVALID_NPWP', row, 'NPWP', "Tidak terdiri dari 16 digit angka")  
        #validasi sahare portion
        val_share_portion = str(row['SHARE_PORTION']).strip()    
        if validate_not_blank(val_share_portion) and not validate_is_numeric(val_share_portion):   
            add_to_error('INVALID_SHARE_PORTION', row, 'SHARE_PORTION', "Tidak terdiri dari 2 digit angka")
        #validasi jabatan
        val_jabatan = str(row['JABATAN']).strip()    
        if validate_not_blank(val_jabatan) and not validate_is_numeric(val_jabatan):   
            add_to_error('INVALID_JABATAN', row, 'JABATAN', "Tidak terdiri dari 2 digit angka") 
        #validasi provinsi
        val_provinsi = str(row['PROVINSI']).strip()    
        if validate_not_blank(val_provinsi) and not validate_is_numeric(val_provinsi):  
            add_to_error('INVALID_PROVINSI', row, 'PROVINSI', "Tidak terdiri dari 2 digit angka")                       
        

    # Simpan ke satu file Excel dengan banyak sheet
    output_path = os.path.join(current_dir, 'DATA_CUSTOMERPERSONAL_TIDAK_VALID.xlsx')
    
    # Gunakan writer untuk membuat file dengan banyak sheet
    with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
        found_any_error = False
        for sheet_name, data in sheets_data.items():
            if data:
                df_error = pd.DataFrame(data)
                df_error.to_excel(writer, sheet_name=sheet_name, index=False)
                found_any_error = True
        
    if found_any_error:
        print(f"Selesai! File detail error tersimpan di: {output_path}")
    else:
        print("Luar biasa! Tidak ditemukan data yang tidak valid.")

if __name__ == "__main__":
    run_validation()