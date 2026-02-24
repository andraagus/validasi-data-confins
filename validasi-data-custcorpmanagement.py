import pandas as pd
import os
import re

# ==========================================
# STEP 1: FUNGSI VALIDASI MODULAR
# ==========================================

def validate_not_blank(value):
    val_str = str(value).strip()
    return val_str != "" and val_str.lower() not in ['nan', 'none']

def validate_is_numeric(value):
    return str(value).strip().isdigit()

def validate_is_decimal(value):
    try:
        clean_value = str(value).strip().replace(',', '')
        float(clean_value)
        return True
    except ValueError:
        return False

def validate_date_format(value):
    val = str(value).strip()
    if len(val) == 10 and val[2] == '-' and val[5] == '-':
        parts = val.split('-')
        return all(p.isdigit() for p in parts)
    return False

def validate_no_special_chars(value):
    pattern = r"[!@#$%^&*()+?/><}{\[\]\-_=]"
    return not bool(re.search(pattern, str(value).strip()))

def get_cell_value(value):
    if pd.isna(value) or str(value).lower() == 'nan':
        return ""
    return str(value).strip()

def validate_relasi_IDNO_BIRTHDATE(id_no, birth_date, gender):
    id_no = str(id_no).strip()
    birth_date = str(birth_date).strip()
    gender = str(gender).strip().upper()
    if len(id_no) < 12 or len(birth_date) < 10: return False
    try:
        id_dd, id_mm, id_yy = id_no[6:8], id_no[8:10], id_no[10:12]
        b_dd, b_mm, b_yy = birth_date[0:2], birth_date[3:5], birth_date[8:10]
        if gender == 'M':
            return id_dd == b_dd and id_mm == b_mm and id_yy == b_yy
        elif gender == 'F':
            calc_dd = int(id_dd) - 40
            return str(calc_dd).zfill(2) == b_dd and id_mm == b_mm and id_yy == b_yy
        return False
    except: return False

# ==========================================
# STEP 2: PROSES VALIDASI PER SHEET PER KOLOM
# ==========================================

def run_validation():
    current_dir = os.path.dirname(os.path.abspath(__file__))
    
    f_corp = next((f for f in os.listdir(current_dir) if 'custcorporate' in f.lower() and f.endswith('.txt')), None)
    f_mngt = next((f for f in os.listdir(current_dir) if f.lower().startswith('custcorpmanagement_') and f.endswith('.txt')), None)
    f_acc = next((f for f in os.listdir(current_dir) if 'account' in f.lower() and f.endswith('.txt')), None)

    if not f_corp or not f_mngt:
        print("Error: File .txt tidak lengkap.")
        return

    df_a = pd.read_csv(os.path.join(current_dir, f_corp), sep='|', dtype=str)
    df_b = pd.read_csv(os.path.join(current_dir, f_mngt), sep='|', dtype=str)
    df_c = pd.read_csv(os.path.join(current_dir, f_acc), sep='|', dtype=str) if f_acc else pd.DataFrame()

    df_merged = pd.merge(df_b, df_a[['CUST_NO', 'CUST_NAME']].drop_duplicates('CUST_NO'), on='CUST_NO', how='left')
    if not df_c.empty:
        df_merged = pd.merge(df_merged, df_c[['CUST_NO', 'PARTNER_NAME', 'AGRMNT_NO']].drop_duplicates('CUST_NO'), on='CUST_NO', how='left')

    # Inisialisasi Dictionary untuk Sheet yang sangat spesifik
    sheets_data = {
        'ERR_CUST_NOT_FOUND': [],
        'ERR_SHAREHOLDER_TYPE': [],
        'ERR_SEX': [],
        'ERR_MNGMNT_ADDR': [],
        'ERR_MNGMNT_RT': [],
        'ERR_MNGMNT_RW': [],
        'ERR_MNGMNT_KEL': [],
        'ERR_MNGMNT_KEC': [],
        'ERR_MNGMNT_CITY': [],
        'ERR_MNGMNT_ZIPCODE': [],
        'ERR_MNGMNT_ID_NO': [],
        'ERR_MNGMNT_BIRTH_DATE': [],
        'ERR_MNGMNT_BIRTH_PLACE': [],
        'ERR_NPWP': [],
        'ERR_SHARE_PORTION': [],
        'ERR_JABATAN': [],
        'ERR_PROVINSI': [],
        'ERR_ESTABLISHMENT_YEAR': []
    }

    def add_to_error(sheet, row, col, msg):
        sheets_data[sheet].append({
            'CUST_NO': row.get('CUST_NO'),
            'CUST_NAME': row.get('CUST_NAME', 'TIDAK DITEMUKAN'),
            'PARTNER_NAME': row.get('PARTNER_NAME', 'N/A'),
            'AGRMNT_NO': row.get('AGRMNT_NO', 'N/A'),
            'DATA_ORIGINAL': row.get(col),
            'KETERANGAN': msg
        })

    print("Memulai validasi...")

    for _, row in df_merged.iterrows():
        # 0. Master Corporate Check
        if pd.isna(row['CUST_NAME']):
            add_to_error('ERR_CUST_NOT_FOUND', row, 'CUST_NO', "CUST_NO tidak ada di master corporate")

        # 1. Shareholder Type (P/C)
        val = get_cell_value(row.get('SHAREHOLDER_TYPE'))
        if validate_not_blank(val) and val.upper() not in ['P', 'C']:
            add_to_error('ERR_SHAREHOLDER_TYPE', row, 'SHAREHOLDER_TYPE', "Wajib P atau C")

        # 2. Sex (F/M)
        val = get_cell_value(row.get('SEX'))
        if validate_not_blank(val) and val.upper() not in ['F', 'M']:
            add_to_error('ERR_SEX', row, 'SEX', "Wajib F atau M")

        # 3. Alamat & Karakter Khusus (Satu sheet per kolom)
        for col, sheet in [('MNGMNT_ADDR', 'ERR_MNGMNT_ADDR'), ('MNGMNT_KEL', 'ERR_MNGMNT_KEL'), 
                           ('MNGMNT_KEC', 'ERR_MNGMNT_KEC'), ('MNGMNT_CITY', 'ERR_MNGMNT_CITY'),
                           ('MNGMNT_BIRTH_PLACE', 'ERR_MNGMNT_BIRTH_PLACE')]:
            val = get_cell_value(row.get(col))
            if validate_not_blank(val) and not validate_no_special_chars(val):
                add_to_error(sheet, row, col, "Terdapat karakter khusus")

        # 4. RT & RW
        for col, sheet in [('MNGMNT_RT', 'ERR_MNGMNT_RT'), ('MNGMNT_RW', 'ERR_MNGMNT_RW')]:
            val = get_cell_value(row.get(col))
            if validate_not_blank(val) and (not val.isdigit() or len(val) > 3):
                add_to_error(sheet, row, col, "Harus angka & maks 3 digit")

        # 5. Zipcode
        val = get_cell_value(row.get('MNGMNT_ZIPCODE'))
        if validate_not_blank(val) and (not val.isdigit() or len(val) != 5):
            add_to_error('ERR_MNGMNT_ZIPCODE', row, 'MNGMNT_ZIPCODE', "Harus 5 digit angka")

        # 6. ID No (Relasi NIK)
        id_no = get_cell_value(row.get('ID_NO'))
        id_type = get_cell_value(row.get('ID_TYPE')).upper()
        b_date = get_cell_value(row.get('BIRTH_DT'))
        sex = get_cell_value(row.get('SEX'))
        if validate_not_blank(id_no):
            if not validate_no_special_chars(id_no):
                add_to_error('ERR_MNGMNT_ID_NO', row, 'ID_NO', "Ada karakter khusus")
            elif len(id_no) == 16 and id_type in ['NIK', 'KTP', 'ID NO']:
                if not validate_relasi_IDNO_BIRTHDATE(id_no, b_date, sex):
                    add_to_error('ERR_MNGMNT_ID_NO', row, 'ID_NO', f"NIK tidak sinkron dengan Birth Date ({b_date})")

        # 7. Birth Date
        if validate_not_blank(b_date) and not validate_date_format(b_date):
            add_to_error('ERR_MNGMNT_BIRTH_DATE', row, 'BIRTH_DT', "Format wajib DD-MM-YYYY")

        # 7.b. Birth Place (Karakter Khusus)
        val = get_cell_value(row.get('BIRTH_PLACE'))
        if validate_not_blank(val) and not validate_no_special_chars(val):
            add_to_error('ERR_MNGMNT_BIRTH_PLACE', row, 'BIRTH_PLACE', "Terdapat karakter khusus")

        # 8. NPWP
        val = get_cell_value(row.get('NPWP_NO'))
        if validate_not_blank(val) and (not val.isdigit() or len(val) != 16):
            add_to_error('ERR_NPWP', row, 'NPWP_NO', "Wajib 16 digit angka")

        # 9. Share Portion
        val = get_cell_value(row.get('SHARE_PORTION'))
        if validate_not_blank(val) and not validate_is_decimal(val):
            add_to_error('ERR_SHARE_PORTION', row, 'SHARE_PORTION', "Harus format angka/desimal")

        # 10. Jabatan, Provinsi, Est Year
        for col, sheet in [('JABATAN', 'ERR_JABATAN'), ('PROVINSI', 'ERR_PROVINSI')]:
            val = get_cell_value(row.get(col))
            if validate_not_blank(val) and not val.isdigit():
                add_to_error(sheet, row, col, "Harus kode angka")

        val = get_cell_value(row.get('ESTABLISHMENT_YEAR'))
        if validate_not_blank(val) and (not val.isdigit() or len(val) != 4):
            add_to_error('ERR_ESTABLISHMENT_YEAR', row, 'ESTABLISHMENT_YEAR', "Harus 4 digit tahun")

    # --- SIMPAN KE EXCEL ---
    output_path = os.path.join(current_dir, 'DETAIL_ERROR_PER_KOLOM.xlsx')
    valid_sheets = {name: data for name, data in sheets_data.items() if data}

    if valid_sheets:
        with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
            for name, data in valid_sheets.items():
                pd.DataFrame(data).to_excel(writer, sheet_name=name, index=False)
        print(f"Selesai! File detail error per sheet: {output_path}")
    else:
        print("Data Management Bersih!")

if __name__ == "__main__":
    run_validation()