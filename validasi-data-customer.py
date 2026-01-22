import pandas as pd
import os
import re

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
    return len(str(value).strip()) == 4

def validate_is_exactly_5_digits(value):
    return len(str(value).strip()) == 5

# ==========================================
# STEP 2, 3, & 4: PROSES DAN PENYIMPANAN
# ==========================================

def run_validation():
    current_dir = os.path.dirname(os.path.abspath(__file__))
    
    file_a = next((f for f in os.listdir(current_dir) if ('coraccount' in f.lower() or 'coreaccount' in f.lower()) and f.endswith('.txt')), None)
    file_b = next((f for f in os.listdir(current_dir) if f.lower().startswith('customer_') and f.endswith('.txt')), None)

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
        'INVALID_CUST_TYPE': [],
        'INVALID_ADDRESS': [],
        'INVALID_KELURAHAN': [],
        'INVALID_KECAMATAN': [],
        'INVALID_ZIPCODE': [],
        'INVALID_MOBILE': [],
        'INVALID_DATI_II': [],
        'INVALID_BIRTH_INFO': []
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
        c_type = str(row['CUST_TYPE']).strip().upper()

        # 1. Validasi NPWP
        val_npwp = str(row['NPWP_NO'])
        if not validate_is_numeric(val_npwp) or len(val_npwp.strip()) > 16:
            add_to_error('INVALID_NPWP', row, 'NPWP_NO', "Bukan angka atau > 16 digit")

        # 2. Validasi Cust Type
        if not validate_not_blank(row['CUST_TYPE']) or c_type not in ['C', 'P']:
            add_to_error('INVALID_CUST_TYPE', row, 'CUST_TYPE', "Wajib C atau P")

        # 3. Validasi Address
        val_addr = str(row['CUST_ADDR'])
        if not validate_not_blank(val_addr) or not validate_not_two_digits(val_addr) or not validate_not_only_numeric(val_addr):
            add_to_error('INVALID_ADDRESS', row, 'CUST_ADDR', "Blank / Hanya 2 digit / Hanya angka")

        # 4. Validasi Kelurahan
        val_kel = str(row['CUST_KEL'])
        if not validate_not_blank(val_kel) or not validate_not_two_digits(val_kel) or not validate_not_only_numeric(val_kel):
            add_to_error('INVALID_KELURAHAN', row, 'CUST_KEL', "Blank / Hanya 2 digit / Hanya angka")

        # 5. Validasi Kecamatan
        val_kec = str(row['CUST_KEC'])
        if not validate_not_blank(val_kec) or not validate_not_two_digits(val_kec) or not validate_not_only_numeric(val_kec):
            add_to_error('INVALID_KECAMATAN', row, 'CUST_KEC', "Blank / Hanya 2 digit / Hanya angka")

        # 6. Validasi Zipcode
        val_zip = str(row['CUST_ZIPCODE'])
        if not validate_not_blank(val_zip) or not validate_is_numeric(val_zip) or not validate_is_exactly_5_digits(val_zip):
            add_to_error('INVALID_ZIPCODE', row, 'CUST_ZIPCODE', "Bukan angka atau tidak 5 digit")

        # 7. Validasi Mobile
        val_mob = str(row['MOBILE_PHN'])
        if not validate_not_blank(val_mob) or not validate_is_numeric(val_mob):
            add_to_error('INVALID_MOBILE', row, 'MOBILE_PHN', "Harus angka dan tidak boleh blank")

        # 8. Validasi DATI II
        val_dati = str(row['DATI_II'])
        if not validate_not_blank(val_dati) or not validate_is_numeric(val_dati) or not validate_is_exactly_4_digits(val_dati) or not validate_no_special_chars(val_dati):
            add_to_error('INVALID_DATI_II', row, 'DATI_II', "Bukan 4 digit angka atau ada special char")

        # 9. Validasi Birth Info (Khusus P)
        if c_type == 'P':
            bp = str(row['BIRTH_PLACE'])
            bd = str(row['BIRTH_DT'])
            if not validate_not_blank(bp) or not validate_not_only_numeric(bp) or not validate_no_special_chars(bp):
                add_to_error('INVALID_BIRTH_INFO', row, 'BIRTH_PLACE', "Format tempat lahir salah")
            if not validate_not_blank(bd) or not validate_not_only_numeric(bd):
                add_to_error('INVALID_BIRTH_INFO', row, 'BIRTH_DT', "Format tanggal lahir salah (harus numeric)")

    # Simpan ke satu file Excel dengan banyak sheet
    output_path = os.path.join(current_dir, 'DATA_CUSTOMER_TIDAK_VALID.xlsx')
    
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