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
    return len(str(value).strip()) == 4 and str(value).strip().isdigit()

def validate_is_exactly_5_digits(value):
    return len(str(value).strip()) == 5 and str(value).strip().isdigit()

def validate_is_exactly_16_digits(value): 
    return len(str(value).strip()) == 16 and str(value).strip().isdigit()
# ==========================================
# STEP 2, 3, & 4: PROSES DAN PENYIMPANAN
# ==========================================

def run_validation():
    current_dir = os.path.dirname(os.path.abspath(__file__))
    
    file_a = next((f for f in os.listdir(current_dir) if ('coraccount' in f.lower() or 'coreaccount' in f.lower()) and f.endswith('.txt')), None)
    file_b = next((f for f in os.listdir(current_dir) if f.lower().startswith('custcorporate_') and f.endswith('.txt')), None)

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
        'INVALID_ESTABLISHMENT_YEAR': [],
        'INVALID_DEED_PLACE': [],
        'INVALID_DEED_NO': [],
        'INVALID_DEED_DT': [],
        'INVALID_TGL_AKTEAWAL': [],
        'INVALID_NO_AKTEAKHIR': [],
        'INVALID_TEMPAT_PENDIRIAN_PERUSAHAAN': [],
        'INVALID_KODE_JENIS_BADAN_USAHA': [],
        'INVALID_TGL_AKTA_AKHIR': []
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
     
        # 2. Validasi Establishment Year
        val_est_year = str(row['ESTABLISHMENT_YEAR'])
        if not validate_is_exactly_4_digits(val_est_year) or not validate_is_numeric(val_est_year) or not validate_not_blank(val_est_year):
            add_to_error('INVALID_ESTABLISHMENT_YEAR', row, 'ESTABLISHMENT_YEAR', "Bukan 4 digit angka")
        # 3. Validasi Deed Place
        val_deed_place = str(row['DEED_PLACE'])
        if not validate_not_blank(val_deed_place) or not validate_no_special_chars(val_deed_place) or not validate_not_only_numeric(val_deed_place):
            add_to_error('INVALID_DEED_PLACE', row, 'DEED_PLACE', "Kosong atau ada karakter khusus")
        # 4. Validasi Deed No
        val_deed_no = str(row['DEED_NO'])
        if not validate_not_blank(val_deed_no) or not validate_no_special_chars(val_deed_no) or not validate_not_only_numeric(val_deed_no):
            add_to_error('INVALID_DEED_NO', row, 'DEED_NO', "Kosong atau ada karakter khusus")
        # 5. Validasi Deed Date
        val_deed_dt = str(row['DEET_DT'])
        if not validate_not_blank(val_deed_dt):
            add_to_error('INVALID_DEED_DT', row, 'DEED_DT', "Kosong")
        # 6. Validasi Tgl Akte Awal
        val_tgl_akteawal = str(row['TGL_AKTEAWAL'])
        if not validate_not_blank(val_tgl_akteawal):
            add_to_error('INVALID_TGL_AKTEAWAL', row, 'TGL_AKTEAWAL', "Kosong")
        # 7. Validasi No Akte Akhir
        val_no_akteakhir = str(row['NO_AKTEAKHIR'])
        if not validate_not_blank(val_no_akteakhir) or not validate_no_special_chars(val_no_akteakhir) or not validate_not_only_numeric(val_no_akteakhir):
            add_to_error('INVALID_NO_AKTEAKHIR', row, 'NO_AKTEAKHIR', "Kosong atau ada karakter khusus")
        # 8. Validasi Tempat Pendirian Perusahaan
        val_tempat_pendirian = str(row['TEMPAT_PENDIRIAN_PERUSAHAAN'])
        if not validate_not_blank(val_tempat_pendirian) or not validate_no_special_chars(val_tempat_pendirian):
            add_to_error('INVALID_TEMPAT_PENDIRIAN_PERUSAHAAN', row, 'TEMPAT_PENDIRIAN_PERUSAHAAN', "Kosong atau ada karakter khusus")
        # 9. Validasi Kode Jenis Badan Usaha
        val_kode_jenis_badan = str(row['KODE_JENIS_BADAN_USAHA'])
        if not validate_not_blank(val_kode_jenis_badan) or not validate_not_two_digits(val_kode_jenis_badan):
            add_to_error('INVALID_KODE_JENIS_BADAN_USAHA', row, 'KODE_JENIS_BADAN_USAHA', "Kosong, 2 digit, atau bukan angka")
        # 10. Validasi Tgl Akta Akhir
        val_tgl_akta_akhir = str(row['TGL_AKTA_AKHIR'])
        if not validate_not_blank(val_tgl_akta_akhir):
            add_to_error('INVALID_TGL_AKTA_AKHIR', row, 'TGL_AKTA_AKHIR', "Kosong")


    # Simpan ke satu file Excel dengan banyak sheet
    output_path = os.path.join(current_dir, 'DATA_CUSCORPORATE_TIDAK_VALID.xlsx')
    
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