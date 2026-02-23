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

def validate_relasi_IDNO_BIRTHDATE(id_no, birth_date, gender):
    """
    Validasi relasi antara ID_NO (NIK) dan BIRTH_DATE.
    Format BIRTH_DATE yang diharapkan: DD-MM-YYYY
    """
    id_no = str(id_no).strip()
    birth_date = str(birth_date).strip()
    gender = str(gender).strip().upper()

    # Pastikan panjang ID_NO cukup (minimal 12 digit untuk pengecekan ini)
    if len(id_no) < 12 or len(birth_date) < 10:
        return False

    # Ambil komponen dari ID_NO (Index Python mulai dari 0)
    # Digit 7-8: Tanggal, 9-10: Bulan, 11-12: Tahun (YY)
    id_dd = id_no[6:8]
    id_mm = id_no[8:10]
    id_yy = id_no[10:12]

    # Ambil komponen dari BIRTH_DATE (DD-MM-YYYY)
    b_dd = birth_date[0:2]
    b_mm = birth_date[3:5]
    b_yy = birth_date[8:10] # Ambil 2 digit terakhir tahun

    try:
        if gender == 'M':
            # Pria: ID_DD harus sama dengan B_DD
            return id_dd == b_dd and id_mm == b_mm and id_yy == b_yy
        
        elif gender == 'F':
            # Wanita: (ID_DD - 40) harus sama dengan B_DD
            # Contoh: Lahir tanggal 01, maka di ID_NO tertulis 41
            calc_dd = int(id_dd) - 40
            # Kembalikan ke string dengan leading zero jika < 10 (misal '01')
            str_calc_dd = str(calc_dd).zfill(2)
            
            return str_calc_dd == b_dd and id_mm == b_mm and id_yy == b_yy
        
        else:
            return False # Gender tidak valid
            
    except ValueError:
        return False # Jika ID_NO digit 7-8 bukan angka

# ==========================================
# STEP 2, 3, & 4: PROSES DAN PENYIMPANAN
# ==========================================

def run_validation():
    current_dir = os.path.dirname(os.path.abspath(__file__))
    
    file_coreaccount = next((f for f in os.listdir(current_dir) if ('coraccount' in f.lower() or 'coreaccount' in f.lower()) and f.endswith('.txt')), None)
    file_customerpersonal = next((f for f in os.listdir(current_dir) if f.lower().startswith('customerpersonal_') and f.endswith('.txt')), None)

    if not file_coreaccount or not file_customerpersonal:
        print("Error: File .txt tidak ditemukan di folder.")
        return

    print(f"Membaca data...")
    df_a = pd.read_csv(os.path.join(current_dir, file_coreaccount), sep='|', dtype=str)
    df_b = pd.read_csv(os.path.join(current_dir, file_customerpersonal), sep='|', dtype=str)

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
        'INVALID_BIRTH_INFO': [],
        'INVALID_ID_NO': [],
        'INVALID_MOTHER_MAIDEN_NAME': [],
        'INVALID_GENDER': [],
        'INVALID_MR_JOB_POSITION': [],
        'INVALID_MARITAL_STAT': [],
        'INVALID_TOTAL_INCOME': [],
        'INVALID_SPOUSE_NAME': [],
        'INVALID_SPOUSE_ID_NO': [],
        'INVALID_SPOUSE_BIRTH_DT': [],
        'INVALID_CUST_CITY': [],
        'INVALID_KODE_SUMBER_PENGHASILAN': [],
        'INVALID_YEARLY_INCOME': [],
        'INVALID_PENDIDIKAN': []

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
       
        # 1. Validasi NPWP
        val_npwp = str(row['NPWP_NO'])
        if not validate_is_numeric(val_npwp) or len(val_npwp.strip()) > 16:
            add_to_error('INVALID_NPWP', row, 'NPWP_NO', "Bukan angka atau > 16 digit")

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

        # 10 validasi ID No
        val_id = str(row['ID_NO'])
        if not validate_not_blank(val_id) or not validate_is_numeric(val_id) or not validate_is_exactly_16_digits(val_id):
            add_to_error('INVALID_ID_NO', row, 'ID_NO', "Bukan angka atau tidak 16 digit")
            add_to_error(validate_relasi_IDNO_BIRTHDATE(val_id, row['BIRTH_DT'], row['MR_GENDER']), row, 'ID_NO', "Relasi ID_NO dengan BIRTH_DT dan GENDER tidak valid")


         # 11. Validasi Nama ibu kandung
        val_mother = str(row['MOTHER_MAIDEN_NAME'])
        if not validate_not_blank(val_mother):
            add_to_error('INVALID_MOTHER_MAIDEN_NAME', row, 'MOTHER_MAIDEN_NAME', "Blank")

        # 12. Validasi Gender
        val_gender = str(row['MR_GENDER'])
        if not validate_not_blank(val_gender) or val_gender not in ['F', 'M']:
            add_to_error('INVALID_GENDER', row, 'GENDER', "Wajib F atau M")

        # 13. Validasi Jabatan
        val_job = str(row['MR_JOB_POSITION'])
        if not validate_not_blank(val_job) or not validate_not_only_numeric(val_job):
            add_to_error('INVALID_MR_JOB_POSITION', row, 'MR_JOB_POSITION', "Blank")

        # 14. Validasi Status Perkawinan
        val_marital = str(row['MARITAL_STAT'])
        if not validate_not_blank(val_marital) or val_marital not in ['S', 'M', 'D']:
            add_to_error('INVALID_MARITAL_STAT', row, 'MARITAL_STAT', "Wajib S,M,D")

        # 15. Validasi Pendapatan
        val_income = str(row['TOTAL_INCOME'])
        if not validate_not_blank(val_income) or not validate_is_numeric(val_income):
            add_to_error('INVALID_TOTAL_INCOME', row, 'TOTAL_INCOME', "Harus angka")

        # 16. Validasi Nama Pasangan
        if val_marital == 'M':
            val_spouse = str(row['SPOUSE_NAME'])
            if not validate_not_blank(val_spouse):
                add_to_error('INVALID_SPOUSE_NAME', row, 'SPOUSE_NAME', "Blank")

        # 17. Validasi ID Pasangan
            val_spouse_id = str(row['SPOUSE_ID_NO'])
            if not validate_not_blank(val_spouse_id) or not validate_is_numeric(val_spouse_id) or not validate_is_exactly_16_digits(val_spouse_id):
                add_to_error('INVALID_SPOUSE_ID_NO', row, 'SPOUSE_ID_NO', "Bukan angka atau tidak 16 digit")

        # 18. Validasi Tanggal Lahir Pasangan
            val_spouse_bd = str(row['SPOUSE_BIRTH_DT'])
            if not validate_not_blank(val_spouse_bd) or not validate_not_only_numeric(val_spouse_bd):
                add_to_error('INVALID_SPOUSE_BIRTH_DT', row, 'SPOUSE_BIRTH_DT', "Format tanggal lahir salah (harus numeric)")

        # 19. Validasi Kota
        val_city = str(row['CUST_CITY'])
        if not validate_not_blank(val_city):
            add_to_error('INVALID_CUST_CITY', row, 'CUST_CITY', "Blank")

        # 20. Validasi Kode Sumber Pendapatan
        val_kode = str(row['KODE_SUMBER_PENGHASILAN'])
        if not validate_not_blank(val_kode) or val_kode not in ['1', '2', '3', '4']:
            add_to_error('INVALID_KODE_SUMBER_PENGHASILAN', row, 'KODE_SUMBER_PENGHASILAN', "Wajib 1,2,3,4")

        # 21. Validasi Pendapatan Tahunan
        val_yearly = str(row['YEARLY_INCOME'])
        if not validate_not_blank(val_yearly) or not validate_is_numeric(val_yearly):
            add_to_error('INVALID_YEARLY_INCOME', row, 'YEARLY_INCOME', "Harus angka")

        # 22. Validasi Pendidikan
        val_pendidikan = str(row['PENDIDIKAN'])
        if not validate_not_blank(val_pendidikan) or not validate_is_numeric(val_pendidikan) or not validate_no_special_chars(val_pendidikan):
            add_to_error('INVALID_PENDIDIKAN', row, 'PENDIDIKAN', "Wajib Numeric")


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