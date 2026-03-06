import pandas as pd
import os
from tqdm import tqdm

# ==========================================
# STEP 1: FUNGSI VALIDASI MODULAR (UNIT)
# ==========================================

def validate_not_blank(value):
    """Memeriksa apakah nilai tidak kosong, 'nan', atau hanya spasi."""
    val_str = str(value).strip()
    return val_str != "" and val_str.lower() != 'nan'

def validate_lunas(row):
    """
    Memvalidasi logika pelunasan berdasarkan CONTRACT_STATUS, DEFAULT_STATUS, dan sisa kewajiban.
    Mengembalikan tuple (is_valid, message).
    """
    contract_status = str(row.get('CONTRACT_STATUS', '')).strip().upper()
    default_status = str(row.get('DEFAULT_STATUS', '')).strip().upper()
    os_principal = row.get('OS_PRINCIPAL_AMT', 0)
    os_interest = row.get('OS_INTEREST_AMT', 0)

    # Kondisi error 1: Seharusnya sudah lunas tapi status kontrak belum EXP
    if contract_status != 'EXP' and os_principal < 100 and os_interest < 100:
        return (False, "Status loan tidak valid, Seharusnya sudah lunas")

    # Kondisi error 2: Status EXP, default NM/NA, tapi masih ada kewajiban
    if contract_status == 'EXP' and default_status in ['NM', 'NA'] and (os_principal > 100 or os_interest > 100):
        return (False, "Status loan tidak valid karena masih ada kewajiban")

    # Jika tidak ada kondisi error yang terpenuhi, maka data dianggap valid.
    return (True, None)

# ==========================================
# STEP 2: PROSES UTAMA VALIDASI
# ==========================================

def run_validation():
    current_dir = os.path.dirname(os.path.abspath(__file__))

    # Cari file-file yang diperlukan
    try:
        file_core = next(f for f in os.listdir(current_dir) if 'coraccount' in f.lower() and f.endswith('.txt'))
        file_cust = next(f for f in os.listdir(current_dir) if f.lower().startswith('customer_') and f.endswith('.txt'))
        file_pers = next(f for f in os.listdir(current_dir) if f.lower().startswith('customerpersonal_') and f.endswith('.txt'))
        file_corp = next(f for f in os.listdir(current_dir) if f.lower().startswith('custcorporate_') and f.endswith('.txt'))
    except StopIteration:
        print("Error: Salah satu atau lebih file .txt (coraccount, customer, customerpersonal, custcorporate) tidak ditemukan.")
        return

    print("Membaca data...")
    df_core = pd.read_csv(os.path.join(current_dir, file_core), sep='|', dtype=str)
    df_cust = pd.read_csv(os.path.join(current_dir, file_cust), sep='|', dtype=str)
    df_pers = pd.read_csv(os.path.join(current_dir, file_pers), sep='|', dtype=str)
    df_corp = pd.read_csv(os.path.join(current_dir, file_corp), sep='|', dtype=str)

    # --- Pra-pemrosesan untuk efisiensi ---
    # Konversi kolom jumlah ke numerik untuk perbandingan
    try:
        df_core['OS_PRINCIPAL_AMT'] = pd.to_numeric(df_core['OS_PRINCIPAL_AMT'], errors='coerce').fillna(0)
        df_core['OS_INTEREST_AMT'] = pd.to_numeric(df_core['OS_INTEREST_AMT'], errors='coerce').fillna(0)
    except KeyError as e:
        print(f"Error: Kolom {e} tidak ditemukan di file coreaccount. Pastikan nama kolom sudah benar.")
        return

    # Buat set CUST_NO untuk pengecekan relasi yang cepat
    cust_no_in_customer = set(df_cust['CUST_NO'])
    cust_no_in_personal = set(df_pers['CUST_NO'])
    cust_no_in_corporate = set(df_corp['CUST_NO'])
    valid_cust_no_relation = cust_no_in_personal.union(cust_no_in_corporate)

    # Daftar kolom yang akan divalidasi tidak boleh kosong
    columns_to_validate_not_blank = [
        "GENERATED_DT", "PARTNER_CODE", "PARTNER_NAME", "PARTNER_AGRMNT_NO", "AGRMNT_NO",
        "ASSET_CATEGORY_CODE", "ASSET_NAME", "ASSET_PRICE_AMT", "CURR_CODE", "CUST_NAME",
        "CUST_NO", "LAST_INST_DT", "EFFECTIVE_DT", "EFFECTIVE_RATE_PRCNT", "FIRST_INST_DT",
        "FIRST_INST_TYPE", "FLAT_RATE_PRCNT", "OPRT_BATCH_NO", "INCOME_RECOG_AMT", "INST_AMT",
        "DRAWDOWN_DT", "NEXT_INST_DUE_DT", "NTF_AMT", "OS_DENDA_CUST", "OS_DENDA_OPRT",
        "OS_INTEREST_AMT", "OS_INTEREST_UNDUE_AMT", "OS_PRINCIPAL_AMT", "OS_PRINCIPAL_UNDUE_AMT",
        "PROD_OFFERING_CODE", "BRANCH_CODE", "RRD_DT", "TENOR", "DOWN_PAYMENT", "INST_SEQ_NO",
        "OVERDUE_DAYS", "CONTRACT_STATUS", "DEFAULT_STATUS", "PROD_OFFERING_NAME",
        "PURPOSE_OF_FINANCING", "COLLECTIBILITY_STAT", "TANGGAL_MACET", "UNPAID_ACCRUE_INTEREST",
        "NEXT_INST_DUE_OS_PRINCIPAL", "NEXT_INST_DUE_OS_INTEREST", "KODE_CABANG_PARTNER",
        "COST_OF_FUND_PERCENTAGE", "RISK_PREMIUM_PERCENTAGE", "SUBSIDY_DAYS",
        "OS_PRINCIPAL_DUE_AMT", "OS_INTEREST_DUE_AMT"
    ]

    # Inisialisasi dictionary untuk menampung data error per sheet
    sheets_data = {
        'INVALID_LUNAS_LOGIC': [],
        'CUST_NO_NOT_IN_CUSTOMER': [],
        'CUST_NO_NOT_IN_PERS_OR_CORP': []
    }
    # Buat key untuk setiap kolom yang akan divalidasi blank
    for col in columns_to_validate_not_blank:
        sheets_data[f'BLANK_{col.upper()}'] = []

    def add_to_error(sheet_name, row, col_name, message):
        sheets_data[sheet_name].append({
            'PARTNER_NAME': row.get('PARTNER_NAME'),
            'PARTNER_AGRMNT_NO': row.get('PARTNER_AGRMNT_NO'),
            'AGRMNT_NO': row.get('AGRMNT_NO'),
            'CUST_NO': row.get('CUST_NO'),
            'CUST_NAME': row.get('CUST_NAME'),
            'DATA_ORIGINAL': row.get(col_name),
            'KETERANGAN_ERROR': message
        })

    print("Memulai validasi data coreaccount...")
    # Terapkan tqdm untuk progress bar
    for _, row in tqdm(df_core.iterrows(), total=df_core.shape[0], desc="Validasi Baris", bar_format="{l_bar}{bar:25}{r_bar}", colour='green'):
        cust_no = row.get('CUST_NO')

        # Validasi 1: Semua kolom tidak boleh blank
        for col_name in columns_to_validate_not_blank:
            if col_name in row and not validate_not_blank(row[col_name]):
                add_to_error(f'BLANK_{col_name.upper()}', row, col_name, "Kolom tidak boleh kosong atau 'nan'")

        # Validasi 2: Logika Lunas
        is_valid, message = validate_lunas(row)
        if not is_valid:
            # Buat string yang lebih informatif untuk kolom DATA_ORIGINAL
            original_data_str = (
                f"CONTRACT_STATUS: {row.get('CONTRACT_STATUS')}, "
                f"DEFAULT_STATUS: {row.get('DEFAULT_STATUS')}, "
                f"OS_PRINCIPAL: {row.get('OS_PRINCIPAL_AMT')}, "
                f"OS_INTEREST: {row.get('OS_INTEREST_AMT')}"
            )
            # Tambahkan data konteks ke baris sementara untuk pelaporan
            row['LUNAS_CONTEXT'] = original_data_str
            add_to_error('INVALID_LUNAS_LOGIC', row, 'LUNAS_CONTEXT', message)

        # Validasi 3: CUST_NO harus ada di customer.txt
        if cust_no not in cust_no_in_customer:
            add_to_error('CUST_NO_NOT_IN_CUSTOMER', row, 'CUST_NO', "CUST_NO tidak ditemukan di file master customer")

        # Validasi 4: CUST_NO harus ada di customerpersonal.txt atau custcorporate.txt
        if cust_no not in valid_cust_no_relation:
            add_to_error('CUST_NO_NOT_IN_PERS_OR_CORP', row, 'CUST_NO', "CUST_NO tidak ditemukan di file customerpersonal maupun custcorporate")

    # --- STEP 3: PENYIMPANAN HASIL ---
    output_path = os.path.join(current_dir, 'DATA_COREACCOUNT_TIDAK_VALID.xlsx')
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