def validate_vendor_code(df_proc, df_sap):
    """
    Validate vendor codes in Procurement against SAP.
    - Ensures all vendor_code in Procurement exist in SAP.
    - Returns Procurement enriched with SAP fields.
    """
    proc_sap = df_proc.merge(
        df_sap,
        on="vendor_code",
        how="left",
        suffixes=("_proc", "_sap")
    )

    # Missing vendor_code in SAP?
    missing = proc_sap[proc_sap["vendor_name_sap"].isna()]
    if not missing.empty:
        raise ValueError(f"Vendor codes not found in SAP: {missing['vendor_code'].tolist()}")

    return proc_sap


def validate_vendor_name(proc_sap, df_inv):
    """
    Validate vendor names:
    - Compares Invoice vendor_name vs SAP vendor_name.
    - If mismatch, corrects Invoice vendor_name and flags update.
    """
    merged = proc_sap.merge(
        df_inv,
        on="invoice_number",
        how="left",
        suffixes=("_proc", "_inv")
    )

    merged["vendor_name_updated"] = False
    mask = merged["vendor_name_inv"] != merged["vendor_name_sap"]

    merged.loc[mask, "vendor_name_inv"] = merged.loc[mask, "vendor_name_sap"]
    merged.loc[mask, "vendor_name_updated"] = True

    return merged

def validate_vendor_bank_account(proc_sap):
    """
    Validate vendor bank accounts:
    - Compares Procurement bank_account vs SAP bank_account.
    - Flags mismatches.
    """
    proc_sap["bank_account_mismatch"] = (
        proc_sap["bank_account_proc"] != proc_sap["bank_account_sap"]
    )

    return proc_sap
# Step 1: Validate vendor codes
proc_sap = validate_vendor_code(df_proc, df_sap)

# Step 2: Validate vendor names (Invoice vs SAP)
merged = validate_vendor_name(proc_sap, df_inv)

# Step 3: Validate vendor bank accounts (Proc vs SAP)
proc_sap_checked = validate_vendor_bank_account(proc_sap)

print(merged[["invoice_number", "vendor_code", "vendor_name_sap", "vendor_name_inv", "vendor_name_updated"]])
print(proc_sap_checked[["invoice_number", "vendor_code", "bank_account_proc", "bank_account_sap", "bank_account_mismatch"]])

