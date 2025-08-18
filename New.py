
import pandas as pd

# Expected columns (rename via args if yours differ):
# df_proc: ['invoice_number','vendor_code','bank_account', 'id_column', ...]
# df_sap:  ['vendor_code','vendor_name','bank_account']
# df_inv:  ['invoice_number','vendor_name', ...]

def validate_vendor_code(
    df_proc: pd.DataFrame,
    df_sap: pd.DataFrame,
    *,
    id_col="id_column",
    proc_vendor_code="vendor_code",
    sap_vendor_code="vendor_code",
    sap_vendor_name="vendor_name",
    sap_bank_col="bank_account",
):
    """
    Enrich df_proc with SAP lookups and validate vendor_code only for H rows.
    - Adds: 'vendor_name_sap', 'bank_account_sap'
    - Raises ValueError if any H-row vendor_code is missing in SAP.
    - Preserves original df_proc index and order.
    """
    # Create SAP lookup series (no index changes)
    sap = df_sap.set_index(sap_vendor_code)
    name_map = sap[sap_vendor_name]
    bank_map = sap[sap_bank_col] if sap_bank_col in sap.columns else pd.Series(dtype=object)

    # Only for H rows
    h_mask = df_proc[id_col] == "H"

    # Add SAP columns (mapped, preserves index)
    df_proc.loc[h_mask, "vendor_name_sap"] = df_proc.loc[h_mask, proc_vendor_code].map(name_map)
    if not bank_map.empty:
        df_proc.loc[h_mask, "bank_account_sap"] = df_proc.loc[h_mask, proc_vendor_code].map(bank_map)

    # Missing vendor_code in SAP → vendor_name_sap is NaN
    missing_mask = h_mask & df_proc["vendor_name_sap"].isna()
    if missing_mask.any():
        missing_codes = df_proc.loc[missing_mask, proc_vendor_code].astype(str).unique().tolist()
        raise ValueError(f"Vendor code(s) not found in SAP (H rows only): {missing_codes}")

    return df_proc  # now enriched with *_sap columns (index preserved)


def validate_vendor_name(
    df_proc_enriched: pd.DataFrame,
    df_inv: pd.DataFrame,
    *,
    id_col="id_column",
    inv_invoice_col="invoice_number",
    inv_vendor_name_col="vendor_name",
    proc_invoice_col="invoice_number",
):
    """
    Compare Invoice vendor_name vs SAP vendor_name and correct only H rows.
    - Adds: 'vendor_name_inv', 'vendor_name_updated' (bool)
    - Overwrites invoice vendor_name value in a returned *invoice copy* for audit.
    - Preserves original indices (proc and inv).
    """
    # Build an invoice name lookup (no merge, no index change)
    inv_name_map = df_inv.set_index(inv_invoice_col)[inv_vendor_name_col]

    h_mask = df_proc_enriched[id_col] == "H"

    # Attach invoice vendor name onto procurement rows for comparison view
    df_proc_enriched.loc[h_mask, "vendor_name_inv"] = df_proc_enriched.loc[h_mask, proc_invoice_col].map(inv_name_map)

    # Find mismatches among H rows
    mismatch_mask = h_mask & (df_proc_enriched["vendor_name_inv"].notna()) & (
        df_proc_enriched["vendor_name_inv"] != df_proc_enriched["vendor_name_sap"]
    )

    # Prepare a corrected copy of invoices (keep invoice index)
    df_inv_corrected = df_inv.copy()
    df_inv_corrected["vendor_name_updated"] = False

    # Get the correct names from SAP by invoice_number
    to_fix = df_proc_enriched.loc[mismatch_mask, [proc_invoice_col, "vendor_name_sap"]].drop_duplicates()

    # Map invoice_number -> correct SAP name, then update invoice rows
    fix_map = to_fix.set_index(proc_invoice_col)["vendor_name_sap"]
    rows_to_update = df_inv_corrected[inv_invoice_col].isin(fix_map.index)

    df_inv_corrected.loc[rows_to_update, inv_vendor_name_col] = (
        df_inv_corrected.loc[rows_to_update, inv_invoice_col].map(fix_map)
    )
    df_inv_corrected.loc[rows_to_update, "vendor_name_updated"] = True

    return df_proc_enriched, df_inv_corrected


def validate_vendor_bank_account(
    df_proc_enriched: pd.DataFrame,
    *,
    id_col="id_column",
    proc_bank_col="bank_account",
    sap_bank_col="bank_account_sap",
):
    """
    Validate bank account (Proc vs SAP) only for H rows.
    - Adds/updates: 'bank_account_mismatch' (bool) on df_proc_enriched.
    - Preserves original index.
    """
    h_mask = df_proc_enriched[id_col] == "H"
    df_proc_enriched["bank_account_mismatch"] = False
    if sap_bank_col in df_proc_enriched.columns and proc_bank_col in df_proc_enriched.columns:
        df_proc_enriched.loc[h_mask, "bank_account_mismatch"] = (
            df_proc_enriched.loc[h_mask, proc_bank_col] != df_proc_enriched.loc[h_mask, sap_bank_col]
        )
    return df_proc_enriched
