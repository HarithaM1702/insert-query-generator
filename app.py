import streamlit as st
import pandas as pd
import re
import io
import os
from datetime import datetime

# Optional example image path provided by the system (developer note)
SAMPLE_IMAGE_PATH = "/mnt/data/A_flat-style_digital_illustration_depicts_a_chatbo.png"

st.set_page_config(page_title="Insert Query Construction", layout="wide")

st.title("Insert Query Construction (Rule-based)")

st.markdown("""
Upload a CSV file (any headers).  
Type a prompt that describes what you want like:
- `Please construct Insert Queries for the MHNET_Testdata.csv file, populate table name as ` + backtick + `project.dataset.table` + backtick + `, Create insert queries with claim_num as "MHNETClaimnumber1". For the create 25 Insert queries and populate line_num from 1 upto 25. Please set "type_of_bill" value as '111'. Put the created insert queries in a file "MHNETInsertQueriesfor_first25lines.txt"`  
Then click **Process prompt**.  
(See examples and helpers below.)
""")

# Show sample image if present (developer-provided path)
if os.path.exists(SAMPLE_IMAGE_PATH):
    st.image(SAMPLE_IMAGE_PATH, caption="Sample UI art (optional)", use_column_width=True)

# 1) Upload CSV
uploaded_file = st.file_uploader("Upload CSV file (or .xlsx)", type=["csv", "xlsx"])

# 2) Prompt input
prompt = st.text_area("Enter prompt / instructions (write rules in plain English):", height=140,
                      placeholder='Example: Create 25 inserts, table name `project.dataset.table`, claim_num as "MHNETClaimnumber1", line_num from 1 upto 25, set "type_of_bill" value as \'111\'.')

# 3) File name input (optional)
default_filename = "insert_queries_{}.txt".format(datetime.now().strftime("%Y%m%d_%H%M%S"))
output_filename = st.text_input("Output file name (when you click Save):", value=default_filename)

# Initialize session state storage for queries so subsequent prompts can append/modify
if "queries" not in st.session_state:
    st.session_state.queries = []
if "last_df" not in st.session_state:
    st.session_state.last_df = None
if "last_table" not in st.session_state:
    st.session_state.last_table = None

# Helper: parse prompt using regexes (basic)
def parse_prompt(text):
    parsed = {
        "table": None,
        "claim_pattern": None,
        "line_range": None,   # tuple (start, end) or None
        "num_rows": None,     # integer number of rows to create
        "append_count": None,
        "forced_values": {},  # dict column -> value
        "enclose_with": "'",
        "add_more": False
    }

    if not text:
        return parsed

    # Table name inside backticks `...`
    m = re.search(r'`([^`]+)`', text)
    if m:
        parsed["table"] = m.group(1).strip()

    # claim number pattern (claim_num as "X" or claim_num as 'X')
    m = re.search(r'claim[_ ]num(?:ber)?\s+as\s+"([^"]+)"', text, flags=re.IGNORECASE)
    if not m:
        m = re.search(r"claim[_ ]num(?:ber)?\s+as\s+'([^']+)'", text, flags=re.IGNORECASE)
    if m:
        parsed["claim_pattern"] = m.group(1).strip()

    # line_num from X upto/to Y
    m = re.search(r'line[_ ]num(?:ber)?\s+from\s+(\d+)\s*(?:to|upto)\s*(\d+)', text, flags=re.IGNORECASE)
    if m:
        start = int(m.group(1)); end = int(m.group(2))
        parsed["line_range"] = (start, end)
        parsed["num_rows"] = end - start + 1

    # create N insert(s) or create N Insert queries
    m = re.search(r'create\s+(\d+)\s+insert', text, flags=re.IGNORECASE)
    if m:
        parsed["num_rows"] = int(m.group(1))

    # add additional N lines / add N lines
    m = re.search(r'add(?:itional)?\s+(\d+)\s+lines', text, flags=re.IGNORECASE)
    if m:
        parsed["append_count"] = int(m.group(1))
        parsed["add_more"] = True

    # "populate line_num as 1" -> set a specific line_num value for all produced rows
    m = re.search(r'populate\s+line[_ ]num\s+as\s+(\d+)', text, flags=re.IGNORECASE)
    if m:
        parsed["line_range"] = (int(m.group(1)), int(m.group(1)))
        parsed["num_rows"] = 1

    # forced set like: set "type_of_bill" value as '111' or set type_of_bill as '111'
    for forced in re.finditer(r'set\s+"?([A-Za-z0-9_]+)"?\s+value\s+as\s+\'?([^\'"\s]+)\'?', text, flags=re.IGNORECASE):
        col = forced.group(1).strip()
        val = forced.group(2).strip()
        parsed["forced_values"][col] = val

    # also accept patterns like type_of_bill as '111' without 'set'
    for forced in re.finditer(r'([A-Za-z0-9_]+)\s+value\s+as\s+\'?([^\'"\s]+)\'?', text, flags=re.IGNORECASE):
        col = forced.group(1).strip()
        val = forced.group(2).strip()
        parsed["forced_values"].setdefault(col, val)

    # Enclose with single quotes or double? Default is single quote
    if "double quote" in text.lower():
        parsed["enclose_with"] = '"'
    return parsed

# Helper: safe quoting for SQL values
def quote_val(val, enclose="'"):
    if val is None:
        return "NULL"
    s = str(val)
    if s.strip().upper() == "NULL" or s == "":
        return "NULL"
    # if numeric-looking, return as is (but careful: many things are strings in CSV)
    # We'll treat everything as string except pure numbers:
    try:
        # But don't force numeric - keep safe behavior: if str is digits and not too long treat as numeric
        if s.replace('.', '', 1).isdigit():
            return s
    except:
        pass
    # escape single quote by doubling
    if enclose == "'":
        s = s.replace("'", "''")
    elif enclose == '"':
        s = s.replace('"', '""')
    return f"{enclose}{s}{enclose}"

# Build SQL INSERTs
def build_insert_queries(df, table, claim_pattern=None, line_range=None, forced_values=None, num_rows=None, enclose="'", start_from=1):
    # df: a pandas DataFrame with data
    # We'll iterate rows from df as source; if df has fewer rows than num_rows, we'll cycle rows
    queries = []
    cols = df.columns.tolist()
    forced_values = forced_values or {}

    # Decide how many rows to generate
    if num_rows is None:
        num_rows = len(df)

    # generate indices to use from df (cycle if needed)
    df_rows = df.to_dict(orient="records")
    n_src = len(df_rows) if len(df_rows) > 0 else 1

    # prepare line numbers
    line_nums = None
    if line_range:
        line_nums = list(range(line_range[0], line_range[1]+1))
        # if num_rows provided and line_nums length differs, adjust
        if num_rows and len(line_nums) != num_rows:
            # If line_nums shorter, we will use as many as len(line_nums)
            num_rows = len(line_nums)
    else:
        # if no line_range but num_rows provided, create default lines starting at start_from
        line_nums = list(range(start_from, start_from + num_rows))

    for i in range(num_rows):
        src_row = df_rows[i % n_src] if n_src > 0 else {}
        values = []
        for c in cols:
            # if forced value present for this column, use forced value
            if c in forced_values:
                val = forced_values[c]
            else:
                val = src_row.get(c, "")
            values.append(quote_val(val, enclose))
        # allow claim_pattern and line_num overrides
        if claim_pattern:
            # replace placeholder like {n} or append index
            claim_val = None
            if "{n}" in claim_pattern:
                claim_val = claim_pattern.replace("{n}", str(i+1))
            elif claim_pattern.endswith(str(1)) and claim_pattern[:-1].isdigit() is False:
                # crude fallback: try to append number to pattern base if pattern ends with digit 1 in examples
                # if pattern like MHNETClaimnumber1 and they asked create multiple, we increment last number
                base = claim_pattern.rstrip('0123456789')
                claim_val = f"{base}{i+1}"
            else:
                # default append index
                claim_val = f"{claim_pattern}{i+1}"
            # if claim_num column exists in columns, set it (override)
            if "claim_num" in cols:
                idx = cols.index("claim_num")
                values[idx] = quote_val(claim_val, enclose)
        # set line_num if present
        if "line_num" in cols:
            idx_ln = cols.index("line_num")
            ln = line_nums[i] if i < len(line_nums) else (start_from + i)
            values[idx_ln] = quote_val(ln, enclose if enclose != "'" else "'")
        # override forced values again in case they apply
        for fcol, fval in forced_values.items():
            if fcol in cols:
                idxf = cols.index(fcol)
                values[idxf] = quote_val(fval, enclose)
        col_list = ", ".join(cols)
        val_list = ", ".join(values)
        sql = f"INSERT INTO {table} ({col_list}) VALUES ({val_list});"
        queries.append(sql)
    return queries

# When user clicks process - main logic
if st.button("Process prompt"):
    if not uploaded_file:
        st.error("Please upload a CSV (or .xlsx) file first.")
    elif not prompt or prompt.strip() == "":
        st.error("Please enter a prompt describing what you want.")
    else:
        # Read the uploaded file
        try:
            if uploaded_file.name.lower().endswith(".csv"):
                df = pd.read_csv(uploaded_file, dtype=str, keep_default_na=False)
            else:
                # excel
                df = pd.read_excel(uploaded_file, dtype=str)
            # Normalize NaNs to empty strings
            df = df.fillna("")
            st.session_state.last_df = df.copy()
        except Exception as e:
            st.error(f"Could not read file: {e}")
            df = None

        if df is not None:
            st.success(f"CSV loaded. {len(df)} rows, {len(df.columns)} columns.")
            st.write("Detected headers (first row):")
            st.write(list(df.columns))

            parsed = parse_prompt(prompt)
            st.write("---")
            st.write("Parsed instructions (best-effort):")
            st.json(parsed)

            # decide table name (fallback to default if none)
            table = parsed["table"] or st.session_state.get("last_table") or "MY_DEFAULT_TABLE"
            st.session_state.last_table = table

            # handle append/additional
            if parsed.get("append_count") and parsed.get("add_more"):
                # Append N additional queries using the same logic as last generation
                add_n = parsed["append_count"]
                # We'll generate add_n more rows using last parsed pattern if exists
                # Find claim pattern and forced values
                claim_pattern = parsed.get("claim_pattern") or None
                forced = parsed.get("forced_values", {})
                enclose = parsed.get("enclose_with", "'")
                # Determine starting index: if there are previous queries that used line_num we try to continue
                existing = st.session_state.queries
                start_n = 1
                if existing:
                    start_n = len(existing) + 1
                new_qs = build_insert_queries(df, table, claim_pattern=claim_pattern,
                                              line_range=None, forced_values=forced,
                                              num_rows=add_n, enclose=enclose, start_from=start_n)
                st.session_state.queries.extend(new_qs)
                st.success(f"Appended {len(new_qs)} queries.")
            else:
                # Normal generation (not append)
                num_rows = parsed.get("num_rows")
                claim_pattern = parsed.get("claim_pattern")
                line_range = parsed.get("line_range")
                forced = parsed.get("forced_values", {})
                enclose = parsed.get("enclose_with", "'")
                # If no num_rows set, default to number of rows available or 10
                if num_rows is None:
                    num_rows = min(25, len(df)) if len(df) > 0 else 1
                new_queries = build_insert_queries(df, table, claim_pattern=claim_pattern,
                                                   line_range=line_range, forced_values=forced,
                                                   num_rows=num_rows, enclose=enclose, start_from=1)
                # Replace previous queries with new set (user can append later)
                st.session_state.queries = new_queries
                st.success(f"Generated {len(new_queries)} queries.")

# Show output area with generated queries
st.markdown("---")
st.subheader("Generated Insert Queries (preview)")

if st.session_state.queries:
    queries_text = "\n".join(st.session_state.queries)
    st.text_area("Queries (editable) — feel free to edit, then Save to download", value=queries_text, height=350, key="preview_area")
    # Download button
    btn = st.download_button("Download queries as file", data=queries_text.encode('utf-8'),
                             file_name=output_filename, mime="text/plain")
    st.write("You can change the file name above before downloading.")
else:
    st.info("No queries yet. Upload CSV and click Process prompt to generate queries.")

# Extra controls: Clear / Reset
st.markdown("---")
c1, c2 = st.columns(2)
with c1:
    if st.button("Clear queries"):
        st.session_state.queries = []
        st.success("Cleared generated queries.")
with c2:
    if st.button("Reload last CSV into preview"):
        if st.session_state.last_df is not None:
            st.write("Re-loading last CSV sample:")
            st.write(st.session_state.last_df.head(5))
        else:
            st.info("No CSV loaded in this session.")

# Small help examples for user
st.markdown("---")
st.subheader("Prompt Examples (copy-paste and edit)")
st.markdown("""
- Example 1: `Please construct Insert Queries for the MHNET_Testdata.csv file, populate table name as ` + backtick + `project.dataset.table` + backtick + `, Enclose the values in the insert queries with ''. Create insert queries with claim_num as "MHNETClaimnumber1". For the created Insert queries and populate line_num from 1 upto 25. Please set "type_of_bill" value as '111'. Put the created insert queries in a file "MHNETInsertQueriesfor_first25lines.txt"`  

- Example 2 (append): `Please add additional 40 lines to same file`  

- Example 3: `Please construct Insert Queries for the SFMSC_Testdata.csv file, populate table name as ` + backtick + `project.dataset.table` + backtick + `, Create 10 Insert queries and populate line_num as 1.`

Notes:
- If prompt parsing misses something, you can edit the queries in the preview box before saving.
- This app is rule-based: it looks for patterns in the prompt — exact natural language understanding is limited, but common patterns (table in backticks, claim_num as "...", line_num ranges, set "col" value as 'x') are supported.
""")
