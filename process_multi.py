import sys
import os
import mimetypes
import pandas as pd
import time
import numpy as np
from sqlalchemy import create_engine
from datetime import datetime
from openpyxl import load_workbook  # for accurate rows/cols without loading whole sheet

# Connection settings
server = 'DESKTOP-GELE1R0'
database = 'rdmc'
username = 'rdmc'
password = 'rdmc'

connection_string = f"mssql+pyodbc://{username}:{password}@{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server"
engine = create_engine(connection_string)


def extract_metadata(file_path,original_name, uploaded_by=None):
    
    if not os.path.exists(file_path):
        return {"error": f"File not found: {file_path}"}
    
    stats = os.stat(file_path)
    mime_type, _ = mimetypes.guess_type(file_path)
    xls=pd.ExcelFile(file_path)
    metadata = {
        "file_name": original_name,
        "file_path": file_path,
        "size_bytes": stats.st_size,
        "mime_type": mime_type,
        "created_system": time.ctime(stats.st_ctime),
        "modified_system": time.ctime(stats.st_mtime),
        "uploaded_by": uploaded_by,
        "uploaded_at": time.ctime(),
        "num_sheets": None,
        "sheet_names": None,
        "sheet_rows": None,
        "sheet_columns": None,
        "error": None
    }
    
    # if Excel, extract sheet-level metadata
    if mime_type is None:
        try:
        # Open Excel file
            xls = pd.ExcelFile(file_path)

            sheet_names = []
            sheet_rows = []
            sheet_cols = []

            for sheet in xls.sheet_names:
                df = pd.read_excel(file_path, sheet_name=sheet)

                sheet_names.append(sheet)
                sheet_rows.append(str(df.shape[0]))  # number of rows with data
                sheet_cols.append(str(df.shape[1]))  # number of columns with data

            metadata["num_sheets"] = len(sheet_names)
            metadata["sheet_names"] = ",".join(sheet_names)
            metadata["sheet_rows"] = ",".join(sheet_rows)
            metadata["sheet_columns"] = ",".join(sheet_cols)

        except Exception as e:
            metadata["num_sheets"] = 0
            metadata["sheet_names"] = f"Error: {e}"
            metadata["sheet_rows"] = ""
            metadata["sheet_columns"] = ""

    return metadata

def process_education(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    
    # Ensure PROJECTEDCOMPLETIONDATE is datetime
    df['PROJECTEDCOMPLETIONDATE'] = pd.to_datetime(
        df['PROJECTEDCOMPLETIONDATE'], format='%Y/%m', errors='coerce'
    )

    degree_rank = {
        'Masters': 5,
        'Bachelor': 4,
        'Associate': 3,
        'Certificate': 2,
        'Other': 1
    }

    # Function to compute last_degree per candidate
    def determine_last_degree(group: pd.DataFrame) -> pd.Series:
        if group['PROJECTEDCOMPLETIONDATE'].isna().all():
            if group['SCHOOLNAME'].isna().all():
                return pd.Series([None] * len(group), index=group.index)
            else:
                ranked_group = group[group['DEGREE'].isin(degree_rank)]
                if ranked_group.empty:
                    return pd.Series([None] * len(group), index=group.index)
                ranked_group = ranked_group.copy()
                ranked_group['rank'] = ranked_group['DEGREE'].map(degree_rank)
                best_degree = ranked_group.loc[ranked_group['rank'].idxmax(), 'DEGREE']
                return pd.Series([best_degree] * len(group), index=group.index)

        # Sort by PROJECTEDCOMPLETIONDATE descending
        group = group.sort_values('PROJECTEDCOMPLETIONDATE', ascending=False)
        
        for _, row in group.iterrows():
            degree = row['DEGREE']
            if pd.notna(row['PROJECTEDCOMPLETIONDATE']) and pd.notna(degree) and degree != 'Other':
                return pd.Series([degree] * len(group), index=group.index)

        return pd.Series(['Other'] * len(group), index=group.index)

    # Add last_degree column
    df['last_degree'] = (
    df.groupby('CANDIDATEID')
      .apply(determine_last_degree, include_groups=False)
      .reset_index(level=0, drop=True)
)

    # Function to pick one row per candidate
    def pick_one_row(group: pd.DataFrame) -> pd.DataFrame:
        row=group.sort_values('PROJECTEDCOMPLETIONDATE', ascending=False).head(1)
        row["CANDIDATEID"] = group.name
        return row

    # Get unique row per candidate
    unique_df = (
    df.groupby('CANDIDATEID', group_keys=False)
      .apply(pick_one_row, include_groups=False
             )
      .reset_index(drop=True)
)
    
    return unique_df

def process_work_experience(df: pd.DataFrame) -> pd.DataFrame:
    """
    Process Work_Experience DataFrame to select the latest work experience
    per candidate based on STARTDATE, handling missing dates and CURRENTJOB.

    Args:
        df (pd.DataFrame): Input work experience DataFrame with columns:
            - CANDIDATEID
            - STARTDATE
            - CURRENTJOB (optional)

    Returns:
        pd.DataFrame: Processed DataFrame with one representative row per candidate.
    """
    df = df.copy()
    
    # Ensure STARTDATE is datetime
    df['STARTDATE'] = pd.to_datetime(df['STARTDATE'], errors='coerce')

    # Step 1: Latest row per candidate where STARTDATE is not null
    latest_idx = df[df['STARTDATE'].notna()].groupby('CANDIDATEID')['STARTDATE'].idxmax()
    df_latest = df.loc[latest_idx].reset_index(drop=True)

    # Step 2: Handle rows where STARTDATE is NaT
    temp = df[df['STARTDATE'].isna()].copy()

    # Remove candidates already included in df_latest
    df_latest_ids = set(df_latest['CANDIDATEID'].unique())
    temp = temp[~temp['CANDIDATEID'].isin(df_latest_ids)].reset_index(drop=True)

    # Step 3: Separate CURRENTJOB missing vs not missing
    temp_no_current = temp[temp['CURRENTJOB'].isna()]
    temp_with_current = temp[temp['CURRENTJOB'].notna()]

    # Keep only one row per candidate for CURRENTJOB not missing
    temp_with_current_unique = temp_with_current.drop_duplicates(subset='CANDIDATEID').reset_index(drop=True)

    # Step 4: Concatenate all relevant rows
    df_final = pd.concat([df_latest, temp_no_current, temp_with_current_unique], ignore_index=True)
    
    return df_final

def process_domicile(df: pd.DataFrame) -> pd.DataFrame:
    """
    Process Domicile DataFrame by dropping columns that are completely empty.

    Args:
        df (pd.DataFrame): Input Domicile DataFrame.

    Returns:
        pd.DataFrame: Processed Domicile DataFrame with empty columns removed.
    """
    df = df.copy()
    
    # Drop columns where all values are NaN
    df = df.dropna(axis=1, how='all')
    
    return df

def process_candidate_details(df: pd.DataFrame) -> pd.DataFrame:
    """
    Process Candidate Details DataFrame by dropping columns that are completely empty.

    Args:
        df (pd.DataFrame): Input Candidate Details DataFrame.

    Returns:
        pd.DataFrame: Processed Candidate Details DataFrame with empty columns removed.
    """
    df = df.copy()
    
    # Drop columns where all values are NaN
    df = df.dropna(axis=1, how='all')
    
    return df

def calculate_experience(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate total years of experience for each candidate,
    merging overlapping or continuous job periods.
    """

    # Ensure dates are datetime
    df['STARTDATE'] = pd.to_datetime(df['STARTDATE'], errors='coerce')
    df['ENDDATE'] = pd.to_datetime(df['ENDDATE'], errors='coerce')

    # Drop rows with no STARTDATE
    df = df.dropna(subset=['STARTDATE']).copy()

    # Replace missing ENDDATE with today's date
    today = pd.to_datetime(datetime.today().date())
    df['ENDDATE'] = df['ENDDATE'].fillna(today)

    results = []

    for candidate_id, group in df.groupby('CANDIDATEID'):
        # Sort by start date
        jobs = group[['STARTDATE','ENDDATE']].sort_values('STARTDATE').values.tolist()

        merged = []
        for start, end in jobs:
            start = pd.to_datetime(start)  # ensure Timestamp
            end = pd.to_datetime(end)
            
            if not merged:
                merged.append([start, end])
            else:
                last_start, last_end = merged[-1]
                # Merge if overlapping or continuous
                if start <= last_end:
                    merged[-1][1] = max(last_end, end)
                else:
                    merged.append([start, end])

        # Total duration in days
        total_days = sum((end - start).days for start, end in merged)
        total_years = round(total_days / 365, 2)
        if total_years == "-":
            exp_group = "-"
        elif total_years < 0.5:
            exp_group = "No Experience"
        elif total_years <= 5:
            exp_group = "0-5 Years"
        elif total_years <= 10:
            exp_group = "6-10 Years"
        elif total_years <= 15:
            exp_group = "11-15 Years"
        elif total_years <= 20:
            exp_group = "16-20 Years"
        else:
            exp_group = "21+ Years"

        results.append([candidate_id, total_years, exp_group])

        

    return pd.DataFrame(results, columns=['CANDIDATEID','TOTAL_EXPERIENCE_YEARS','EXPERIENCE_GROUP'])

def current_experience(df):
    # Ensure date columns are datetime
    df['STARTDATE'] = pd.to_datetime(df['STARTDATE'], errors='coerce')
    df['ENDDATE'] = pd.to_datetime(df['ENDDATE'], errors='coerce')
    
    # Filter current jobs
    current_jobs = df[df['CURRENTJOB'] == 'Y'].copy()
    
    # Take the latest start date per candidate
    latest_start = current_jobs.groupby('CANDIDATEID')['STARTDATE'].max().reset_index()
    
    # Calculate experience in years
    latest_start['Experience with Current Employers in Years'] = latest_start['STARTDATE'].apply(lambda x: (datetime.today() - x).days / 365.25)
    
    return latest_start[['CANDIDATEID', 'Experience with Current Employers in Years']]

def get_latest_certificate(df: pd.DataFrame) -> pd.DataFrame:
    # Ensure PROJECTEDCOMPLETIONDATE is datetime (ignore errors for blanks)
    df['PROJECTEDCOMPLETIONDATE'] = pd.to_datetime(df['PROJECTEDCOMPLETIONDATE'], errors='coerce', format='%Y/%m')

    # Filter only Certificate degree
    cert_df = df[df['DEGREE'].str.strip().str.lower() == "certificate"].copy()

    if cert_df.empty:
        return pd.DataFrame(columns=['CANDIDATEID', 'AREAOFSTUDY'])

    # Sort by CANDIDATEID and PROJECTEDCOMPLETIONDATE (latest first)
    cert_df = cert_df.sort_values(by=['CANDIDATEID', 'PROJECTEDCOMPLETIONDATE'], ascending=[True, False])

    # Drop duplicates keeping the latest date (or first occurrence if date is NaT)
    result = cert_df.groupby('CANDIDATEID', as_index=False).first()[['CANDIDATEID', 'AREAOFSTUDY']]
    result = result[['CANDIDATEID', 'AREAOFSTUDY']].rename(columns={'AREAOFSTUDY': 'CERTIFICATE'})
    return result

def assign_category(df: pd.DataFrame) -> pd.DataFrame:
    """
    Assigns 'category' and 'category_district' based on:
    - Candidate Province/County
    - Candidate City
    - CNIC
    Expects columns: Candidate ID, Candidate City, Candidate Province/County, CNIC
    """

    # Normalize strings for safe matching
    df["province_clean"] = df["Candidate Province/County"].fillna("").str.strip().str.lower()
    df["city_clean"] = df["Candidate City"].fillna("").str.strip().str.lower()
    df["cnic_clean"] = df["CNIC Number"].fillna("").astype(str).str.strip()

    # Define city groups
    list_p1 = [
        "humai","mashki cha","nok cha","durbun cha","baiduk","kachaw",
        "tang kachaw","kirtaka","amalaf koh","sarzeh","miskan","lashkaryab"
    ]
    list_p2 = [
        "chagai","nokkundi","dalbandin","chagai","taftan","saindak","yakmach"
    ]
    combined_list = list_p1 + list_p2  # For P3 exclusion

    # Initialize category as blank (so unassigned rows remain blank)
    df["category"] = pd.Series(dtype="object")

    # --- Rule 1: Province contains "baloch" OR blank
    province_baloch_or_blank = df["province_clean"].str.contains("baloch", na=False) | (df["province_clean"] == "")

    # Rule 2: City in P1 list
    df.loc[province_baloch_or_blank & df["city_clean"].isin([c.lower() for c in list_p1]), "category"] = "P1"

    # Rule 3: City in P2 list
    df.loc[province_baloch_or_blank & df["city_clean"].isin([c.lower() for c in list_p2]), "category"] = "P2"

    # Rule 4: Province blank AND CNIC exists AND CNIC starts with 5 → treat as Balochistan
    cnic_baloch = (df["province_clean"] == "") & (df["cnic_clean"].str.startswith("5"))
    df.loc[cnic_baloch & (~df["city_clean"].isin([c.lower() for c in combined_list])), "category"] = "P3"

    # Rule 5: Province blank AND CNIC blank AND city = Quetta → P3
    df.loc[
        (df["province_clean"] == "") &
        (df["cnic_clean"] == "") &
        (df["city_clean"] == "quetta"),
        "category"
    ] = "P3"

    # Rule 6: Province not blank and does NOT contain "baloch" → P4
    df.loc[~df["province_clean"].str.contains("baloch", na=False) & (df["province_clean"] != ""), "category"] = "P4"

    # Rule 7: Province contains "balochistan" AND city not in combined list → P3
    df.loc[
        df["province_clean"].str.contains("balochistan", na=False) &
        (~df["city_clean"].isin([c.lower() for c in combined_list])),
        "category"
    ] = "P3"

    # Remaining rows where category is still NaN → keep blank
    df["category"] = df["category"].fillna("")

    # --- Category District mapping
    df["category_district"] = df["category"].map({
        "P1": "Chagi",
        "P2": "Chagi",
        "P3": "ROB",
        "P4": "ROP",
        "": ""   # blank
    })
    abc=df.drop(columns=["province_clean", "city_clean", "cnic_clean"])
     
   
    return abc[['Candidate ID','category','category_district']]#df.drop(columns=["province_clean", "city_clean", "cnic_clean"])


if __name__ == "__main__":
    (
        file1_path, file1_name,
        file2_path, file2_name,
        file3_path, file3_name,
        file4_path, file4_name,
        output_path,
        uploaded_by   # ✅ new arg
    ) = sys.argv[1:]

    all_metadata = []
    for p, n in [
        (file1_path, file1_name),
        (file2_path, file2_name),
        (file3_path, file3_name),
        (file4_path, file4_name),
    ]:
        all_metadata.append(extract_metadata(p, n, uploaded_by=uploaded_by))

    

    metadata_df = pd.DataFrame(all_metadata, columns=[
        "file_name", "file_path", "size_bytes", "mime_type",
        "created_system", "modified_system", "uploaded_by", "uploaded_at",
        "num_sheets", "sheet_names", "sheet_rows", "sheet_columns", "error"
    ])


    candidate_details = None
    domicile_cnic     = None
    education         = None
    work_experience   = None
    # Insert metadata; only write output if DB insert succeeds
    try:
        metadata_df.to_sql("file_metadata", engine, if_exists="append", index=False)

        candidate_details = pd.read_excel(file1_path,header=1)
        domicile_cnic     = pd.read_excel(file2_path,header=0)
        education         = pd.read_excel(file3_path,header=1)
        work_experience   = pd.read_excel(file4_path,header=1)

      


        # Save first file to output AFTER successful insert
        df_Education = process_education(education)
        df_WorkExperience = process_work_experience(work_experience)
        df_Domicile = process_domicile(domicile_cnic)
        df_CandidateDetails = process_candidate_details(candidate_details)

        wx=calculate_experience(work_experience)
        currentEx=current_experience(work_experience)
        cert=get_latest_certificate(education)
        

        df_CandidateDetails = df_CandidateDetails.rename(columns={'Candidate ID': 'Candidate ID'})
        df_Education = df_Education.rename(columns={'CANDIDATEID': 'Candidate ID'})
        df_Domicile = df_Domicile.rename(columns={'Candidate Number': 'Candidate ID'})
        df_WorkExperience = df_WorkExperience.rename(columns={'CANDIDATEID': 'Candidate ID'})
        df_wx = wx.rename(columns={'CANDIDATEID': 'Candidate ID'})
        df_cx=currentEx.rename(columns={'CANDIDATEID': 'Candidate ID'})
        df_cert=cert.rename(columns={'CANDIDATEID': 'Candidate ID'})
       
        #############PPPPP#################
        #############PPPPP#################
        Ps_merged_df = (df_CandidateDetails
            .merge(df_Domicile, on='Candidate ID', how='inner')
            )
        Ps=assign_category(Ps_merged_df)
        df_Ps=Ps.rename(columns={'Candidate ID': 'Candidate ID'})
        #############PPPPP#################
        #############PPPPP#################
        merged_df = (
                df_CandidateDetails
                .merge(df_Education, on='Candidate ID', how='inner')
                .merge(df_Domicile, on='Candidate ID', how='inner')
                .merge(df_WorkExperience, on='Candidate ID', how='inner')
                .merge(df_wx, on='Candidate ID', how='left')
                .merge(df_cx, on='Candidate ID', how='left')
                .merge(df_cert, on='Candidate ID', how='left')
                .merge(df_Ps, on='Candidate ID', how='left')
            )
        merged_df["Work Experience (yes/No)"] = np.where(
        merged_df["TOTAL_EXPERIENCE_YEARS"].astype(str).str.strip().isin(["0", "-", "","nan"]), 
        "No", 
        "Yes"
    )
        merged_df["S. No"] = range(1, len(merged_df) + 1)

        # Example mapping of CNIC first digit to province
        province_map = {
            "1": "Khyber Pakhtunkhwa",
            "2": "FATA",
            "3": "Punjab",
            "4": "Sindh",
            "5": "Balochistan",
            "6": "Islamabad",
            "7": "Gilgit-Baltistan",
            "8": "AJK"
        }

        def assign_province(row):
            # 1. Category check
            if row["category"] in ["P1", "P2", "P3"]:
                return "Balochistan"
            
            # 2. CNIC check
            cnic = str(row.get("CNIC Number", ""))  # convert to string safely
            if cnic and cnic[0] in province_map:
                return province_map[cnic[0]]
            
            # 3. Default blank
            return ""

        # Apply function
        merged_df["Candidate Province/County"] = merged_df.apply(assign_province, axis=1)

        merged_df["PROJECTEDCOMPLETIONDATE"] = pd.to_datetime(
            merged_df["PROJECTEDCOMPLETIONDATE"], errors="coerce"
        ).dt.year

       

        # Get current year
        current_year = datetime.now().year

        # Calculate difference
        merged_df["Degree-Current Year calculation"] = current_year - merged_df["PROJECTEDCOMPLETIONDATE"]

        # If year is missing -> keep blank
        merged_df["Degree-Current Year calculation"] = merged_df["Degree-Current Year calculation"].fillna("")

        columns_needed = [
            "Candidate ID",
            "CANDIDATENAME",
            "Candidate Email",
            "Candidate Phone",
            "Candidate Country",
            "Candidate City",
            "Candidate Province/County",
            "Candidate Ethnicity",
            "category",
           "category_district",
            "SCHOOLNAME",
            "AREAOFSTUDY",
            "PROJECTEDCOMPLETIONDATE",
            "DEGREE",
            "GRADUATED",
            "last_degree",
            "CERTIFICATE",
            "SCHOOLNAME",
            "CNIC Number",
            "Please select your gender",
            "Please select your nationality",
            "Please indicate your Date of Birth",
            "Please select your ethnicity",
            "Please state your domicile",
            "CURRENTJOB",
            "PREVIOUSEMPLOYER",
            "Experience with Current Employers in Years",
            "JOBTITLE",
            "EXPERIENCE_GROUP",
            "TOTAL_EXPERIENCE_YEARS",
            "Degree-Current Year calculation",
            "Work Experience (yes/No)",
            "S. No"
        ]
        rename_map = {
    "CANDIDATENAME": "Candidate Name on Element",
    "SCHOOLNAME": "Institute/University",
    #"AREAOFSTUDY": "Area of Study",
    "PROJECTEDCOMPLETIONDATE": "Degree/Education completion Year",
    "DEGREE": "Education (*)",
    "category": "Category", 
    "category_district":"Category/District",
    "last_degree": "Education Level (comment)",
    "CERTIFICATE": "Certification (if Any)",
    "CNIC Number": "CNIC",
    "Candidate Province/County":"Province",
    "Please select your gender": "Gender",
    "Please select your nationality": "Nationality",
    "Please indicate your Date of Birth": "Date of Birth",
    "Please select your ethnicity": "Candidate Ethnicity",
    "Please state your domicile": "Domicile",
    "CURRENTJOB": "Currently Employed / Unemployed",
    "PREVIOUSEMPLOYER": "Current Employer",
    "Experience with Current Employers in Years": "Experience with current employer (years)",
    "JOBTITLE": "Current position",
    "EXPERIENCE_GROUP": "Year of Experience Group",
    "TOTAL_EXPERIENCE_YEARS": "Total Experience (Years)",
    "Degree-Current Year calculation":"Degree-Current Year calculation",
    "Work Experience (yes/No)":"Work Experience (yes/No)",
    "S. No":"S. No"
}
        for col in columns_needed:
            if col not in merged_df.columns:
                merged_df[col] = np.nan
        df_m = merged_df[columns_needed].rename(columns=rename_map)

###############################################################33


        from datetime import datetime

        # Assuming df_m is already created from merged_df
        today = pd.to_datetime("today")

        # Convert Date of Birth column to datetime safely
        df_m["Date of Birth"] = pd.to_datetime(df_m["Date of Birth"], errors="coerce")

        # Calculate Age in years
        df_m["Age"] = ((today - df_m["Date of Birth"]).dt.days // 365)

        # Create Age Group column using your defined buckets
        def age_group(age):
            if pd.isna(age):
                return ""
            elif age == "-":
                return "-"
            elif age < 18:
                return "Under 18"
            elif age <= 25:
                return "18-25"
            elif age <= 35:
                return "26-35"
            elif age <= 45:
                return "36-45"
            elif age <= 55:
                return "46-55"
            elif age <= 65:
                return "56-65"
            else:
                return "65+"

        df_m["Age Group"] = df_m["Age"].apply(age_group)

#################################################################
        custom_order=[
            'S. No',
            'Candidate ID',
            'Position',
            'Job Requisition ID',
            'Candidate Name on Element',
            'Candidate Email',
            'Candidate Phone',
            'CNIC',
            'Candidate City',
            'Category',
            'Category/District',
            'Province',
            'Domicile',
            'Candidate Ethnicity',
            'Gender',
            'Education Level (comment)',
            'Education (*)',
            'Institute/University',
            'Major',
            'Specialty',
            'Degree/Education completion Year',
            'Degree-Current Year calculation',
            'Certification (if Any)',
            'Skilled / Unskilled',
            'Currently Employed / Unemployed',
            'Work Experience (yes/No)',
            'Total Experience (Years)',
            'Year of Experience Group',
            'Current position',
            'Current Employer',
            'Experience with current employer (years)',
            'Category (Junior/Middle or Senior Profile)',
            'Date of Birth',
            'Age',
            'Age Group',
            'Data Source',
            'Data Added on'

        ]
###############################################################
        df=df_m
        for col in custom_order:
            if col not in df.columns:
                df[col] = ""
##############################################################
        #df1 = pd.read_excel(file1_path)
        df_final=df[custom_order]
        df_final.to_excel(output_path, index=False)

        # Print the path for Node to stream
        print(output_path)
    except Exception as e:
        # Send error to stderr and fail
        import sys as _sys
        print(str(e), file=_sys.stderr)
        _sys.exit(1)


