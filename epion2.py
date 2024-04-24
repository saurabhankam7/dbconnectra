import mysql.connector
from datetime import datetime
# Establish connection to the destination database
dest_db_connection = mysql.connector.connect(
    user="root", password="rational", host="194.233.80.131", database="cashlessai"
)

# Check if the connection to the source database is successful
if dest_db_connection.is_connected():
    print("Connection to destination database created")
else:
    print("Connection to destination database failed")
    exit()  # Exit the script if connection failed

# Establish connection to MySQL server
conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="rational",
    database="epion"
)

# Create a cursor object to execute SQL queries
cursor = conn.cursor()
cursor1 = dest_db_connection.cursor()

cutoff_date = datetime(2024, 3, 3).strftime('%Y-%m-%d')

# Define the select query
select_query = "SELECT adm.PatientID, adm.ADMNo, adm.ClientID, adm.LocationID, adm.DepartmentName, adm.BedName, adm.ADMDateTime, adm.AdmittingDocName, adm.MLCNotes, adm.Status, adm.IsDischarge, adm.Activate, reg.PatientName, reg.Address, reg.City, reg.GSM1, reg.ADHARNo, reg.PANNo, reg.DOB, reg.AgeYear, reg.AgeMonth, reg.AgeDays, reg.Gender, ClaimFormNo, PolicyDetails, PolicyStartDate, PolicyEndDate, WardName, SponsorName, SponsorTypeName, ClaimFormNo, CoPayment, StaffNo, MembershipID FROM trnregistration reg INNER JOIN trnadmission adm ON reg.PatientID = adm.PatientID INNER JOIN trnsponsor ON trnsponsor.OpIpID = adm.AdmissionID AND trnsponsor.OpIpFlag = 0;"

# Execute the select query
cursor.execute(select_query)

# Fetch all the rows
rows = cursor.fetchall()

# Print the rows and check if values are present in test.trnadmission table's ThirdPartyAdmissionID column
for row in rows:
    print(row[0])  # Assuming the value you want to check is in the first column
    # Check if the value exists in test.trnadmission table's ThirdPartyAdmissionID column
    tAdmissionID=row[0]
   
    check_query = "SELECT AdmissionID FROM trnadmission WHERE ThirdPartyAdmissionID = '" + str(tAdmissionID) + "' AND Activate = 1 AND ADMDateTime > '" + cutoff_date + "'"
    print(check_query)
    cursor1.execute(check_query)
    #result = cursor1.fetchone()
    result = cursor1.fetchall()
    print(result)
    if result:
        print("Value exists in test.trnadmission table's ThirdPartyAdmissionID column")
    else:
        insert_query = """INSERT INTO trnadmission 
    (UHID, ADMNo, ADMDateTime, PatientName, Gender, GSM1, DOB, AgeYear, AgeMonth, AgeDay, Address, City, AdmittingDocName, Department, WardName, RoomName, BedName, SponsorType, SponsorName, PolicyStartDate, PolicyEndDate, PolicyDetails, ThirdPartyAdmissionID, ADHARNo, PANNo, DoctorID1, DoctorID2, AdmittingDocName1, MLCNotes, Status, Remark, IsDischarge, DischargeDate, DischargeReason, ClaimFormNo, CoPayment, StaffNoOrEmployeeID, MembershipID, UserName, EStatus, CashlessTAT, ClientID, LocationID, Activate)
    VALUES 
    (%(UHID)s, %(ADMNo)s, %(ADMDateTime)s, %(PatientName)s, %(Gender)s, %(GSM1)s, %(DOB)s, 
    %(AgeYear)s, %(AgeMonth)s, %(AgeDay)s, %(Address)s, %(City)s, %(AdmittingDocName)s, %(Department)s, 
    %(WardName)s, %(RoomName)s, %(BedName)s, %(SponsorType)s, %(SponsorName)s, %(PolicyStartDate)s, 
    %(PolicyEndDate)s, %(PolicyDetails)s, %(ThirdPartyAdmissionID)s, %(ADHARNo)s, %(PANNo)s,
    %(DoctorID1)s, %(DoctorID2)s, %(AdmittingDocName1)s, %(MLCNotes)s, %(Status)s, %(Remark)s, 
    %(IsDischarge)s, %(DischargeDate)s, %(DischargeReason)s, %(ClaimFormNo)s, %(CoPayment)s, 
    %(StaffNoOrEmployeeID)s, %(MembershipID)s, 
    %(UserName)s, %(EStatus)s, %(CashlessTAT)s, %(ClientID)s, %(LocationID)s, %(Activate)s
        )"""

        # Define the values to be inserted
        data = {
            "UHID": 0,   
            "ADMNo": row[1],
            "ADMDateTime": row[6],   
            "PatientName": row[12],   
            "Gender": row[22],   
            "GSM1": row[15],   
            "DOB": row[18],   
            "AgeYear":  row[19],   
            "AgeMonth": row[20],   
            "AgeDay": row[21],   
            "Address": row[13],   
            "City": row[14],   
            "AdmittingDocName": row[7],
            "Department": row[4],
            "WardName": row[27],
            "RoomName": "",   
            "BedName": row[5],
            "SponsorType": row[29],   
            "SponsorName": row[28],   
            "PolicyStartDate": row[25],   
            "PolicyEndDate": row[26],   
            "PolicyDetails": row[24],   
            "ThirdPartyAdmissionID": row[0],
            "ADHARNo": row[16],   
            "PANNo": row[17],    
            "DoctorID1": 0,   
            "DoctorID2": 0,   
            "AdmittingDocName1": "",
            "MLCNotes": row[8],
            "Status": row[9],
            "Remark": "",   
            "IsDischarge": row[10],
            "DischargeDate": "2024-04-23",   
            "DischargeReason": "",   
            "ClaimFormNo": row[23],   
            "CoPayment": 0,   
            "StaffNoOrEmployeeID": row[31],   
            "MembershipID": row[32],   
            "UserName": "",   
            "EStatus": "",   
            "CashlessTAT": 0,   
            "ClientID": row[2],
            "LocationID": row[3],
            "Activate": row[11]
        }

        # Execute the query to insert data into the trnadmission table
        cursor1.execute(insert_query, data)
        dest_db_connection.commit()



        print("Value does not exist in test.trnadmission table's ThirdPartyAdmissionID column")

# Close the cursor and connection
cursor.close()
conn.close()

cursor1.close()
dest_db_connection.close()


# 'ECHS - (EX-SERVICEMEN CONTRIBUTORY HEALTH SCHEME)'