import mysql.connector
dest_db_connection = mysql.connector.connect(
    user="root", password="rational", host="194.233.80.131", database="test"
)

# Check if the connection to the source database is successful
if dest_db_connection.is_connected():
    print("Connection to source database created")
else:
    print("Connection to source database failed")
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

select_query="SELECT * FROM trnadmission where date(ADMDateTime) >= '2024-04-23'"


cursor.execute(select_query)

rows = cursor.fetchall()

# Print the rows
for row in rows:

    print(row[0])
    







# # Define the SQL query to insert data into the trnadmission table
# insert_query = """
# INSERT INTO trnadmission (
#     ADMNo, PatientID, ClientID, LocationID, DepartmentID,
#     DepartmentName, BedID, BedName, WardName, AdmittingDocID,
#     AdmittingDocName, AdmittingDocID1, AdmittingDocName1,
#     RefDoctorID, RefDoctorName, MedicoLegalCase, MLCNotes,
#     Status, AdmittedFor, Notes, RelativeDetails,
#     CommonTemplateID, DoctorDiscount, IsDischarge,
#     IsBillGenerated, IsPaymentMade, Activate
# ) VALUES (
#     %(ADMNo)s, %(PatientID)s, %(ClientID)s, %(LocationID)s, %(DepartmentID)s,
#     %(DepartmentName)s, %(BedID)s, %(BedName)s, %(WardName)s, %(AdmittingDocID)s,
#     %(AdmittingDocName)s, %(AdmittingDocID1)s, %(AdmittingDocName1)s,
#     %(RefDoctorID)s, %(RefDoctorName)s, %(MedicoLegalCase)s, %(MLCNotes)s,
#     %(Status)s, %(AdmittedFor)s, %(Notes)s, %(RelativeDetails)s,
#     %(CommonTemplateID)s, %(DoctorDiscount)s, %(IsDischarge)s,
#     %(IsBillGenerated)s, %(IsPaymentMade)s, %(Activate)s
# )
# """

# # Define the values to be inserted
# data = {
#     'ADMNo': 'EP/4495',
#     'PatientID': 170,
#     'ClientID': 2,
#     'LocationID': 2,
    # 'DepartmentID': 1,
#     'DepartmentName': 'NAESTHESIA',
#     'BedID': 96,
#     'BedName': 'GEN 414',
#     'WardName': 'GENERAL COMMON',
#     'AdmittingDocID': 279,
#     'AdmittingDocName': 'Dr. AKANKSHA  RATHOD',
#     'AdmittingDocID1': 0,
#     'AdmittingDocName1': '',
#     'RefDoctorID': 0,
#     'RefDoctorName': '',
#     'MedicoLegalCase': False,
#     'MLCNotes': '',
#     'Status': '',
#     'AdmittedFor': '',
#     'Notes': '',
#     'RelativeDetails': '',
#     'CommonTemplateID': 1,
#     'DoctorDiscount': 0.000,
#     'IsDischarge': False,
#     'IsBillGenerated': False,
#     'IsPaymentMade': False,
#     'Activate': True
# }

# # Execute the query to insert data into the trnadmission table
# cursor.execute(insert_query, data)

# Commit the transaction
conn.commit()

# Close the cursor and connection
cursor.close()
conn.close()
