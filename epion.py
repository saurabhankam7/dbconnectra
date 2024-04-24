import mysql.connector
import schedule
import time

def save_data_hourly():
    try:
        # Establish connection to the destination database
        dest_db_connection = mysql.connector.connect(
            user="root", password="rational", host="194.233.80.131", database="cashlessai"
        )

        # Check if the connection to the destination database is successful
        if dest_db_connection.is_connected():
            print("Connection to destination database created")
        else:
            print("Connection to destination database failed")
            return  # Return if connection failed

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

        # Define the select query
        select_query = """
        SELECT adm.PatientID, adm.ADMNo, adm.ClientID, adm.LocationID, adm.DepartmentName, adm.BedName, 
        adm.ADMDateTime, adm.AdmittingDocName, adm.MLCNotes, adm.Status, adm.IsDischarge, adm.Activate, 
        reg.PatientName, reg.Address, reg.City, reg.GSM1, reg.ADHARNo, reg.PANNo, reg.DOB, reg.AgeYear, 
        reg.AgeMonth, reg.AgeDays, reg.Gender, ClaimFormNo, PolicyDetails, PolicyStartDate, PolicyEndDate, 
        WardName, SponsorName, SponsorTypeName, ClaimFormNo, CoPayment, StaffNo, MembershipID 
        FROM trnregistration reg 
        INNER JOIN trnadmission adm ON reg.PatientID = adm.PatientID 
        INNER JOIN trnsponsor ON trnsponsor.OpIpID = adm.AdmissionID AND trnsponsor.OpIpFlag = 0;
        """

        # Execute the select query
        cursor.execute(select_query)

        # Fetch all the rows
        rows = cursor.fetchall()

        for row in rows:
            tAdmissionID = row[0]
            check_query = "SELECT AdmissionID FROM trnadmission WHERE ThirdPartyAdmissionID = %s AND Activate = 1"
            cursor1.execute(check_query, (tAdmissionID,))
            result = cursor1.fetchone()
            if not result:
                print(row[0])
                print(result)
                insert_query = """
                INSERT INTO trnadmission 
                (UHID, ADMNo, ADMDateTime, PatientName, Gender, GSM1, DOB, AgeYear, AgeMonth, AgeDays, Address, 
                City, AdmittingDocName, Department, WardName, RoomName, BedName, SponsorType, SponsorName, 
                PolicyStartDate, PolicyEndDate, PolicyDetails, ThirdPartyAdmissionID, ADHARNo, PANNo, DoctorID1, 
                DoctorID2, AdmittingDocName1, MLCNotes, Status, Remark, IsDischarge, DischargeDate, 
                DischargeReason, ClaimFormNo, CoPayment, StaffNoOrEmployeeID, MembershipID, UserName, EStatus, 
                CashlessTAT, ClientID, LocationID, Activate)
                VALUES 
                (0, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """

                # Define the values to be inserted
                data = {
                    "ADMNo": row[1],
                    "ADMDateTime": row[6],
                    "PatientName": row[12],
                    "Gender": row[22],
                    "GSM1": row[15],
                    "DOB": row[18],
                    "AgeYear": row[19],
                    "AgeMonth": row[20],
                    "AgeDays": row[21],
                    "Address": row[13],
                    "City": row[14],
                    "AdmittingDocName": row[7],
                    "Department": row[4],
                    "WardName": row[27],
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

        # Close the cursor and connection
        cursor.close()
        conn.close()
        cursor1.close()
        dest_db_connection.close()
        print("Data saved successfully (Hourly).")
    except Exception as e:
        print("Error occurred:", str(e))

def save_data_daily():
    # Function to save data daily at 8 PM
    print("Data saved successfully (Daily at 8 PM).")

# Schedule the job to run every hour
schedule.every().hour.do(save_data_hourly)

# Schedule the job to run daily at 8 PM
schedule.every().day.at("20:00").do(save_data_daily)

# Infinite loop to keep the script running
while True:
    schedule.run_pending()
    time.sleep(1)
