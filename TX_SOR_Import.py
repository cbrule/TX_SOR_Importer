import requests, zipfile, os, shutil, time, pypyodbc, smtplib, textwrap, mechanize, sys
from bs4 import BeautifulSoup
from contextlib import contextmanager
from subprocess import Popen
from email.mime.text import MIMEText
from requests import Session

localDir = ""
fileName = localDir + "/TX_SOR.zip"
URL="https://records.txdps.state.tx.us/SexOffenderRegistry/Profile/Security/Login"
dlURL = "https://records.txdps.state.tx.us/SexOffenderRegistry/Home/Export/DownloadFile?exportcode=DATA-CURRENT"
headers={"User-Agent":"Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36"}
username=""
password=""

def sendMail(SUBJECT, msg):
	TO = ""
	FROM = ""

	msg = MIMEText(msg)
	msg['Subject'] = SUBJECT
	msg['To'] = TO
	msg['From'] = FROM

	server = smtplib.SMTP('mail.org.com')
	server.sendmail(FROM, TO, msg.as_string())
	server.quit()

def write_file(txt):
    file_name = localDir + "TX_SOR_log.txt"
    with open(file_name, 'a') as logfile:
        logfile.write(str(time.ctime()) + '\n' + txt + '\n\n')

def downloadFile():
	s = Session()
	values = {'UserName': username, 'Password': password}
	s.post(URL, data=values)

	file = s.get(dlURL)

	if file.status_code == 200:
	    with open(fileName, 'wb') as f:
	        f.write(file.content)

@contextmanager
def open_db_connection(connection_string, commit=False):
    try:
    	connection = pypyodbc.connect(connection_string)
    	cursor = connection.cursor()    
        yield cursor
    except pypyodbc.DatabaseError as err:
        write_file(err.message)
        print(err.message)
        cursor.execute("ROLLBACK")
        raise err
    else:
        if commit:
            cursor.execute("COMMIT")
        else:
            cursor.execute("ROLLBACK")
    finally:
        connection.close()

try:
	#delete old files
	if os.path.exists(localDir + "TX_SOR/"):
		shutil.rmtree(localDir + "TX_SOR/")
		write_file(localDir + "TX_SOR/" + " contents deleted.")
	else:
		write_file("Attempted to delete contents of " + localDir + "TX_SOR/" + " but an error occurred.")

	#remake folder
	os.makedirs(localDir + "TX_SOR/")
	
	#download file
	downloadFile()

	try:
		#unzip file
		zip = zipfile.ZipFile(fileName)
		zip.extractall(localDir + "TX_SOR/")
		write_file("TX_SOR data downloaded and unzipped.")
	except Exception as e:
		print e
		write_file("An error occurred while trying to update TX_SOR data. " + str(e))
		sendMail("Error on TX_SOR import", "Need to update user data.")
		sys.exit()

	#run stored proc to truncate tables in database
	with open_db_connection("DRIVER={ODBC Driver 11 for SQL Server};SERVER=servername\servername;DATABASE=dbName;Trusted_Connection=yes;", True) as cursor:
	    SQLCommand = ('[dbo].[TX_SOR_Truncate]')
	    cursor.execute(SQLCommand)
	write_file("TX_SOR tables truncated.")

	#run .bat file to load new data into database
	p = Popen("4_ImportData.bat", cwd=localDir, shell=True)
	stdout, stderr = p.communicate()
	write_file("Latest TX_SOR data loaded into database.")

except Exception as e:
	print e
	write_file("An error occurred while trying to update TX_SOR data. " + str(e))
	sendMail("Error on TX_SOR import", "An error occurred while trying to update TX_SOR data." + str(e))