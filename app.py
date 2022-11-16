from flask import Flask
from flask import render_template,request,url_for
from datetime import datetime
import pymysql
import pymysql.cursors
import traceback
from flask import request
import os
import time
import logging
from pathlib import Path
from dotenv import load_dotenv
import boto3
import logging
import pandas as pd


app=Flask(__name__)
app.config['SECRET_KEY'] = '123'

logging.basicConfig(filename='record.log', level=logging.DEBUG, format=f'%(asctime)s %(levelname)s %(name)s %(threadName)s : %(message)s')


@app.errorhandler(404)
def page_not_found(error):
    return 'This route does not exist {}'.format(request.url), 404


         
# function for setting DB cvonnection 
def setup_connection():
    user = 'admin'
    DB_PASSWORD = 'HeyMultilex9087'
    DB_PORT = 3306
    passw = DB_PASSWORD
    host =  'multilex-db.csgyi8splofr.ap-south-1.rds.amazonaws.com'
    port = DB_PORT
    database = 'preipo'
    conn = pymysql.connect(host=host,port=port,user=user,passwd=passw,db=database,cursorclass = pymysql.cursors.DictCursor)
    return conn

#function for getting table record 
def get_table_record(table_name,rowid):
    conn = setup_connection()
    row12=rowid[0]
    row22=rowid[1]
    cur = conn.cursor()
    cur.execute(f"SELECT * FROM {table_name} where id in (%s,%s)", (row12,row22))
    data = cur.fetchall()

    cur.close()
    conn.close()
    app.logger.info(data)
    return data


@app.route("/",methods=['GET','POST'])
def home():
    msg="Welcome to PREIPO Management Portal!!"
    return render_template("index1.html", msg=msg)


#function to add new News Source
@app.route("/insert_into_table/<table_name>",methods=['GET','POST'])
def insert_into_table(table_name):
    msg="Welcome!! Please add the News Source details!!"

    comment=None
    if request.method=="GET":
        return render_template("insert_new_record.html", msg=msg)

    elif request.method == 'POST' and 'name1' in request.form and 'present' in request.form :
        name1 = request.form['name1']
        present = request.form['present']
        comment=request.form['comment']
        app.logger.info("name1   ",name1)
        app.logger.info("present  ",present)
        app.logger.info("comment ",comment)
        try:

            conn = setup_connection()
            app.logger.info("connected")
            cur = conn.cursor()
            cur.execute('insert into News_source (name,present,comment) values (%s,%s,%s)',(name1,present,comment,))
            cur.close()
            conn.commit()
            conn.close()
            msg="Record has been inserted successfully"
            return render_template("insert_new_record.html", msg=msg)
        except:
            traceback.print_exc()


#function for retrieving new source with present 0 - not started 
@app.route("/get_source_not_started/<table_name>",methods=['GET','POST'])
def get_source_not_started(table_name):
    msg="Here are two Randomly chosen News source which are not scraped yet !!"
    try:
        conn = setup_connection()
        app.logger.info("connected")
        cur = conn.cursor()
        cur.execute(f"SELECT * FROM {table_name} where present=0  ORDER BY RAND ( ) LIMIT 2")
        data = cur.fetchall()
        cur.close()
        conn.close()
        return render_template("index.html", data = data,msg=msg)
    except:
        traceback.print_exc()
        
#function for retrieving new source with present 1 - in  Progress 
  
@app.route("/get_source_in_progress/<table_name>",methods=['GET','POST'])
def get_source_in_progress(table_name):
    msg="Welcome!! You are viewing report for the  News Sources for which Scraping is in Progress!!"

    try:
        conn = setup_connection()
        app.logger.info("connected")
        cur = conn.cursor()
        cur.execute(f"SELECT * FROM {table_name} where present=1")
        data = cur.fetchall()
        cur.close()
        conn.close()
        return render_template("in_progress.html", data = data,msg=msg)
    except:
        traceback.print_exc()
        
        
#function to be called for updating record 

@app.route("/update_table/<table_name>",methods=['GET','POST'])

def update_table(table_name): 
        msg=" Record Update status"
        try:        
            present=request.form.getlist('present[]')
            comment=request.form.getlist('comment[]')
            rowid=request.form.getlist('rowid[]')
            app.logger.info(rowid)
            table_name='News_source'

            conn = setup_connection()

            app.logger.info("connected update")
            
            for i in range (2):
                present1=present[i]
                row1=rowid[i]
                comment1=comment[i] or None
                cursor=conn.cursor()
                cursor.execute("UPDATE News_source SET present=%s,comment=%s WHERE id=%s" , (present1, comment1,row1))
                conn.commit()
                cursor.close()
                
            conn.close()
            msg1="You have successfully updated the records!"
            msg=msg1  
                                   
        except:
            traceback.print_exc()
        
        if(msg==msg1):
            data1=get_table_record(table_name,rowid)
            return render_template("update1.html", msg=msg,data=data1)


        return render_template("update1.html", msg=msg)


@app.route("/get_completed_sources/<table_name>")
def get_completed_sources(table_name):
    msg="You are viewing the report for News Sources fow which scraping is completed"
    try:
        conn=setup_connection()
        app.logger.info("Connected")
        cur=conn.cursor()
        cur.execute(f"SELECT * FROM {table_name} WHERE present=2")
        data=cur.fetchall()
        cur.close()
        conn.close()
        return render_template("get_completed_news_sources.html",data=data,msg=msg)
    except:
        traceback.print_exc()


#upload final file to DB -Vishwajeets function

def upload_file_to_db(df,company):

        #connect to DB 
        conn =setup_connection()
        cursor = conn.cursor()
        err_rows = []
        # read each rows of xls
        df = df[df['Companies'].str.contains(company)]
        app.logger.info("df in table ",df)
        for i,row in df.iterrows():
            #if company is not already updated in Multilex table 
            
            try:
                row["publish_date"] = str(row["publish_date"])
                scraped_date=str(row["scraped_date"])

                if(len(str(row["text"]))>5000):
                    row["text"] = row["text"][0:5000]
                if(len(str(row["Companies"]))>100):
                    row["Companies"] = row["Companies"][0:100]
                
                if(str(row["publish_date"]) )== scraped_date:
                    if(len(re.findall(r'\d{1,2}/\d{1,2}/\d{4}',row["scraped_date"]))):
                        i = re.findall(r'\d{1,2}/\d{1,2}/\d{4}',row["scraped_date"])[0]
                        i = i.split("/")
                        temp = i[0]
                        i[0] = i[1]
                        i[1] = temp
                        i[2] = "20"+str(i[2])
                        row["publish_date"] = "-".join(i)
                        
                    elif(len(re.findall(r'\d{4}-\d{1,2}-\d{1,2}',row["scraped_date"]))):
                        i = re.findall(r'\d{4}-\d{1,2}-\d{1,2}',row["scraped_date"])[0]
                        row["publish_date"] = i
                if(len(re.findall(r'\d{1,2}-\d{1,2}-\d{4}',row["publish_date"]))):
                        i = re.findall(r'\d{1,2}-\d{1,2}-\d{4}',row["publish_date"])[0]
                        i = "-".join(i.split("-")[::-1])
                        row["publish_date"] = i
                

                #print("row", row)
                # get the news source name from link column
                source = str(get_source(row["link"]))
                app.logger.info("news_source",source)
                app.logger.info("news source")
                app.logger.info(source)



                #check in news_sourec table for the entry of this news_source , if it is not present , add the same 
                try:
                    dta=None
                    sid=""
                    cursor.execute("select id from News_source where name = %s",(source,))
                    dta = cursor.fetchone()
                    app.logger.info("inside select id ", str(dta))
                    if (dta !=None):
                        sid =dta['id']
                        app.logger.info("sid exists in News_source table", sid)
                    elif (dta==None):
                        try:
                            sql_insert="INSERT INTO News_source(name)  VALUES("+"'"+source +"')"
                            app.logger.info("sql_ins ......", sql_insert)
                            cursor.execute(sql_insert)
                            conn.commit()
                            time.sleep(3)
                        except:
                            app.logger.info("sql insert in news_source failed")
                        #cursor.execute("INSERT INTO news_source(name) VALUES(%s)",(source,))
                        #print("source inserted in News_source table ")
                        try:
                            dta1=None
                            cursor.execute("select id from News_source where name = %s",(source,))
                            dta1 = cursor.fetchone()
                        
                            if( dta1 !=None):
                                sid =dta1['id']
                                app.logger.info("news source is now inserted in news_source table", sid)
                        except:
                            app.logger.info("insert into News_source is not done properly")
                except:
                    traceback.print_exc()
                    app.logger.info("source is not present in News_source table ")
                    
                    

                data = [str(row["publish_date"]),str(row["scraped_date"]),str(row["title"]),
                str(row["text"]),str(row["Companies"]),str(row["Country"]),str(row["link"]),str(row["Comments"]),str(row["update"]),sid]
                #adding row into multilex table 
                app.logger.info("data",data)
                app.logger.info("data to be inserted in multilex table")
                app.logger.info(data)
                try:
                    sql = "INSERT INTO Multilex(publish_date,scraped_date,title,text,Companies,Country,link,Comments,Update_news,source_name) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"

                    cursor.execute(sql,data)
                    app.logger.info(" data inserted successfully in multilex table")
                    app.logger.info("data inserted successfully in multilex table")
                except:
                    app.logger.info("inserting data in multilex table failed")
            except:
                err_rows.append(str(i) + " " + str(row["publish_date"]) + "  " + str(row["Companies"]))
                app.logger.info("inside except")
        textfile = open("error_rows.txt","a")
        for val,i in enumerate(err_rows):
            try:
                textfile.write(str(i))
                textfile.write("\n")
            except:
                app.logger.info(val)
        # print(err_rows)
        textfile.close()
        cursor.close()
        conn.commit()

def update_xls_if_company_exists_and_updatedb(filename):    
    try:
            #read the data from PreIPO file
            #file1=app.config['UPLOAD_FOLDER']+filename

            app.logger.info("file1", filename)
            df=pd.read_excel(filename)
            # remove blank space from left hand side of all columns
            df['Companies']=df['Companies'].str.strip()
            #print("df1 after removing space from left",df)
            #get company list
            company_list=df['Companies'].tolist()
            app.logger.info("company list")
            app.logger.info(company_list)
            app.logger.info("company list",company_list)
            conn=setup_connection()
            table_name="Multilex"
            #itearte over compamny list and check for entry in multilex table for each company
            for company in company_list:
                if (pd.isnull(company )== False):
                    app.logger.info("company",company)
                    app.logger.info("company")
                    app.logger.info(company)
                    company=company.lstrip()
                    cur=conn.cursor()
                    cur.execute(f"SELECT * FROM {table_name} WHERE Companies=%s",(company,))
                    data=cur.fetchone()
                    if data!= None:
                        #print("data",data)
                        #is comany entry is already present in the multilex table , update xls with the word 'update'
                        df.loc[df.Companies==company,'update']="Update"
                        #print("updated")
                        df.to_excel(filename,index=False)

                    else:
                        # upload the records  to DB  for which company entry is not present in the Multilex table
                        upload_file_to_db(df,company)


            cur.close()
            conn.close()
    except:
            traceback.print_exc()

#Vishwajeet's function 
def get_source(i):
  try:
    first = i.split("/")[2]
    if first.startswith("www"):
      return "".join(first.split(".")[1:-1])
    else:
      return "".join(first.split(".")[0:-1])
  except:
    return "#"



@app.route("/copy_file",methods=['GET','POST'])
def copy_file():
    try:
        app.logger.info("inside.....")
        msg="Select the file to Upload to s3 bucket  under folder Partiallycleanedipofile "
        return render_template("files_link_upload.html",msg=msg)
    except:
        traceback.print_exc()



def sendmail(filename,sender_email,receiver_email,recv_mail_bcc,mail_subject,mail_text):
    import smtplib,ssl,email
    from email import encoders
    from email.mime.base import MIMEBase
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText
    
    app.logger.info("inside sendmail .....")
    app.logger.info("filepath    .......", filename) 
    port=465

    #subject = "IPO file "
    body=mail_text
    #body = " please find final IPO file attached"
    smtp_server="smtp.gmail.com"
    #sender_email="Multilex123@gmail.com"
    #receiver_email = ['vishwajeethogale307@gmail.com', 'sharikavallambatla@gmail.com','shashank.gurunaga@gmail.com','sharikavallambatlapes@gmail.com']

    #receiver_email="suparna.gurunaga@gmail.com"
    #recv_mail_bcc="shashank.gurunaga@gmail.com"
    password="koknaikeqibharxi"
    
    
    # Create a multipart message and set headers
    message = MIMEMultipart()
    message["From"] = sender_email
    #message["To"] = receiver_email
    message['To'] = str(", ".join(receiver_email))
    message["Subject"] = mail_subject
    message["Bcc"] = recv_mail_bcc  

    # Add body to email
    message.attach(MIMEText(body, "plain"))

    #filename = "document.pdf"  # In same directory as script

    # Open PDF file in binary mode
    with open(filename, "rb") as attachment:
        # Add file as application/octet-stream
        # Email client can usually download this automatically as attachment
        part = MIMEBase("application", "octet-stream")
        part.set_payload(attachment.read())

    # Encode file in ASCII characters to send by email    
    encoders.encode_base64(part)

    # Add header as key/value pair to attachment part
    part.add_header(
        "Content-Disposition",
        f"attachment; filename= {filename}",
    )

    # Add attachment to message and convert message to string
    message.attach(part)
    text = message.as_string()
    
    context = ssl.create_default_context()
    with smtplib.SMTP_SSL("smtp.gmail.com", port, context=context) as server:
        server.login(sender_email, password)
        server.sendmail(sender_email, receiver_email, text)

    app.logger.info("end of sendmail ")


#function to upload to s3 bucket 
def s3bucketcopy(local_file,bucket_name,s3_file):
    import boto3
    from botocore.exceptions import NoCredentialsError

    # read  s3 bucket connection data from .env file 
    env_path = Path('.', '.env')
    load_dotenv(dotenv_path=env_path)


    client = boto3.client(
    's3',
    aws_access_key_id = os.getenv('aws_access_key_id'),
    aws_secret_access_key = os.getenv('aws_secret_access_key'),
    region_name = os.getenv('region_name')
    )

    # Fetch the list of existing buckets
    clientResponse = client.list_buckets()

    # Print the bucket names one by one
    app.logger.info('Printing bucket names...')
    for bucket in clientResponse['Buckets']:
        app.logger.info(f'Bucket Name: {bucket["Name"]}')
       

   
    try:
        client.upload_file(local_file, bucket_name, s3_file)
        app.logger.info("Upload Successful")
        return True
    except FileNotFoundError:
        app.logger.info("The file was not found")
        return False
    except NoCredentialsError:
        app.logger.info("Credentials not available")
        return False
  

@app.route("/copy1", methods=['GET','POST'])
def copy1():
    try:
        if request.method == 'POST':
       
            file = request.files.get('file')
            
            # get file name selected by user .eg a.xls 
            filename1=file.filename
            
            
            
            # update the final cleaned  xls in AWS project directory under test sub folder 
            folder=os.getcwd()

            upload_folder= os.path.join(folder,'test')
            #app.config['UPLOAD_FOLDER'] = upload_folder        
             
            file.save(os.path.join(upload_folder,filename1))
            app.logger.info("File copy successful !!!")                
  
            #copy file to S3 bucket from test directory 
            # if copy to S3 bucket is successful , remobve it from the project directory
            local_file =os.path.join(upload_folder, filename1)

            app.logger.info("local file ", local_file) 


            bucket_name="multilex"
            s3_file="Partiallycleanedipofile/"+filename1

            s3bucketcopy(local_file, bucket_name, s3_file)



            #send mail
            #filename_path=os.path.join(upload_folder,filename1)
            sender_email="Multilex123@gmail.com"
            receiver_email = ['vishwajeethogale307@gmail.com', 'sharikavallambatla@gmail.com','shashank.gurunaga@gmail.com','sharikavallambatlapes@gmail.com']

            #receiver_email="suparna.gurunaga@gmail.com"
            recv_mail_bcc="shashank.gurunaga@gmail.com"
            mail_subject="Today's Final , cleaned PREIPO is report attached and also uploaded to s3 bucket "
            mail_text="Today's final , cleaned PREIPO is report attached and also uploaded to s3 bucket"

            sendmail(local_file,sender_email,receiver_email,recv_mail_bcc,mail_subject,mail_text)



             # Requirement 1 :for each row of xls , read company name , if company name is present in multilex table , update the 'update'column in Sharika's xls as "update"
            #Requiremet 2 : upload xls data to DB
                # iterate through each row of xls
                # get the news_source name by processing the 'link' column text - using existing module ( get_spurce function)
                # check in news source table if teh news source which is taken from link of xls , is already present in the news_source table
                # if the news_source is present in teh news_source table , then get the id
                # if news_source is not present in the news_source table , , then insert this news_source and get the coreesponding id
                # after we get the id , we are insert the corresponding row of xls for that newssource in the multilex table using the above newsource id as foreign key
                # the process is repeated for each row/record in Sharika's  xls
            #also update each record of xls in multilex table with newsource id as foriegn key

            #upload xls content to multilex database 
            update_xls_if_company_exists_and_updatedb(local_file)

            msg="file has been saved s3 bucket and multilex table has been updated successfully"
            return render_template("files_link_upload.html",msg=msg)

    except: 
        traceback.print_exc()



@app.route("/copy_file_ipo",methods=['GET','POST'])
def copy_file_ipo():
    try:
        msg="Select the file to Upload in S3 bucket  under Uncleanedipofile folder"
        return render_template("upload_daily_preipo.html",msg=msg)
    except:
        traceback.print_exc()


@app.route("/copy1_ipo", methods=['GET','POST'])
def copy1_ipo():
    try:
        if request.method == 'POST':

            file = request.files.get('file')

            # get file name selected by user .eg a.xls 
            filename1=file.filename



            # update the final cleaned  xls in AWS project directory under test sub folder 
            folder=os.getcwd()

            upload_folder= os.path.join(folder,'test')

            file.save(os.path.join(upload_folder,filename1))
            app.logger.info("File copy successful !!!")
  
            #copy file to S3 bucket from test directory 
            # if copy to S3 bucket is successful , remobve it from the project directory
            local_file =os.path.join(upload_folder, filename1)

            app.logger.info("local file ", local_file)


            bucket_name="multilex"
            s3_file="Uncleanedipofile/"+filename1




            s3bucketcopy(local_file, bucket_name, s3_file)

             #send mail
            receiver_email = ['sharikavallambatla@gmail.com','shashank.gurunaga@gmail.com','sharikavallambatlapes@gmail.com','maheshpanwar351@gmail.com']
            sender_email="Multilex123@gmail.com"
            #receiver_email = ['gurunaga@gmail.com', 'suparna.gurunaga@gmail.com','shashank.gurunaga@gmail.com','sharikavallambatlapes@gmail.com']

            recv_mail_bcc="shashank.gurunaga@gmail.com"

            mail_subject="Today's PREIPO is report attached for cleaning and also uploaded to s3 bucket"
            mail_text="Today's PREIPO is report attached for cleaning and also uploaded to s3 bucket"

            sendmail(local_file,sender_email,receiver_email,recv_mail_bcc,mail_subject,mail_text)

            msg="file has been saved s3 bucket and mail has been sent "
            return render_template("upload_daily_preipo.html",msg=msg)

    except:
        traceback.print_exc()

#list files of 3s3 bucket
def list_files(bucket,prefix):

     # read  s3 bucket connection data from .env file 
    env_path = Path('.', '.env')
    load_dotenv(dotenv_path=env_path)


    client = boto3.client(
        's3',
        aws_access_key_id = os.getenv('aws_access_key_id'),
        aws_secret_access_key = os.getenv('aws_secret_access_key'),
        region_name = os.getenv('region_name')
        )


    response = client.list_objects_v2(
        Bucket=bucket,
        Prefix=prefix)
    
    contents = []
    for content in response.get('Contents', []):
        # 	it comes in form of Uncleanedipofile/PREIPO_Final_Report_2022-10-20.csv
        list1=[]
        list1=content['Key'].split("/")
        if len(list1[1]) != 0:
            contents.append(list1[1])
    return contents


@app.route("/storage")
def storage():
    contents = list_files("multilex","Uncleanedipofile/")

    return render_template('storage.html', contents=contents)

@app.route("/download_file/<file_name>", methods=['GET', 'POST'])

def download_file(file_name):
    if request.method == 'POST'   :
        bucket="multilex"
       
        # read  s3 bucket connection data from .env file 
        env_path = Path('.', '.env')
        load_dotenv(dotenv_path=env_path)



        client = boto3.client(
        's3',
        aws_access_key_id = os.getenv('aws_access_key_id'),
        aws_secret_access_key = os.getenv('aws_secret_access_key'),
        region_name = os.getenv('region_name')
        )
        
        # get current server directory
        folder=os.getcwd()

        download_folder= os.path.join(folder,'test')
        output1= os.path.join(download_folder,file_name)

        # download from s3 bucket to test folder in project directory

        file_name1="Uncleanedipofile/"+file_name
        client.download_file(Bucket=bucket,Key=file_name1,Filename=output1) 
        
        # download as attachment in browser  
        from flask import send_file
        return send_file(output1,as_attachment=True)



@app.route("/copy_file_ipo_first",methods=['GET','POST'])
def copy_file_ipo_first():
    try:
        app.logger.info("inside.....")
        msg="Select the file to Upload in S3 bucket under Firstcleanedipofile folder to validate"
        return render_template("first_cleaned_report.html",msg=msg)
    except:
        traceback.print_exc()


@app.route("/copy1_ipo_first", methods=['GET','POST'])
def copy1_ipo_first():
    try:
        if request.method == 'POST':

            file = request.files.get('file')

            # get file name selected by user .eg a.xls 
            filename1=file.filename

            # update the final cleaned  xls in AWS project directory under test sub folder 
            folder=os.getcwd()

            upload_folder= os.path.join(folder,'test')

            file.save(os.path.join(upload_folder,filename1))
            app.logger.info("File copy successful !!!")
            local_file =os.path.join(upload_folder, filename1)
            app.logger.info("local file ", local_file)

            bucket_name="multilex"
            s3_file="Firstcleanedipofile/"+filename1


            s3bucketcopy(local_file, bucket_name, s3_file)
            msg="File has been uploaded to  s3 bucket for validation "
            return render_template("first_cleaned_report.html",msg=msg)

    except:
        traceback.print_exc()


def check_for_companyname_in_multilex(company_name1):
    conn =setup_connection()
    cursor = conn.cursor()
    try:
                    dta=None
                    cursor.execute("select companies from Multilex where companies = %s",(company_name1,))
                    dta = cursor.fetchone()
                    app.logger.info("inside select company ", str(dta))
                    if (dta !=None):
                        company1 =dta['companies']
                        app.logger.info("company exists in Multilex table", company1)
                        return company1
                    return None

    except:
         app.logger.info("select company from multilex table is not done properly")


@app.route("/validate_file_ipo_first",methods=['GET','POST'])
def validate_file_ipo_first():
    #display the list of files
    contents = list_files("multilex","Firstcleanedipofile/")

    return render_template('storage_validate.html', contents=contents)

    #there should be only one file in th firstipo folder in s3 bucket . once it is processed , it should move to second  folder in S3 bucket 
    # aspart of thevalidation - read the xls and then compare each row with multilex table company name , if matches delete the row
    #after delete, update the final xls


@app.route("/validate_file/<file_name>", methods=['GET', 'POST'])
def validate_file(file_name):
    import io
    from io import BytesIO
    

    if request.method == 'POST'   :
        bucket="multilex"

        # read  s3 bucket connection data from .env file
        env_path = Path('.', '.env')
        load_dotenv(dotenv_path=env_path)



        client = boto3.client(
        's3',
        aws_access_key_id = os.getenv('aws_access_key_id'),
        aws_secret_access_key = os.getenv('aws_secret_access_key'),
        region_name = os.getenv('region_name')
        )

        # get current server directory
        folder=os.getcwd()
        app.logger.info('folder info %s',folder)

        # get file to validate from s3 bucket
        file_name1="Firstcleanedipofile/"+file_name
        try:
            obj= client.get_object(Bucket=bucket,Key=file_name1)
            # read teh file  in panda dataframe
            df_1 = pd.read_excel(io.BytesIO(obj['Body'].read()))
            app.logger.info('df initial info %s',df_1)
            #df_3=pd.DataFrame()
            #df_3=df_1
            # list to hold the indexes to drop 
            lst_index = [] 

            #for each row in xls 
            for i in range(len(df_1)):
                if (df_1.loc[i, "update"] != 'Update'):
                    #read comany name 
                    #company_name1=df_1.loc[i, "Issuer Name"]
                    company_name1=df_1.loc[i, "Companies"]
                    app.logger.info(company_name1)
                    #function to match with database  for the company name and get the company name from database table 
                    company1=check_for_companyname_in_multilex(company_name1)
                    # if company name exists in database 
                    if (company1 != None) :
                        # srtip the leading and trailing space
                        company1=company1.strip()
                        # get the database company name to lower case to compare with dafarame company name value 
                        company1=str(company1).lower()
                        # get the df company name to lower case
                        company_df=df_1['Companies'][i]
                        #company_df=df_1['Issuer Name'][i]
                        company_df=str(company_df).lower()
                        app.logger.info( "company1", company1)

                        app.logger.info("company_df", company_df)
                        # compare databse company name with dataframe company name 
                        if ( company1== company_df):
                            # if they match , put the index value in lst_index
                            lst_index.append(i)
                            app.logger.info("lst_index",lst_index)
            # drop the corresponding rows from th edataframe with matching company name in multilex       
            app.logger.info("lst_index_final", lst_index)
            df_1.drop(index=lst_index, inplace = True)
            app.logger.info( "final df_1", df_1['Companies'])
            #app.logger.info( "final df_1", df_1['Issuer Name'])
            #save the dataframe after dropping rows to excel and upload to s3 bucket subfolder secondcleanedipofile 
            bucket_name="multilex"
            s3_file="Secondcleanedipofile/"+file_name
            df_1.to_excel(file_name)

            s3bucketcopy(file_name, bucket_name, s3_file)


            # remove the file from firstcleanedreport folder in s3 bucket
            client.delete_object(Bucket=bucket_name, Key=file_name1)            
            app.logger.info('delete file from s3 bucket is done')

            #delete the xls from project folder which was created by dataframe to_excel method
            os.remove(file_name)

            #display message 
            msg="Processed file has been saved s3 bucket undeer Secondcleanedipofile folder"
            return render_template("storage_validate.html",msg=msg)

        except Exception as e:
            app.logger.info(e)


    
@app.route("/storage_validated")
def storage_validated():
    contents = list_files("multilex","Secondcleanedipofile/")

    return render_template('storage_validated.html', contents=contents)
    

@app.route("/download_file_validated/<file_name>", methods=['GET', 'POST'])

def download_file_validated(file_name):
    if request.method == 'POST'   :
        bucket="multilex"

        # read  s3 bucket connection data from .env file 
        env_path = Path('.', '.env')
        load_dotenv(dotenv_path=env_path)



        client = boto3.client(
        's3',
        aws_access_key_id = os.getenv('aws_access_key_id'),
        aws_secret_access_key = os.getenv('aws_secret_access_key'),
        region_name = os.getenv('region_name')
        )

        # get current server directory
        folder=os.getcwd()

        download_folder= os.path.join(folder,'test')
        output1= os.path.join(download_folder,file_name)

        # download from s3 bucket to test folder in project directory

        file_name1="Secondcleanedipofile/"+file_name
        client.download_file(Bucket=bucket,Key=file_name1,Filename=output1)

        # download as attachment in browser  
        from flask import send_file
        return send_file(output1,as_attachment=True)    
