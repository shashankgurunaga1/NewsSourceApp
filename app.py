from flask import Flask,render_template,request,url_for
from datetime import datetime
import pymysql
import pymysql.cursors
import traceback
from flask import request
import os


app=Flask(__name__)
app.config['SECRET_KEY'] = '123'



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
    #print("row12 ", row12)
    #print("row22" , row22)
    cur = conn.cursor()
    cur.execute(f"SELECT * FROM {table_name} where id in (%s,%s)", (row12,row22))
    data = cur.fetchall()

    cur.close()
    conn.close()
    print(data)
    return data


@app.route("/",methods=['GET','POST'])
def home():
    msg="Welcome!! You are in News Source Scraping Page!!"
    return render_template("index1.html", msg=msg)


#function for retrieving new source with present 0 - not started 
@app.route("/get_source_not_started/<table_name>",methods=['GET','POST'])
def get_source_not_started(table_name):
    msg="Welcome!! You are in News Source Scraping  Not Started Page!!"
    try:
        conn = setup_connection()
        print("connected")
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
    msg="Welcome!! You are in News Source Scraping in Progress Page!!"

    try:
        conn = setup_connection()
        print("connected")
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
            print(rowid)
            table_name='News_source'

            conn = setup_connection()

            print("connected update")
            
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
    msg="Completed Records"
    try:
        conn=setup_connection()
        print("Connected")
        cur=conn.cursor()
        cur.execute(f"SELECT * FROM {table_name} WHERE present=2")
        data=cur.fetchall()
        cur.close()
        conn.close()
        return render_template("get_completed_news_sources.html",data=data,msg=msg)
    except:
        traceback.print_exc()


#upload final file to DB -Vishwajeets function

def upload_file_to_db(df):

        #df = pd.read_csv( file1)
        #connect to DB
        conn =setup_connection()
        cursor = conn.cursor()
        err_rows = []
        # read each rows of xls
        for i,row in df.iterrows():
            try:
                row["publish_date"] = str(row["publish_date"])

                if(len(str(row["text"]))>5000):
                    row["text"] = row["text"][0:5000]
                if(len(str(row["Companies"]))>100):
                    row["Companies"] = row["Companies"][0:100]
                '''
                if(row["publish_date"]) == "Date":
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
                '''

                #print("row", row)
                # get the news source name from link column
                source = str(get_source(row["link"]))
                print("news_source",source)


                #check in news_sourec table for the entry of this news_source , if it is not present , add the same
                try:
                    sid = cursor.execute("select id from news_source where name = %s",(source,))
                    dta = cursor.fetchone()
                    print("inside select id ", str(dta))
                    sid =dta['id']
                    if( dta !=None):
                        print("sid exists in News_source table", sid)
                    else:
                        cursor.execute("INSERT INTO news_source(name) VALUES(%s)",(source,))
                        print("source inserted in News_source table ")
                        cursor.execute("select id from news_source where name = %s",(source,))
                        dta = cursor.fetchone()
                        sid =dta['id']

                        if( dta !=None):
                            print("news source is now inserted in news_source table", sid)
                except:
                    traceback.print_exc()
                    print("source is not present in News_source table ")



                data = [str(row["publish_date"]),str(row["scraped_date"]),str(row["title"]),
                str(row["text"]),str(row["Companies"]),str(row["Country"]),str(row["link"]),str(row["Comments"]),str(row["update"]),sid]
                #adding row into multilex table
                print("data",data)
                try:
                    sql = "INSERT INTO multilex(publish_date,scraped_date,title,text,Companies,Country,link,Comments,Update_news,source_name) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"

                    cursor.execute(sql,data)
                    print(" data inserted successfully in multilex table")
                except:
                    print("inserting data in multilex table failed")
            except:
                err_rows.append(str(i) + " " + str(row["publish_date"]) + "  " + str(row["Companies"]))
                print("inside except")
        textfile = open("error_rows.txt","a")
        for val,i in enumerate(err_rows):
            try:
                textfile.write(str(i))
                textfile.write("\n")
            except:
                print(val)
        # print(err_rows)
        textfile.close()
        cursor.close()
        conn.commit()

def update_xls_if_company_exists_and_updatedb(filename):    
        try:

            # if matching IPO company record is present in Multilex table , update the xls with "update"   for the company
            #read the data from PreIPO file 
            df=pd.read_excel(filename)
            # remove blank space from left hand side of all columns
            df['Companies']=df['Companies'].str.strip()
            #print("df1 after removing space from lest",df)
            #get company list
            company_list=df['Companies'].tolist()
            print("company list",company_list)
            conn=setup_connection()
            table_name="multilex"
            #itearte over compamny list and check for entry in multilex table for each company
            for company in company_list:
                if (pd.isnull(company )== False):
                    print("company",company)  
                    company=company.lstrip()
                    cur=conn.cursor()
                    cur.execute(f"SELECT * FROM {table_name} WHERE Companies=%s",(company,))
                    data=cur.fetchone()
                    if data!= None:
                        #print("data",data)
                        df.loc[df.Companies==company,'update']="Update"
                        #print("updated")
                        df.to_excel(filename,index=False)
            
            # upload the file to DB 
            upload_file_to_db(df)
            
            
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
        print("inside.....")
        msg="Select the file to Upload"
        return render_template("files_link_upload.html",msg=msg)
    except:
        traceback.print_exc()


@app.route("/copy1", methods=['GET','POST'])
def copy1():
    try:
        if request.method == 'POST':
       
            file = request.files.get('file')
            
            # Requirement 1 :for each row of xls , read company name , if company name is present in multilex table , update the 'update'column in Sharika's xls as update 
            #Requiremet 2 : upload xls data to DB 
                # iterate through each row of xls
                # get the news_source name by processing the 'link' column text
                # check in news source table if teh news source which is taken from link of xls , is already present in the news_source table 
                # if the news_source is present in teh news_source table , then get the id 
                # if news_source is not present in the news_source table , , then insert this news_source and get the coreesponding id 
                # after we get the id , we are insert the corresponding row of xls for that newssource in the multilex table using the above newsource id as foreign key
                # the process is repeated for each row/record in Sharika's  xls 
            #also update each record of xls in multilex table with newsource id as foriegn key 
           
            filename1=file.filename
            
            #update_xls_if_company_exists_and_updatedb(filename1)
            
            
            #Requirement 3 : update the final xls in AWS project directory under test sub folder 
            folder=os.getcwd()

            upload_folder= os.path.join(folder,'test')
            #app.config['UPLOAD_FOLDER'] = upload_folder        
             
            file.save(os.path.join(upload_folder,filename1))
            print("File copy successful !!!")                
  

            msg="file has been saved and multilex table has been updated successfully"
            return render_template("files_link_upload.html",msg=msg)

    except: 
        traceback.print_exc()

