from flask import Flask,render_template,request,url_for
from datetime import datetime
import pymysql
import pymysql.cursors
import traceback
from flask import request


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
