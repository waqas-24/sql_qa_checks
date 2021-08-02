import sys
import mysql.connector
from mysql.connector import errorcode
import tkinter as tk

print(f"Python Version {sys.version}")

# Connecting to mysqldb
def get_db_connection():
    try:
        return mysql.connector.connect(host="localhost", user="root", password="admin_password")    
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with your user name or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist")
        else:
            print(err)

#create table SQL for qa_tests table
create_table_qa_tests="""
    CREATE TABLE IF NOT EXISTS qa_tests (
    id INT NOT NULL AUTO_INCREMENT,
    code VARCHAR(100), 
    description VARCHAR(150), 
    enabled VARCHAR(1), 
    parameter VARCHAR(1000),
    test_sql VARCHAR(1000),
    exp_result INT,
    PRIMARY KEY (id)
    )"""

#create table SQL for channel_table_tbl1 table
create_table_channel_table="""
    CREATE TABLE IF NOT EXISTS channel_table_tbl1 (    
    channel_code VARCHAR(100)
    )"""

# trigger to automate code column value insertion
create_code_trigger="""
CREATE TRIGGER generate_code 
BEFORE INSERT ON qa_tests FOR EACH ROW
BEGIN        
    SET @MAX_ID = (SELECT MAX(id) from qa_tests);
    IF @MAX_ID IS NULL THEN
    SET @MAX_ID =0;
    END IF;
    
    SET NEW.code = CONCAT('qa_ch_',CAST(@MAX_ID+1 as CHAR(50)));
END;
"""
def instert_into_channel_type(db_con, val):
    with db_con.cursor() as mycursor:    
        mycursor.execute(f"""INSERT INTO channel_table_tbl1 (channel_code) VALUES('{val}')""")
        db_con.commit()


def instert_into_qa_tests(db_con, val):
    insert_sql = """ INSERT INTO qa_tests (description,enabled,parameter,test_sql,exp_result) 
                      VALUES(%s,%s,%s,%s,%s) """    
    with db_con.cursor() as mycursor:    
        mycursor.execute(insert_sql, val)
        db_con.commit()


def initiate_qa_db(db_name):
    
    with db_con.cursor() as mycursor:    
        mycursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_name}")
        mycursor.execute(f"USE {db_name}")
        mycursor.execute("DROP TABLE IF EXISTS qa_tests")
        mycursor.execute("DROP TRIGGER IF EXISTS generate_code;")
        mycursor.execute(create_table_qa_tests)     
        mycursor.execute(create_code_trigger)
        mycursor.execute("DROP TABLE IF EXISTS channel_table_tbl1")
        mycursor.execute(create_table_channel_table)     
        db_con.commit()

def run_sql_test(db_con,code,**kwargs):
    
    with db_con.cursor(dictionary=True,buffered=True) as mycursor:  
        test_result=""
        error = None
        qa_check_flag=""        
        mycursor.execute(f"SELECT * FROM qa_tests where code='{code}'")
        data=mycursor.fetchall()
        param_list=data[0]['parameter'].replace(" ","").split(",")    #[0] is used as code will always return 1 row as it's unique in the table
        sql_to_exec=data[0]['test_sql']
        exp_result=data[0]['exp_result']        
        for param in param_list:            
            sql_to_exec=sql_to_exec.replace(f"**{param}**",kwargs[param])        
            # print(f"sql_to_exec {sql_to_exec}")

        with db_con.cursor(dictionary=True,buffered=True) as mycursor:
            try:
                mycursor.execute(sql_to_exec)
                test_result=mycursor.fetchall()
            except mysql.connector.Error as err:
                error=err

# Console Output
        if error is None:
            print(f"This is an error ##### {error}")
            sql_test_result=[val for val in test_result[0].values()][0]  #fetching the result of sql test
            if exp_result == sql_test_result:
                qa_check_flag="Passed"
            else: 
                qa_check_flag="Failed"
            print(f"QA Test Code ==> {data[0]['code']}\n")        
            print(f"SQL executed for test ==> {sql_to_exec}\n")
            print(f"QA Test result ==> {sql_test_result}\n")
            print(f"QA Test Passed/Failed ==> {qa_check_flag}")
        else:
            print(f"This test failed with error\n {error}")

#Setting up GUI to present results
        if error is None:
            window = tk.Tk()
            tk.Label(text="SQL QA Check", foreground="white", background="black").pack()
            tk.Label(text="").pack() #adding blank line
            tk.Label(text=f"SQL Test Code: {data[0]['code']}\n\n").pack()
            tk.Label(text=f"SQL executed for test",  foreground="white", background="black").pack()
            tk.Label(text=f"SQL executed for test {sql_to_exec}\n\n").pack()
            tk.Label(text=f"QA Test result",  foreground="white", background="black").pack()
            tk.Label(text=f"{sql_test_result}\n\n").pack()
            tk.Label(text=f"QA Test Passed/Failed",  foreground="white", background="black").pack()
            tk.Label(text=f"{qa_check_flag}\n\n").pack()
            tk.Button(text="Click here to close", width=15, height=2, bg="blue", fg="white", command=window.destroy ).pack()
            window.mainloop()
        else:
            window = tk.Tk()
            tk.Label(text="SQL QA Check", foreground="white", background="black").pack()
            tk.Label(text="").pack() #adding blank line
            tk.Label(text=f"SQL Test Code: {data[0]['code']}\n\n").pack()
            tk.Label(text=f"This test failed with error",  foreground="white", background="black").pack()
            tk.Label(text=f"{error}\n\n").pack()            
            tk.Button(text="Click here to close", width=15, height=2, bg="blue", fg="white", command=window.destroy ).pack()
            window.mainloop()


#Initiating qa_checks db in mysql
db_con = get_db_connection()
initiate_qa_db("qa_checks")

# Inserting three test records in qa_tests table
# adding ** at the start and end of the parameter to replace them with run time parameter values

insert_test1=('Runs the SQL against the Channel table to count duplicates. Duplicates count must be 0','Y','env',"""Select count(*) from (select 
channel_code, count(*) 
from channel_table_**env**
group by channel_code
having count(*) > 1) AS X""",0)
instert_into_qa_tests(db_con,insert_test1)
insert_test2=('Check the FK between channel_code and its child table channel_transaction to identify orphans at a given date','Y','env, date',"""select count(*)
from channel_transaction_**env** A, 
channel_table_**env** B
left join on (A.channel_code 
= B.channel_code)
where B.channel_code is null
and B.transaction_date = 
**date**""",0)
instert_into_qa_tests(db_con,insert_test2)
insert_test3=('Counts the records in channel_transaction table at a given date that have amount null','N','env',"""select count(*) from 
channel_transaction_**env**
where transaction_date = **date** and transaction_amount 
is null""",0)
instert_into_qa_tests(db_con,insert_test3)

# Inserting three test records in in channel_type table(with ) including wi
insert_channel_type_data=('val1')
instert_into_channel_type(db_con,insert_channel_type_data)
insert_channel_type_data=('val2')
instert_into_channel_type(db_con,insert_channel_type_data)
insert_channel_type_data=('val3')
instert_into_channel_type(db_con,insert_channel_type_data)     
   

#running QA check  qa_ch_1 and providing parameter and its value
run_sql_test(db_con,"qa_ch_1",env="tbl1")
