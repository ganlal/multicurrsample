# -*- coding: utf-8 -*-
"""
Created on Tue Aug  6 15:01:30 2024

@author: ganla

"""
import sqlite3,csv

def create_tables():
    sql_statements = [ 
        """DROP TABLE IF EXISTS rs2_gl_data ;""",
        """DROP TABLE IF EXISTS curr_exp_ref ;""",  
        """DROP TABLE IF EXISTS deal_control ;""",      
        """DROP TABLE IF EXISTS rs2_to_bnz_acct_map ;""",
        """CREATE TABLE IF NOT EXISTS rs2_gl_data (
                rec_type text null,
                record_date text null,
                gl_acct_number text null,
                dr_cr text,
                loc_currency text null,
                loc_amount text null,
                acct_currency text null,
                acct_amount text null,
                loc_acct_fx_rate text null,
                loc_curr_code text null,
                loc_amount_value numeric null,
                acct_curr_code text null,
                acct_amount_value numeric null,
                loc_acct_fx_rate_value real null,
                file_number text null,
                id INTEGER PRIMARY KEY
        );""",
        """CREATE TABLE IF NOT EXISTS curr_exp_ref (
                curr_num text null,
                curr_code text null,
                exponent float null
        );""" ,       
        """CREATE TABLE IF NOT EXISTS deal_control (
                record_date text null,
                curr_num text null,
                curr_code text null,
                dr_cr text null,
                curr_amount  float null,
                deal_info text null
        );""" ,
        """CREATE TABLE IF NOT EXISTS rs2_to_bnz_acct_map (
                rs2_gl_acct_number text null,
                curr_code text null,
                bnz_acct_number text null,
                acct_system text null,
                net_post_all_flag text null,
                net_post_cr_flag text null,
                net_post_dr_flag text null,   
                particular_config text null,
                code_config text null,
                reference_config text null,
                other_acct_config text null,
                id INTEGER PRIMARY KEY

        );""" ,        
        ]
        # create a database connection
    try:
        with sqlite3.connect(r'C:\data\mc\mc.db') as conn:
            cursor = conn.cursor()
            for statement in sql_statements:
                cursor.execute(statement)
                    
            conn.commit()
            print()
    except sqlite3.Error as e:
        print(e)
        
        


create_tables()

try:
    conn = sqlite3.connect(r'C:\data\mc\mc.db')
    print("Connected to SQLite")

#step-01: read and load the configuration data into a dictionary 
    with open("c:/data/mc/config.csv",newline='') as config_data:
        reader = csv.DictReader(config_data)
        config_dict = {}
        for row in reader:
            #print(row['config_tag'],row['config_value'])
            config_dict[row['config_tag']] = row['config_value']


#step-02: load mapping table data -  curr_exp_ref.csv 
#use dictionary key =  curr_info_file
    cur = conn.cursor()
    with open(config_dict["curr_info_file"],'r') as fin: # `with` statement available in 2.5+
        # csv.DictReader uses first line in file for column headings by default
        dr = csv.DictReader(fin) # comma is default delimiter
        to_db = [(i['curr_num'], i['curr_code'], i['exponent']) for i in dr]
    
    cur.executemany("INSERT INTO curr_exp_ref (curr_num, curr_code,exponent) VALUES (?, ?,?);"
                    , to_db)
    cur.close()
    


    
#load all the config data 
    cur1 = conn.cursor()
    isql = """INSERT INTO curr_exp_ref (curr_num,curr_code, exponent ) VALUES(?,?,?); """
    f = open("c:/data/mc/curr_exp_ref.txt","r")
    for l in f:
        data_tuple = (l[0:3],l[3:6],float(l[6:7]) )
        cur1.execute(isql,data_tuple)
    cur1.close()     
        
# load the dictionary for curr_num, curr_code
    cur1 = conn.cursor()
    cur1.execute("""SELECT curr_num, curr_code FROM curr_exp_ref """)
    curr_code_dict = { id: name for (id, name) in cur1.fetchall() }
    cur1.close()  
#    print(curr_code_dict)


# load the rs2_to-BNZ_account_map data 

    cur1 = conn.cursor()
    cur1.execute("""SELECT curr_num, exponent FROM curr_exp_ref """)
    curr_exponent_dict = { id: name for (id, name) in cur1.fetchall() }
    cur1.close() 
#    print(curr_exponent_dict)


# insert the GL file into the rawtable..
    cursor = conn.cursor()
    rowid = 1
    isql = """INSERT INTO rs2_gl_data
                      (rec_type,record_date, gl_acct_number, dr_cr, loc_currency
                       ,loc_amount,acct_currency, acct_amount, loc_acct_fx_rate
                       ,loc_curr_code,loc_amount_value,acct_curr_code,acct_amount_value
                       ,loc_acct_fx_rate_value, file_number) 
                      VALUES (?, ?, ?, ?, ?,?, ?, ?, ?, ?,?,?,?,?,?);"""


    f = open("c:/data/mc/RS2GL_DATA.txt","r")
    for l in f:
        #print(l[0:2])
        if l[0:2] == 'BD':

            loc_curr_code = curr_code_dict[l[46:49]]
            loc_cur_divisor = 10**curr_exponent_dict[l[46:49]]
            loc_amount_value = round((float(l[49:61])/loc_cur_divisor) if l[45:46] == 'C'  else (-float(l[49:61])/loc_cur_divisor),2) 

            acct_curr_code = curr_code_dict[l[61:64]]
            acct_cur_divisor = 10**curr_exponent_dict[l[61:64]] 
            acct_amount_value = round((float(l[64:76])/acct_cur_divisor) if l[45:46] == 'C'  else (-float(l[64:76])/acct_cur_divisor),2) 
            
            
            data_tuple = (l[0:2],l[2:10],l[10:45].strip(),l[45:46],l[46:49],l[49:61]
                          ,l[61:64],l[64:76],l[76:84]
                          ,loc_curr_code
                          ,loc_amount_value
                          ,acct_curr_code
                          ,acct_amount_value
                          ,float(l[76:84]),l[84:94])
            cursor.execute(isql,data_tuple)

            rowid += 1

#  now do some checks and flag errors. Local_amt_value must be zero
 
    cur1 = conn.cursor()
    cur1.execute("""SELECT round(sum(loc_amount_value),2) loca_amount_net FROM rs2_gl_data """)
    local_amt_net_check = cur1.fetchall()[0][0]
    if  local_amt_net_check != 0:
        print("Local amount net is not zero - raise error", str(local_amt_net_check))
    else:
       print("Passed the locl amount check", str(local_amt_net_check))        
# now construct the SQL to add the DEAL-CONTROL-<CURR> ENTRIES
       cur1.execute(""" INSERT INTO rs2_gl_data (rec_type,record_date,gl_acct_number,dr_cr
            ,loc_currency,loc_amount,acct_currency,acct_amount,loc_acct_fx_rate
            ,loc_curr_code,loc_amount_value,acct_curr_code,acct_amount_value,loc_acct_fx_rate_value,file_number)
            SELECT 'BD' as REC_TYPE, record_date, 'DEAL-CONTROL-'||acct_curr_Code gl_acct_number
            ,CASE WHEN -sum(acct_amount_value) >= 0 THEN 'C' ELSE 'D' END dr_cr
            ,loc_currency, '000000000000' loc_amount
            , acct_currency  ,  '000000000000' acct_amount ,'0' loc_acct_fx_rate
            , loc_curr_code , -round(sum(loc_amount_value),2) loc_amt_value
            , acct_curr_code, -round(sum(acct_amount_value),2) acct_amt_value, 0.00 loc_acct_fx_value, '9999999999' file_number 
            FROM  rs2_gl_data 
            GROUP BY  record_date, loc_currency, acct_currency,loc_curr_code,acct_curr_code;
      """ ) 
       print("Added DEAL-CONTROL-ENTRIES to rs2_gl_data")
 
       cur1.execute(""" INSERT INTO deal_control (record_date,dr_cr,curr_num,curr_code,curr_amount
                        ,deal_info)
           SELECT record_date, CASE WHEN dr_cr = 'C' THEN 'D' ELSE 'C' END dr_cr 
                , acct_currency, acct_curr_code , - acct_amount_value
                , CASE WHEN dr_cr = 'C' THEN 'Sell ' || round(acct_amount_value,2) || ' ' || acct_curr_code || ' to get NZD'
                  ELSE 'Buy ' || acct_amount_value || ' ' || acct_curr_code || ' from NZD'  END deal_info
          FROM rs2_gl_data 
          WHERE acct_curr_code <> 'NZD' AND gl_acct_number LIKE 'DEAL-CONTROL%';
      """ ) 
       print("Created DEAL information to deal_control table")
    cur1.close() 


    

    conn.commit()        
    f.close()    
    print("all data loaded")


except sqlite3.Error as e:
    conn.close()        
    f.close() 
    print(e)

finally:
    if conn:
        conn.close()
        print("The SQLite connection is closed")
