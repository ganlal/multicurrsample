# -*- coding: utf-8 -*-
"""
Created on Sat Aug 17 07:31:00 2024

@author: ganla
"""

# -*- coding: utf-8 -*-
"""
Created on Tue Aug  6 15:01:30 2024

@author: ganla

"""
import sqlite3,csv,os

def create_tables(db_name):
    sql_statements = [ 
        """DROP TABLE IF EXISTS rs2_gl_data ;""",
        """DROP TABLE IF EXISTS curr_exp_ref ;""",  
        """DROP TABLE IF EXISTS deal_control ;""",      
        """DROP TABLE IF EXISTS rs2_to_bnz_acct_map ;""",
        """DROP TABLE IF EXISTS bnz_acct_post; """,
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
        """CREATE TABLE IF NOT EXISTS bnz_acct_post (
                record_date text null,
                acct_system text null,
                curr_num text null,
                bnz_acct_number text null,
                tran_code text null,
                tran_amount number null,
                parti text null,
                code text null,
                reference text null,
                other_acct text null,
                group_ref text null,
                id INTEGER PRIMARY KEY
        );""" , 
        
        ]
        # create a database connection
    try:
        with sqlite3.connect(db_name) as conn:
            cursor = conn.cursor()
            for statement in sql_statements:
                cursor.execute(statement)
                    
            conn.commit()
            print()
    except sqlite3.Error as e:
        print(e)
        
  
def get_pcr_data(instr,var_dict):
    if instr[0:1] == "c":
        return instr[2:14]
    else:
        return var_dict[instr[2:99]]
    
def cre_bulk_insert_data(in_rst,group_ref):
    bulk_ins_data = []    
    for l in in_rst:
       src_list = list(l)
       #print(l)
       tgt_list = [src_list[0],src_list[1],src_list[2],src_list[3],src_list[4],src_list[5]]
       var_dict = {"file_number":src_list[6][4:10]+"-"+src_list[7][5:10]
                  ,"record_date":src_list[0]
                  ,"gl_acct_number":src_list[8][0:20]}
       # append particular data 
       tgt_list.append(get_pcr_data(src_list[9], var_dict))
       # append code data  
       tgt_list.append(get_pcr_data(src_list[10], var_dict))
       # append reference data 
       tgt_list.append(get_pcr_data(src_list[11], var_dict))
       # append other name data 
       tgt_list.append(get_pcr_data(src_list[12], var_dict))
       # append group_ref data  
       tgt_list.append(group_ref)
       bulk_ins_data.append(tuple(tgt_list))
       #print(tgt_list)
       #vbulk_ins_data = bulk_ins_data.append(tuple(tgt_list))
    return(bulk_ins_data)    
    


#step-01: read and load the configuration data into a dictionary 

try:
    with open("c:/data/mc/config.csv",newline='') as config_data:
        reader = csv.DictReader(config_data)
        config_dict = {}
        for row in reader:
            #print(row['config_tag'],row['config_value'])
            config_dict[row['config_tag']] = row['config_value']
    
    print("Step-01: loading the config file completed",config_dict )
#step-02: change the working directoty & create the tables in the specified database
    # print(os.getcwd())  # Prints the current working directory
    
    os.chdir(config_dict["working_dir"])
    db_name = os.getcwd()+"/"+config_dict["db_name"]
    # print(db_name)
    create_tables(db_name)
    
    conn = sqlite3.connect(db_name)
    print("Connected to SQLite")    

#Step-03: load the currency config
    cur = conn.cursor()

    with open(config_dict["curr_info_file"],'r') as fin: 
        # csv.DictReader uses first line in file for column headings by default
        dr = csv.DictReader(fin) # comma is default delimiter
        to_db = [(i['curr_num'], i['curr_code'], i['exponent']) for i in dr]
    
    cur.executemany("INSERT INTO curr_exp_ref (curr_num, curr_code,exponent) VALUES (?, ?,?);", to_db)
    cur.close()

# load the dictionary for curr_num curr_code to curr_code
    cur1 = conn.cursor()
    cur1.execute("""SELECT curr_num, curr_code FROM curr_exp_ref """)
    curr_code_dict = { id: name for (id, name) in cur1.fetchall() }
    cur1.close()  

# load the dictionary for curr_num curr_code to exponent
    cur1 = conn.cursor()
    cur1.execute("""SELECT curr_num, exponent FROM curr_exp_ref """)
    curr_exponent_dict = { id: name for (id, name) in cur1.fetchall() }
    cur1.close()     

#Step-04: load the rs2 to bnz config mapping 
    cur = conn.cursor()
    with open(config_dict["rs2_to_bnz_acct_map_file"],'r') as fin: 
        # csv.DictReader uses first line in file for column headings by default
        dr = csv.DictReader(fin) # comma is default delimiter
        #print(dr.fieldnames)
        to_db = [(i['rs2_gl_acct_number'], i['curr_code'], i['bnz_acct_number']
                  ,i['acct_system'], i['net_post_all_flag'], i['net_post_cr_flag']
                  ,i['net_post_dr_flag'], i['particular_config'], i['code_config']
                  ,i['reference_config'], i['other_acct_config']
                  )
                 for i in dr]
    
    cur.executemany("""INSERT INTO rs2_to_bnz_acct_map (rs2_gl_acct_number, curr_code, bnz_acct_number
                  ,acct_system ,net_post_all_flag ,net_post_cr_flag ,net_post_dr_flag
                  ,particular_config ,code_config ,reference_config ,other_acct_config     
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?);""", to_db)
    cur.close()

#Step-05: - load the sample data for RS2_GL dile 
    cur = conn.cursor()
    rowid = 1
    isql = """INSERT INTO rs2_gl_data
                      (rec_type,record_date, gl_acct_number, dr_cr, loc_currency
                       ,loc_amount,acct_currency, acct_amount, loc_acct_fx_rate
                       ,loc_curr_code,loc_amount_value,acct_curr_code,acct_amount_value
                       ,loc_acct_fx_rate_value, file_number) 
                      VALUES (?, ?, ?, ?, ?,?, ?, ?, ?, ?,?,?,?,?,?);"""


    f = open(config_dict["rs2_data_file"],'r')
    to_db=[]
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
            to_db.append(data_tuple)
            # cur.execute(isql,data_tuple)
            rowid += 1    
# close the cursor             
    cur.executemany(isql, to_db)
    cur.close()

#Step-06: create the balancing entries to inser back into rs2_gl_data

    cur1 = conn.cursor()
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

# step-07: Create Deal entries for getting processing the deals.. 
   
    cur1.execute(""" INSERT INTO deal_control (record_date,dr_cr,curr_num,curr_code,curr_amount
                      ,deal_info)
         SELECT record_date, CASE WHEN dr_cr = 'C' THEN 'D' ELSE 'C' END dr_cr 
              , acct_currency, acct_curr_code , - acct_amount_value
              , CASE WHEN dr_cr = 'C' THEN 'Sell ' || round(acct_amount_value,2) || ' ' || acct_curr_code || ' to get NZD'
                ELSE 'Buy ' || acct_amount_value || ' ' || acct_curr_code || ' from NZD'  END deal_info
        FROM rs2_gl_data 
        WHERE acct_curr_code <> 'NZD' AND gl_acct_number LIKE 'DEAL-CONTROL%';
    """ )    
    cur1.close()  
# step-08: Now insert BNZ Postings 

# part-1= look for all net  postings 

    sql = """SELECT a.record_date,b.acct_system, a.acct_currency, b.bnz_acct_number
    ,CASE WHEN sum(acct_amount_value) >= 0 THEN '50' ELSE '00' END tran_code 
    ,ROUND(SUM(acct_amount_value),2) post_amount  
    ,MIN(a.file_number) file_no_min
    ,MAX(a.file_number) file_no_max
    ,MIN(a.gl_acct_number) other_acct
	,b.particular_config ,b.code_config	,b.reference_config ,b.other_acct_config
    FROM rs2_gl_data a  
    JOIN rs2_to_bnz_acct_map b ON a.gl_acct_number = b.rs2_gl_acct_number
    WHERE b.net_post_all_flag = 'Y' 
    GROUP BY a.record_date,a.acct_currency,  b.bnz_acct_number
	,b.particular_config, b.code_config	,b.reference_config	,b.other_acct_config;  """
    
    cur1 = conn.cursor()
    cur1.execute(sql)
    rst = cur1.fetchall()
# create the insert data list with rows as tuples..     
    group_ref = "G01-Net All data "
    to_db = cre_bulk_insert_data(rst,group_ref)
    isql = """INSERT INTO bnz_acct_post ( record_date,acct_system,curr_num,bnz_acct_number
            ,tran_code,tran_amount,parti,code,reference,other_acct,group_ref)
            VALUES (?, ?, ?, ?, ?,?, ?, ?, ?, ?,?);"""
    #print(to_db)    
    cur1.executemany(isql, to_db)
    cur1.close()    

# part-2 create CR only net posting 
    sql = """SELECT a.record_date,b.acct_system, a.acct_currency, b.bnz_acct_number
    ,'50' tran_code 
    ,ROUND(SUM(acct_amount_value),2) post_amount  
    ,MIN(a.file_number) file_no_min
    ,MAX(a.file_number) file_no_max
    ,MIN(a.gl_acct_number) other_acct
	,b.particular_config ,b.code_config	,b.reference_config ,b.other_acct_config
    FROM rs2_gl_data a  
    JOIN rs2_to_bnz_acct_map b ON a.gl_acct_number = b.rs2_gl_acct_number
    WHERE b.net_post_all_flag = 'N'
    AND b.net_post_cr_flag = 'Y'
    AND a.dr_cr = 'C'
    GROUP BY a.record_date,a.acct_currency,  b.bnz_acct_number
	,b.particular_config, b.code_config	,b.reference_config	,b.other_acct_config;  """
    
    cur1 = conn.cursor()
    cur1.execute(sql)
    rst = cur1.fetchall()
# create the insert data list with rows as tuples..     
    group_ref = "G02-CR Net Post data"
    to_db = cre_bulk_insert_data(rst,group_ref)
    isql = """INSERT INTO bnz_acct_post ( record_date,acct_system,curr_num,bnz_acct_number
            ,tran_code,tran_amount,parti,code,reference,other_acct,group_ref)
            VALUES (?, ?, ?, ?, ?,?, ?, ?, ?, ?,?);"""
    #print(to_db)    
    cur1.executemany(isql, to_db)
    cur1.close()  

# part-3 create DR only net posting 
    sql = """SELECT a.record_date,b.acct_system, a.acct_currency, b.bnz_acct_number
    ,'00' tran_code 
    ,ROUND(SUM(acct_amount_value),2) post_amount  
    ,MIN(a.file_number) file_no_min
    ,MAX(a.file_number) file_no_max
    ,MIN(a.gl_acct_number) other_acct
	,b.particular_config ,b.code_config	,b.reference_config ,b.other_acct_config
    FROM rs2_gl_data a  
    JOIN rs2_to_bnz_acct_map b ON a.gl_acct_number = b.rs2_gl_acct_number
    WHERE b.net_post_all_flag = 'N'
    AND b.net_post_dr_flag = 'Y'
    AND a.dr_cr = 'D'
    GROUP BY a.record_date,a.acct_currency,  b.bnz_acct_number
	,b.particular_config, b.code_config	,b.reference_config	,b.other_acct_config;  """
    
    cur1 = conn.cursor()
    cur1.execute(sql)
    rst = cur1.fetchall()
# create the insert data list with rows as tuples..     
    group_ref = "G03-DR Net Post data"
    to_db = cre_bulk_insert_data(rst,group_ref)
    isql = """INSERT INTO bnz_acct_post ( record_date,acct_system,curr_num,bnz_acct_number
            ,tran_code,tran_amount,parti,code,reference,other_acct,group_ref)
            VALUES (?, ?, ?, ?, ?,?, ?, ?, ?, ?,?);"""
    #print(to_db)    
    cur1.executemany(isql, to_db)
    cur1.close()      

# part-4 create cr  individual posting 
    sql = """SELECT a.record_date,b.acct_system, a.acct_currency, b.bnz_acct_number
    ,'50' tran_code 
    ,ROUND(acct_amount_value,2) post_amount  
    ,a.file_number file_no_min
    ,a.file_number file_no_max
    ,a.gl_acct_number other_acct
	,b.particular_config ,b.code_config,b.reference_config ,b.other_acct_config
    FROM rs2_gl_data a  
    JOIN rs2_to_bnz_acct_map b ON a.gl_acct_number = b.rs2_gl_acct_number
    WHERE b.net_post_all_flag = 'N' AND b.net_post_cr_flag = 'N' 
    AND ROUND(acct_amount_value,2) >= 0; """
    
    cur1 = conn.cursor()
    cur1.execute(sql)
    rst = cur1.fetchall()
# create the insert data list with rows as tuples..     
    group_ref = "G04-Individual CR posting .. "
    to_db = cre_bulk_insert_data(rst,group_ref)
    isql = """INSERT INTO bnz_acct_post ( record_date,acct_system,curr_num,bnz_acct_number
            ,tran_code,tran_amount,parti,code,reference,other_acct,group_ref)
            VALUES (?, ?, ?, ?, ?,?, ?, ?, ?, ?,?);"""
    #print(to_db)    
    cur1.executemany(isql, to_db)
    cur1.close()   
# part-5 create dr  individual posting 
    sql = """SELECT a.record_date,b.acct_system, a.acct_currency, b.bnz_acct_number
    , '00' tran_code 
    ,ROUND(acct_amount_value,2) post_amount  
    ,a.file_number file_no_min
    ,a.file_number file_no_max
    ,a.gl_acct_number other_acct
	,b.particular_config ,b.code_config,b.reference_config ,b.other_acct_config
    FROM rs2_gl_data a  
    JOIN rs2_to_bnz_acct_map b ON a.gl_acct_number = b.rs2_gl_acct_number
    WHERE b.net_post_all_flag = 'N' AND b.net_post_dr_flag = 'N'
    AND ROUND(acct_amount_value,2) < 0;"""
    
    cur1 = conn.cursor()
    cur1.execute(sql)
    rst = cur1.fetchall()
# create the insert data list with rows as tuples..     
    group_ref = "G05-Individual DR posting .. "
    to_db = cre_bulk_insert_data(rst,group_ref)
    isql = """INSERT INTO bnz_acct_post ( record_date,acct_system,curr_num,bnz_acct_number
            ,tran_code,tran_amount,parti,code,reference,other_acct,group_ref)
            VALUES (?, ?, ?, ?, ?,?, ?, ?, ?, ?,?);"""
    #print(to_db)    
    cur1.executemany(isql, to_db)
    cur1.close() 


# fina step - commit all the changes ..
    conn.commit()       
    print("All changes committed")
    if conn:
        conn.close()

except sqlite3.Error as e:
    conn.close()        
    print(e)

finally:
    if conn:
        conn.close()
        print("The SQLite connection is closed")
