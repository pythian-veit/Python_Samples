import mysql.connector
import db_creds as c
import math
import random
import sys
import argparse
import pylib.et_utility_libs as util
import dbPyUtils as u
import pysftp
import os
import datetime


def containsAny(str, set):
    """Check whether 'str' contains ANY of the chars in 'set'"""
    return 1 in [c in str for c in set]


def connect_mysql(mysql_conn_info):
    cnx = mysql.connector.connect(**mysql_conn_info)
    return cnx


def members_count(bu, start_date, exp_date, control_ratio):
   
    cnx_dmart = connect_mysql(dmart_conn_info)
    sql = '''CREATE TABLE `{0}` (
                          `member_id` int(10) unsigned NOT NULL,
                          `gender` enum('M','F') NOT NULL DEFAULT 'F',
                          `is_control` tinyint(1) NOT NULL,
                          `start_date` datetime DEFAULT NULL,
                          `expiration_date` datetime DEFAULT NULL,
                          `rank` int NOT NULL 
                          ) ENGINE=InnoDB DEFAULT CHARSET=latin1'''.format(temp_table_1)
    print sql
    u.doSQL(cnx_dmart, sql)


    if bu == 'NR':

        sql = '''insert into {0} (member_id, gender, is_control, start_date, expiration_date, rank)
                 select m.member_id,
                        m.gender,
                        case when rand() > {1} then 0 else 1 end as is_control,
                        '{2}' start_date,  
                        '{3}' expiration_date,
                        @curRank := @curRank + 1 AS rank
                   from fakecompany.et_members_upload m,
                        (SELECT @curRank := 0) r 
                  WHERE ( month(join_date) = month(current_date) -- people who joined this month
                         -- if it's dec 2017 then recovery to get October and November
                         -- or (
                         --       year(current_date)=2017 and month(current_date)=12 and  month(join_date)=month(current_date)-1
                         --    )
                         -- or (
                         --       year(current_date)=2017 and month(current_date)=12 and  month(join_date)=month(current_date)-2
                         --    )
                        )
                    and year(join_date) < year(current_date) -- make sure not getting current month joiners
                    and member_status = 'active' 
                    and rack_optin <> 'none' 
                    and optin = 'none' -- Remove HL members, they already get a HL Anniv Email
                    and m.member_id not in
                    (select member_id 
                       from et_anniv_retention
                      where start_date > date_sub(current_date, INTERVAL 330 DAY)
                        and business_unit = 'NR'
                        and is_control=0
                    ) 
               '''.format(temp_table_1, control_ratio, start_date, exp_date)

    elif bu == 'HL':
        sql = '''insert into {0} (member_id, gender, is_control, start_date, expiration_date, rank)
                 select m.member_id,
                        m.gender,
                        case when rand() > {1} then 0 else 1 end as is_control,
                        '{2}' start_date,  
                        '{3}' expiration_date,
                        @curRank := @curRank + 1 AS rank
                   from fakecompany.et_members_upload m,
                        (SELECT @curRank := 0) r 
                  WHERE ( month(join_date) = month(current_date) -- people who joined this month
                         -- if it's dec 2017 then recovery to get October and November
                         -- or (
                         --      year(current_date)=2017 and month(current_date)=12 and  month(join_date)=month(current_date)-1
                         --   )
                         -- or (
                         --      year(current_date)=2017 and month(current_date)=12 and  month(join_date)=month(current_date)-2
                         --   )
                        )
                    and year(join_date) < year(current_date) -- make sure not getting current month joiners
                    and member_status = 'active' 
                    and optin <> 'none' 
                    and m.member_id not in 
                    (select member_id 
                       from et_anniv_retention
                      where start_date > date_sub(current_date, INTERVAL 330 DAY)
                        and business_unit = 'HL'
                        and is_control=0
                    ) 
               '''.format(temp_table_1, control_ratio, start_date, exp_date)

    print sql
    u.doSQL(cnx_dmart, sql)
    cnx_dmart.commit()


    sql = '''CREATE UNIQUE INDEX {0}_idx1 on {0} (rank)'''.format(temp_table_1) 
    print sql
    u.doSQL(cnx_dmart, sql)


    cursor = cnx_dmart.cursor()
    sql = "select count(*) from {0}".format(temp_table_1)
    print sql
    cursor.execute(sql)
    row = cursor.fetchone()
    mem_count = row[0]
    cursor.close()
    cnx_dmart.close()

    return mem_count


def gen_rnd_coups(coupon_type, amount, min_purchase, coup_prefix, max_discount, bu, run_date):
    coup_count = 0
    coup_length = 9
    allowed = 'bdghjklmnqrtvwxz123456789'
    alpha = {'b','d','g','h','j','k','l','m','n','q','r','t','v','w','x','z'}       

    cnx_dmart = connect_mysql(dmart_conn_info)
    sql = '''CREATE TABLE `{0}` (
              `coupon_code` varchar(25) NOT NULL
             ) ENGINE=InnoDB DEFAULT CHARSET=latin1'''.format(temp_table_2)
    print sql
    u.doSQL(cnx_dmart, sql)

    sql = '''CREATE TABLE `{0}` (
              `coupon_code` varchar(25) NOT NULL
             ) ENGINE=InnoDB DEFAULT CHARSET=latin1'''.format(temp_table_3)
    print sql
    u.doSQL(cnx_dmart, sql)

    sql = '''CREATE TABLE `{0}` (
              `coupon_code` varchar(25) NOT NULL,
              `rank` int NOT NULL
             ) ENGINE=InnoDB DEFAULT CHARSET=latin1'''.format(temp_table_4)
    print sql
    u.doSQL(cnx_dmart, sql)

    sql = '''CREATE TABLE `{0}` (
             `coupon_code` varchar(25) NOT NULL,
             `member_id` int(10) unsigned NOT NULL,
             `gender` enum('M','F') NOT NULL DEFAULT 'F',
             `is_control` tinyint(1) NOT NULL,
             `start_date` datetime DEFAULT NULL,
             `expiration_date` datetime DEFAULT NULL
             ) ENGINE=InnoDB DEFAULT CHARSET=latin1'''.format(temp_table_5)
    print sql
    u.doSQL(cnx_dmart, sql)


    # Generate the coupon codes
    cursor = cnx_dmart.cursor()
    while coup_count < coups_req:
    
        coup = coup_prefix
 
        while len(coup) < 9:
            rand_idx= random.randint(0,24)
            coup = coup + allowed[rand_idx]
    
        if containsAny(coup, alpha):    
            coup_count = coup_count + 1
            insert_sql = "INSERT INTO {0} (coupon_code) VALUES (%(coup)s)".format(temp_table_2)
            #print insert_sql
            cursor.execute(insert_sql, {"coup": coup})
            #print coup
            if coup_count % 10000 == 0:
              print "commiting: ", coup_count
              cnx_dmart.commit()
            
        else:
            print "coupon has no alpha chars, skipping", coup 
 
    cnx_dmart.commit()
    cursor.close()


    # Get distinct codes
    sql = '''insert into {0} (coupon_code)
             select distinct coupon_code 
               FROM `{1}`'''.format(temp_table_3, temp_table_2)
    u.doSQL(cnx_dmart, sql)
    cnx_dmart.commit()


    # Calculate an arbitrary rank for the codes
    sql = '''insert into {0} (coupon_code, rank)
             select coupon_code, 
                    @curRank := @curRank + 1 AS rank
               FROM `{1}` t,
                    (SELECT @curRank := 0) r
                      where coupon_code not in
                     (select coupon_code
                       from coupons)'''.format(temp_table_4, temp_table_3)
    u.doSQL(cnx_dmart, sql)
    cnx_dmart.commit()


    sql = '''CREATE UNIQUE INDEX {0}_idx1 on {0} (rank)'''.format(temp_table_4)   
    print sql
    u.doSQL(cnx_dmart, sql)


    # Join the members and the codes on rank
    # Unpaired codes are discarded in this way
    sql = '''insert into {0}(coupon_code, member_id, gender, is_control, start_date, expiration_date)
             select coupon_code,
                    member_id,
                    gender,
                    is_control,
                    start_date,
                    expiration_date
               from {1} t1 join {2} t3 on (t1.rank = t3.rank)'''.format(temp_table_5, temp_table_1, temp_table_4)
    u.doSQL(cnx_dmart, sql)
    cnx_dmart.commit()

    sql = '''CREATE UNIQUE INDEX {0}_idx1 on {0} (coupon_code)'''.format(temp_table_5)
    print sql
    u.doSQL(cnx_dmart, sql)

    YYYYMMDD = datetime.datetime.now().strftime('%Y%m%d')
        
    # Insert the member and code pairs into the ET retention table
    sql = '''insert into `et_anniv_retention`(coupon_code, member_id, gender, is_control, start_date, expiration_date, run_date, batch_id, business_unit)
             select case when is_control then 'CONTROL{0}_{1}' else coupon_code end as coupon_code,       
                    member_id,
                    gender,
                    is_control,
                    start_date,
                    expiration_date,
                    '{2}',
                    '{3}',
                    '{1}'
               from {4}'''.format(YYYYMMDD, bu, run_date, batch_id, temp_table_5)
    u.doSQL(cnx_dmart, sql)
    cnx_dmart.commit()
    print "Coupons inserted into et_anniv_retention"



    cnx_dmart.close()


def generate_csv_file():
    print 'generate the csv file and save to et_dumps'
    cnx_dmart = connect_mysql(dmart_conn_info)

    sql="""select "coupon_code",
                  "member_id",
                  "email",
                  "gender",
                  "is_control",
                  "start_date",
                  "expiration_date",
                  "group_name",
                  "member_uuid"
                  union all
                  select
                  t.coupon_code,
                  t.member_id,
                  ifnull(m.email,""),
                  ifnull(t.gender,""),
                  ifnull(t.is_control,""),
                  if(t.start_date = '0000-00-00 00:00:00','1971-01-01 00:00:00',ifnull(t.start_date,'1971-01-01 00:00:00')),
                  if(t.expiration_date = '0000-00-00 00:00:00','1971-01-01 00:00:00',ifnull(t.expiration_date,'1971-01-01 00:00:00')),
                  "",
                  ifnull(m.member_uuid,"")
                  from {0} t join et_anniv_retention e on (e.member_id = t.member_id and e.coupon_code = t.coupon_code)
           left join fakecompany.members m on t.member_id = m.member_id
            where t.is_control=0
              and e.batch_id = '{1}'
              and e.business_unit = '{2}'                        
             into outfile '{3}' FIELDS TERMINATED BY ','  OPTIONALLY ENCLOSED BY '"' LINES TERMINATED BY '\n'
        """.format(temp_table_5, batch_id, bu, output_file)

    print sql
    cur = u.doSQL(cnx_dmart, sql)
    cur.close()
    cnx_dmart.close()


def ftp_output_to_sfmc(ftp_dir, use_encryption, private_key_ftp):
    # This is so will know we are using the global defined in main
    global output_file

    print 'ftp_dir is {0}'.format(ftp_dir)
    if use_encryption:
        bashCommand ="gpg --trust-model always --output {0}.gpg --encrypt --recipient info@exacttarget.com {0}"
        os.system(bashCommand.format(output_file))
        output_file+=".gpg" 
        print "Encrypted file is {0}".format(output_file)

    # start ftp and move to correct folder
    sftp =  pysftp.Connection('ftp.s6.exacttarget.com', username='99999999', private_key=private_key_ftp) 
    sftp.chdir(ftp_dir) 
	
    sftp.put(output_file)
    print "copied file " + output_file

    sftp.close()


def gen_coup_ins_on_master():

    cnx_dmart = connect_mysql(dmart_conn_info)

    sql="""select concat('insert ignore into coupons(coupon_code, coupon_type, amount, min_purchase, max_discount, single_member_use, start_date, expiration_date) ', 
                          'values (\\'', t.coupon_code, '\\', ',
                          '\\'{0}', '\\', ',
                          {1}, ', ',
                          {2}, ', ',
                          {3}, ', ',
                          1, ', ',
                          '\\'{4}', '\\', ',
                          '\\'{5}', '\\'',
                          ');')
             from {6} t JOIN et_anniv_retention e
               on (e.member_id = t.member_id and e.coupon_code = t.coupon_code) 
            where t.is_control=0
              and e.batch_id = '{7}'
              and e.business_unit = '{8}'
             into outfile '{9}'""".format(coupon_type, amount, min_purchase, max_discount, start_date, exp_date, temp_table_5, batch_id, bu, sql_output_file)
    print sql
    cur = u.doSQL(cnx_dmart, sql)
    cur.close()
    cnx_dmart.close()


def run_coup_ins_on_master():
    f = open(sql_output_file)
    sql = f.read()
    #print sql

    host = master_conn_info['host']
    database = master_conn_info['database']
    
    command = "mysql --defaults-file=../etc/my_pala.cnf -h {0} {1} < {2}".format(host, database, sql_output_file)
    print command
    os.system(command) 


def mark_et_anniv_retention_as_sent():
    cnx_dmart = connect_mysql(dmart_conn_info)
    sql ="""update et_anniv_retention e JOIN {0} t
            on (e.member_id = t.member_id and e.coupon_code = t.coupon_code)
            set e.sent_to_et_date = current_timestamp()
            where e.sent_to_et_date is null
            and t.is_control=0
            and e.batch_id = '{1}'
            and e.business_unit = '{2}';
            
            drop table {0};
            drop table {3};
            drop table {4};
            drop table {5};
            drop table {6};
            """.format(temp_table_5, batch_id, bu, temp_table_1, temp_table_2, temp_table_3, temp_table_4)

    print "now updating sent_to_et_date:",sql
    cur = u.doSQL_many(cnx_dmart, sql)
    cur.close()
    cnx_dmart.close()


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument('--env', default='DEV')
    parser.add_argument('--bu')
    parser.add_argument('--start_date')
    parser.add_argument('--exp_date')
    parser.add_argument('--control_ratio')
    parser.add_argument('--coupon_type')
    parser.add_argument('--amount')
    parser.add_argument('--min_purchase')
    parser.add_argument('--coup_prefix')
    parser.add_argument('--max_discount')
    parser.add_argument('--output_suffix')
    parser.add_argument('--ftp_dir')
    parser.add_argument('--use_encryption', default=True)

    args = parser.parse_args()
    #print args
    
    global bu
    bu = args.bu
    start_date = args.start_date
    exp_date = args.exp_date
    exp_date += " 23:59:59"
    
    control_ratio = args.control_ratio
    coupon_type = args.coupon_type
    amount = args.amount
    min_purchase = args.min_purchase
    coup_prefix = args.coup_prefix
    max_discount = args.max_discount
    output_suffix = args.output_suffix
    ftp_dir = args.ftp_dir
    use_encryption = args.use_encryption

    global master_conn_info
    global dmart_conn_info

    if args.env=='PROD':
         master_conn_info = c.hl_MASTER
         dmart_conn_info = c.hl_dmart
    else:
         master_conn_info = c.hl_DMART_TEST
         dmart_conn_info = c.hl_DMART_TEST

    private_key_ftp = c.PRIVATE_KEY_FTP
    batch_id = util.generate_batch_id()
    MM = batch_id[0:2]
    DD = batch_id[2:4]
    HH = batch_id[5:7]
    MI = batch_id[7:9]
    SS = batch_id[10:12]
    YYYY = datetime.datetime.now().strftime('%Y')
    run_date = '{0}-{1}-{2} {3}:{4}:{5}'.format(YYYY, MM, DD, HH, MI, SS)
    print "Run date: ", run_date
     

    global output_file
    global sql_output_file
    network_file_dump_path = '/opt/netapp/dmart01/et_dumps'
    output_file = '{0}/{1}_{2}.csv'.format(network_file_dump_path, batch_id, output_suffix)
    sql_output_file = '{0}/{1}_{2}_coupons_inserts.sql'.format(network_file_dump_path, batch_id, bu)


    global temp_table_1
    global temp_table_2
    global temp_table_3
    global temp_table_4
    global temp_table_5

    temp_table_1 = 'et_anniv_coups_tmp_1_{0}_{1}'.format(batch_id, bu)
    temp_table_2 = 'et_anniv_coups_tmp_2_{0}_{1}'.format(batch_id, bu)
    temp_table_3 = 'et_anniv_coups_tmp_3_{0}_{1}'.format(batch_id, bu)
    temp_table_4 = 'et_anniv_coups_tmp_4_{0}_{1}'.format(batch_id, bu)
    temp_table_5 = 'et_anniv_coups_tmp_5_{0}_{1}'.format(batch_id, bu)    


    coups_req = members_count(bu, start_date, exp_date, control_ratio)
    print "Anniversary members: ", coups_req

    # Generate more coupons than anniv members in case of some already existing in the coupons table
    coups_req = math.ceil(coups_req * 1.5)
    print "Coupons needed: ", coups_req
    gen_rnd_coups(coupon_type, amount, min_purchase, coup_prefix, max_discount, bu, run_date)
    gen_coup_ins_on_master()
    run_coup_ins_on_master()   
 
    #skip ftp step if not set (can be unset for testing)
    if ftp_dir:
        generate_csv_file()
        ftp_output_to_sfmc(ftp_dir, use_encryption, private_key_ftp)
        mark_et_anniv_retention_as_sent()

    print "anniv_et300.py processing complete"