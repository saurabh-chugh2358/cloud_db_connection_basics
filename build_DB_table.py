import mysql.connector

cnx = mysql.connector.connect(user='<<db_instance_primary_user_name>>',
                              password='<<db_instance_password>>',
                              host='g24431890-mysql.c1lczmt6e8zi.us-east-1.rds.amazonaws.com',
                              database='G24431890_ssanames')
crsr = cnx.cursor()


def load_data(file_path, in_year):
    curr_record = {}

    for line in file_path.readlines():
        curr_record = line.split(",")
        query = """INSERT INTO Names_Frequency(FirstName,Gender,Frequency) 
                VALUES('%s','%s',%d)""" % (curr_record[0], curr_record[1], int(curr_record[2]), in_year)
        args = (curr_record[0], curr_record[1], int(curr_record[2]))
        crsr.execute(query)
        cnx.commit()


for x in range(1880, 2017):
    file_names = open("names_data/yob"+str(x)+".txt", 'r')
    load_data(file_names, x)

crsr.close()
cnx.close()