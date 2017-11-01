import mysql.connector

cnx = mysql.connector.connect(user='<<db_instance_primary_user_name>>',
                              password='<<db_instance_password>>',
                              host='g24431890-mysql.c1lczmt6e8zi.us-east-1.rds.amazonaws.com',
                              database='G24431890_ssanames')

def load_data(file_path, in_year):
    curr_record = {}

    for line in file_path.readlines():
        curr_record = line.split(",")
        query = """INSERT INTO Names_Frequency_Data(FirstName,Gender,Frequency,NameYear) 
                VALUES('%s','%s',%d,%d)""" % (curr_record[0], curr_record[1], int(curr_record[2]), in_year)
        args = (curr_record[0], curr_record[1], int(curr_record[2]), in_year)
        crsr.execute(query)
        cnx.commit()
    return

crsr = cnx.cursor()

for x in range(1880, 2017):
    names_file = open("/Users/nikhilswami7/Downloads/names/"+"yob"+str(x)+".txt", 'r')
    print(names_file)
    load_data(names_file, x)

crsr.close()
cnx.close()