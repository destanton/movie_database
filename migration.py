import csv
import psycopg2

connection = psycopg2.connect("dbname=my_movie_db user=my_movie_db")
cursor = connection.cursor()

cursor.execute("DROP TABLE IF EXISTS item_info CASCADE;")

create_table_command = """
CREATE TABLE item_info
(
    movie_id SERIAL PRIMARY KEY NOT NULL,
    movie_title VARCHAR(150),
    release_date VARCHAR(30),
    video_release_date VARCHAR(50),
    imdb_url VARCHAR(150),
    unknown INT,
    action INT,
    adventure INT,
    animation INT,
    children INT,
    comedy INT,
    crime INT,
    documentary INT,
    drama INT,
    fantasy INT,
    film_noir INT,
    horror INT,
    musical INT,
    mystery INT,
    romance INT,
    sci_fi INT,
    thriller INT,
    war INT,
    western INT
);

CREATE UNIQUE INDEX item_info_movie_id_uindex ON public.item_info (movie_id);"""
cursor.execute(create_table_command)

with open("item.csv") as in_file:
    contents = csv.reader(in_file, delimiter="|")
    # contents = list(contents)
    # print(list(contents[1]))
    for row in contents:
        cursor.execute("INSERT INTO item_info VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);", (
        row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10], row[12],
        row[12], row[13], row[14], row[15], row[16], row[17], row[18], row[19],
        row[20], row[21], row[22], row[23]))
connection.commit()

cursor.execute("DROP TABLE IF EXISTS user_info CASCADE;")

create_table_command = """
CREATE TABLE public.user_info
(
    user_id SERIAL PRIMARY KEY NOT NULL,
    age INT,
    gender VARCHAR(20),
    occupation VARCHAR(50),
    zip_code VARCHAR(10)
);
CREATE UNIQUE INDEX user_info_user_id_uindex ON public.user_info (user_id);"""
cursor.execute(create_table_command)

with open("user.csv") as in_file:
    contents = csv.reader(in_file, delimiter="|")
    for row in contents:
        cursor.execute("INSERT INTO user_info VALUES (%s, %s, %s, %s, %s);", (row[:]))
connection.commit()


cursor.execute("DROP TABLE IF EXISTS data_info CASCADE;")

create_table_command = """
CREATE TABLE public.data_info
(
    user_id INT,
    item_id INT,
    rating INT,
    time_stamp INT,
    CONSTRAINT data_info_item_info_movie_id_fk FOREIGN KEY (item_id) REFERENCES item_info (movie_id),
    CONSTRAINT data_info_user_info_user_id_fk FOREIGN KEY (user_id) REFERENCES user_info (user_id)
);
"""
cursor.execute(create_table_command)

with open("data.csv") as in_file:
    contents = csv.reader(in_file, delimiter="\t")
    for row in contents:
        cursor.execute("INSERT INTO data_info VALUES (%s, %s, %s, %s);", (row[:]))
connection.commit()








cursor.close()
connection.close()
