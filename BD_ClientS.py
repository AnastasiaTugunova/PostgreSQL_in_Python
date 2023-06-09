import psycopg2


def drop_tb(conn):
    with conn.cursor() as cur:
        cur.execute("""
        DROP TABLE phone_client;
        DROP TABLE client;
        """)
        return "База данных удалена"


def create_tb(conn):
    with conn.cursor() as cur:
        cur.execute("""
        CREATE TABLE IF NOT EXISTS client(
            id SERIAL  PRIMARY KEY,
            first_name VARCHAR(20)  NOT NULL,
            last_name  VARCHAR(30)  NOT NULL,
            email      VARCHAR(100) UNIQUE NOT NULL);
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS phone_client(
            phone_id SERIAL PRIMARY KEY,
            phone_number    VARCHAR(12) UNIQUE,
            client_id       INTEGER NOT NULL REFERENCES client(id) ON DELETE CASCADE);
        """)
        conn.commit()
    return "База данных создана"


def add_new_client(conn, first_name, last_name, email, phones=None):
    with conn.cursor() as cur:

        if phones == None:
            cur.execute("""
            INSERT INTO client(first_name,last_name,email)
            VALUES      (%s,%s,%s);
            """, (first_name,last_name,email))

        if phones != None:
            cur.execute("""
            INSERT INTO client(first_name,last_name,email)
            VALUES      (%s,%s,%s);
            """, (first_name, last_name, email))

            cur.execute("""
            SELECT id 
            FROM   client 
            WHERE  email=%s;
                """, (email,))

            client_id = cur.fetchone()[0]
            cur.execute("""
            INSERT INTO phone_client(client_id, phone_number) 
            VALUES      (%s,%s);
            """, (client_id, phones))
        conn.commit()
    return f"Клиент под номером id {client_id} добавлен в базу данных"

def add_phone_number(conn, client_id, phone):
    with conn.cursor() as curs:
        curs.execute("""
        INSERT INTO phone_client(client_id, phone_number) 
        VALUES      (%s,%s);
        """, (client_id, phone))
    conn.commit()
    return f"Номер {phone} с id {client_id} добавлен в базу данных"

def change_client(conn, client_id, first_name=None, last_name=None, email=None, phones=None):
    with conn.cursor() as curs:
        if first_name != None:
            curs.execute("""
            UPDATE client 
            SET    first_name = %s
            WHERE  id = %s;
            """, (first_name, client_id))

        if last_name != None:
            curs.execute("""
            UPDATE client
            SET    last_name = %s
            WHERE  id = %s;
            """, (last_name, client_id))

        if email != None:
            curs.execute("""
            UPDATE client
            SET    email = %s
            WHERE  id = %s;
            """, (email, client_id))

        if phones != None:
            curs.execute("""
            UPDATE phone_client
            SET    phone_number = %s 
            WHERE  client_id = %s;
            """, (phones, client_id))
        conn.commit()
    return "Информация в базе данных изменена"

def delete_phone(conn, client_id, phone):
    with conn.cursor() as curs:

        curs.execute("""
        SELECT phone_number, client_id
        FROM   phone_client
        WHERE  phone_number = %s 
        AND    client_id = %s;
        """, (phone, client_id))

        if not curs.fetchone():
            return f'Номер клиента {phone} под id {client_id} не существует'

        curs.execute("""
        DELETE FROM phone_client
        WHERE       phone_number = %s 
        AND         client_id = %s;
        """, (phone, client_id))
        conn.commit()
    return f'Номер клиента {phone} под id {client_id} удален с базы данных'

def delete_client(conn, client_id):
    with conn.cursor() as curs:
        curs.execute("""
        SELECT id 
        FROM   client
        WHERE  id = %s;
        """, (client_id,))

        if not curs.fetchone():
            return f"Клиент под id {client_id} не найден"

        curs.execute("""
        DELETE FROM client
        WHERE       id = %s;
        """, (client_id,))
        conn.commit()
    return f"Клиент под id {client_id} удален из базы"

def find_client(conn, first_name=None, last_name=None, email=None, phone=None):
    with conn.cursor() as curs:
        if first_name != None or last_name != None or email != None:
            curs.execute("""
            SELECT first_name,last_name,email
            FROM   client
            WHERE  first_name = %s
            OR     last_name = %s
            OR     email = %s;
            """, (first_name, last_name, email))

            inf = curs.fetchall()
            if not inf:
                return "Клиента не существует"
            else:
                return f"Клиент найден: {inf}"
        else:
            curs.execute("""
            SELECT cl.id, first_name
            FROM   client AS cl
            JOIN   phone_client AS pc 
            ON     cl.id = pc.client_id
            WHERE  phone_number LIKE %s;
            """, (phone,))

            inf = curs.fetchall()
            if not inf:
                return "Клиента с таким номером не существует"
            else:
                return inf


if __name__ == '__main__':
    with psycopg2.connect(database="DB_Client", user="....", password="....") as conn:
        print(drop_tb(conn))

        print(create_tb(conn))

        print(add_new_client(conn, 'Иван', 'Петров', 'petrov@mail.com', '89910009996'))
        print(add_new_client(conn, 'Сергей', 'Иванов', 'ivan@g.com', '89963255587'))
        print(add_new_client(conn, 'Павел', 'Кондратьев', 'pav@g.com', '89456124885'))

        print(add_phone_number(conn, 1, '80002342322'))

        print(change_client(conn, 2, 'Сергей23', 'Петров2', 'ivan1rov@mail.com'))
        print(change_client(conn, 3, 'Павел45', 'Петров2', 'pav1rov@mail.com'))
        print(change_client(conn, 2, None, None, None, '895548'))

        print(delete_phone(conn, 1, '80002342322'))
        print(delete_phone(conn, 2, '89456424885'))

        print(delete_client(conn, 2))
        print(delete_client(conn, 4))

        print(find_client(conn, 'Сергей23', 'Петров2', 'ivan1rov@mail.com', '80002342322'))
        print(find_client(conn, 'Павел12341', None, None, None))

    conn.close()