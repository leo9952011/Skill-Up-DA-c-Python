from decouple import config

DB_USERNAME = config("DB_USERNAME")
DB_HOST = config("DB_HOST")
DB_PORT = config("DB_PORT")
DB_NAME = config("DB_NAME")
DB_PASSWORD = config("DB_PASSWORD")


print(DB_USERNAME)
print(DB_HOST)
print(DB_PORT)
print(DB_NAME)
print(DB_PASSWORD)
